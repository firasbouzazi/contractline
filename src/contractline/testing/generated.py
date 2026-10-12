"""Generated tests: run synthetic data through the real pipeline and check the
harness's guarantees (ADR-0014).

Nobody writes these tests by hand, so nobody can weaken them to make a
pipeline pass. They are derived from the contracts and gold models alone.
"""

import json
import secrets as token_source
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from contractline.core.contract import (
    Contract,
    HistoryType,
    MaskingStrategy,
    Severity,
    SourceAdapter,
)
from contractline.core.naming import Layer, build_dataset_name
from contractline.core.project import Project
from contractline.spark.bronze import map_files, map_kafka_to_envelope, write_bronze_batch
from contractline.spark.checks import find_duplicate_keys, find_scd2_violations
from contractline.spark.gold import build_silver_schema, run_gold, run_gold_test
from contractline.spark.history import IS_CURRENT, get_business_key
from contractline.spark.interfaces import SparkCompute
from contractline.spark.quality import evaluate_row_rules
from contractline.spark.silver import run_silver
from contractline.testing.synthetic import Message, SyntheticData, generate

KAFKA_SCHEMA = (
    "key binary, value binary, topic string, partition int, offset long, timestamp timestamp"
)


class EphemeralSecrets:
    """Random masking keys for one test run: no real key is ever needed in CI."""

    def __init__(self) -> None:
        self._keys: dict[str, bytes] = {}

    def get_secret(self, name: str) -> bytes:
        return self._keys.setdefault(name, token_source.token_bytes(32))


@dataclass(frozen=True)
class CheckResult:
    target: str
    check: str
    passed: bool
    details: list[str] = field(default_factory=list)


@dataclass
class TestReport:
    env: str
    results: list[CheckResult] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return all(r.passed for r in self.results)

    def add(self, target: str, check: str, problems: list[str]) -> None:
        self.results.append(CheckResult(target, check, not problems, problems[:5]))

    def to_json(self) -> str:
        return json.dumps(
            {"env": self.env, "passed": self.passed, "results": [asdict(r) for r in self.results]},
            indent=2,
            default=str,
        )


def _kafka_frame(
    compute: SparkCompute, contract: Contract, messages: list[Message], start: int
) -> DataFrame:
    now = datetime.now(UTC).replace(tzinfo=None)
    rows = [
        (
            m.key.encode() if m.key is not None else None,
            m.value.encode() if m.value is not None else None,
            contract.source.location,
            0,
            start + i,
            now,
        )
        for i, m in enumerate(messages)
    ]
    return compute.spark.createDataFrame(rows, KAFKA_SCHEMA)


def _files_frame(
    compute: SparkCompute, contract: Contract, messages: list[Message], number: int, at: datetime
) -> DataFrame:
    identity, extension = contract.identity, contract.source.format
    path = f"synthetic/{identity.domain}/{identity.name}/{number}.{extension}"
    rows = [(m.value, path, at.replace(tzinfo=None)) for m in messages]
    return compute.spark.createDataFrame(
        rows, "payload string, file_path string, file_modified_at timestamp"
    )


def _table(compute: SparkCompute, logical: str) -> DataFrame | None:
    name = compute.build_table_name(logical)
    return compute.spark.table(name) if compute.spark.catalog.tableExists(name) else None


def find_raw_pii(df: DataFrame, raw_values: set[str]) -> list[str]:
    """String values of ``df`` that are raw PII from the synthetic source."""
    if not raw_values:
        return []
    strings = [f.name for f in df.schema.fields if isinstance(f.dataType, T.StringType)]
    if not strings:
        return []
    spark = df.sparkSession
    raw = spark.createDataFrame([(v,) for v in raw_values], "raw string")
    values = df.select(F.explode(F.array(*[F.col(c) for c in strings])).alias("raw"))
    leaks = values.join(raw, "raw", "left_semi").distinct().limit(5).collect()
    return [f"raw PII found: {row['raw']}" for row in leaks]


def check_conformance(df: DataFrame, contract: Contract) -> list[str]:
    expected = {f.name: f.dataType for f in build_silver_schema(contract).fields}
    actual = {f.name: f.dataType for f in df.schema.fields}
    problems = [f"missing column {n}" for n in expected if n not in actual]
    problems += [f"unexpected column {n}" for n in actual if n not in expected]
    problems += [
        f"{n} is {actual[n].simpleString()}, expected {t.simpleString()}"
        for n, t in expected.items()
        if n in actual and actual[n] != t
    ]
    return problems


def check_quality(df: DataFrame, contract: Contract) -> list[str]:
    """Silver must satisfy every quarantine and block row rule on unmasked columns."""
    masked = set(contract.privacy.masking) if contract.privacy else set()
    rules = tuple(
        r
        for r in contract.quality
        if r.severity is not Severity.WARN
        and r.type != "referential_integrity"
        and not set(r.columns) & masked
    )
    current = df.where(F.col(IS_CURRENT)) if IS_CURRENT in df.columns else df
    framed = current.select(
        "*",
        F.array().cast("array<string>").alias("_errors"),
        F.lit("insert").alias("_op"),
    )
    for column in ("_sequence", "_source_offset"):
        if column not in framed.columns:
            framed = framed.withColumn(column, F.monotonically_increasing_id())
    only_rules = Contract(**{**contract.__dict__, "quality": rules})
    checked = evaluate_row_rules(framed, only_rules, lambda *_: None)
    failing = checked.where((F.size("_quarantine_reasons") > 0) | (F.size("_blocking") > 0))
    return [
        ", ".join(row["_quarantine_reasons"] + row["_blocking"])
        for row in failing.limit(5).collect()
    ]


def check_history(df: DataFrame, contract: Contract, data: SyntheticData) -> list[str]:
    keys = get_business_key(contract)
    kind = contract.history.type
    if kind is HistoryType.SCD2:
        problems = find_scd2_violations(df, keys)
        current = df.where(F.col(IS_CURRENT))
    elif kind in (HistoryType.SCD1, HistoryType.SNAPSHOT):
        problems = find_duplicate_keys(df, keys)
        current = df
    else:
        return (
            find_duplicate_keys(df, list(contract.schema.primary_key))
            if contract.schema.primary_key
            else []
        )
    masked = set(contract.privacy.masking) if contract.privacy else set()
    if data.deleted_keys and not set(keys) & masked:
        alive = {tuple(r[k] for k in keys) for r in current.select(*keys).collect()}
        problems += [
            f"deleted key {k} still has a current row" for k in sorted(data.deleted_keys & alive)
        ]
    return problems


def run_generated_tests(
    compute: SparkCompute,
    project: Project,
    env: str,
    seed: int = 7,
    progress: Callable[[str], None] = lambda _: None,
) -> TestReport:
    """Run every contract and gold model of ``project`` on synthetic data in ``env``."""
    report = TestReport(env)
    secrets = EphemeralSecrets()
    generated: dict[str, SyntheticData] = {}
    raw_pii: set[str] = set()
    latest = datetime(2000, 1, 1)

    for contract in project.contracts:
        domain, entity = contract.identity.domain, contract.identity.name
        target = f"{domain}.{entity}"
        progress(f"silver {target}")
        references = {
            f"{name}.{column}": values
            for name, data in generated.items()
            for column, values in data.key_values.items()
        }
        data = generate(contract, seed=seed, references=references)
        generated[target] = data
        raw_pii |= data.raw_pii
        now = data.now.replace(tzinfo=None)
        latest = max(latest, now)
        bronze = build_dataset_name(env, Layer.BRONZE, domain, entity)
        offset = 0
        try:
            for number, batch in enumerate(data.batches):
                if contract.source.adapter is SourceAdapter.FILES:
                    raw = _files_frame(compute, contract, batch, number, data.batch_times[number])
                    envelope = map_files(raw, contract)
                else:
                    raw = _kafka_frame(compute, contract, batch, offset)
                    envelope = map_kafka_to_envelope(raw, contract)
                offset += len(batch)
                write_bronze_batch(compute, bronze, envelope, number)
                run_silver(compute, contract, env, secrets, now)
        except Exception as exc:  # the pipeline itself failing is a test failure
            report.add(target, "pipeline runs", [f"{type(exc).__name__}: {exc}".splitlines()[0]])
            continue
        report.add(target, "pipeline runs", [])

        silver = _table(compute, build_dataset_name(env, Layer.SILVER, domain, entity))
        if silver is None:
            report.add(target, "silver table exists", ["no silver table was written"])
            continue
        quarantine = _table(compute, build_dataset_name(env, Layer.QUARANTINE, domain, entity))
        quarantined = quarantine.count() if quarantine is not None else 0
        report.add(target, "contract conformance", check_conformance(silver, contract))
        report.add(
            target,
            "bad records quarantined",
            []
            if quarantined >= data.invalid_count
            else [f"{data.invalid_count} invalid records sent, {quarantined} quarantined"],
        )
        report.add(target, "quality rules hold", check_quality(silver, contract))
        report.add(target, "no raw PII after bronze", find_raw_pii(silver, raw_pii))
        report.add(target, "history invariants", check_history(silver, contract, data))
        dropped = [
            name
            for name, m in (contract.privacy.masking.items() if contract.privacy else [])
            if m.strategy is MaskingStrategy.DROP and name in silver.columns
        ]
        report.add(target, "dropped columns absent", [f"{c} is still in silver" for c in dropped])

    for model in project.gold_models:
        target = f"{model.domain}.{model.name}"
        progress(f"gold {target}")
        for test in model.tests:
            result = run_gold_test(compute, model, test, project.find_contract, project.find_gold)
            details = [result.error] if result.error else []
            details += [f"missing row {r}" for r in result.missing]
            details += [f"unexpected row {r}" for r in result.unexpected]
            report.add(target, f"model test: {test.name}", details)
        try:
            run_gold(compute, model, env, latest)
        except Exception as exc:
            report.add(
                target, "builds on synthetic data", [f"{type(exc).__name__}: {exc}".splitlines()[0]]
            )
            continue
        report.add(target, "builds on synthetic data", [])
        gold = _table(compute, build_dataset_name(env, Layer.GOLD, model.domain, model.name))
        if gold is not None:
            report.add(target, "no raw PII in gold", find_raw_pii(gold, raw_pii))
    return report


def format_report(report: TestReport) -> str:
    lines = []
    for result in report.results:
        mark = "PASS" if result.passed else "FAIL"
        lines.append(f"{mark}  {result.target}: {result.check}")
        lines.extend(f"        {d}" for d in result.details)
    passed = sum(r.passed for r in report.results)
    lines.append(f"{passed}/{len(report.results)} checks passed")
    return "\n".join(lines)


def summarize(report: TestReport) -> dict[str, Any]:
    return {"passed": report.passed, "checks": len(report.results)}
