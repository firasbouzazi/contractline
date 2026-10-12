"""Run gold models and their mandatory tests on Spark (ADR-0013)."""

from collections import Counter
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from contractline.core.contract import Contract, HistoryType, MaskingStrategy, Severity
from contractline.core.gold import REF_PATTERN, GoldModel, GoldTest, InputRef
from contractline.core.naming import Layer, build_dataset_name
from contractline.spark import history
from contractline.spark.interfaces import SparkCompute
from contractline.spark.quality import (
    QualityGateError,
    evaluate_dataset_rules,
    evaluate_row_rules,
    raise_if_dataset_blocked,
    summarize_warnings,
)
from contractline.spark.types import to_spark_type

ContractLoader = Callable[[str, str], Contract]
"""(domain, entity) -> the silver dataset's contract."""


class GoldSchemaError(ValueError):
    """The SQL's output does not match the declared columns."""


class GoldIsolationError(ValueError):
    """The SQL read data it did not declare as an input."""


@dataclass(frozen=True)
class GoldTestResult:
    name: str
    passed: bool
    missing: list[tuple[Any, ...]] = field(default_factory=list)
    unexpected: list[tuple[Any, ...]] = field(default_factory=list)
    error: str | None = None


@dataclass(frozen=True)
class GoldReport:
    model: str
    rows: int
    warnings: dict[str, int]


def build_silver_schema(contract: Contract) -> T.StructType:
    """Columns of a contract's silver table: masked contract columns plus history columns."""
    masking = contract.privacy.masking if contract.privacy else {}
    fields = [
        T.StructField(c.name, to_spark_type(c.type))
        for c in contract.schema.columns
        if not (c.name in masking and masking[c.name].strategy is MaskingStrategy.DROP)
    ]
    if contract.history.type is HistoryType.SCD2:
        fields += [
            T.StructField(history.VALID_FROM, T.TimestampType()),
            T.StructField(history.VALID_TO, T.TimestampType()),
            T.StructField(history.IS_CURRENT, T.BooleanType()),
            T.StructField(history.HASH_DIFF, T.StringType()),
        ]
    else:
        fields.append(T.StructField(history.EVENT_AT, T.TimestampType()))
    fields += [
        T.StructField("_sequence", T.LongType()),
        T.StructField("_source_offset", T.StringType()),
        T.StructField("_ingested_at", T.TimestampType()),
    ]
    return T.StructType(fields)


def build_gold_schema(model: GoldModel) -> T.StructType:
    return T.StructType([T.StructField(c.name, to_spark_type(c.type)) for c in model.columns])


def render_sql(model: GoldModel, names: Mapping[str, str]) -> str:
    """Replace each ``{{ ref('...') }}`` with the given table or view name."""
    return REF_PATTERN.sub(lambda m: names[m.group(1)], model.sql)


def _to_text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def build_typed_rows(
    compute: SparkCompute, rows: list[dict[str, Any]], schema: T.StructType
) -> DataFrame:
    """YAML rows (missing columns are null) as a DataFrame of ``schema``.

    Values go through text and ``try_cast`` so dates, timestamps and decimals can
    be written naturally in YAML; a value that does not fit fails loudly.
    """
    names = [f.name for f in schema.fields]
    text_schema = T.StructType([T.StructField(n, T.StringType()) for n in names])
    text_rows = [tuple(_to_text(row.get(n)) for n in names) for row in rows]
    text = compute.spark.createDataFrame(text_rows, text_schema)
    typed = text.select(*[F.col(f.name).try_cast(f.dataType).alias(f.name) for f in schema.fields])
    for f in schema.fields:
        broken = text.where(F.col(f.name).isNotNull() & F.col(f.name).try_cast(f.dataType).isNull())
        sample = broken.select(f.name).first()
        if sample is not None:
            raise ValueError(f"{f.name}: '{sample[0]}' is not a valid {f.dataType.simpleString()}")
    return typed


def check_output_schema(df: DataFrame, model: GoldModel) -> DataFrame:
    """Select the declared columns in order; fail on missing, extra or mistyped ones."""
    actual = {f.name: f.dataType for f in df.schema.fields}
    declared = [c.name for c in model.columns]
    problems = [f"missing column {n}" for n in declared if n not in actual]
    problems += [f"undeclared column {n}" for n in actual if n not in declared]
    for column in model.columns:
        expected = to_spark_type(column.type)
        if column.name in actual and actual[column.name] != expected:
            problems.append(
                f"{column.name} is {actual[column.name].simpleString()}, declared {column.type}"
            )
    if problems:
        raise GoldSchemaError(f"{model.domain}.{model.name}: " + "; ".join(problems))
    return df.select(*declared)


def evaluate_gold_quality(
    output: DataFrame, model: GoldModel, previous_rows: int | None, now: datetime
) -> dict[str, int]:
    """Quality rules on the model output: raises on ``block``, returns warnings."""
    contract = model.contract
    framed = output.select(
        "*",
        F.array().cast("array<string>").alias("_errors"),
        F.lit("insert").alias("_op"),
        F.monotonically_increasing_id().alias("_sequence"),
        F.monotonically_increasing_id().cast("string").alias("_source_offset"),
    )
    checked = evaluate_row_rules(framed, contract, lambda *_: None).localCheckpoint()
    blocked = checked.select(F.explode("_blocking").alias("m")).groupBy("m").count().collect()
    if blocked:
        summary = "; ".join(f"{r['m']} ({r['count']} rows)" for r in blocked)
        raise QualityGateError(f"{model.domain}.{model.name}: {summary}")
    findings = evaluate_dataset_rules(output, contract, output.count(), previous_rows, now)
    raise_if_dataset_blocked(findings, contract)
    warnings = dict(summarize_warnings(checked))
    warnings.update({f.message: 1 for f in findings if f.severity is Severity.WARN})
    return warnings


def _input_logical_name(env: str, ref: InputRef) -> str:
    layer = Layer.SILVER if ref.layer == "silver" else Layer.GOLD
    return build_dataset_name(env, layer, ref.domain, ref.entity)


def _table_files(compute: SparkCompute, table: str) -> set[str]:
    return set(compute.spark.table(table).inputFiles())


def run_gold(
    compute: SparkCompute, model: GoldModel, env: str, now: datetime | None = None
) -> GoldReport:
    """Build a gold model from its inputs and replace its table."""
    names: dict[str, str] = {}
    allowed_files: set[str] = set()
    for ref in model.inputs:
        table = compute.build_table_name(_input_logical_name(env, ref))
        names[str(ref)] = table
        allowed_files |= _table_files(compute, table)

    output = compute.spark.sql(render_sql(model, names))
    stray = set(output.inputFiles()) - allowed_files
    if stray:
        raise GoldIsolationError(
            f"{model.domain}.{model.name} reads data outside its declared inputs: "
            + ", ".join(sorted(stray)[:3])
        )
    output = check_output_schema(output, model).localCheckpoint()

    target = build_dataset_name(env, Layer.GOLD, model.domain, model.name)
    target_name = compute.build_table_name(target)
    previous = (
        compute.spark.table(target_name).count()
        if compute.spark.catalog.tableExists(target_name)
        else None
    )
    warnings = evaluate_gold_quality(
        output, model, previous, now or datetime.now(UTC).replace(tzinfo=None)
    )
    compute.ensure_schema(target)
    output.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        target_name
    )
    return GoldReport(model=f"{model.domain}.{model.name}", rows=output.count(), warnings=warnings)


def run_gold_test(
    compute: SparkCompute,
    model: GoldModel,
    test: GoldTest,
    load_contract: ContractLoader,
    load_gold: Callable[[str, str], GoldModel],
) -> GoldTestResult:
    """Run the SQL on the test's input rows and compare with the expected rows."""
    names: dict[str, str] = {}
    try:
        for ref in model.inputs:
            if ref.layer == "silver":
                schema = build_silver_schema(load_contract(ref.domain, ref.entity))
            else:
                schema = build_gold_schema(load_gold(ref.domain, ref.entity))
            rows = [dict(r) for r in test.given.get(str(ref), [])]
            if history.IS_CURRENT in schema.fieldNames():
                for row in rows:
                    row.setdefault(history.IS_CURRENT, True)
            view = f"__test_{ref.layer}_{ref.domain}_{ref.entity}"
            build_typed_rows(compute, rows, schema).createOrReplaceTempView(view)
            names[str(ref)] = view
        output = check_output_schema(compute.spark.sql(render_sql(model, names)), model)
        columns = list(test.expect[0]) if test.expect else [c.name for c in model.columns]
        expected_schema = T.StructType(
            [f for f in build_gold_schema(model).fields if f.name in columns]
        )
        expected = build_typed_rows(compute, test.expect, expected_schema)
        ordered = [f.name for f in expected_schema.fields]
        actual_rows = Counter(tuple(r) for r in output.select(*ordered).collect())
        expected_rows = Counter(tuple(r) for r in expected.select(*ordered).collect())
    except Exception as exc:  # a test that cannot run is a failed test, with the reason
        return GoldTestResult(test.name, passed=False, error=str(exc).splitlines()[0])
    missing = list((expected_rows - actual_rows).elements())
    unexpected = list((actual_rows - expected_rows).elements())
    return GoldTestResult(test.name, not missing and not unexpected, missing, unexpected)
