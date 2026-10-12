"""Quality block: evaluate a contract's rules with their severities (ADR-0012).

Row rules (``not_null``, ``accepted_values``, ``range``,
``referential_integrity``, and ``unique`` on append-only data) mark individual
records. Dataset rules (``unique`` on keyed history, ``row_count_anomaly``,
``freshness``) look at the result as a whole.

Severities:

- ``quarantine``: the record goes to the quarantine table instead of silver.
- ``block``: the whole batch stops; nothing is written, and the next run
  retries the same batch.
- ``warn``: the record or dataset passes; the run report lists the warning.

Schema problems found by the standardize block are always quarantined.
Delete events are only checked for schema problems: a delete must go through
even if the deleted row once broke a rule.
"""

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from contractline.core.contract import Contract, HistoryType, QualityRule, RuleType, Severity
from contractline.core.diff import compute_duration_seconds
from contractline.spark.bronze import OP_DELETE
from contractline.spark.types import to_spark_type

ReferenceLoader = Callable[[str, str, str], DataFrame | None]
"""(domain, entity, column) -> one-column DataFrame of valid values, or None if missing."""

ROW_RULES = {
    RuleType.NOT_NULL,
    RuleType.ACCEPTED_VALUES,
    RuleType.RANGE,
    RuleType.REFERENTIAL_INTEGRITY,
}


class QualityGateError(RuntimeError):
    """A ``block`` rule failed: the batch must not be published."""


@dataclass(frozen=True)
class DatasetFinding:
    rule: str
    severity: Severity
    message: str


def describe_rule(rule: QualityRule) -> str:
    columns = ", ".join(rule.columns)
    return f"{rule.type}({columns})" if columns else str(rule.type)


def is_row_rule(rule: QualityRule, contract: Contract) -> bool:
    if rule.type in ROW_RULES:
        return True
    return rule.type is RuleType.UNIQUE and contract.history.type is HistoryType.APPEND


def _literal(value: Any, column_type: str) -> Column:
    return F.lit(value).cast(to_spark_type(column_type))


def _accepted_values_failure(column: Column, values: list[Any], column_type: str) -> Column:
    allowed = [_literal(v, column_type) for v in values]
    return column.isNotNull() & ~column.isin(*allowed)


def _build_failure(
    df: DataFrame,
    rule: QualityRule,
    contract: Contract,
    references: ReferenceLoader,
    existing: DataFrame | None,
) -> tuple[DataFrame, Column]:
    """Return ``df`` (maybe with helper columns) and a boolean "this row fails" column."""
    params = rule.params
    if rule.type is RuleType.NOT_NULL:
        return df, F.col(rule.columns[0]).isNull()
    name = rule.columns[0]
    column_type = contract.schema.column(name).type  # type: ignore[union-attr]
    value = F.col(name)
    if rule.type is RuleType.ACCEPTED_VALUES:
        return df, _accepted_values_failure(value, params["values"], column_type)
    if rule.type is RuleType.RANGE:
        failure = F.lit(False)
        if "min" in params:
            failure = failure | (value < _literal(params["min"], column_type))
        if "max" in params:
            failure = failure | (value > _literal(params["max"], column_type))
        return df, value.isNotNull() & failure
    if rule.type is RuleType.REFERENTIAL_INTEGRITY:
        domain, entity, ref_column = params["references"].split(".")
        reference = references(domain, entity, ref_column)
        if reference is None:
            return df, value.isNotNull()
        flag = f"_ref_{name}"
        matches = reference.select(F.col(ref_column).alias(name)).distinct()
        matches = matches.withColumn(flag, F.lit(True))
        joined = df.join(matches, on=name, how="left")
        return joined, value.isNotNull() & F.col(flag).isNull()
    if rule.type is RuleType.UNIQUE:
        keys = list(rule.columns)
        order = Window.partitionBy(*keys).orderBy("_sequence", "_source_offset")
        flag = "_unique_seen"
        df = df.withColumn("_unique_rank", F.row_number().over(order))
        if existing is not None:
            seen = existing.select(*keys).distinct().withColumn(flag, F.lit(True))
            df = df.join(seen, on=keys, how="left")
        else:
            df = df.withColumn(flag, F.lit(None).cast("boolean"))
        return df, (F.col("_unique_rank") > 1) | F.col(flag).isNotNull()
    raise ValueError(f"{rule.type} is not a row rule")


def evaluate_row_rules(
    df: DataFrame,
    contract: Contract,
    references: ReferenceLoader,
    existing: DataFrame | None = None,
) -> DataFrame:
    """Add ``_quarantine_reasons``, ``_warnings`` and ``_blocking`` arrays.

    ``df`` is the output of ``standardize``; ``existing`` is the current silver
    table (used by ``unique`` on append-only data).
    """
    reasons: dict[Severity, list[Column]] = {s: [] for s in Severity}
    is_delete = F.col("_op") == OP_DELETE
    original_columns = df.columns
    for rule in contract.quality:
        if not is_row_rule(rule, contract):
            continue
        df, failure = _build_failure(df, rule, contract, references, existing)
        message = f"{rule.severity}: {describe_rule(rule)} failed"
        reasons[rule.severity].append(F.when(~is_delete & failure, F.lit(message)))

    def collect(items: list[Column]) -> Column:
        return F.filter(F.array(*items, F.lit(None).cast("string")), lambda c: c.isNotNull())

    return df.select(
        *original_columns,
        F.concat(F.col("_errors"), collect(reasons[Severity.QUARANTINE])).alias(
            "_quarantine_reasons"
        ),
        collect(reasons[Severity.WARN]).alias("_warnings"),
        collect(reasons[Severity.BLOCK]).alias("_blocking"),
    )


def raise_if_blocked(checked: DataFrame, contract: Contract) -> None:
    blocked = checked.where(F.size("_blocking") > 0)
    failures = blocked.select(F.explode("_blocking").alias("m")).groupBy("m").count().collect()
    if failures:
        summary = "; ".join(f"{r['m']} ({r['count']} records)" for r in failures)
        raise QualityGateError(
            f"{contract.identity.domain}.{contract.identity.name}: batch blocked: {summary}"
        )


def evaluate_dataset_rules(
    current: DataFrame,
    contract: Contract,
    incoming_count: int,
    previous_count: int | None,
    now: datetime,
) -> list[DatasetFinding]:
    """Check the dataset as it would look after this batch.

    ``current`` is the candidate set of current rows (for append history, all rows).
    """
    findings: list[DatasetFinding] = []
    for rule in contract.quality:
        if is_row_rule(rule, contract):
            continue
        label = describe_rule(rule)
        if rule.type is RuleType.UNIQUE:
            duplicates = (
                current.groupBy(*rule.columns).count().where(F.col("count") > 1).limit(5).collect()
            )
            if duplicates:
                sample = ", ".join(str([r[c] for c in rule.columns]) for r in duplicates)
                findings.append(
                    DatasetFinding(label, rule.severity, f"{label}: duplicate values {sample}")
                )
        elif rule.type is RuleType.ROW_COUNT_ANOMALY:
            if previous_count:
                change = abs(incoming_count - previous_count) / previous_count
                if change > rule.params["max_change_ratio"]:
                    findings.append(
                        DatasetFinding(
                            label,
                            rule.severity,
                            f"{label}: {incoming_count} records vs {previous_count} last run "
                            f"({change:.0%} change)",
                        )
                    )
        elif rule.type is RuleType.FRESHNESS:
            column = rule.columns[0]
            newest = current.agg(F.max(column).alias("m")).first()
            max_age = compute_duration_seconds(rule.params["max_age"])
            newest_value = newest["m"] if newest else None
            if newest_value is None:
                findings.append(DatasetFinding(label, rule.severity, f"{label}: no data"))
            else:
                if not isinstance(newest_value, datetime):
                    newest_value = datetime.combine(newest_value, datetime.min.time())
                age = (now - newest_value).total_seconds()
                if age > max_age:
                    findings.append(
                        DatasetFinding(
                            label,
                            rule.severity,
                            f"{label}: newest value is {int(age)}s old (max {max_age}s)",
                        )
                    )
    return findings


def raise_if_dataset_blocked(findings: list[DatasetFinding], contract: Contract) -> None:
    blocking = [f.message for f in findings if f.severity is Severity.BLOCK]
    if blocking:
        raise QualityGateError(
            f"{contract.identity.domain}.{contract.identity.name}: batch blocked: "
            + "; ".join(blocking)
        )


def summarize_warnings(checked: DataFrame) -> Mapping[str, int]:
    rows = checked.select(F.explode("_warnings").alias("m")).groupBy("m").count().collect()
    return {r["m"]: r["count"] for r in rows}
