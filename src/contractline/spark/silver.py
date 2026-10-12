"""Bronze to silver: the governed path every record takes (principle 5).

For each new bronze micro-batch: drift check, standardize, quality rules,
quarantine, masking, history, dataset rules, then one atomic write to silver.
A ``block`` failure stops before anything is written; the batch is retried on
the next run. Every batch leaves a run record in ``<env>_harness.contractline.runs``.
"""

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime

from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from contractline.core.contract import Contract, HistoryType
from contractline.core.naming import Layer, build_dataset_name, build_harness_table_name
from contractline.core.secrets import SecretProvider
from contractline.spark import history
from contractline.spark.bronze import ENVELOPE_COLUMNS, OP_DELETE
from contractline.spark.interfaces import SparkCompute
from contractline.spark.privacy import apply_masking
from contractline.spark.quality import (
    DatasetFinding,
    ReferenceLoader,
    evaluate_dataset_rules,
    evaluate_row_rules,
    raise_if_blocked,
    raise_if_dataset_blocked,
    summarize_warnings,
)
from contractline.spark.standardize import check_drift, standardize

RUNS_TABLE = "runs"
RUNS_SCHEMA = T.StructType(
    [
        T.StructField("dataset", T.StringType(), nullable=False),
        T.StructField("layer", T.StringType(), nullable=False),
        T.StructField("batch_id", T.StringType(), nullable=False),
        T.StructField("contract_version", T.StringType(), nullable=False),
        T.StructField("records_in", T.LongType(), nullable=False),
        T.StructField("records_valid", T.LongType(), nullable=False),
        T.StructField("records_quarantined", T.LongType(), nullable=False),
        T.StructField("warnings", T.MapType(T.StringType(), T.LongType()), nullable=False),
        T.StructField("undeclared_columns", T.ArrayType(T.StringType()), nullable=False),
        T.StructField("finished_at", T.TimestampType(), nullable=False),
    ]
)


@dataclass
class SilverReport:
    dataset: str
    batch_id: str
    records_in: int
    records_valid: int
    records_quarantined: int
    warnings: dict[str, int] = field(default_factory=dict)
    findings: list[DatasetFinding] = field(default_factory=list)
    undeclared_columns: list[str] = field(default_factory=list)


def _table_exists(compute: SparkCompute, logical_name: str) -> bool:
    return compute.spark.catalog.tableExists(compute.build_table_name(logical_name))


def _ensure_table(compute: SparkCompute, logical_name: str, schema: T.StructType) -> DeltaTable:
    compute.ensure_schema(logical_name)
    return (
        DeltaTable.createIfNotExists(compute.spark)
        .tableName(compute.build_table_name(logical_name))
        .addColumns(schema)
        .execute()
    )


def build_reference_loader(compute: SparkCompute, env: str) -> ReferenceLoader:
    """Referential integrity reads the current rows of another silver dataset."""

    def load(domain: str, entity: str, column: str) -> DataFrame | None:
        logical = build_dataset_name(env, Layer.SILVER, domain, entity)
        if not _table_exists(compute, logical):
            return None
        table = compute.spark.table(compute.build_table_name(logical))
        if history.IS_CURRENT in table.columns:
            table = table.where(F.col(history.IS_CURRENT))
        return table.select(column)

    return load


def _previous_count(compute: SparkCompute, env: str, dataset: str) -> int | None:
    runs = build_harness_table_name(env, RUNS_TABLE)
    if not _table_exists(compute, runs):
        return None
    last = (
        compute.spark.table(compute.build_table_name(runs))
        .where((F.col("dataset") == dataset) & (F.col("layer") == "silver"))
        .orderBy(F.col("finished_at").desc())
        .select("records_in")
        .first()
    )
    return None if last is None else int(last["records_in"])


def _record_run(compute: SparkCompute, env: str, contract: Contract, report: SilverReport) -> None:
    runs = build_harness_table_name(env, RUNS_TABLE)
    _ensure_table(compute, runs, RUNS_SCHEMA)
    row = (
        report.dataset,
        "silver",
        report.batch_id,
        contract.identity.version,
        report.records_in,
        report.records_valid,
        report.records_quarantined,
        report.warnings,
        report.undeclared_columns,
        datetime.now(UTC).replace(tzinfo=None),
    )
    compute.spark.createDataFrame([row], RUNS_SCHEMA).write.format("delta").mode(
        "append"
    ).saveAsTable(compute.build_table_name(runs))


def _write_quarantine(
    compute: SparkCompute, env: str, contract: Contract, checked: DataFrame
) -> int:
    rows = checked.where(F.size("_quarantine_reasons") > 0).select(
        *[c for c in ENVELOPE_COLUMNS if c in checked.columns],
        F.col("_quarantine_reasons").alias("_reasons"),
        F.current_timestamp().alias("_quarantined_at"),
    )
    count = rows.count()
    if count == 0:
        return 0
    logical = build_dataset_name(
        env, Layer.QUARANTINE, contract.identity.domain, contract.identity.name
    )
    table = _ensure_table(compute, logical, rows.schema)
    (
        table.alias("t")
        .merge(rows.alias("s"), "t._source_offset = s._source_offset")
        .whenNotMatchedInsertAll()
        .execute()
    )
    return count


def process_batch(
    compute: SparkCompute,
    contract: Contract,
    env: str,
    bronze_batch: DataFrame,
    batch_id: str,
    secrets: SecretProvider,
    now: datetime | None = None,
) -> SilverReport:
    """Process one bronze micro-batch into silver (and quarantine)."""
    domain, entity = contract.identity.domain, contract.identity.name
    dataset = f"{domain}.{entity}"
    silver_logical = build_dataset_name(env, Layer.SILVER, domain, entity)
    silver_name = compute.build_table_name(silver_logical)
    existing = compute.spark.table(silver_name) if _table_exists(compute, silver_logical) else None
    now = now or datetime.now(UTC).replace(tzinfo=None)

    undeclared = check_drift(bronze_batch, contract)
    standardized = standardize(bronze_batch, contract)
    if existing is not None and contract.history.type is HistoryType.APPEND:
        # A replayed batch: records already in silver are not new duplicates.
        published = existing.select("_source_offset")
        standardized = standardized.join(published, "_source_offset", "left_anti")
    checked = evaluate_row_rules(
        standardized,
        contract,
        build_reference_loader(compute, env),
        existing if contract.history.type is HistoryType.APPEND else None,
    ).localCheckpoint()
    raise_if_blocked(checked, contract)

    records_in = checked.count()
    valid = checked.where(F.size("_quarantine_reasons") == 0)
    records_valid = valid.count()
    masked = apply_masking(valid, contract, secrets)

    write, current = _plan_history(
        compute, contract, env, silver_logical, existing, masked, secrets
    )
    findings = evaluate_dataset_rules(
        current, contract, records_in, _previous_count(compute, env, dataset), now
    )
    raise_if_dataset_blocked(findings, contract)

    quarantined = _write_quarantine(compute, env, contract, checked)
    write()
    report = SilverReport(
        dataset=dataset,
        batch_id=batch_id,
        records_in=records_in,
        records_valid=records_valid,
        records_quarantined=quarantined,
        warnings=dict(summarize_warnings(checked)),
        findings=findings,
        undeclared_columns=undeclared,
    )
    _record_run(compute, env, contract, report)
    return report


def load_logged_events(
    compute: SparkCompute,
    contract: Contract,
    env: str,
    affected: DataFrame,
    secrets: SecretProvider,
) -> DataFrame | None:
    """Every earlier change of the affected keys, replayed from bronze.

    SCD2 needs the full change log: two identical versions stored as one row in
    silver may need to be split again when a late change lands between them.
    Records that were quarantined stay out. Silver's own rows are still used as
    well, for history that retention already removed from bronze.
    """
    domain, entity = contract.identity.domain, contract.identity.name
    bronze_logical = build_dataset_name(env, Layer.BRONZE, domain, entity)
    if not _table_exists(compute, bronze_logical):
        return None
    keys = history.get_business_key(contract)
    bronze = compute.spark.table(compute.build_table_name(bronze_logical))
    masked_keys = set(contract.privacy.masking) if contract.privacy else set()
    if not masked_keys & set(keys):
        # Cheap pre-filter on the raw key text before parsing whole payloads.
        raw_keys = [F.get_json_object("_payload", f"$.{k}").alias(k) for k in keys]
        wanted = affected.select(*[F.col(k).cast("string").alias(k) for k in keys])
        bronze = bronze.select("*", *raw_keys).join(wanted, keys, "left_semi").drop(*keys)
    quarantine_logical = build_dataset_name(env, Layer.QUARANTINE, domain, entity)
    if _table_exists(compute, quarantine_logical):
        quarantined = compute.spark.table(compute.build_table_name(quarantine_logical))
        bronze = bronze.join(quarantined.select("_source_offset"), "_source_offset", "left_anti")
    valid = standardize(bronze, contract).where(F.size("_errors") == 0)
    events = history.build_incoming_events(apply_masking(valid, contract, secrets), contract)
    return events.join(affected, keys, "left_semi")


def _plan_history(
    compute: SparkCompute,
    contract: Contract,
    env: str,
    silver_logical: str,
    existing: DataFrame | None,
    masked: DataFrame,
    secrets: SecretProvider,
) -> tuple[Callable[[], None], DataFrame]:
    """Compute the new silver state; return a writer and the candidate current rows."""
    silver_name = compute.build_table_name(silver_logical)
    events = history.build_incoming_events(masked, contract)
    values = history.get_value_columns(masked, contract)
    kind = contract.history.type

    if kind is HistoryType.APPEND:
        rows = events.where(F.col("_op") != OP_DELETE).drop("_op")

        def write_append() -> None:
            table = _ensure_table(compute, silver_logical, rows.schema)
            (
                table.alias("t")
                .merge(rows.alias("s"), "t._source_offset = s._source_offset")
                .whenNotMatchedInsertAll()
                .execute()
            )

        current = rows if existing is None else existing.unionByName(rows)
        return write_append, current

    if kind is HistoryType.SNAPSHOT:
        latest = events.agg(F.max("_ingested_at").alias("m")).first()
        rows = events.where(
            (F.col("_ingested_at") == (latest["m"] if latest else None))
            & (F.col("_op") != OP_DELETE)
        ).drop("_op")

        def write_snapshot() -> None:
            compute.ensure_schema(silver_logical)
            rows.write.format("delta").mode("overwrite").option(
                "overwriteSchema", "true"
            ).saveAsTable(silver_name)

        return write_snapshot, rows

    keys = history.get_business_key(contract)
    affected = events.select(*keys).distinct()
    if kind is HistoryType.SCD2:
        previous = (
            history.rebuild_scd2_events(existing.join(affected, keys, "left_semi"), keys, values)
            if existing is not None
            else None
        )
        all_events = events if previous is None else events.unionByName(previous)
        logged = load_logged_events(compute, contract, env, affected, secrets)
        if logged is not None:
            all_events = all_events.unionByName(logged)
        rebuilt = history.compute_scd2(all_events, contract).localCheckpoint()
        match = [*keys, history.VALID_FROM]
        rebuilt_current = rebuilt.where(F.col(history.IS_CURRENT))
        untouched = (
            existing.where(F.col(history.IS_CURRENT)).join(affected, keys, "left_anti")
            if existing is not None
            else None
        )
    else:
        previous = (
            history.rebuild_scd1_events(existing.join(affected, keys, "left_semi"), values)
            if existing is not None
            else None
        )
        all_events = events if previous is None else events.unionByName(previous)
        rebuilt = history.compute_scd1(all_events, contract).localCheckpoint()
        match = keys
        rebuilt_current = rebuilt
        untouched = existing.join(affected, keys, "left_anti") if existing is not None else None

    current = rebuilt_current if untouched is None else untouched.unionByName(rebuilt_current)

    def write_keyed() -> None:
        table = _ensure_table(compute, silver_logical, rebuilt.schema)
        target_df = compute.spark.table(silver_name)
        history.merge_rebuilt_keys(table, target_df, rebuilt, affected, match)

    return write_keyed, current


def run_silver(
    compute: SparkCompute, contract: Contract, env: str, secrets: SecretProvider
) -> list[SilverReport]:
    """Process every bronze batch not yet in silver, then stop (``availableNow``)."""
    domain, entity = contract.identity.domain, contract.identity.name
    bronze = compute.build_table_name(build_dataset_name(env, Layer.BRONZE, domain, entity))
    silver_logical = build_dataset_name(env, Layer.SILVER, domain, entity)
    reports: list[SilverReport] = []
    failures: list[BaseException] = []

    def handle(batch: DataFrame, epoch: int) -> None:
        try:
            reports.append(process_batch(compute, contract, env, batch, str(epoch), secrets))
        except BaseException as exc:
            failures.append(exc)
            raise

    query = (
        compute.spark.readStream.format("delta")
        # Erasure and retention delete from bronze; those commits are not new data.
        .option("skipChangeCommits", "true")
        .table(bronze)
        .writeStream.foreachBatch(handle)
        .option("checkpointLocation", compute.build_checkpoint_path(silver_logical))
        .trigger(availableNow=True)
        .start()
    )
    try:
        query.awaitTermination()
    except Exception:
        if failures:
            raise failures[0] from None
        raise
    if failures:
        raise failures[0]
    return reports
