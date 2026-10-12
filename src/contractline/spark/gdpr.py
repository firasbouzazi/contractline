"""GDPR operations: right to erasure and retention purge (ADR-0016).

Both delete from every layer that holds the data (bronze, quarantine, silver)
and rebuild the gold models that read it. Each operation leaves an audit
record in ``<env>_harness.contractline.erasures`` or ``.purges``; erasure
records never contain the subject's key, only its keyed hash.

Deleted rows stay readable through Delta time travel until ``VACUUM`` removes
the old files. Pass ``vacuum_hours`` to run it as part of the operation.
"""

import hashlib
import hmac
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

from delta.tables import DeltaTable
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from contractline.core.contract import Contract, MaskingStrategy
from contractline.core.diff import compute_duration_seconds
from contractline.core.naming import Layer, build_dataset_name, build_harness_table_name
from contractline.core.project import Project
from contractline.core.secrets import SecretProvider
from contractline.spark.gold import run_gold
from contractline.spark.interfaces import SparkCompute
from contractline.spark.privacy import (
    HASH_SECRET,
    TOKEN_HEX_LENGTH,
    TOKEN_PREFIX,
    TOKEN_SECRET,
    build_hmac_sha256,
)

ERASURES_TABLE = "erasures"
PURGES_TABLE = "purges"
ERASURES_SCHEMA = T.StructType(
    [
        T.StructField("request_id", T.StringType(), nullable=False),
        T.StructField("dataset", T.StringType(), nullable=False),
        T.StructField("subject_hash", T.StringType(), nullable=False),
        T.StructField("contract_version", T.StringType(), nullable=False),
        T.StructField("rows_deleted", T.MapType(T.StringType(), T.LongType()), nullable=False),
        T.StructField("gold_rebuilt", T.ArrayType(T.StringType()), nullable=False),
        T.StructField("vacuumed", T.BooleanType(), nullable=False),
        T.StructField("completed_at", T.TimestampType(), nullable=False),
    ]
)
PURGES_SCHEMA = T.StructType(
    [
        T.StructField("dataset", T.StringType(), nullable=False),
        T.StructField("cutoff", T.TimestampType(), nullable=False),
        T.StructField("rows_deleted", T.MapType(T.StringType(), T.LongType()), nullable=False),
        T.StructField("gold_rebuilt", T.ArrayType(T.StringType()), nullable=False),
        T.StructField("vacuumed", T.BooleanType(), nullable=False),
        T.StructField("completed_at", T.TimestampType(), nullable=False),
    ]
)


class ErasureError(ValueError):
    pass


@dataclass(frozen=True)
class OperationRecord:
    dataset: str
    rows_deleted: dict[str, int]
    gold_rebuilt: list[str] = field(default_factory=list)
    vacuumed: bool = False


def compute_subject_hash(secrets: SecretProvider, key_text: str) -> str:
    """The keyed hash that identifies an erased subject in audit records."""
    return hmac.new(secrets.get_secret(HASH_SECRET), key_text.encode(), hashlib.sha256).hexdigest()


def _now() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def _delete(compute: SparkCompute, logical: str, condition: Column) -> int:
    name = compute.build_table_name(logical)
    if not compute.spark.catalog.tableExists(name):
        return 0
    count = compute.spark.table(name).where(condition).count()
    if count:
        DeltaTable.forName(compute.spark, name).delete(condition)
    return count


def _vacuum(compute: SparkCompute, logicals: list[str], hours: int) -> None:
    spark = compute.spark
    # Below Delta's 7-day safety margin only when the operator asks for it explicitly.
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    try:
        for logical in logicals:
            name = compute.build_table_name(logical)
            if spark.catalog.tableExists(name):
                DeltaTable.forName(spark, name).vacuum(hours)
    finally:
        spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "true")


def _rebuild_readers(compute: SparkCompute, project: Project, env: str, dataset: str) -> list[str]:
    """Rebuild, in order, every gold model that reads ``dataset`` directly or indirectly."""
    affected = {f"silver.{dataset}"}
    rebuilt = []
    for model in project.gold_models:
        if not {str(i) for i in model.inputs} & affected:
            continue
        table = compute.build_table_name(
            build_dataset_name(env, Layer.GOLD, model.domain, model.name)
        )
        if not compute.spark.catalog.tableExists(table):
            continue  # never built here: it holds nothing to remove
        run_gold(compute, model, env)
        affected.add(f"gold.{model.domain}.{model.name}")
        rebuilt.append(f"{model.domain}.{model.name}")
    return rebuilt


def _append(
    compute: SparkCompute, env: str, table: str, schema: T.StructType, row: tuple[object, ...]
) -> None:
    logical = build_harness_table_name(env, table)
    compute.ensure_schema(logical)
    compute.spark.createDataFrame([row], schema).write.format("delta").mode("append").saveAsTable(
        compute.build_table_name(logical)
    )


def _silver_key_literal(contract: Contract, key_text: str, secrets: SecretProvider) -> str:
    """What the erasure key looks like in silver, after its masking."""
    privacy = contract.privacy
    if privacy is None:
        raise ErasureError("no privacy section")
    masking = privacy.masking.get(privacy.erasure_key)
    if masking is None or masking.strategy is MaskingStrategy.KEEP:
        return key_text
    params = masking.params or {}
    if masking.strategy is MaskingStrategy.HASH:
        key = secrets.get_secret(params.get("key", HASH_SECRET))
        return hmac.new(key, key_text.encode(), hashlib.sha256).hexdigest()
    if masking.strategy is MaskingStrategy.TOKENIZE:
        key = secrets.get_secret(params.get("key", TOKEN_SECRET))
        digest = hmac.new(key, key_text.encode(), hashlib.sha256).hexdigest()
        return TOKEN_PREFIX + digest[:TOKEN_HEX_LENGTH]
    raise ErasureError(f"erasure key masked with {masking.strategy} cannot be found")


def erase_subject(
    compute: SparkCompute,
    project: Project,
    env: str,
    dataset: str,
    key_text: str,
    request_id: str,
    secrets: SecretProvider,
    vacuum_hours: int | None = None,
) -> OperationRecord:
    """Delete every record of one data subject from one dataset, in all layers.

    ``key_text`` is the erasure key's value as text (``"42"``). All SCD2
    versions go, not just the current one.
    """
    domain, entity = dataset.split(".")
    contract = project.find_contract(domain, entity)
    if contract.privacy is None:
        raise ErasureError(f"{dataset} holds no personal data (no privacy section)")
    column = contract.privacy.erasure_key
    raw_match = F.get_json_object("_payload", f"$.{column}") == F.lit(key_text)
    silver_value = _silver_key_literal(contract, key_text, secrets)
    layers = {
        "bronze": build_dataset_name(env, Layer.BRONZE, domain, entity),
        "quarantine": build_dataset_name(env, Layer.QUARANTINE, domain, entity),
        "silver": build_dataset_name(env, Layer.SILVER, domain, entity),
    }
    deleted = {
        "bronze": _delete(compute, layers["bronze"], raw_match),
        "quarantine": _delete(compute, layers["quarantine"], raw_match),
        "silver": _delete(
            compute, layers["silver"], F.col(column).cast("string") == F.lit(silver_value)
        ),
    }
    rebuilt = _rebuild_readers(compute, project, env, dataset)
    vacuumed = vacuum_hours is not None
    if vacuum_hours is not None:
        gold = [build_dataset_name(env, Layer.GOLD, *m.split(".")) for m in rebuilt]
        _vacuum(compute, [*layers.values(), *gold], vacuum_hours)
    record = OperationRecord(dataset, deleted, rebuilt, vacuumed)
    _append(
        compute,
        env,
        ERASURES_TABLE,
        ERASURES_SCHEMA,
        (
            request_id,
            dataset,
            compute_subject_hash(secrets, key_text),
            contract.identity.version,
            deleted,
            rebuilt,
            vacuumed,
            _now(),
        ),
    )
    return record


def build_erased_filter(
    compute: SparkCompute, contract: Contract, env: str, secrets: SecretProvider
) -> Callable[[DataFrame], DataFrame]:
    """Drop records of erased subjects, so a replay from the source cannot bring
    them back. Applied by silver to every batch."""
    logical = build_harness_table_name(env, ERASURES_TABLE)
    name = compute.build_table_name(logical)
    privacy = contract.privacy
    if privacy is None or not compute.spark.catalog.tableExists(name):
        return lambda df: df
    dataset = f"{contract.identity.domain}.{contract.identity.name}"
    erased = (
        compute.spark.table(name)
        .where(F.col("dataset") == dataset)
        .select(F.col("subject_hash").alias("_erased_hash"))
        .distinct()
    )
    if erased.isEmpty():
        return lambda df: df
    key = secrets.get_secret(HASH_SECRET)
    column = privacy.erasure_key

    def drop_erased(df: DataFrame) -> DataFrame:
        hashed = build_hmac_sha256(key, F.col(column).cast("string"))
        return (
            df.withColumn("_erased_hash", hashed)
            .join(erased, "_erased_hash", "left_anti")
            .drop("_erased_hash")
        )

    return drop_erased


def purge_expired(
    compute: SparkCompute,
    project: Project,
    env: str,
    now: datetime | None = None,
    vacuum_hours: int | None = None,
) -> list[OperationRecord]:
    """Delete records older than each contract's retention period, in all layers.

    Periods use 365-day years and 30-day months, the same approximation as the
    rest of the harness; the cutoff is recorded with each purge.
    """
    now = now or _now()
    records = []
    for contract in project.contracts:
        if contract.privacy is None:
            continue
        domain, entity = contract.identity.domain, contract.identity.name
        dataset = f"{domain}.{entity}"
        retention = contract.privacy.retention
        cutoff = now - timedelta(seconds=compute_duration_seconds(retention.period))
        raw_time = F.get_json_object("_payload", f"$.{retention.column}").try_cast("timestamp")
        layers = {
            "bronze": build_dataset_name(env, Layer.BRONZE, domain, entity),
            "quarantine": build_dataset_name(env, Layer.QUARANTINE, domain, entity),
            "silver": build_dataset_name(env, Layer.SILVER, domain, entity),
        }
        expired_raw = raw_time < F.lit(cutoff)
        deleted = {
            "bronze": _delete(compute, layers["bronze"], expired_raw),
            "quarantine": _delete(compute, layers["quarantine"], expired_raw),
            "silver": _delete(
                compute,
                layers["silver"],
                F.col(retention.column).cast("timestamp") < F.lit(cutoff),
            ),
        }
        rebuilt = _rebuild_readers(compute, project, env, dataset) if any(deleted.values()) else []
        vacuumed = vacuum_hours is not None
        if vacuum_hours is not None:
            _vacuum(compute, list(layers.values()), vacuum_hours)
        records.append(OperationRecord(dataset, deleted, rebuilt, vacuumed))
        _append(
            compute,
            env,
            PURGES_TABLE,
            PURGES_SCHEMA,
            (dataset, cutoff, deleted, rebuilt, vacuumed, _now()),
        )
    return records
