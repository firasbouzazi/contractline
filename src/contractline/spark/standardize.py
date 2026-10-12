"""Standardize: bronze envelope to typed records with a list of problems.

Each record keeps its envelope metadata and gains one typed column per contract
column plus ``_errors``, the reasons it cannot reach silver. Nothing is
dropped here; the quality block decides what to quarantine.
"""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from contractline.core.contract import Contract, DriftPolicy
from contractline.spark.bronze import OP_DELETE, OP_INSERT, OP_SNAPSHOT, OP_UPDATE
from contractline.spark.types import to_spark_type

KNOWN_OPS = (OP_INSERT, OP_UPDATE, OP_DELETE, OP_SNAPSHOT)
METADATA_COLUMNS = (
    "_key",
    "_payload",
    "_op",
    "_source_offset",
    "_sequence",
    "_source_at",
    "_ingested_at",
    "_batch_id",
)


class SchemaDriftError(RuntimeError):
    """Raised when a source sends columns the contract does not declare and the
    drift policy is ``fail``."""


def _error_if(condition: Column, message: Column | str) -> Column:
    text = message if isinstance(message, Column) else F.lit(message)
    return F.when(condition, text)


def find_unknown_columns(bronze: DataFrame, contract: Contract) -> list[str]:
    """Payload keys that the contract does not declare (schema drift)."""
    declared = [c.name for c in contract.schema.columns]
    keys = F.explode(F.json_object_keys(F.col("_payload")))
    found = (
        bronze.where(F.col("_op") != "unknown")
        .select(keys.alias("key"))
        .where(~F.col("key").isin(declared))
        .distinct()
        .collect()
    )
    return sorted(row["key"] for row in found)


def standardize(bronze: DataFrame, contract: Contract) -> DataFrame:
    """Parse ``_payload`` into typed contract columns and collect ``_errors``.

    A value that is present but does not fit its type (``"abc"`` for a long)
    is an error, never a silent null. Decimals are parsed straight from the
    JSON text so no precision is lost.
    """
    columns = contract.schema.columns
    as_text = F.from_json(F.col("_payload"), "map<string,string>")
    decimals = [c for c in columns if c.type.startswith("decimal")]
    typed_decimals = (
        F.from_json(
            F.col("_payload"),
            ", ".join(f"`{c.name}` {c.type}" for c in decimals),
        )
        if decimals
        else None
    )

    df = bronze.dropDuplicates(["_source_offset"]).withColumn("_text", as_text)
    if typed_decimals is not None:
        df = df.withColumn("_decimals", typed_decimals)

    typed: list[Column] = []
    errors: list[Column] = [
        _error_if(~F.col("_op").isin(*KNOWN_OPS), "schema: unreadable source record"),
        _error_if(
            F.col("_op").isin(*KNOWN_OPS) & F.col("_text").isNull(),
            "schema: payload is not a JSON object",
        ),
    ]
    for column in columns:
        text = F.col("_text")[column.name]
        if column.type.startswith("decimal"):
            value = F.col("_decimals")[column.name]
        else:
            value = text.try_cast(to_spark_type(column.type))
        typed.append(value.alias(column.name))
        errors.append(
            _error_if(
                text.isNotNull() & value.isNull(),
                F.concat(
                    F.lit(f"schema: {column.name} value '"),
                    F.substring(text, 1, 50),
                    F.lit(f"' is not a valid {column.type}"),
                ),
            )
        )
        if not column.nullable:
            errors.append(
                _error_if(
                    # A delete may only carry the key (Postgres without REPLICA IDENTITY FULL).
                    F.col("_op").isin(OP_INSERT, OP_UPDATE, OP_SNAPSHOT)
                    & F.col("_text").isNotNull()
                    & text.isNull(),
                    f"schema: {column.name} is required",
                )
            )

    if contract.schema.drift_policy is DriftPolicy.QUARANTINE:
        declared = F.array(*[F.lit(c.name) for c in columns])
        unknown = F.array_except(F.coalesce(F.json_object_keys("_payload"), F.array()), declared)
        errors.append(
            _error_if(
                F.size(unknown) > 0,
                F.concat(F.lit("schema: undeclared columns "), F.array_join(unknown, ", ")),
            )
        )

    return df.select(
        *[F.col(c) for c in METADATA_COLUMNS],
        *typed,
        F.filter(F.array(*errors), lambda e: e.isNotNull()).alias("_errors"),
    )


def check_drift(bronze: DataFrame, contract: Contract) -> list[str]:
    """Apply the ``fail`` drift policy; return undeclared columns for reporting."""
    unknown = find_unknown_columns(bronze, contract)
    if unknown and contract.schema.drift_policy is DriftPolicy.FAIL:
        raise SchemaDriftError(
            f"{contract.identity.domain}.{contract.identity.name}: the source sends columns "
            f"the contract does not declare: {', '.join(unknown)} (drift_policy: fail)"
        )
    return unknown
