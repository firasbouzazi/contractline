"""The bronze envelope (principle 7) and the mappers that fill it.

Every source lands in the same table shape, so silver never knows where data
came from:

==================  =========  ==================================================
column              type       meaning
==================  =========  ==================================================
``_key``            string     source key as text (Kafka message key), nullable
``_payload``        string     JSON object of the record's values
``_op``             string     ``insert``, ``update``, ``delete``, ``snapshot``,
                               or ``unknown`` for messages that could not be read
``_source_system``  string     ``kafka``, ``files``, ...
``_source_location`` string    topic, folder, ...
``_source_offset``  string     unique position in the source (topic/partition/offset)
``_sequence``       long       order of changes for one key (CDC log position)
``_source_at``      timestamp  when the change happened at the source
``_ingested_at``    timestamp  when the harness landed it
``_batch_id``       string     the ingestion batch that landed it
==================  =========  ==================================================

Bronze never rejects a record: anything unreadable lands with ``_op = unknown``
and is quarantined by silver. Payload values are left as the source sent them,
except where a source encodes a type in its own way (Debezium sends dates as
days since epoch); the mapper turns those into ISO text using the contract.
"""

from delta.tables import DeltaTable
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from contractline.core.contract import Contract
from contractline.spark.interfaces import SparkCompute

ENVELOPE_SCHEMA = T.StructType(
    [
        T.StructField("_key", T.StringType()),
        T.StructField("_payload", T.StringType()),
        T.StructField("_op", T.StringType(), nullable=False),
        T.StructField("_source_system", T.StringType(), nullable=False),
        T.StructField("_source_location", T.StringType(), nullable=False),
        T.StructField("_source_offset", T.StringType(), nullable=False),
        T.StructField("_sequence", T.LongType()),
        T.StructField("_source_at", T.TimestampType()),
        T.StructField("_ingested_at", T.TimestampType(), nullable=False),
        T.StructField("_batch_id", T.StringType(), nullable=False),
    ]
)
ENVELOPE_COLUMNS = tuple(f.name for f in ENVELOPE_SCHEMA.fields)

OP_INSERT = "insert"
OP_UPDATE = "update"
OP_DELETE = "delete"
OP_SNAPSHOT = "snapshot"
OP_UNKNOWN = "unknown"

_DEBEZIUM_OPS = {"c": OP_INSERT, "u": OP_UPDATE, "d": OP_DELETE, "r": OP_SNAPSHOT}


def _kafka_position(raw: DataFrame) -> list[Column]:
    return [
        F.lit("kafka").alias("_source_system"),
        F.col("topic").alias("_source_location"),
        F.concat_ws("/", F.col("topic"), F.col("partition"), F.col("offset")).alias(
            "_source_offset"
        ),
    ]


def map_kafka_json(raw: DataFrame) -> DataFrame:
    """Business events: each Kafka message value is one JSON record, appended."""
    return raw.select(
        F.col("key").cast("string").alias("_key"),
        F.col("value").cast("string").alias("_payload"),
        F.lit(OP_INSERT).alias("_op"),
        *_kafka_position(raw),
        F.col("offset").cast("long").alias("_sequence"),
        F.col("timestamp").alias("_source_at"),
    )


def _normalize_debezium_values(values: Column, contract: Contract) -> Column:
    """Turn Debezium's numeric date and timestamp encodings into ISO text.

    Requires the connector setting ``decimal.handling.mode=string`` so decimals
    arrive as exact strings (ADR-0011).
    """
    date_columns = [c.name for c in contract.schema.columns if c.type == "date"]
    timestamp_columns = [c.name for c in contract.schema.columns if c.type == "timestamp"]

    def convert(key: Column, value: Column) -> Column:
        is_number = value.rlike(r"^-?[0-9]+$")
        as_date = F.date_format(
            F.date_add(F.lit("1970-01-01").cast("date"), value.cast("int")), "yyyy-MM-dd"
        )
        as_timestamp = F.date_format(
            F.timestamp_micros(value.cast("long")), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'"
        )
        result = value
        if date_columns:
            result = F.when(key.isin(date_columns) & is_number, as_date).otherwise(result)
        if timestamp_columns:
            result = F.when(key.isin(timestamp_columns) & is_number, as_timestamp).otherwise(result)
        return result

    return F.transform_values(values, convert)


def map_debezium(raw: DataFrame, contract: Contract) -> DataFrame:
    """Debezium change events (JSON, no embedded schema) to envelope rows.

    Tombstones (null values, used for log compaction after deletes) are dropped:
    the delete event before them already carries the change.
    """
    value = F.col("value").cast("string")
    op_code = F.get_json_object(value, "$.op")
    op = F.coalesce(
        F.create_map(*[x for k, v in _DEBEZIUM_OPS.items() for x in (F.lit(k), F.lit(v))])[op_code],
        F.lit(OP_UNKNOWN),
    )
    image = F.when(op == OP_DELETE, F.get_json_object(value, "$.before")).otherwise(
        F.get_json_object(value, "$.after")
    )
    values = F.from_json(image, "map<string,string>")
    payload = F.when(op == OP_UNKNOWN, value).otherwise(
        F.to_json(_normalize_debezium_values(values, contract))
    )
    source_ms = F.get_json_object(value, "$.source.ts_ms").cast("long")
    return raw.where(F.col("value").isNotNull()).select(
        F.col("key").cast("string").alias("_key"),
        payload.alias("_payload"),
        op.alias("_op"),
        *_kafka_position(raw),
        F.coalesce(
            F.get_json_object(value, "$.source.lsn").cast("long"), F.col("offset").cast("long")
        ).alias("_sequence"),
        F.coalesce(F.timestamp_millis(source_ms), F.col("timestamp")).alias("_source_at"),
    )


def add_ingestion_columns(envelope: DataFrame, batch_id: str) -> DataFrame:
    """Stamp a batch with its id and landing time, in envelope column order."""
    return envelope.select(
        *[c for c in envelope.columns if c not in ("_ingested_at", "_batch_id")],
        F.current_timestamp().alias("_ingested_at"),
        F.lit(batch_id).alias("_batch_id"),
    ).select(*ENVELOPE_COLUMNS)


def ensure_bronze_table(compute: SparkCompute, logical_name: str) -> str:
    """Create the bronze Delta table if needed and return its physical name."""
    compute.ensure_schema(logical_name)
    table = compute.build_table_name(logical_name)
    (
        DeltaTable.createIfNotExists(compute.spark)
        .tableName(table)
        .addColumns(ENVELOPE_SCHEMA)
        .property("contractline.layer", "bronze")
        .execute()
    )
    return table


def write_bronze_batch(
    compute: SparkCompute, logical_name: str, envelope: DataFrame, batch_id: int
) -> None:
    """Append one micro-batch to bronze exactly once.

    Delta's idempotent-write options make a retried micro-batch a no-op: the
    table remembers the last ``txnVersion`` written for this ``txnAppId``.
    Bronze is append-only; silver also drops duplicate ``_source_offset`` values,
    so a replay from an older checkpoint cannot create duplicates downstream.
    """
    table = ensure_bronze_table(compute, logical_name)
    (
        add_ingestion_columns(envelope, str(batch_id))
        .write.format("delta")
        .mode("append")
        .option("txnAppId", f"contractline:{logical_name}")
        .option("txnVersion", batch_id)
        .saveAsTable(table)
    )
