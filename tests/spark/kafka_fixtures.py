"""Build DataFrames shaped like Spark's Kafka source, without a broker."""

import json
from datetime import UTC, datetime
from typing import Any

from pyspark.sql import DataFrame, SparkSession

KAFKA_SCHEMA = (
    "key binary, value binary, topic string, partition int, offset long, timestamp timestamp"
)
FIXED_TIME = datetime(2026, 1, 1, tzinfo=UTC)


def build_kafka_rows(
    spark: SparkSession,
    messages: list[tuple[Any, Any]],
    topic: str = "cdc.crm.public.customers",
) -> DataFrame:
    """``messages`` are (key, value) pairs; dicts are serialised as JSON, None stays null."""

    def encode(item: Any) -> bytes | None:
        if item is None:
            return None
        text = item if isinstance(item, str) else json.dumps(item)
        return text.encode()

    rows = [
        (encode(key), encode(value), topic, 0, offset, FIXED_TIME)
        for offset, (key, value) in enumerate(messages)
    ]
    return spark.createDataFrame(rows, KAFKA_SCHEMA)


def build_debezium_event(
    op: str,
    after: dict[str, Any] | None,
    before: dict[str, Any] | None = None,
    lsn: int = 100,
    ts_ms: int = 1_767_225_600_000,
) -> dict[str, Any]:
    return {
        "before": before,
        "after": after,
        "source": {"connector": "postgresql", "lsn": lsn, "ts_ms": ts_ms},
        "op": op,
        "ts_ms": ts_ms + 5,
    }
