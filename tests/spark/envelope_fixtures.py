"""Build bronze envelope DataFrames directly, for silver block tests."""

import json
from datetime import datetime
from typing import Any

from pyspark.sql import DataFrame, SparkSession

from contractline.spark.bronze import ENVELOPE_SCHEMA

BASE_TIME = datetime(2026, 1, 1, 12, 0)


def build_envelope(
    spark: SparkSession,
    records: list[dict[str, Any]],
) -> DataFrame:
    """Each record: ``payload`` (dict or raw string) plus optional envelope fields.

    Defaults: op ``insert``, sequence and offset from the position in the list.
    """
    rows = []
    for i, record in enumerate(records):
        payload = record.get("payload")
        if isinstance(payload, dict):
            payload = json.dumps(payload)
        rows.append(
            (
                record.get("key"),
                payload,
                record.get("op", "insert"),
                "kafka",
                "test.topic",
                record.get("offset", f"test.topic/0/{i}"),
                record.get("sequence", i),
                record.get("source_at", BASE_TIME),
                record.get("ingested_at", BASE_TIME),
                record.get("batch_id", "0"),
            )
        )
    return spark.createDataFrame(rows, ENVELOPE_SCHEMA)
