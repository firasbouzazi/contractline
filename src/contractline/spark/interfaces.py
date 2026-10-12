"""Interfaces between Spark-family blocks and the platform adapters."""

from typing import Protocol

from pyspark.sql import SparkSession


class SparkCompute(Protocol):
    """A place to run Spark and keep Delta tables: local or Databricks."""

    @property
    def spark(self) -> SparkSession: ...

    def build_table_name(self, logical_name: str) -> str:
        """Physical table name for a logical ``<catalog>.<schema>.<table>`` name."""
        ...

    def ensure_schema(self, logical_name: str) -> None:
        """Create the schema (database) holding ``logical_name`` if it is missing."""
        ...

    def build_checkpoint_path(self, logical_name: str) -> str:
        """Storage path for streaming checkpoints that feed ``logical_name``."""
        ...
