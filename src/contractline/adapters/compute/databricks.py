"""Databricks compute adapter (ADR-0019).

Logical names are Unity Catalog names as they are: ``prod_silver.sales.customers``
is catalog ``prod_silver``, schema ``sales``, table ``customers``. Catalogs are
created by Terraform (``infra/terraform/databricks``); schemas on first use.
Streaming checkpoints live in a Unity Catalog volume per environment.

The Spark session is the job's own session when running on Databricks, or a
Databricks Connect session from a laptop or CI.
"""

from dataclasses import dataclass

from pyspark.sql import SparkSession


class DatabricksSessionError(RuntimeError):
    pass


def _connect_session() -> SparkSession:
    try:
        from databricks.connect import DatabricksSession
    except ImportError as exc:
        raise DatabricksSessionError(
            "not running on Databricks and databricks-connect is not installed: "
            "pip install 'contractline[databricks]'"
        ) from exc
    session: SparkSession = DatabricksSession.builder.getOrCreate()
    return session


@dataclass
class DatabricksCompute:
    """Implements ``contractline.spark.interfaces.SparkCompute`` on Databricks."""

    env: str
    checkpoint_volume: str | None = None
    """Defaults to ``/Volumes/<env>_harness/contractline/checkpoints``."""
    _session: SparkSession | None = None

    @property
    def spark(self) -> SparkSession:
        if self._session is None:
            self._session = SparkSession.getActiveSession() or _connect_session()
        return self._session

    def build_table_name(self, logical_name: str) -> str:
        catalog, schema, table = logical_name.split(".")
        return f"`{catalog}`.`{schema}`.`{table}`"

    def ensure_schema(self, logical_name: str) -> None:
        catalog, schema, _ = logical_name.split(".")
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")

    def build_checkpoint_path(self, logical_name: str) -> str:
        root = self.checkpoint_volume or f"/Volumes/{self.env}_harness/contractline/checkpoints"
        return f"{root}/{logical_name}"
