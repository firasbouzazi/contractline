"""Local compute adapter: PySpark in local mode with Delta Lake tables.

Local Spark without a catalog has two-level names (``database.table``), so the
logical name ``prod_silver.sales.customers`` becomes
``prod_silver__sales.customers`` (ADR-0007).
"""

from functools import cache
from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1"
LOGICAL_NAME_PARTS = 3


def build_physical_name(logical_name: str) -> str:
    """``<catalog>.<schema>.<table>`` to ``<catalog>__<schema>.<table>``."""
    parts = logical_name.split(".")
    if len(parts) != LOGICAL_NAME_PARTS:
        raise ValueError(f"expected <catalog>.<schema>.<table>, got {logical_name!r}")
    catalog, schema, table = parts
    return f"{catalog}__{schema}.{table}"


@cache
def _get_spark(warehouse_dir: str) -> SparkSession:
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("contractline")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.sql.warehouse.dir", warehouse_dir)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.databricks.delta.snapshotPartitions", "2")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
    )
    spark = configure_spark_with_delta_pip(builder, extra_packages=[KAFKA_PACKAGE]).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


class LocalSparkDelta:
    """Implements ``contractline.spark.interfaces.SparkCompute`` on a laptop."""

    def __init__(self, root: Path) -> None:
        self.root = root.resolve()
        (self.root / "warehouse").mkdir(parents=True, exist_ok=True)
        (self.root / "checkpoints").mkdir(parents=True, exist_ok=True)

    @property
    def spark(self) -> SparkSession:
        # One session per process: Spark cannot run two local sessions side by side.
        return _get_spark(str(self.root / "warehouse"))

    def build_table_name(self, logical_name: str) -> str:
        return build_physical_name(logical_name)

    def ensure_schema(self, logical_name: str) -> None:
        database = self.build_table_name(logical_name).split(".")[0]
        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS `{database}`")

    def build_checkpoint_path(self, logical_name: str) -> str:
        return str(self.root / "checkpoints" / logical_name)
