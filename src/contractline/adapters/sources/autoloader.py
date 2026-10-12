"""Files source on Databricks: Auto Loader (``cloudFiles``) feeding the same
mapper as the local files adapter, so bronze is identical on both platforms."""

from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from contractline.core.contract import Contract, IngestMode, SourceAdapter
from contractline.spark.bronze import UnsupportedSourceError, map_files
from contractline.spark.types import to_spark_type


@dataclass(frozen=True)
class AutoLoaderSource:
    schema_location: str
    """Where Auto Loader keeps its file-tracking state (a Unity Catalog volume path)."""

    def read_raw_stream(self, spark: SparkSession, contract: Contract) -> DataFrame:
        source = contract.source
        if source.adapter is not SourceAdapter.FILES:
            raise UnsupportedSourceError(f"{contract.identity.name} is not a files contract")
        columns = contract.schema.columns
        reader = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.schemaLocation", f"{self.schema_location}/{contract.identity.name}")
            .options(**{k: str(v) for k, v in (source.options or {}).items()})
        )
        if source.mode is IngestMode.FULL:
            reader = reader.option("cloudFiles.maxFilesPerTrigger", "1")
        if source.format == "json":
            frame = reader.option("cloudFiles.format", "text").load(source.location)
            payload = F.col("value")
        elif source.format == "csv":
            text = T.StructType([T.StructField(c.name, T.StringType()) for c in columns])
            frame = (
                reader.option("cloudFiles.format", "csv")
                .option("header", "true")
                .schema(text)
                .load(source.location)
            )
            payload = F.to_json(F.struct(*[F.col(c.name) for c in columns]))
        elif source.format == "parquet":
            typed = T.StructType([T.StructField(c.name, to_spark_type(c.type)) for c in columns])
            frame = (
                reader.option("cloudFiles.format", "parquet").schema(typed).load(source.location)
            )
            payload = F.to_json(F.struct(*[F.col(c.name) for c in columns]))
        else:
            raise UnsupportedSourceError(f"files format {source.format!r} is not supported")
        return frame.select(
            payload.alias("payload"),
            F.col("_metadata.file_path").alias("file_path"),
            F.col("_metadata.file_modification_time").alias("file_modified_at"),
        )

    def read_envelope_stream(self, spark: SparkSession, contract: Contract) -> DataFrame:
        return map_files(self.read_raw_stream(spark, contract), contract)
