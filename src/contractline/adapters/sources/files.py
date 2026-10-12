"""Files source adapter: CSV, JSON lines and Parquet from a folder.

Locally the folder is a path; on object storage any Hadoop URI works. Each run
reads the files that appeared since the last checkpoint (Spark's file source
with an ``availableNow`` trigger). On Databricks, Auto Loader replaces this
reader and feeds the same mapper (milestone 5).
"""

from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from contractline.core.contract import Contract, IngestMode, SourceAdapter
from contractline.spark.bronze import UnsupportedSourceError, map_files
from contractline.spark.types import to_spark_type


@dataclass(frozen=True)
class FilesSource:
    def read_raw_stream(self, spark: SparkSession, contract: Contract) -> DataFrame:
        """Rows as ``payload`` (JSON text), ``file_path``, ``file_modified_at``."""
        if contract.source.adapter is not SourceAdapter.FILES:
            raise UnsupportedSourceError(f"{contract.identity.name} is not a files contract")
        source = contract.source
        options = {k: str(v) for k, v in (source.options or {}).items()}
        reader = spark.readStream.options(**options)
        # A full snapshot must be read alone, so each file becomes its own batch.
        if source.mode is IngestMode.FULL:
            reader = reader.option("maxFilesPerTrigger", "1")
        columns = contract.schema.columns
        if source.format == "json":
            frame = reader.format("text").load(source.location)
            payload = F.col("value")
        elif source.format == "csv":
            text_schema = T.StructType([T.StructField(c.name, T.StringType()) for c in columns])
            frame = (
                reader.format("csv")
                .option("header", "true")
                .schema(text_schema)
                .load(source.location)
            )
            payload = F.to_json(F.struct(*[F.col(c.name) for c in columns]))
        elif source.format == "parquet":
            typed = T.StructType([T.StructField(c.name, to_spark_type(c.type)) for c in columns])
            frame = reader.format("parquet").schema(typed).load(source.location)
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
