"""Kafka source adapter, including Debezium CDC topics.

Reads with Spark Structured Streaming. The pipeline runs it with an
``availableNow`` trigger, so each run consumes everything new since the last
checkpoint and stops.
"""

from dataclasses import dataclass, field

from pyspark.sql import DataFrame, SparkSession

from contractline.core.contract import Contract, SourceAdapter
from contractline.spark.bronze import UnsupportedSourceError, map_kafka_to_envelope

__all__ = ["KafkaSource", "UnsupportedSourceError"]


@dataclass(frozen=True)
class KafkaSource:
    bootstrap_servers: str
    options: dict[str, str] = field(default_factory=dict)
    """Extra ``kafka.*`` or source options, for example security settings."""

    def read_envelope_stream(self, spark: SparkSession, contract: Contract) -> DataFrame:
        if contract.source.adapter is not SourceAdapter.KAFKA:
            raise UnsupportedSourceError(f"{contract.identity.name} is not a Kafka contract")
        raw = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", self.bootstrap_servers)
            .option("subscribe", contract.source.location)
            .option("startingOffsets", "earliest")
            # Topics with retention shorter than the pipeline's schedule lose data;
            # that must fail loudly rather than skip silently.
            .option("failOnDataLoss", "true")
            .options(**self.options)
            .load()
        )
        return self.map_to_envelope(raw, contract)

    @staticmethod
    def map_to_envelope(raw: DataFrame, contract: Contract) -> DataFrame:
        return map_kafka_to_envelope(raw, contract)
