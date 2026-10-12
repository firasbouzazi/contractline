"""Run a whole project in one environment: ingest, silver, gold, in plan order.

This is the only module that wires platform adapters to blocks.
"""

from collections.abc import Callable
from dataclasses import dataclass, field

from contractline.adapters.sources.files import FilesSource
from contractline.adapters.sources.kafka import KafkaSource
from contractline.core.contract import Contract, SourceAdapter
from contractline.core.project import Project
from contractline.core.secrets import SecretProvider
from contractline.spark.bronze import ingest_to_bronze
from contractline.spark.gold import GoldReport, run_gold
from contractline.spark.interfaces import SparkCompute, SparkSource
from contractline.spark.silver import SilverReport, run_silver

SourceFactory = Callable[[Contract], SparkSource]


class MissingSourceError(ValueError):
    pass


@dataclass
class RunSummary:
    env: str
    bronze_batches: dict[str, int] = field(default_factory=dict)
    silver: list[SilverReport] = field(default_factory=list)
    gold: list[GoldReport] = field(default_factory=list)


def build_source_factory(kafka_bootstrap: str | None) -> SourceFactory:
    def factory(contract: Contract) -> SparkSource:
        if contract.source.adapter is SourceAdapter.KAFKA:
            if not kafka_bootstrap:
                raise MissingSourceError(
                    f"{contract.identity.domain}.{contract.identity.name} reads Kafka: "
                    "pass --kafka-bootstrap-servers"
                )
            return KafkaSource(kafka_bootstrap)
        if contract.source.adapter is SourceAdapter.FILES:
            return FilesSource()
        raise MissingSourceError(f"no source adapter for {contract.source.adapter}")

    return factory


def run_project(
    compute: SparkCompute,
    project: Project,
    env: str,
    sources: SourceFactory,
    secrets: SecretProvider,
    progress: Callable[[str], None] = lambda _: None,
) -> RunSummary:
    summary = RunSummary(env)
    for contract in project.contracts:
        name = f"{contract.identity.domain}.{contract.identity.name}"
        progress(f"ingest {name}")
        summary.bronze_batches[name] = ingest_to_bronze(compute, sources(contract), contract, env)
        progress(f"silver {name}")
        summary.silver.extend(run_silver(compute, contract, env, secrets))
    for model in project.gold_models:
        progress(f"gold {model.domain}.{model.name}")
        summary.gold.append(run_gold(compute, model, env))
    return summary
