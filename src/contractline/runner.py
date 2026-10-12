"""Run a whole project in one environment: ingest, silver, gold, in plan order.

This is the only module that wires platform adapters to blocks.
"""

from collections.abc import Callable
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path

from contractline.adapters.compute.databricks import DatabricksCompute
from contractline.adapters.compute.spark_delta_local import LocalSparkDelta
from contractline.adapters.secrets_databricks import DatabricksSecretProvider
from contractline.adapters.sources.autoloader import AutoLoaderSource
from contractline.adapters.sources.files import FilesSource
from contractline.adapters.sources.kafka import KafkaSource
from contractline.core.contract import Contract, SourceAdapter
from contractline.core.plan import StepKind, build_plan
from contractline.core.project import Project
from contractline.core.secrets import EnvSecretProvider, SecretProvider
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


class UnknownStepError(ValueError):
    pass


def run_step(
    compute: SparkCompute,
    project: Project,
    env: str,
    step_id: str,
    sources: SourceFactory,
    secrets: SecretProvider,
) -> str:
    """Run one plan step (what an orchestrator task does). Returns a summary line."""
    step = next((s for s in build_plan(project, env) if s.id == step_id), None)
    if step is None:
        raise UnknownStepError(f"no step {step_id!r} in this project")
    if step.kind is StepKind.GOLD:
        report = run_gold(compute, project.find_gold(step.domain, step.entity), env)
        return f"{step_id}: {report.rows} rows"
    contract = project.find_contract(step.domain, step.entity)
    if step.kind is StepKind.INGEST:
        batches = ingest_to_bronze(compute, sources(contract), contract, env)
        return f"{step_id}: {batches} batch(es) landed"
    reports = run_silver(compute, contract, env, secrets)
    total = sum(r.records_in for r in reports)
    quarantined = sum(r.records_quarantined for r in reports)
    return f"{step_id}: {total} records, {quarantined} quarantined"


class Platform(StrEnum):
    LOCAL = "local"
    DATABRICKS = "databricks"


def build_platform(
    platform: Platform,
    env: str,
    workdir: Path,
    kafka_bootstrap: str | None,
    secret_scope: str = "contractline",  # noqa: S107 - the scope's name, not a secret
) -> tuple[SparkCompute, SourceFactory, SecretProvider]:
    """Compute, sources and secrets for one platform. The blocks never change."""
    if platform is Platform.LOCAL:
        return LocalSparkDelta(workdir), build_source_factory(kafka_bootstrap), EnvSecretProvider()
    compute = DatabricksCompute(env)
    kafka = build_source_factory(kafka_bootstrap)
    autoloader = AutoLoaderSource(f"/Volumes/{env}_harness/contractline/autoloader")

    def sources(contract: Contract) -> SparkSource:
        if contract.source.adapter is SourceAdapter.FILES:
            return autoloader
        return kafka(contract)

    return compute, sources, DatabricksSecretProvider(secret_scope)
