"""Compile a project into an ordered pipeline plan for one environment.

The plan is engine-neutral: steps, the tables they read and write, and their
dependencies. ``contractline plan`` prints it, ``contractline run`` executes it,
and the Airflow adapter turns it into Dags (milestone 3).
"""

from dataclasses import dataclass
from enum import StrEnum

from contractline.core.contract import Contract, RuleType
from contractline.core.gold import GoldModel
from contractline.core.naming import Layer, build_dag_id, build_dataset_name, build_resource_tags
from contractline.core.project import Project


class StepKind(StrEnum):
    INGEST = "ingest"
    SILVER = "silver"
    GOLD = "gold"


@dataclass(frozen=True)
class Step:
    id: str
    kind: StepKind
    domain: str
    entity: str
    reads: tuple[str, ...]
    writes: tuple[str, ...]
    depends_on: tuple[str, ...]
    tags: tuple[tuple[str, str], ...] = ()


def _silver_dependencies(contract: Contract, known: set[str]) -> list[str]:
    refs = []
    for rule in contract.quality:
        if rule.type is RuleType.REFERENTIAL_INTEGRITY:
            domain, entity, _ = rule.params["references"].split(".")
            step = build_dag_id(domain, entity, Layer.SILVER)
            if step in known and (domain, entity) != (
                contract.identity.domain,
                contract.identity.name,
            ):
                refs.append(step)
    return refs


def build_plan(project: Project, env: str) -> list[Step]:
    steps: list[Step] = []
    silver_ids = {
        build_dag_id(c.identity.domain, c.identity.name, Layer.SILVER) for c in project.contracts
    }
    for contract in project.contracts:
        domain, entity = contract.identity.domain, contract.identity.name
        tags = tuple(sorted(build_resource_tags(contract).items()))
        bronze = build_dataset_name(env, Layer.BRONZE, domain, entity)
        ingest_id = build_dag_id(domain, entity, Layer.BRONZE)
        steps.append(
            Step(
                ingest_id,
                StepKind.INGEST,
                domain,
                entity,
                reads=(f"{contract.source.adapter}:{contract.source.location}",),
                writes=(bronze,),
                depends_on=(),
                tags=tags,
            )
        )
        steps.append(
            Step(
                build_dag_id(domain, entity, Layer.SILVER),
                StepKind.SILVER,
                domain,
                entity,
                reads=(bronze,),
                writes=(
                    build_dataset_name(env, Layer.SILVER, domain, entity),
                    build_dataset_name(env, Layer.QUARANTINE, domain, entity),
                ),
                depends_on=(ingest_id, *_silver_dependencies(contract, silver_ids)),
                tags=tags,
            )
        )
    for model in project.gold_models:
        steps.append(_gold_step(model, env))
    return steps


def _gold_step(model: GoldModel, env: str) -> Step:
    reads = []
    depends = []
    for ref in model.inputs:
        layer = Layer.SILVER if ref.layer == "silver" else Layer.GOLD
        reads.append(build_dataset_name(env, layer, ref.domain, ref.entity))
        depends.append(build_dag_id(ref.domain, ref.entity, layer))
    tags = (
        ("contract_version", model.version),
        ("domain", model.domain),
        ("owner", model.owner),
        ("pii", "false"),
    )
    return Step(
        build_dag_id(model.domain, model.name, Layer.GOLD),
        StepKind.GOLD,
        model.domain,
        model.name,
        reads=tuple(reads),
        writes=(build_dataset_name(env, Layer.GOLD, model.domain, model.name),),
        depends_on=tuple(depends),
        tags=tags,
    )


def format_plan(steps: list[Step]) -> str:
    lines = []
    for number, step in enumerate(steps, start=1):
        lines.append(f"{number:>2}. {step.id} [{step.kind}]")
        lines.append(f"      reads:  {', '.join(step.reads)}")
        lines.append(f"      writes: {', '.join(step.writes)}")
        if step.depends_on:
            lines.append(f"      after:  {', '.join(step.depends_on)}")
    return "\n".join(lines)
