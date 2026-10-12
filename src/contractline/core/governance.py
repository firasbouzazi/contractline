"""Governance plan: the tags and access rules every table must carry, derived
from contracts (ADR-0019). Platform adapters apply it: Delta table properties
locally, Unity Catalog tags and grants on Databricks."""

from dataclasses import dataclass, field

from contractline.core.contract import MaskingStrategy, PiiClass
from contractline.core.naming import Layer, build_dataset_name, build_resource_tags
from contractline.core.project import Project

RAW_LAYERS = (Layer.BRONZE, Layer.QUARANTINE)


@dataclass(frozen=True)
class TablePolicy:
    table: str
    """Logical name."""
    layer: Layer
    tags: dict[str, str]
    column_tags: dict[str, dict[str, str]] = field(default_factory=dict)
    readers: tuple[str, ...] = ()
    """Groups allowed to read. Raw layers holding PII: only the domain's PII readers."""


def build_pii_reader_group(domain: str) -> str:
    return f"{domain}_pii_readers"


def build_reader_group(domain: str) -> str:
    return f"{domain}_readers"


def build_governance_plan(project: Project, env: str) -> list[TablePolicy]:
    policies: list[TablePolicy] = []
    for contract in project.contracts:
        domain, entity = contract.identity.domain, contract.identity.name
        tags = build_resource_tags(contract)
        masking = contract.privacy.masking if contract.privacy else {}
        pii_columns = {c.name: c for c in contract.schema.columns if c.pii is not PiiClass.NONE}
        for layer in (Layer.BRONZE, Layer.QUARANTINE, Layer.SILVER):
            raw = layer in RAW_LAYERS
            column_tags = {}
            if not raw:
                for name, column in pii_columns.items():
                    strategy = masking[name].strategy if name in masking else MaskingStrategy.KEEP
                    if strategy is MaskingStrategy.DROP:
                        continue
                    column_tags[name] = {"pii": str(column.pii), "masking": str(strategy)}
            readers = (
                (build_pii_reader_group(domain),)
                if raw and contract.has_pii
                else (build_pii_reader_group(domain), build_reader_group(domain))
            )
            policies.append(
                TablePolicy(
                    table=build_dataset_name(env, layer, domain, entity),
                    layer=layer,
                    tags={
                        **tags,
                        "layer": str(layer),
                        "raw_pii": str(raw and contract.has_pii).lower(),
                    },
                    column_tags=column_tags,
                    readers=readers,
                )
            )
    for model in project.gold_models:
        policies.append(
            TablePolicy(
                table=build_dataset_name(env, Layer.GOLD, model.domain, model.name),
                layer=Layer.GOLD,
                tags={
                    "domain": model.domain,
                    "owner": model.owner,
                    "contract_version": model.version,
                    "pii": "false",
                    "layer": "gold",
                    "raw_pii": "false",
                },
                readers=(build_reader_group(model.domain), build_pii_reader_group(model.domain)),
            )
        )
    return policies
