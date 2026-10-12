"""Names generated from contracts (ADR-0007).

Nobody types dataset, topic, Dag or job names by hand: they are built here from
the contract. The names are *logical*; a compute adapter maps a logical
dataset name to what its engine supports (for example a two-level name on
local Spark without a catalog).
"""

import re
from enum import StrEnum

from contractline.core.contract import Contract


class Layer(StrEnum):
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"
    QUARANTINE = "quarantine"


class GoldKind(StrEnum):
    FACT = "fct"
    DIMENSION = "dim"
    AGGREGATE = "agg"


STANDARD_ENVIRONMENTS = ("local", "dev", "staging", "prod")
_EPHEMERAL_ENVIRONMENT = re.compile(r"^pr[1-9][0-9]*$")

RESERVED_TAGS = ("domain", "owner", "contract_version", "pii")


def is_valid_environment(env: str) -> bool:
    return env in STANDARD_ENVIRONMENTS or bool(_EPHEMERAL_ENVIRONMENT.match(env))


def build_environment_for_pull_request(number: int) -> str:
    if number < 1:
        raise ValueError(f"pull request numbers start at 1, got {number}")
    return f"pr{number}"


def _check_environment(env: str) -> None:
    if not is_valid_environment(env):
        allowed = ", ".join(STANDARD_ENVIRONMENTS)
        raise ValueError(f"unknown environment {env!r}: use one of {allowed} or pr<number>")


def build_dataset_name(env: str, layer: Layer, domain: str, entity: str) -> str:
    """``<env>_<layer>.<domain>.<entity>``, e.g. ``prod_silver.sales.customers``."""
    _check_environment(env)
    return f"{env}_{layer}.{domain}.{entity}"


def build_gold_model_name(env: str, domain: str, kind: GoldKind, name: str) -> str:
    """``<env>_gold.<domain>.<kind>_<name>``, e.g. ``prod_gold.sales.fct_orders``."""
    return build_dataset_name(env, Layer.GOLD, domain, f"{kind}_{name}")


def build_dag_id(domain: str, entity: str, layer: Layer) -> str:
    """``<domain>__<entity>__<layer>``, e.g. ``sales__customers__silver``."""
    return f"{domain}__{entity}__{layer}"


def build_job_name(env: str, domain: str, entity: str) -> str:
    """``contractline-<env>-<domain>-<entity>``, hyphenated for infrastructure names."""
    _check_environment(env)
    return "-".join(("contractline", env, domain, entity)).replace("_", "-")


def build_topic_name(domain: str, entity: str, major_version: int) -> str:
    """Business event topic: ``<domain>.<entity>.v<major>``, e.g. ``sales.orders.v1``."""
    return f"{domain}.{entity}.v{major_version}"


def build_cdc_topic_name(source_system: str, schema: str, table: str) -> str:
    """Debezium topic: ``cdc.<source_system>.<schema>.<table>``."""
    return f"cdc.{source_system}.{schema}.{table}"


def build_resource_tags(contract: Contract) -> dict[str, str]:
    """Tags every generated resource carries, followed by the contract's own tags."""
    tags = {
        "domain": contract.identity.domain,
        "owner": contract.identity.owner,
        "contract_version": contract.identity.version,
        "pii": "true" if contract.has_pii else "false",
    }
    return {**tags, **dict(contract.tags or {})}
