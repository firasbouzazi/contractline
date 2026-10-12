from pathlib import Path

import pytest

from contractline.core.naming import (
    GoldKind,
    Layer,
    build_cdc_topic_name,
    build_dag_id,
    build_dataset_name,
    build_environment_for_pull_request,
    build_gold_model_name,
    build_job_name,
    build_resource_tags,
    build_topic_name,
    is_valid_environment,
)
from contractline.core.validation import load_contract


@pytest.mark.parametrize(
    ("env", "layer", "expected"),
    [
        ("prod", Layer.SILVER, "prod_silver.sales.customers"),
        ("pr142", Layer.SILVER, "pr142_silver.sales.customers"),
        ("dev", Layer.QUARANTINE, "dev_quarantine.sales.customers"),
        ("local", Layer.BRONZE, "local_bronze.sales.customers"),
    ],
)
def test_build_dataset_name(env: str, layer: Layer, expected: str) -> None:
    assert build_dataset_name(env, layer, "sales", "customers") == expected


@pytest.mark.parametrize("env", ["production", "pr0", "pr", "PR1", "dev_1", ""])
def test_unknown_environments_are_rejected(env: str) -> None:
    assert not is_valid_environment(env)
    with pytest.raises(ValueError, match="unknown environment"):
        build_dataset_name(env, Layer.SILVER, "sales", "customers")


def test_build_environment_for_pull_request() -> None:
    assert build_environment_for_pull_request(142) == "pr142"
    with pytest.raises(ValueError, match="start at 1"):
        build_environment_for_pull_request(0)


def test_build_gold_model_name() -> None:
    assert build_gold_model_name("prod", "sales", GoldKind.FACT, "orders") == (
        "prod_gold.sales.fct_orders"
    )


def test_build_dag_id() -> None:
    assert build_dag_id("sales", "customers", Layer.SILVER) == "sales__customers__silver"


def test_build_job_name_uses_hyphens() -> None:
    assert build_job_name("prod", "finance", "exchange_rates") == (
        "contractline-prod-finance-exchange-rates"
    )


def test_build_topic_names() -> None:
    assert build_topic_name("sales", "orders", 1) == "sales.orders.v1"
    assert build_cdc_topic_name("crm", "public", "customers") == "cdc.crm.public.customers"


def test_build_resource_tags(examples_dir: Path) -> None:
    contract = load_contract(examples_dir / "sales" / "customers.yaml")

    assert build_resource_tags(contract) == {
        "domain": "sales",
        "owner": "sales-data@example.com",
        "contract_version": "1.0.0",
        "pii": "true",
        "cost_center": "sales",
    }
