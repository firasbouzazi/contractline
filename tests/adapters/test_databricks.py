"""Databricks adapter pieces that do not need a workspace."""

from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import yaml

from contractline.adapters.compute.databricks import DatabricksCompute
from contractline.adapters.compute.spark_delta_local import LocalSparkDelta
from contractline.adapters.governance.local import TAG_PREFIX, apply_table_tags
from contractline.adapters.governance.unity_catalog import build_statements
from contractline.adapters.orchestration.databricks_bundle import (
    HEADER,
    build_bundle_resources,
    to_quartz,
)
from contractline.adapters.secrets_databricks import DatabricksSecretProvider
from contractline.core.governance import build_governance_plan
from contractline.core.naming import Layer, build_dataset_name
from contractline.core.plan import build_plan
from contractline.core.project import load_project
from contractline.core.secrets import MissingSecretError

ROOT = Path(__file__).parent.parent.parent
EXAMPLES = ROOT / "examples"


def test_unity_catalog_names_are_the_logical_names() -> None:
    compute = DatabricksCompute("prod")

    assert compute.build_table_name("prod_silver.sales.customers") == (
        "`prod_silver`.`sales`.`customers`"
    )
    assert compute.build_checkpoint_path("prod_bronze.sales.customers") == (
        "/Volumes/prod_harness/contractline/checkpoints/prod_bronze.sales.customers"
    )


@pytest.mark.parametrize(
    ("cron", "quartz"),
    [("*/15 * * * *", "0 0/15 * * * ?"), ("0 0 * * *", "0 0 0 * * ?")],
)
def test_quartz(cron: str, quartz: str) -> None:
    assert to_quartz(cron) == quartz


def test_quartz_rejects_unknown_cron() -> None:
    with pytest.raises(ValueError, match="no Quartz equivalent"):
        to_quartz("5 4 * * 1")


@pytest.fixture(scope="module")
def bundle() -> dict[str, Any]:
    text = build_bundle_resources(build_plan(load_project([EXAMPLES]), "dev"))
    assert text.startswith(HEADER)
    loaded: dict[str, Any] = yaml.safe_load(text)
    jobs: dict[str, Any] = loaded["resources"]["jobs"]
    return jobs


def test_one_job_per_step(bundle: dict[str, Any]) -> None:
    assert len(bundle) == 10
    task = bundle["sales__customers__silver"]["tasks"][0]["python_wheel_task"]
    assert task["parameters"][:6] == [
        "run-step",
        "sales__customers__silver",
        "--env",
        "${bundle.target}",
        "--platform",
        "databricks",
    ]


def test_jobs_are_scheduled_or_triggered_like_the_dags(bundle: dict[str, Any]) -> None:
    assert bundle["sales__customers__bronze"]["schedule"]["quartz_cron_expression"] == (
        "0 0/30 * * * ?"
    )
    trigger = bundle["sales__fct_orders__gold"]["trigger"]["table_update"]
    assert trigger["condition"] == "ANY_UPDATED"
    assert trigger["table_names"] == [
        "${bundle.target}_silver.sales.orders",
        "${bundle.target}_silver.sales.customers",
    ]


def test_committed_bundle_is_up_to_date() -> None:
    generated = build_bundle_resources(build_plan(load_project([EXAMPLES]), "dev"))

    assert (EXAMPLES / "databricks" / "resources" / "jobs.yml").read_text() == generated


def test_governance_restricts_raw_pii() -> None:
    policies = {p.table: p for p in build_governance_plan(load_project([EXAMPLES]), "prod")}

    bronze = policies["prod_bronze.sales.customers"]
    silver = policies["prod_silver.sales.customers"]
    assert bronze.readers == ("sales_pii_readers",)
    assert bronze.tags["raw_pii"] == "true"
    assert silver.readers == ("sales_pii_readers", "sales_readers")
    assert silver.column_tags == {
        "email": {"pii": "direct", "masking": "hash"},
        "birth_date": {"pii": "indirect", "masking": "generalize"},
    }  # full_name is dropped before silver, so it has no column there
    assert policies["prod_gold.sales.fct_orders"].tags["pii"] == "false"


def test_unity_catalog_statements() -> None:
    statements = build_statements(build_governance_plan(load_project([EXAMPLES]), "prod"))

    assert (
        "ALTER TABLE `prod_silver`.`sales`.`customers` ALTER COLUMN `email` "
        "SET TAGS ('masking' = 'hash', 'pii' = 'direct');"
    ) in statements
    assert "GRANT SELECT ON TABLE `prod_bronze`.`sales`.`customers` TO `sales_pii_readers`;" in (
        statements
    )
    assert "GRANT SELECT ON TABLE `prod_bronze`.`sales`.`customers` TO `sales_readers`;" not in (
        statements
    )


def test_local_governance_tags_tables(compute: LocalSparkDelta, env: str) -> None:
    project = load_project([EXAMPLES])
    logical = build_dataset_name(env, Layer.SILVER, "sales", "customers")
    compute.ensure_schema(logical)
    compute.spark.createDataFrame([("x",)], "email string").write.format("delta").saveAsTable(
        compute.build_table_name(logical)
    )

    tagged = apply_table_tags(compute, build_governance_plan(project, env))

    assert tagged == [logical]
    properties = {
        r["key"]: r["value"]
        for r in compute.spark.sql(
            f"SHOW TBLPROPERTIES {compute.build_table_name(logical)}"
        ).collect()
    }
    assert properties[f"{TAG_PREFIX}domain"] == "sales"
    assert properties[f"{TAG_PREFIX}email.pii"] == "direct"


def test_databricks_secret_provider() -> None:
    def get(scope: str, key: str) -> str:
        if key == "pii_hash":
            return "a" * 32
        raise KeyError(key)

    fake = SimpleNamespace(secrets=SimpleNamespace(get=get))
    provider = DatabricksSecretProvider("contractline", dbutils=fake)

    assert provider.get_secret("pii_hash") == b"a" * 32
    with pytest.raises(MissingSecretError, match="'pii_token' not found in scope 'contractline'"):
        provider.get_secret("pii_token")
