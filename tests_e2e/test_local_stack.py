"""End to end on the Docker stack: Postgres -> Debezium -> Kafka -> bronze ->
silver -> gold. Run with ``make e2e`` (it starts the stack and sends the data).

Kept out of the default test run: it needs Docker, Kafka and a few minutes.
"""

import os
from pathlib import Path

import pytest
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from contractline.adapters.compute.spark_delta_local import LocalSparkDelta
from contractline.core.naming import Layer, build_dataset_name
from contractline.core.project import load_project
from contractline.runner import RunSummary, build_source_factory, run_project
from contractline.spark.checks import find_scd2_violations
from contractline.testing.generated import EphemeralSecrets, find_raw_pii

ROOT = Path(__file__).parent.parent
BOOTSTRAP = os.environ.get("CONTRACTLINE_E2E_KAFKA", "localhost:29092")
ENV = "local"


@pytest.fixture(scope="module")
def compute(tmp_path_factory: pytest.TempPathFactory) -> LocalSparkDelta:
    return LocalSparkDelta(tmp_path_factory.mktemp("e2e"))


@pytest.fixture(scope="module")
def summary(compute: LocalSparkDelta) -> RunSummary:
    project = load_project([ROOT / "examples"])
    return run_project(
        compute, project, ENV, build_source_factory(BOOTSTRAP), EphemeralSecrets(), print
    )


def table(compute: LocalSparkDelta, layer: Layer, entity: str) -> DataFrame:
    logical = build_dataset_name(ENV, layer, "sales", entity)
    return compute.spark.table(compute.build_table_name(logical))


def test_cdc_reaches_silver_with_history(compute: LocalSparkDelta, summary: RunSummary) -> None:
    customers = table(compute, Layer.SILVER, "customers")

    assert customers.where(F.col("_is_current")).count() >= 2
    # infra/e2e-data.sh moves customer 2 to "partner": two versions, one current.
    versions = customers.where("customer_id = 2").orderBy("_valid_from").collect()
    assert [v["segment"] for v in versions][-2:] == ["business", "partner"]
    # Customer 3 is deleted at the source: no current row any more.
    assert customers.where("customer_id = 3 AND _is_current").count() == 0
    assert find_scd2_violations(customers, ["customer_id"]) == []


def test_no_raw_pii_in_silver_or_gold(compute: LocalSparkDelta, summary: RunSummary) -> None:
    raw = {"ada@example.com", "bob@example.com", "chloe@example.com", "Ada Example"}

    for layer, entity in [(Layer.SILVER, "customers"), (Layer.GOLD, "fct_orders")]:
        assert find_raw_pii(table(compute, layer, entity), raw) == []


def test_bad_order_is_quarantined(compute: LocalSparkDelta, summary: RunSummary) -> None:
    quarantine = table(compute, Layer.QUARANTINE, "orders").collect()

    assert [r["_reasons"] for r in quarantine] == [["quarantine: accepted_values(status) failed"]]


def test_gold_is_built(compute: LocalSparkDelta, summary: RunSummary) -> None:
    fct = table(compute, Layer.GOLD, "fct_orders").collect()
    revenue = table(compute, Layer.GOLD, "agg_daily_revenue").collect()

    assert sorted(r["order_id"] for r in fct) == ["e2e-1", "e2e-2"]
    assert {r["customer_segment"] for r in fct} == {"consumer", "partner"}
    assert sum(r["order_count"] for r in revenue) == 2


def test_second_run_is_a_no_op(compute: LocalSparkDelta, summary: RunSummary) -> None:
    project = load_project([ROOT / "examples"])
    before = table(compute, Layer.SILVER, "customers").count()

    again = run_project(
        compute, project, ENV, build_source_factory(BOOTSTRAP), EphemeralSecrets(), print
    )

    assert sum(again.bronze_batches.values()) == 0
    assert table(compute, Layer.SILVER, "customers").count() == before
