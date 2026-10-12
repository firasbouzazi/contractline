"""Erasure and retention across bronze, quarantine, silver (all history) and gold."""

import hashlib
import hmac
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest

from contractline.adapters.compute.spark_delta_local import LocalSparkDelta
from contractline.core.naming import Layer, build_dataset_name, build_harness_table_name
from contractline.core.project import Project, load_project
from contractline.spark.gdpr import ErasureError, erase_subject, purge_expired
from contractline.spark.gold import run_gold
from tests.spark.secrets_fixtures import HASH_KEY, FakeSecrets
from tests.spark.test_silver import Pipeline, customer

ROOT = Path(__file__).parent.parent.parent / "examples"


@pytest.fixture(scope="module")
def project() -> Project:
    return load_project([ROOT / "contracts" / "sales", ROOT / "pipelines"])


def count(compute: LocalSparkDelta, env: str, layer: Layer, entity: str, where: str) -> int:
    name = compute.build_table_name(build_dataset_name(env, layer, "sales", entity))
    return compute.spark.table(name).where(where).count()


def build_history(compute: LocalSparkDelta, project: Project, env: str) -> Pipeline:
    pipeline = Pipeline(compute, project.find_contract("sales", "customers"), env)
    pipeline.run(
        [pipeline.event("snapshot", customer(1, 0)), pipeline.event("snapshot", customer(2, 0))]
    )
    pipeline.run([pipeline.event("update", customer(1, 30, segment="business"))])
    pipeline.run([pipeline.event("snapshot", customer(3, 0, segment="unknown"))])  # quarantined
    return pipeline


def test_erasure_removes_every_trace(compute: LocalSparkDelta, project: Project, env: str) -> None:
    pipeline = build_history(compute, project, env)
    assert count(compute, env, Layer.SILVER, "customers", "customer_id = 1") == 2

    record = erase_subject(compute, project, env, "sales.customers", "1", "REQ-1", FakeSecrets())

    assert record.rows_deleted == {"bronze": 2, "quarantine": 0, "silver": 2}
    assert count(compute, env, Layer.SILVER, "customers", "customer_id = 1") == 0
    assert count(compute, env, Layer.BRONZE, "customers", "_payload LIKE '%c1@example.com%'") == 0
    assert count(compute, env, Layer.SILVER, "customers", "customer_id = 2") == 1

    audit = compute.spark.table(
        compute.build_table_name(build_harness_table_name(env, "erasures"))
    ).collect()
    assert [(r["request_id"], r["dataset"]) for r in audit] == [("REQ-1", "sales.customers")]
    # The audit record identifies the subject only by a keyed hash.
    assert audit[0]["subject_hash"] == hmac.new(HASH_KEY, b"1", hashlib.sha256).hexdigest()
    assert pipeline.history(1) == []


def test_erased_subject_cannot_come_back_on_replay(
    compute: LocalSparkDelta, project: Project, env: str
) -> None:
    pipeline = build_history(compute, project, env)
    erase_subject(compute, project, env, "sales.customers", "1", "REQ-2", FakeSecrets())

    # The source replays old messages (for example after a checkpoint reset).
    pipeline.run([pipeline.event("snapshot", customer(1, 0))])

    assert count(compute, env, Layer.SILVER, "customers", "customer_id = 1") == 0


def test_erasure_reaches_quarantine(compute: LocalSparkDelta, project: Project, env: str) -> None:
    build_history(compute, project, env)

    record = erase_subject(compute, project, env, "sales.customers", "3", "REQ-3", FakeSecrets())

    assert record.rows_deleted["quarantine"] == 1
    assert count(compute, env, Layer.QUARANTINE, "customers", "true") == 0


def test_erasure_rebuilds_gold(compute: LocalSparkDelta, project: Project, env: str) -> None:
    build_history(compute, project, env)
    orders = Pipeline(compute, project.find_contract("sales", "orders"), env)
    order: dict[str, Any] = {
        "order_id": "o-1",
        "customer_id": 1,
        "status": "paid",
        "total_amount": "5.00",
        "currency_code": "EUR",
        "ordered_at": "2026-01-01T11:30:00Z",
    }
    orders.run([orders.event("insert", order)])
    for model in project.gold_models:
        run_gold(compute, model, env, datetime(2026, 1, 1, 12))
    assert count(compute, env, Layer.GOLD, "fct_orders", "customer_segment = 'business'") == 1

    record = erase_subject(compute, project, env, "sales.customers", "1", "REQ-4", FakeSecrets())

    assert record.gold_rebuilt == ["sales.fct_orders", "sales.agg_daily_revenue"]
    assert count(compute, env, Layer.GOLD, "fct_orders", "customer_segment IS NOT NULL") == 0


def test_erasure_with_vacuum_removes_time_travel(
    compute: LocalSparkDelta, project: Project, env: str
) -> None:
    build_history(compute, project, env)
    silver = compute.build_table_name(build_dataset_name(env, Layer.SILVER, "sales", "customers"))
    version = compute.spark.sql(f"DESCRIBE HISTORY {silver}").agg({"version": "max"}).first()[0]  # type: ignore[index]

    record = erase_subject(
        compute, project, env, "sales.customers", "1", "REQ-5", FakeSecrets(), vacuum_hours=0
    )

    assert record.vacuumed
    with pytest.raises(Exception):  # noqa: B017, PT011 - Spark raises a generic error for missing files
        compute.spark.read.option("versionAsOf", version).table(silver).collect()


def test_erasure_needs_personal_data(compute: LocalSparkDelta, project: Project, env: str) -> None:
    with pytest.raises(ErasureError, match="holds no personal data"):
        erase_subject(compute, project, env, "sales.orders", "o-1", "REQ-6", FakeSecrets())


def test_retention_purge(compute: LocalSparkDelta, project: Project, env: str) -> None:
    build_history(compute, project, env)
    # Customers keep data 3 years after updated_at (2026-01-01 10:xx in the fixtures).
    nothing = purge_expired(compute, project, env, now=datetime(2027, 1, 1))
    everything = purge_expired(compute, project, env, now=datetime(2029, 6, 1))

    assert [r.rows_deleted for r in nothing if r.dataset == "sales.customers"] == [
        {"bronze": 0, "quarantine": 0, "silver": 0}
    ]
    [customers] = [r for r in everything if r.dataset == "sales.customers"]
    assert customers.rows_deleted == {"bronze": 4, "quarantine": 1, "silver": 3}
    assert count(compute, env, Layer.SILVER, "customers", "true") == 0
