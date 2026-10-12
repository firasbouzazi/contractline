"""Silver end to end on envelopes: masking, SCD2, quarantine, idempotency."""

import hashlib
import hmac
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from pyspark.sql import DataFrame

from contractline.adapters.compute.spark_delta_local import LocalSparkDelta
from contractline.core.contract import Contract
from contractline.core.naming import Layer, build_dataset_name, build_harness_table_name
from contractline.core.validation import load_contract
from contractline.spark.bronze import write_bronze_batch
from contractline.spark.checks import find_duplicate_keys, find_scd2_violations
from contractline.spark.quality import QualityGateError
from contractline.spark.silver import process_batch
from tests.spark.envelope_fixtures import build_envelope
from tests.spark.secrets_fixtures import HASH_KEY, FakeSecrets

EXAMPLES = Path(__file__).parent.parent.parent / "examples" / "contracts"
T0 = datetime(2026, 1, 1, 10, 0)
NOW = datetime(2026, 1, 1, 12, 0)


def at(minutes: int) -> datetime:
    return T0 + timedelta(minutes=minutes)


def customer(customer_id: int, minutes: int, **changes: Any) -> dict[str, Any]:
    stamp = at(minutes).strftime("%Y-%m-%dT%H:%M:%SZ")
    return {
        "customer_id": customer_id,
        "email": f"c{customer_id}@example.com",
        "full_name": f"Customer {customer_id}",
        "birth_date": "1985-04-12",
        "country_code": "FR",
        "segment": "consumer",
        "created_at": "2025-01-01T00:00:00Z",
        "updated_at": stamp,
        **changes,
    }


class Pipeline:
    """Feeds batches of change events through process_batch for one dataset."""

    def __init__(self, compute: LocalSparkDelta, contract: Contract, env: str) -> None:
        self.compute, self.contract, self.env = compute, contract, env
        self.offset = 0
        self.batches = 0

    def event(self, op: str, payload: dict[str, Any], **extra: Any) -> dict[str, Any]:
        self.offset += 1
        return {
            "payload": payload,
            "op": op,
            "sequence": self.offset,
            "offset": f"t/0/{self.offset}",
            "source_at": extra.pop("source_at", at(self.offset)),
            **extra,
        }

    def run(self, records: list[dict[str, Any]]) -> Any:
        """Land the records in bronze, then process them, as ``run_silver`` does."""
        self.batches += 1
        batch = build_envelope(self.compute.spark, records)
        bronze = build_dataset_name(self.env, Layer.BRONZE, "sales", self.contract.identity.name)
        write_bronze_batch(
            self.compute, bronze, batch.drop("_ingested_at", "_batch_id"), self.batches
        )
        return process_batch(
            self.compute, self.contract, self.env, batch, str(self.batches), FakeSecrets(), NOW
        )

    def table(self, layer: Layer) -> DataFrame:
        logical = build_dataset_name(self.env, layer, "sales", self.contract.identity.name)
        return self.compute.spark.table(self.compute.build_table_name(logical))

    def history(self, customer_id: int) -> list[dict[str, Any]]:
        rows = self.table(Layer.SILVER).where(f"customer_id = {customer_id}").orderBy("_valid_from")
        return [r.asDict() for r in rows.collect()]


@pytest.fixture(scope="module")
def customers() -> Contract:
    return load_contract(EXAMPLES / "sales" / "customers.yaml")


@pytest.fixture(scope="module")
def orders() -> Contract:
    return load_contract(EXAMPLES / "sales" / "orders.yaml")


@pytest.fixture
def pipeline(compute: LocalSparkDelta, customers: Contract, env: str) -> Pipeline:
    return Pipeline(compute, customers, env)


def test_snapshot_lands_masked_current_rows(pipeline: Pipeline) -> None:
    report = pipeline.run(
        [pipeline.event("snapshot", customer(1, 0)), pipeline.event("snapshot", customer(2, 0))]
    )

    [row] = pipeline.history(1)
    assert report.records_in == 2
    assert report.records_valid == 2
    assert row["email"] == hmac.new(HASH_KEY, b"c1@example.com", hashlib.sha256).hexdigest()
    assert "full_name" not in row  # dropped by its masking strategy
    assert row["birth_date"] == date(1985, 1, 1)  # generalized to the year
    assert row["_valid_from"] == at(0)
    assert row["_valid_to"] is None
    assert row["_is_current"] is True


def test_tracked_change_opens_a_new_version(pipeline: Pipeline) -> None:
    pipeline.run([pipeline.event("snapshot", customer(1, 0))])
    pipeline.run([pipeline.event("update", customer(1, 30, segment="business"))])

    versions = pipeline.history(1)
    assert [v["segment"] for v in versions] == ["consumer", "business"]
    assert versions[0]["_valid_to"] == at(30)
    assert [v["_is_current"] for v in versions] == [False, True]


def test_untracked_change_updates_the_current_version(pipeline: Pipeline) -> None:
    pipeline.run([pipeline.event("snapshot", customer(1, 0))])
    pipeline.run([pipeline.event("update", customer(1, 30, birth_date="1990-06-01"))])

    [version] = pipeline.history(1)
    assert version["birth_date"] == date(1990, 1, 1)
    assert version["_valid_from"] == at(0)


def test_late_out_of_order_update_is_slotted_in(pipeline: Pipeline) -> None:
    pipeline.run([pipeline.event("snapshot", customer(1, 0))])
    pipeline.run([pipeline.event("update", customer(1, 60, segment="partner"))])
    pipeline.run([pipeline.event("update", customer(1, 30, segment="business"))])  # late

    versions = pipeline.history(1)
    assert [(v["segment"], v["_valid_from"], v["_valid_to"]) for v in versions] == [
        ("consumer", at(0), at(30)),
        ("business", at(30), at(60)),
        ("partner", at(60), None),
    ]
    assert find_scd2_violations(pipeline.table(Layer.SILVER), ["customer_id"]) == []


def test_delete_closes_history_and_reinsert_reopens_it(pipeline: Pipeline) -> None:
    pipeline.run([pipeline.event("snapshot", customer(1, 0))])
    pipeline.run([pipeline.event("delete", customer(1, 0), source_at=at(40))])

    [closed] = pipeline.history(1)
    assert closed["_valid_to"] == at(40)
    assert closed["_is_current"] is False

    pipeline.run([pipeline.event("insert", customer(1, 50))])
    versions = pipeline.history(1)
    assert [(v["_valid_from"], v["_valid_to"], v["_is_current"]) for v in versions] == [
        (at(0), at(40), False),
        (at(50), None, True),
    ]


def test_same_batch_twice_changes_nothing(pipeline: Pipeline) -> None:
    batch = [
        pipeline.event("snapshot", customer(1, 0)),
        pipeline.event("update", customer(1, 30, segment="business")),
        pipeline.event("delete", customer(1, 30), source_at=at(45)),
        pipeline.event("insert", customer(2, 10)),
    ]
    pipeline.run(batch)
    first = sorted(map(str, pipeline.table(Layer.SILVER).collect()))

    pipeline.run(batch)

    assert sorted(map(str, pipeline.table(Layer.SILVER).collect())) == first


def test_bad_records_are_quarantined_not_published(pipeline: Pipeline) -> None:
    report = pipeline.run(
        [
            pipeline.event("snapshot", customer(1, 0)),
            pipeline.event("snapshot", customer(2, 0, segment="vip")),  # not an accepted value
            pipeline.event("snapshot", {**customer(3, 0), "customer_id": "three"}),  # bad type
        ]
    )

    assert report.records_quarantined == 2
    assert [r["customer_id"] for r in pipeline.table(Layer.SILVER).collect()] == [1]
    quarantine = {
        r["_source_offset"]: r["_reasons"] for r in pipeline.table(Layer.QUARANTINE).collect()
    }
    assert quarantine["t/0/2"] == ["quarantine: accepted_values(segment) failed"]
    assert quarantine["t/0/3"][0].startswith("schema: customer_id value 'three'")
    # The quarantine keeps the raw payload so the record can be fixed and replayed.
    payload = pipeline.table(Layer.QUARANTINE).first()["_payload"]  # type: ignore[index]
    assert "@example.com" in payload


def test_run_records_are_kept(pipeline: Pipeline) -> None:
    pipeline.run([pipeline.event("snapshot", customer(1, 0))])

    runs = pipeline.compute.spark.table(
        pipeline.compute.build_table_name(build_harness_table_name(pipeline.env, "runs"))
    ).collect()

    assert [(r["dataset"], r["records_in"], r["contract_version"]) for r in runs] == [
        ("sales.customers", 1, "1.0.0")
    ]


def test_warning_dataset_rule_lets_the_batch_through(pipeline: Pipeline) -> None:
    pipeline.run([pipeline.event("snapshot", customer(1, 0))])

    # 10 records after 1 is far beyond max_change_ratio 0.5; the rule only warns.
    report = pipeline.run([pipeline.event("update", customer(i, 5)) for i in range(2, 12)])

    assert [f.rule for f in report.findings] == ["row_count_anomaly"]
    assert pipeline.table(Layer.SILVER).count() == 11


def test_quality_gate_error_writes_nothing(
    compute: LocalSparkDelta, customers: Contract, env: str
) -> None:
    rule = customers.quality[0].__class__(
        customers.quality[0].type, customers.quality[0].severity, ("segment",), {}
    )  # unique(segment), block: two consumers in one dataset is a duplicate
    contract = Contract(**{**customers.__dict__, "quality": (rule,)})
    pipeline = Pipeline(compute, contract, env)

    with pytest.raises(QualityGateError, match=r"unique\(segment\): duplicate values"):
        pipeline.run(
            [pipeline.event("snapshot", customer(1, 0)), pipeline.event("snapshot", customer(2, 0))]
        )

    logical = build_dataset_name(env, Layer.SILVER, "sales", "customers")
    assert not compute.spark.catalog.tableExists(compute.build_table_name(logical))


def test_append_history_for_events(compute: LocalSparkDelta, orders: Contract, env: str) -> None:
    pipeline = Pipeline(compute, orders, env)
    order = {
        "order_id": "o-1",
        "customer_id": 1,
        "status": "paid",
        "total_amount": "10.50",
        "currency_code": "EUR",
        "ordered_at": "2026-01-01T11:30:00Z",
    }
    first = [
        pipeline.event("insert", order),
        pipeline.event("insert", {**order, "order_id": "o-2"}),
    ]
    pipeline.run(first)
    report = pipeline.run([*first, pipeline.event("insert", order)])  # replay + a duplicate id

    rows = pipeline.table(Layer.SILVER).collect()
    assert sorted(r["order_id"] for r in rows) == ["o-1", "o-2"]
    assert find_duplicate_keys(pipeline.table(Layer.SILVER), ["order_id"]) == []
    assert report.records_in == 1  # the replayed records are skipped, not re-checked
    assert report.records_quarantined == 1
