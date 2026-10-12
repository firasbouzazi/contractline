import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from contractline.adapters.compute.spark_delta_local import LocalSparkDelta
from contractline.adapters.sources.kafka import KafkaSource, UnsupportedSourceError
from contractline.core.contract import Contract
from contractline.core.naming import Layer, build_dataset_name
from contractline.core.validation import load_contract
from contractline.spark.bronze import (
    ENVELOPE_COLUMNS,
    map_debezium,
    map_kafka_json,
    write_bronze_batch,
)
from tests.spark.kafka_fixtures import build_debezium_event, build_kafka_rows

CUSTOMER = {
    "customer_id": 1,
    "email": "ada@example.com",
    "full_name": "Ada Example",
    "birth_date": 5580,
    "country_code": "FR",
    "segment": "consumer",
    "created_at": "2026-01-01T10:00:00.000000Z",
    "updated_at": "2026-01-01T10:00:00.000000Z",
}


@pytest.fixture(scope="module")
def customers(examples_dir_module: Path) -> Contract:
    return load_contract(examples_dir_module / "sales" / "customers.yaml")


@pytest.fixture(scope="module")
def orders(examples_dir_module: Path) -> Contract:
    return load_contract(examples_dir_module / "sales" / "orders.yaml")


@pytest.fixture(scope="module")
def examples_dir_module() -> Path:
    return Path(__file__).parent.parent.parent / "examples" / "contracts"


def test_debezium_insert_update_delete_snapshot(
    compute: LocalSparkDelta, customers: Contract
) -> None:
    updated = {**CUSTOMER, "segment": "business"}
    raw = build_kafka_rows(
        compute.spark,
        [
            ({"customer_id": 1}, build_debezium_event("r", CUSTOMER, lsn=10)),
            ({"customer_id": 1}, build_debezium_event("c", CUSTOMER, lsn=11)),
            ({"customer_id": 1}, build_debezium_event("u", updated, before=CUSTOMER, lsn=12)),
            ({"customer_id": 1}, build_debezium_event("d", None, before=updated, lsn=13)),
            ({"customer_id": 1}, None),  # tombstone after the delete
        ],
    )

    rows = map_debezium(raw, customers).orderBy("_sequence").collect()

    assert [r["_op"] for r in rows] == ["snapshot", "insert", "update", "delete"]
    assert [r["_sequence"] for r in rows] == [10, 11, 12, 13]
    assert json.loads(rows[2]["_payload"])["segment"] == "business"
    # Deletes carry the before image so silver can close the right version.
    assert json.loads(rows[3]["_payload"])["segment"] == "business"
    assert rows[0]["_key"] == '{"customer_id": 1}'
    assert rows[0]["_source_system"] == "kafka"
    assert rows[0]["_source_location"] == "cdc.crm.public.customers"
    assert rows[0]["_source_offset"] == "cdc.crm.public.customers/0/0"
    assert rows[0]["_source_at"] == datetime(2026, 1, 1)


def test_debezium_dates_and_micro_timestamps_become_iso_text(
    compute: LocalSparkDelta, customers: Contract
) -> None:
    event = {**CUSTOMER, "birth_date": 5580, "updated_at": 1_767_261_600_123_456}
    raw = build_kafka_rows(compute.spark, [(None, build_debezium_event("c", event))])

    payload = json.loads(map_debezium(raw, customers).first()["_payload"])  # type: ignore[index]

    assert payload["birth_date"] == "1985-04-12"
    assert payload["updated_at"] == "2026-01-01T10:00:00.123456Z"
    assert payload["created_at"] == "2026-01-01T10:00:00.000000Z"
    assert payload["customer_id"] == "1"


def test_debezium_keeps_columns_the_contract_does_not_know(
    compute: LocalSparkDelta, customers: Contract
) -> None:
    raw = build_kafka_rows(
        compute.spark, [(None, build_debezium_event("c", {**CUSTOMER, "loyalty_tier": "gold"}))]
    )

    payload = json.loads(map_debezium(raw, customers).first()["_payload"])  # type: ignore[index]

    assert payload["loyalty_tier"] == "gold"


@pytest.mark.parametrize("value", ["not json", '{"op": "x"}', '{"after": {}}'])
def test_unreadable_debezium_messages_still_land(
    compute: LocalSparkDelta, customers: Contract, value: str
) -> None:
    raw = build_kafka_rows(compute.spark, [(None, value)])

    row = map_debezium(raw, customers).first()

    assert row is not None
    assert row["_op"] == "unknown"
    assert row["_payload"] == value


def test_kafka_json_events(compute: LocalSparkDelta) -> None:
    raw = build_kafka_rows(
        compute.spark,
        [("o-1", '{"order_id": "o-1", "total_amount": 12345678901234567.89}')],
        topic="sales.orders.v1",
    )

    row = map_kafka_json(raw).first()

    assert row is not None
    assert row["_op"] == "insert"
    assert row["_key"] == "o-1"
    assert row["_sequence"] == 0
    # The payload is the message as sent: no number is re-encoded on the way in.
    assert "12345678901234567.89" in row["_payload"]


def test_kafka_source_picks_mapper_by_format(
    compute: LocalSparkDelta, customers: Contract, orders: Contract
) -> None:
    raw = build_kafka_rows(compute.spark, [(None, build_debezium_event("c", CUSTOMER))])

    assert KafkaSource.map_to_envelope(raw, customers).first()["_op"] == "insert"  # type: ignore[index]
    assert KafkaSource.map_to_envelope(raw, orders).first()["_op"] == "insert"  # type: ignore[index]


def test_kafka_source_rejects_unsupported_format(
    compute: LocalSparkDelta, orders: Contract
) -> None:
    raw = build_kafka_rows(compute.spark, [])
    avro = Contract(
        **{
            **orders.__dict__,
            "source": orders.source.__class__(**{**orders.source.__dict__, "format": "avro"}),
        }
    )

    with pytest.raises(UnsupportedSourceError, match="'avro' is not supported"):
        KafkaSource.map_to_envelope(raw, avro)


def test_write_bronze_batch_is_idempotent(
    compute: LocalSparkDelta, customers: Contract, env: str
) -> None:
    logical = build_dataset_name(env, Layer.BRONZE, "sales", "customers")
    envelope = map_debezium(
        build_kafka_rows(compute.spark, [(None, build_debezium_event("c", CUSTOMER))]), customers
    )

    write_bronze_batch(compute, logical, envelope, batch_id=0)
    write_bronze_batch(compute, logical, envelope, batch_id=0)  # a retried micro-batch
    write_bronze_batch(compute, logical, envelope, batch_id=1)

    table = compute.spark.table(compute.build_table_name(logical))
    assert tuple(table.columns) == ENVELOPE_COLUMNS
    assert sorted(r["_batch_id"] for r in table.collect()) == ["0", "1"]
    first = table.first()
    assert first is not None
    assert first["_ingested_at"] <= datetime.now(UTC).replace(tzinfo=None)
