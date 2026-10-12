import json
from pathlib import Path
from typing import Any

import pytest

from contractline.core.contract import Contract
from contractline.core.validation import load_contract
from contractline.testing.synthetic import SyntheticData, generate

EXAMPLES = Path(__file__).parent.parent.parent / "examples" / "contracts"


@pytest.fixture(scope="module")
def customers() -> Contract:
    return load_contract(EXAMPLES / "sales" / "customers.yaml")


@pytest.fixture(scope="module")
def orders() -> Contract:
    return load_contract(EXAMPLES / "sales" / "orders.yaml")


def all_values(data: SyntheticData) -> list[dict[str, Any]]:
    return [json.loads(m.value) for batch in data.batches for m in batch if m.value]


def test_same_seed_same_data(customers: Contract) -> None:
    assert generate(customers, seed=3).batches == generate(customers, seed=3).batches
    assert generate(customers, seed=3).batches != generate(customers, seed=4).batches


def test_change_stream_has_every_kind_of_change(customers: Contract) -> None:
    ops = {v["op"] for v in all_values(generate(customers))}

    assert ops == {"r", "u", "d", "c"}


def test_pii_is_synthetic_and_recorded(customers: Contract) -> None:
    data = generate(customers)

    emails = {v for v in data.raw_pii if "@" in v}
    assert emails
    assert all(e.endswith("@example.invalid") for e in emails)
    assert any(v.startswith("Synthetic Person") for v in data.raw_pii)


def test_debezium_encodes_dates_as_days(customers: Contract) -> None:
    first = all_values(generate(customers))[0]

    assert isinstance(first["after"]["birth_date"], int | None)


def test_deleted_keys_and_invalid_records(customers: Contract) -> None:
    data = generate(customers, entities=20)

    assert data.deleted_keys == {(7,)}  # 14 is deleted and inserted again
    assert data.invalid_count == 2  # a wrong type and a value outside accepted_values


def test_some_changes_arrive_late(customers: Contract) -> None:
    data = generate(customers)
    lsns = [v["source"]["lsn"] for v in all_values(data) if "source" in v]

    assert lsns != sorted(lsns)
    assert len(data.batches) == 3


def test_references_point_at_generated_keys(customers: Contract, orders: Contract) -> None:
    parent = generate(customers)
    child = generate(
        orders, references={"sales.customers.customer_id": parent.key_values["customer_id"]}
    )

    ids = {v["customer_id"] for v in all_values(child) if isinstance(v["customer_id"], int)}
    assert ids <= set(parent.key_values["customer_id"]) | {8888, 9999}


def test_drift_column_only_when_policy_tolerates_it(customers: Contract, orders: Contract) -> None:
    def has_drift(contract: Contract) -> bool:
        return any(
            "synthetic_new_column" in (m.value or "") for b in generate(contract).batches for m in b
        )

    assert not has_drift(customers)  # drift_policy: quarantine
    assert has_drift(orders)  # drift_policy: add_columns
