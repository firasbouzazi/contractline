from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from contractline.adapters.compute.spark_delta_local import LocalSparkDelta
from contractline.core.contract import Contract, DriftPolicy, Schema
from contractline.core.validation import load_contract
from contractline.spark.standardize import SchemaDriftError, check_drift, standardize
from tests.spark.envelope_fixtures import build_envelope

EXAMPLES = Path(__file__).parent.parent.parent / "examples" / "contracts"

ORDER = {
    "order_id": "o-1",
    "customer_id": 7,
    "status": "paid",
    "total_amount": 9876543210.12,
    "currency_code": "EUR",
    "ordered_at": "2026-01-01T10:00:00Z",
}


def order_json(**changes: Any) -> str:
    """Hand-written JSON so large decimals keep every digit."""
    values = {**ORDER, **changes}
    parts = []
    for key, value in values.items():
        if value is None:
            parts.append(f'"{key}": null')
        elif isinstance(value, str):
            parts.append(f'"{key}": "{value}"')
        else:
            parts.append(f'"{key}": {value}')
    return "{" + ", ".join(parts) + "}"


@pytest.fixture(scope="module")
def orders() -> Contract:
    return load_contract(EXAMPLES / "sales" / "orders.yaml")


@pytest.fixture(scope="module")
def customers() -> Contract:
    return load_contract(EXAMPLES / "sales" / "customers.yaml")


def with_drift(contract: Contract, policy: DriftPolicy) -> Contract:
    schema = Schema(contract.schema.columns, policy, contract.schema.primary_key)
    return Contract(**{**contract.__dict__, "schema": schema})


def test_decimal_overflow_is_an_error(compute: LocalSparkDelta, orders: Contract) -> None:
    bronze = build_envelope(compute.spark, [{"payload": order_json(total_amount=12345678901234.5)}])

    row = standardize(bronze, orders).first()

    assert row is not None
    assert row["_errors"] == [
        "schema: total_amount value '1.23456789012345E13' is not a valid decimal(12,2)"
    ]


def test_parses_typed_values_exactly(compute: LocalSparkDelta, orders: Contract) -> None:
    bronze = build_envelope(compute.spark, [{"payload": order_json()}])

    row = standardize(bronze, orders).first()

    assert row is not None
    assert row["_errors"] == []
    assert row["customer_id"] == 7
    assert row["total_amount"] == Decimal("9876543210.12")
    assert row["ordered_at"] == datetime(2026, 1, 1, 10, 0)
    assert row["_op"] == "insert"


def test_string_encoded_values_are_accepted(compute: LocalSparkDelta, customers: Contract) -> None:
    payload = {
        "customer_id": "1",
        "email": "a@example.com",
        "birth_date": "1985-04-12",
        "country_code": "FR",
        "segment": "consumer",
        "created_at": "2026-01-01T10:00:00.000000Z",
        "updated_at": "2026-01-01T11:00:00+01:00",
    }
    bronze = build_envelope(compute.spark, [{"payload": payload}])

    row = standardize(bronze, customers).first()

    assert row is not None
    assert row["_errors"] == []
    assert row["customer_id"] == 1
    assert row["birth_date"] == date(1985, 4, 12)
    assert row["updated_at"] == datetime(2026, 1, 1, 10, 0)


@pytest.mark.parametrize(
    ("payload", "op", "expected"),
    [
        (order_json(customer_id="seven"), "insert", ["schema: customer_id value"]),
        (order_json(ordered_at="yesterday"), "insert", ["schema: ordered_at value 'yesterday'"]),
        (order_json(total_amount="lots"), "insert", ["schema: total_amount value 'lots'"]),
        (order_json(status=None), "insert", ["schema: status is required"]),
        ("[1, 2]", "insert", ["schema: payload is not a JSON object"]),
        ("garbage", "unknown", ["schema: unreadable source record"]),
        ('{"order_id": "o-1"}', "delete", []),
    ],
)
def test_collects_errors(
    compute: LocalSparkDelta, orders: Contract, payload: str, op: str, expected: list[str]
) -> None:
    bronze = build_envelope(compute.spark, [{"payload": payload, "op": op}])

    row = standardize(bronze, orders).first()

    assert row is not None
    assert len(row["_errors"]) == len(expected), row["_errors"]
    for error, prefix in zip(row["_errors"], expected, strict=True):
        assert error.startswith(prefix)


def test_duplicate_source_offsets_are_dropped(compute: LocalSparkDelta, orders: Contract) -> None:
    record = {"payload": order_json(), "offset": "t/0/5"}
    bronze = build_envelope(compute.spark, [record, record])

    assert standardize(bronze, orders).count() == 1


def test_drift_quarantine_flags_undeclared_columns(
    compute: LocalSparkDelta, orders: Contract
) -> None:
    contract = with_drift(orders, DriftPolicy.QUARANTINE)
    bronze = build_envelope(compute.spark, [{"payload": order_json(coupon="WELCOME")}])

    row = standardize(bronze, contract).first()

    assert row is not None
    assert row["_errors"] == ["schema: undeclared columns coupon"]


def test_drift_add_columns_ignores_undeclared_columns(
    compute: LocalSparkDelta, orders: Contract
) -> None:
    bronze = build_envelope(compute.spark, [{"payload": order_json(coupon="WELCOME")}])

    row = standardize(bronze, orders).first()

    assert row is not None
    assert row["_errors"] == []
    assert check_drift(bronze, orders) == ["coupon"]


def test_drift_fail_raises(compute: LocalSparkDelta, orders: Contract) -> None:
    contract = with_drift(orders, DriftPolicy.FAIL)
    bronze = build_envelope(compute.spark, [{"payload": order_json(coupon="WELCOME")}])

    with pytest.raises(SchemaDriftError, match="coupon"):
        check_drift(bronze, contract)
