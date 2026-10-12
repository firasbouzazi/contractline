from datetime import datetime
from pathlib import Path
from typing import Any

import pytest
from pyspark.sql import DataFrame

from contractline.adapters.compute.spark_delta_local import LocalSparkDelta
from contractline.core.contract import Contract, QualityRule, RuleType, Severity
from contractline.core.validation import load_contract
from contractline.spark.quality import (
    DatasetFinding,
    QualityGateError,
    evaluate_dataset_rules,
    evaluate_row_rules,
    raise_if_blocked,
    raise_if_dataset_blocked,
    summarize_warnings,
)
from contractline.spark.standardize import standardize
from tests.spark.envelope_fixtures import build_envelope

EXAMPLES = Path(__file__).parent.parent.parent / "examples" / "contracts"
NOW = datetime(2026, 1, 1, 12, 0)
ORDER = {
    "order_id": "o-1",
    "customer_id": 7,
    "status": "paid",
    "total_amount": 10.5,
    "currency_code": "EUR",
    "ordered_at": "2026-01-01T11:30:00Z",
}


@pytest.fixture(scope="module")
def orders() -> Contract:
    return load_contract(EXAMPLES / "sales" / "orders.yaml")


@pytest.fixture(scope="module")
def customers() -> Contract:
    return load_contract(EXAMPLES / "sales" / "customers.yaml")


def check(
    compute: LocalSparkDelta,
    contract: Contract,
    records: list[dict[str, Any]],
    references: dict[str, DataFrame] | None = None,
    existing: DataFrame | None = None,
) -> list[dict[str, Any]]:
    refs = references or {}

    def load(domain: str, entity: str, column: str) -> DataFrame | None:
        return refs.get(f"{domain}.{entity}.{column}")

    standardized = standardize(build_envelope(compute.spark, records), contract)
    checked = evaluate_row_rules(standardized, contract, load, existing)
    return [r.asDict() for r in checked.orderBy("_sequence").collect()]


def customer_ids(compute: LocalSparkDelta, *ids: int) -> DataFrame:
    return compute.spark.createDataFrame([(i,) for i in ids], "customer_id long")


def test_valid_order_passes(compute: LocalSparkDelta, orders: Contract) -> None:
    [row] = check(
        compute,
        orders,
        [{"payload": ORDER}],
        {"sales.customers.customer_id": customer_ids(compute, 7)},
    )

    assert row["_quarantine_reasons"] == []
    assert row["_warnings"] == []
    assert row["_blocking"] == []


@pytest.mark.parametrize(
    ("changes", "reason"),
    [
        ({"status": "lost"}, "quarantine: accepted_values(status) failed"),
        ({"total_amount": -1}, "quarantine: range(total_amount) failed"),
        ({"customer_id": None}, "quarantine: not_null(customer_id) failed"),
    ],
)
def test_quarantine_rules(
    compute: LocalSparkDelta, orders: Contract, changes: dict[str, Any], reason: str
) -> None:
    refs = {"sales.customers.customer_id": customer_ids(compute, 7)}

    [row] = check(compute, orders, [{"payload": {**ORDER, **changes}}], refs)

    assert reason in row["_quarantine_reasons"]


def test_schema_errors_are_quarantine_reasons(compute: LocalSparkDelta, orders: Contract) -> None:
    [row] = check(compute, orders, [{"payload": {**ORDER, "total_amount": "lots"}}])

    assert row["_quarantine_reasons"][0].startswith("schema: total_amount value 'lots'")


def test_referential_integrity_warns(compute: LocalSparkDelta, orders: Contract) -> None:
    refs = {"sales.customers.customer_id": customer_ids(compute, 1, 2)}

    [row] = check(compute, orders, [{"payload": ORDER}], refs)

    assert row["_warnings"] == ["warn: referential_integrity(customer_id) failed"]
    assert row["_quarantine_reasons"] == []


def test_referential_integrity_fails_when_reference_is_missing(
    compute: LocalSparkDelta, orders: Contract
) -> None:
    [row] = check(compute, orders, [{"payload": ORDER}])

    assert row["_warnings"] == ["warn: referential_integrity(customer_id) failed"]


def test_unique_on_append_data(compute: LocalSparkDelta, orders: Contract) -> None:
    existing = compute.spark.createDataFrame([("o-0",)], "order_id string")
    records = [
        {"payload": {**ORDER, "order_id": "o-0"}},  # already in silver
        {"payload": ORDER},
        {"payload": ORDER},  # duplicate within the batch
    ]

    rows = check(compute, orders, records, existing=existing)

    unique_failed = "quarantine: unique(order_id) failed"
    assert [unique_failed in r["_quarantine_reasons"] for r in rows] == [True, False, True]


def test_deletes_skip_row_rules(compute: LocalSparkDelta, orders: Contract) -> None:
    [row] = check(compute, orders, [{"payload": {**ORDER, "status": "lost"}, "op": "delete"}])

    assert row["_quarantine_reasons"] == []
    assert row["_warnings"] == []


def test_blocking_rule_raises(compute: LocalSparkDelta, orders: Contract) -> None:
    rule = QualityRule(
        RuleType.ACCEPTED_VALUES, Severity.BLOCK, ("currency_code",), {"values": ["EUR"]}
    )
    contract = Contract(**{**orders.__dict__, "quality": (rule,)})
    standardized = standardize(
        build_envelope(compute.spark, [{"payload": {**ORDER, "currency_code": "USD"}}]), contract
    )
    checked = evaluate_row_rules(standardized, contract, lambda *_: None)

    with pytest.raises(QualityGateError, match=r"accepted_values\(currency_code\) failed \(1 "):
        raise_if_blocked(checked, contract)


def test_summarize_warnings(compute: LocalSparkDelta, orders: Contract) -> None:
    standardized = standardize(
        build_envelope(
            compute.spark, [{"payload": ORDER}, {"payload": {**ORDER, "order_id": "o-2"}}]
        ),
        orders,
    )
    checked = evaluate_row_rules(standardized, orders, lambda *_: None)

    assert summarize_warnings(checked) == {"warn: referential_integrity(customer_id) failed": 2}


def test_dataset_unique_on_keyed_history(compute: LocalSparkDelta, customers: Contract) -> None:
    current = compute.spark.createDataFrame([(1,), (1,), (2,)], "customer_id long")

    findings = evaluate_dataset_rules(current, customers, 3, None, NOW)

    assert findings == [
        DatasetFinding(
            "unique(customer_id)", Severity.BLOCK, "unique(customer_id): duplicate values [1]"
        )
    ]
    with pytest.raises(QualityGateError, match="duplicate values"):
        raise_if_dataset_blocked(findings, customers)


@pytest.mark.parametrize(
    ("incoming", "previous", "fails"),
    [(100, 100, False), (140, 100, False), (160, 100, True), (10, 100, True), (5, None, False)],
)
def test_row_count_anomaly(
    compute: LocalSparkDelta, customers: Contract, incoming: int, previous: int | None, fails: bool
) -> None:
    current = compute.spark.createDataFrame([(1,)], "customer_id long")

    findings = evaluate_dataset_rules(current, customers, incoming, previous, NOW)

    assert any(f.rule == "row_count_anomaly" for f in findings) is fails


@pytest.mark.parametrize(
    ("newest", "fails"),
    [(datetime(2026, 1, 1, 11, 0), False), (datetime(2026, 1, 1, 9, 0), True), (None, True)],
)
def test_freshness(
    compute: LocalSparkDelta, orders: Contract, newest: datetime | None, fails: bool
) -> None:
    current = compute.spark.createDataFrame(
        [("o-1", newest)], "order_id string, ordered_at timestamp"
    )

    findings = evaluate_dataset_rules(current, orders, 1, 1, NOW)

    assert any(f.rule == "freshness(ordered_at)" for f in findings) is fails
