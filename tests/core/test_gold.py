import copy
from pathlib import Path
from typing import Any

import pytest

from contractline.core.gold import InputRef, find_refs, load_gold_model, parse_gold
from contractline.core.validation import InvalidContractError
from contractline.core.yaml_loader import load_yaml

GOLD = Path(__file__).parent.parent.parent / "examples" / "pipelines" / "sales" / "gold"


@pytest.fixture
def fct_orders() -> tuple[dict[str, Any], str]:
    doc: dict[str, Any] = load_yaml((GOLD / "fct_orders.yaml").read_text())
    return doc, (GOLD / "fct_orders.sql").read_text()


def errors_of(doc: dict[str, Any], sql: str) -> list[str]:
    import yaml

    _, errors = parse_gold(yaml.safe_dump(doc), sql)
    return [str(e) for e in errors]


@pytest.mark.parametrize("name", ["fct_orders", "agg_daily_revenue"])
def test_examples_are_valid(name: str) -> None:
    model = load_gold_model(GOLD / f"{name}.yaml")

    assert model.name == name
    assert model.tests


def test_find_refs() -> None:
    sql = "SELECT * FROM {{ ref('silver.sales.orders') }} JOIN {{ref( 'gold.sales.x' )}}"

    assert find_refs(sql) == ["silver.sales.orders", "gold.sales.x"]


def test_input_ref_round_trip() -> None:
    assert str(InputRef.parse("silver.sales.orders")) == "silver.sales.orders"


@pytest.mark.parametrize(
    ("mutate", "sql_change", "expected"),
    [
        pytest.param(
            lambda d: d.update(tests=[]), None, "tests: [] should be non-empty", id="no-tests"
        ),
        pytest.param(
            lambda d: d["identity"].update(name="orders"),
            None,
            "identity.name: 'orders' does not match",
            id="name-needs-kind-prefix",
        ),
        pytest.param(
            lambda d: d["identity"].pop("description"),
            None,
            "identity: 'description' is a required property",
            id="business-definition-required",
        ),
        pytest.param(
            lambda d: d["schema"]["columns"][0].update(pii="direct"),
            None,
            "schema.columns[0].pii: gold cannot expose direct PII",
            id="no-direct-pii",
        ),
        pytest.param(
            lambda d: d["quality"][0].update(severity="quarantine"),
            None,
            "quality[0].severity: gold has no quarantine",
            id="no-quarantine",
        ),
        pytest.param(
            lambda d: d["inputs"].append("silver.sales.payments"),
            None,
            "inputs: silver.sales.payments is declared but the SQL never reads it",
            id="unused-input",
        ),
        pytest.param(
            lambda d: d["inputs"].remove("silver.sales.customers"),
            None,
            "sql: reads silver.sales.customers, which is not declared in inputs",
            id="undeclared-input",
        ),
        pytest.param(
            lambda d: d["inputs"].append("gold.finance.fct_invoices"),
            lambda s: s + " -- {{ ref('gold.finance.fct_invoices') }}",
            "inputs[2]: gold models may only read gold from their domain",
            id="cross-domain-gold",
        ),
        pytest.param(
            lambda d: d["tests"][0]["given"].update({"silver.sales.refunds": []}),
            None,
            "tests[0].given: silver.sales.refunds is not a declared input",
            id="test-unknown-input",
        ),
        pytest.param(
            lambda d: d["tests"][0]["expect"][0].update(margin=1),
            None,
            "tests[0].expect[0]: 'margin' is not an output column",
            id="test-unknown-column",
        ),
        pytest.param(
            lambda d: d["tests"][0]["expect"][0].pop("status"),
            None,
            "tests[0].expect: every expected row must list the same columns",
            id="test-ragged-rows",
        ),
    ],
)
def test_gold_errors(
    fct_orders: tuple[dict[str, Any], str], mutate: Any, sql_change: Any, expected: str
) -> None:
    doc, sql = copy.deepcopy(fct_orders[0]), fct_orders[1]
    mutate(doc)
    if sql_change:
        sql = sql_change(sql)

    errors = errors_of(doc, sql)

    assert any(e.startswith(expected) for e in errors), errors


def test_missing_sql_file(tmp_path: Path) -> None:
    (tmp_path / "fct_x.yaml").write_text((GOLD / "fct_orders.yaml").read_text())

    with pytest.raises(InvalidContractError, match=r"missing SQL file fct_x\.sql"):
        load_gold_model(tmp_path / "fct_x.yaml")
