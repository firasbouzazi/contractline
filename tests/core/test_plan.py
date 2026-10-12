from pathlib import Path

import pytest

from contractline.core.plan import StepKind, build_plan, format_plan
from contractline.core.project import ProjectError, load_project
from contractline.core.scaffold import build_contract_skeleton
from contractline.core.validation import parse_text

ROOT = Path(__file__).parent.parent.parent / "examples"


def test_project_orders_contracts_and_models() -> None:
    project = load_project([ROOT])

    names = [c.identity.name for c in project.contracts]
    assert names == ["exchange_rates", "customers", "orders", "shipments"]
    # shipments checks its order_id against sales.orders, so orders comes first.
    assert names.index("orders") < names.index("shipments")
    assert [m.name for m in project.gold_models] == ["fct_orders", "agg_daily_revenue"]
    assert project.find_contract("sales", "orders").identity.name == "orders"
    with pytest.raises(ProjectError, match=r"no contract for sales\.refunds"):
        project.find_contract("sales", "refunds")


def test_plan_steps_and_dependencies() -> None:
    steps = build_plan(load_project([ROOT / "contracts" / "sales", ROOT / "pipelines"]), "pr7")

    assert [(s.id, s.kind) for s in steps] == [
        ("sales__customers__bronze", StepKind.INGEST),
        ("sales__customers__silver", StepKind.SILVER),
        ("sales__orders__bronze", StepKind.INGEST),
        ("sales__orders__silver", StepKind.SILVER),
        ("sales__fct_orders__gold", StepKind.GOLD),
        ("sales__agg_daily_revenue__gold", StepKind.GOLD),
    ]
    orders_silver = steps[3]
    assert orders_silver.depends_on == ("sales__orders__bronze", "sales__customers__silver")
    assert orders_silver.writes == ("pr7_silver.sales.orders", "pr7_quarantine.sales.orders")
    assert dict(steps[1].tags)["pii"] == "true"
    assert "after:  sales__fct_orders__gold" in format_plan(steps)


def test_duplicate_contracts_are_rejected(tmp_path: Path) -> None:
    for name in ("a", "b"):
        (tmp_path / f"{name}.yaml").write_text((ROOT / "contracts/sales/orders.yaml").read_text())

    with pytest.raises(ProjectError, match="several contracts define"):
        load_project([tmp_path])


@pytest.mark.parametrize("kind", ["cdc", "events", "files"])
def test_skeletons_are_valid_contracts(kind: str) -> None:
    contract, errors = parse_text(build_contract_skeleton("sales", "returns", kind, "team"))

    assert errors == []
    assert contract is not None
    assert contract.identity.name == "returns"


def test_unknown_skeleton_kind() -> None:
    with pytest.raises(ValueError, match="source kind must be one of"):
        build_contract_skeleton("sales", "returns", "ftp", "team")
