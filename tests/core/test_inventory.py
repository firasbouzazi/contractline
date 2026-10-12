from pathlib import Path

from contractline.core.inventory import build_inventory, format_inventory, inventory_as_dict
from contractline.core.project import load_project

ROOT = Path(__file__).parent.parent.parent / "examples"


def test_inventory_lists_pii_datasets_only() -> None:
    entries = build_inventory(load_project([ROOT]))

    assert [e.dataset for e in entries] == ["sales.customers", "logistics.shipments"]
    customers = entries[0]
    assert customers.erasure_key == "customer_id"
    assert customers.retention == "P3Y"
    assert customers.raw_layers == ("bronze", "quarantine")
    assert customers.read_by_gold == ("sales.fct_orders",)
    assert [(c.column, c.pii, c.masking, c.in_silver) for c in customers.columns] == [
        ("email", "direct", "hash", True),
        ("full_name", "direct", "drop", False),
        ("birth_date", "indirect", "generalize", True),
    ]


def test_inventory_formats() -> None:
    entries = build_inventory(load_project([ROOT]))

    assert inventory_as_dict(entries)["datasets"][1]["columns"][0]["masking"] == "redact"
    assert "| sales.customers | full_name | direct | drop | no |" in format_inventory(entries)
