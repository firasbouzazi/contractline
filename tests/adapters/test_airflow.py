import ast
from pathlib import Path

import pytest

from contractline.adapters.orchestration.airflow import (
    HEADER,
    build_dags,
    find_stale_dags,
    write_dags,
)
from contractline.core.plan import build_plan
from contractline.core.project import load_project
from contractline.core.schedule import build_ingest_schedule

ROOT = Path(__file__).parent.parent.parent
EXAMPLES = ROOT / "examples"


@pytest.fixture(scope="module")
def dags() -> dict[str, str]:
    return build_dags(build_plan(load_project([EXAMPLES]), "dev"), "dev")


@pytest.mark.parametrize(
    ("freshness", "cron"),
    [
        ("PT15M", "*/15 * * * *"),
        ("PT1H", "*/30 * * * *"),
        ("PT2H", "0 * * * *"),
        ("P1D", "0 */12 * * *"),
        ("P7D", "0 0 * * *"),
    ],
)
def test_ingest_runs_at_least_twice_per_sla(freshness: str, cron: str) -> None:
    assert build_ingest_schedule(freshness) == cron


def test_one_valid_python_dag_per_step(dags: dict[str, str]) -> None:
    assert len(dags) == 10
    for name, source in dags.items():
        assert source.startswith(HEADER)
        ast.parse(source, filename=name)


def test_ingest_dags_follow_the_sla(dags: dict[str, str]) -> None:
    source = dags["sales__customers__bronze.py"]

    assert "schedule='*/30 * * * *'" in source  # freshness PT1H
    assert "contractline run-step sales__customers__bronze --env dev" in source
    assert 'outlets=[Asset(name="dev_bronze.sales.customers"' in source
    assert "'pii:true'" in source


def test_silver_is_triggered_by_its_bronze_only(dags: dict[str, str]) -> None:
    source = dags["sales__orders__silver.py"]

    assert 'schedule=Asset(name="dev_bronze.sales.orders"' in source
    assert "dev_silver.sales.customers" not in source.split("with DAG")[1].split("tags")[0]
    # Quarantine is not an asset other Dags react to.
    assert "quarantine" not in source.split("outlets")[1]


def test_gold_is_triggered_by_any_input(dags: dict[str, str]) -> None:
    source = dags["sales__fct_orders__gold.py"]

    assert 'schedule=(Asset(name="dev_silver.sales.orders"' in source
    assert ' | Asset(name="dev_silver.sales.customers"' in source


def test_write_and_check(tmp_path: Path, dags: dict[str, str]) -> None:
    (tmp_path / "hand_written.py").write_text("print('mine')\n")
    (tmp_path / "old__dataset__bronze.py").write_text(HEADER + "# gone\n")

    changes = write_dags(dags, tmp_path)

    assert "removed old__dataset__bronze.py" in changes
    assert (tmp_path / "hand_written.py").exists()  # never touch files we did not generate
    assert find_stale_dags(dags, tmp_path) == []
    (tmp_path / "sales__orders__silver.py").write_text(HEADER + "# edited\n")
    assert find_stale_dags(dags, tmp_path) == ["sales__orders__silver.py is missing or out of date"]


def test_committed_dags_are_up_to_date() -> None:
    generated = build_dags(build_plan(load_project([EXAMPLES]), "dev"), "dev")

    assert find_stale_dags(generated, EXAMPLES / "airflow" / "dags") == []
