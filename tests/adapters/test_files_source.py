"""The files adapter reading real files, through ingest_to_bronze and silver."""

import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest

from contractline.adapters.compute.spark_delta_local import LocalSparkDelta
from contractline.adapters.sources.files import FilesSource
from contractline.core.contract import Contract, Source
from contractline.core.naming import Layer, build_dataset_name
from contractline.core.validation import load_contract
from contractline.spark.bronze import ingest_to_bronze
from contractline.spark.checks import find_scd2_violations
from contractline.spark.silver import run_silver
from tests.spark.secrets_fixtures import FakeSecrets

EXAMPLES = Path(__file__).parent.parent.parent / "examples" / "contracts"
NOW = datetime(2026, 1, 3)


def located(contract: Contract, folder: Path) -> Contract:
    source = Source(**{**contract.source.__dict__, "location": str(folder)})
    return Contract(**{**contract.__dict__, "source": source})


def table(compute: LocalSparkDelta, env: str, layer: Layer, contract: Contract) -> Any:
    logical = build_dataset_name(env, layer, contract.identity.domain, contract.identity.name)
    return compute.spark.table(compute.build_table_name(logical))


def write_rates(folder: Path, name: str, rates: dict[str, str], quoted_at: str) -> None:
    lines = ["currency_code,rate_to_eur,quoted_at"]
    lines += [f"{code},{rate},{quoted_at}" for code, rate in rates.items()]
    (folder / name).write_text("\n".join(lines) + "\n")


@pytest.fixture
def rates(tmp_path: Path) -> tuple[Contract, Path]:
    folder = tmp_path / "rates"
    folder.mkdir()
    return located(load_contract(EXAMPLES / "finance" / "exchange_rates.yaml"), folder), folder


def test_full_snapshots_build_history_and_detect_deletes(
    compute: LocalSparkDelta, env: str, rates: tuple[Contract, Path]
) -> None:
    contract, folder = rates
    write_rates(folder, "2026-01-01.csv", {"USD": "1.10", "TND": "3.30"}, "2026-01-01T06:00:00Z")
    assert ingest_to_bronze(compute, FilesSource(), contract, env) == 1
    run_silver(compute, contract, env, FakeSecrets(), NOW)

    # Next day: USD moves, TND is no longer quoted, GBP appears.
    write_rates(folder, "2026-01-02.csv", {"USD": "1.12", "GBP": "0.85"}, "2026-01-02T06:00:00Z")
    assert ingest_to_bronze(compute, FilesSource(), contract, env) == 1
    run_silver(compute, contract, env, FakeSecrets(), NOW)

    silver = table(compute, env, Layer.SILVER, contract)
    current = {
        r["currency_code"]: str(r["rate_to_eur"]) for r in silver.where("_is_current").collect()
    }
    assert current == {"USD": "1.12000000", "GBP": "0.85000000"}
    assert silver.where("currency_code = 'USD'").count() == 2
    tnd = silver.where("currency_code = 'TND'").first()
    assert tnd is not None
    assert tnd["_is_current"] is False
    assert find_scd2_violations(silver, ["currency_code"]) == []


def test_reading_again_lands_nothing_new(
    compute: LocalSparkDelta, env: str, rates: tuple[Contract, Path]
) -> None:
    contract, folder = rates
    write_rates(folder, "a.csv", {"USD": "1.10"}, "2026-01-01T06:00:00Z")

    assert ingest_to_bronze(compute, FilesSource(), contract, env) == 1
    assert ingest_to_bronze(compute, FilesSource(), contract, env) == 0
    assert table(compute, env, Layer.BRONZE, contract).count() == 1


def test_incremental_json_lines_keep_the_latest_state(
    compute: LocalSparkDelta, env: str, tmp_path: Path
) -> None:
    folder = tmp_path / "shipments"
    folder.mkdir()
    contract = located(load_contract(EXAMPLES / "logistics" / "shipments.yaml"), folder)

    def shipment(status: str, at: str, **extra: Any) -> str:
        row = {
            "shipment_id": "s-1",
            "order_id": "o-1",
            "status": status,
            "recipient_name": "Ada Example",
            "postal_code": "75011",
            "is_delivered": status == "delivered",
            "updated_at": at,
            **extra,
        }
        return json.dumps(row)

    (folder / "10h.json").write_text(shipment("delivered", "2026-01-01T10:00:00Z") + "\n")
    # A late export carries an older change: it must not win.
    (folder / "11h.json").write_text(shipment("in_transit", "2026-01-01T08:00:00Z") + "\n")
    ingest_to_bronze(compute, FilesSource(), contract, env)
    run_silver(compute, contract, env, FakeSecrets(), NOW)

    [row] = table(compute, env, Layer.SILVER, contract).collect()
    assert row["status"] == "delivered"
    assert row["recipient_name"] == "[REDACTED]"
    assert row["postal_code"] == "75"


def test_parquet_files(compute: LocalSparkDelta, env: str, tmp_path: Path) -> None:
    folder = tmp_path / "parquet"
    contract = load_contract(EXAMPLES / "finance" / "exchange_rates.yaml")
    source = Source(**{**contract.source.__dict__, "location": str(folder), "format": "parquet"})
    contract = Contract(**{**contract.__dict__, "source": source})
    compute.spark.createDataFrame(
        [("USD", "1.10", "2026-01-01 06:00:00")], "c string, r string, q string"
    ).selectExpr(
        "c AS currency_code",
        "CAST(r AS DECIMAL(18,8)) AS rate_to_eur",
        "CAST(q AS TIMESTAMP) AS quoted_at",
    ).write.parquet(str(folder))

    ingest_to_bronze(compute, FilesSource(), contract, env)
    run_silver(compute, contract, env, FakeSecrets(), NOW)

    [row] = table(compute, env, Layer.SILVER, contract).collect()
    assert str(row["rate_to_eur"]) == "1.10000000"
