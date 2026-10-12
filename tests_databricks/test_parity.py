"""Cross-platform parity: the same contracts and synthetic data give the same
silver and gold tables locally and on Databricks (ADR-0019).

Needs a workspace: DATABRICKS_HOST, DATABRICKS_TOKEN (or a CLI profile), a
serverless or cluster id for Databricks Connect, and the catalogs created by
infra/terraform/databricks. Runs in the "Databricks" workflow, not by default.
Must run in an environment with databricks-connect and *without* the spark
extra (databricks-connect brings its own pyspark), so the local half runs in a
subprocess with the normal dev environment.
"""

import json
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest

from contractline.adapters.compute.databricks import DatabricksCompute
from contractline.core.naming import Layer, build_dataset_name
from contractline.core.project import load_project
from contractline.spark.bronze import map_files, map_kafka_to_envelope, write_bronze_batch
from contractline.spark.gold import run_gold
from contractline.spark.silver import run_silver
from contractline.testing.synthetic import generate

ROOT = Path(__file__).parent.parent
PROJECT = [ROOT / "examples"]
ENV = os.environ.get("CONTRACTLINE_PARITY_ENV", "dev")
NOW = datetime(2026, 6, 1)
# Lineage columns differ by platform (ingestion times, file paths); values must not.
IGNORED = {"_ingested_at", "_source_offset", "_batch_id"}


class FixedSecrets:
    """The same keys on both platforms, so hashed values can be compared."""

    def get_secret(self, name: str) -> bytes:
        return f"parity-test-key-{name}-0123456789".encode()


def snapshot(compute: Any, logical: str) -> list[str]:
    table = compute.spark.table(compute.build_table_name(logical))
    columns = sorted(c for c in table.columns if c not in IGNORED)
    return sorted(
        json.dumps(r.asDict(), default=str, sort_keys=True)
        for r in table.select(*columns).collect()
    )


def run_everywhere(compute: Any) -> dict[str, list[str]]:
    """Synthetic data through bronze, silver and gold on ``compute``."""
    project = load_project(PROJECT)
    tables: dict[str, list[str]] = {}
    for contract in project.contracts:
        data = generate(contract, seed=11)
        domain, entity = contract.identity.domain, contract.identity.name
        bronze = build_dataset_name(ENV, Layer.BRONZE, domain, entity)
        for number, batch in enumerate(data.batches):
            spark = compute.spark
            if contract.source.adapter == "files":
                raw = spark.createDataFrame(
                    [
                        (
                            m.value,
                            f"parity/{entity}/{number}",
                            data.batch_times[number].replace(tzinfo=None),
                        )
                        for m in batch
                    ],
                    "payload string, file_path string, file_modified_at timestamp",
                )
                envelope = map_files(raw, contract)
            else:
                raw = spark.createDataFrame(
                    [
                        (
                            m.key.encode() if m.key else None,
                            m.value.encode() if m.value else None,
                            contract.source.location,
                            0,
                            i,
                            NOW,
                        )
                        for i, m in enumerate(batch)
                    ],
                    "key binary, value binary, topic string, partition int, offset long, timestamp timestamp",
                )
                envelope = map_kafka_to_envelope(raw, contract)
            write_bronze_batch(compute, bronze, envelope, number)
            run_silver(compute, contract, ENV, FixedSecrets(), data.now.replace(tzinfo=None))
        tables[f"silver.{domain}.{entity}"] = snapshot(
            compute, build_dataset_name(ENV, Layer.SILVER, domain, entity)
        )
    for model in project.gold_models:
        run_gold(compute, model, ENV, NOW)
        tables[f"gold.{model.domain}.{model.name}"] = snapshot(
            compute, build_dataset_name(ENV, Layer.GOLD, model.domain, model.name)
        )
    return tables


@pytest.mark.skipif(not os.environ.get("DATABRICKS_HOST"), reason="needs a Databricks workspace")
def test_same_results_locally_and_on_databricks(tmp_path: Path) -> None:
    local_out = tmp_path / "local.json"
    subprocess.run(  # noqa: S603 - our own module, our own interpreter
        [sys.executable, "-m", "tests_databricks.run_local", str(local_out)],
        check=True,
        cwd=ROOT,
    )
    local = json.loads(local_out.read_text())

    remote = run_everywhere(DatabricksCompute(ENV))

    assert remote.keys() == local.keys()
    for table in local:
        assert remote[table] == local[table], f"{table} differs between platforms"
