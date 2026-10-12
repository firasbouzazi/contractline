from datetime import datetime
from pathlib import Path

import pytest

from contractline.adapters.compute.spark_delta_local import LocalSparkDelta
from contractline.core.contract import Contract
from contractline.core.gold import GoldModel, load_gold_model
from contractline.core.naming import Layer, build_dataset_name
from contractline.core.validation import load_contract
from contractline.spark.gold import (
    GoldIsolationError,
    GoldSchemaError,
    build_silver_schema,
    build_typed_rows,
    run_gold,
    run_gold_test,
)
from contractline.spark.quality import QualityGateError

ROOT = Path(__file__).parent.parent.parent / "examples"
GOLD = ROOT / "pipelines" / "sales" / "gold"
NOW = datetime(2026, 1, 3)


def load_silver_contract(domain: str, entity: str) -> Contract:
    return load_contract(ROOT / "contracts" / domain / f"{entity}.yaml")


def load_gold(domain: str, name: str) -> GoldModel:
    return load_gold_model(ROOT / "pipelines" / domain / "gold" / f"{name}.yaml")


def with_sql(model: GoldModel, sql: str) -> GoldModel:
    return GoldModel(**{**model.__dict__, "sql": sql})


@pytest.mark.parametrize("name", ["fct_orders", "agg_daily_revenue"])
def test_example_model_tests_pass(compute: LocalSparkDelta, name: str) -> None:
    model = load_gold("sales", name)

    [result] = [
        run_gold_test(compute, model, t, load_silver_contract, load_gold) for t in model.tests
    ]

    assert result.error is None
    assert result.missing == []
    assert result.unexpected == []
    assert result.passed


def test_failing_model_test_reports_differences(compute: LocalSparkDelta) -> None:
    model = load_gold("sales", "fct_orders")
    broken = with_sql(model, model.sql.replace("WHERE o.status <> 'cancelled'", ""))

    result = run_gold_test(compute, broken, model.tests[0], load_silver_contract, load_gold)

    assert not result.passed
    assert result.missing == []
    assert [row[0] for row in result.unexpected] == ["o-2"]


def test_model_test_with_wrong_output_type_fails(compute: LocalSparkDelta) -> None:
    model = load_gold("sales", "fct_orders")
    broken = with_sql(model, model.sql.replace("CAST(o.ordered_at AS DATE)", "o.ordered_at"))

    result = run_gold_test(compute, broken, model.tests[0], load_silver_contract, load_gold)

    assert not result.passed
    assert result.error is not None
    assert "order_date is timestamp, declared date" in result.error


def test_build_typed_rows_rejects_bad_values(compute: LocalSparkDelta) -> None:
    schema = build_silver_schema(load_silver_contract("sales", "orders"))

    with pytest.raises(ValueError, match="total_amount: 'lots' is not a valid decimal"):
        build_typed_rows(compute, [{"total_amount": "lots"}], schema)


def test_silver_schema_drops_dropped_columns() -> None:
    names = build_silver_schema(load_silver_contract("sales", "customers")).fieldNames()

    assert "full_name" not in names
    assert {"_valid_from", "_is_current", "email"} <= set(names)


def write_silver(compute: LocalSparkDelta, env: str, model: GoldModel) -> None:
    """Silver tables filled from the model's own test inputs."""
    for ref, rows in model.tests[0].given.items():
        _, domain, entity = ref.split(".")
        schema = build_silver_schema(load_silver_contract(domain, entity))
        prepared = [{**r, "_is_current": r.get("_is_current", True)} for r in rows]
        logical = build_dataset_name(env, Layer.SILVER, domain, entity)
        compute.ensure_schema(logical)
        build_typed_rows(compute, prepared, schema).write.format("delta").saveAsTable(
            compute.build_table_name(logical)
        )


def test_run_gold_builds_both_models(compute: LocalSparkDelta, env: str) -> None:
    fct = load_gold("sales", "fct_orders")
    write_silver(compute, env, fct)

    report = run_gold(compute, fct, env, NOW)
    agg = run_gold(compute, load_gold("sales", "agg_daily_revenue"), env, NOW)

    assert report.rows == 2
    assert report.warnings == {"warn: not_null(customer_segment) failed": 1}
    assert agg.rows == 2
    table = compute.build_table_name(
        build_dataset_name(env, Layer.GOLD, "sales", "agg_daily_revenue")
    )
    assert sorted(r["currency_code"] for r in compute.spark.table(table).collect()) == [
        "EUR",
        "TND",
    ]


def test_run_gold_refuses_undeclared_reads(compute: LocalSparkDelta, env: str) -> None:
    fct = load_gold("sales", "fct_orders")
    write_silver(compute, env, fct)
    bronze = build_dataset_name(env, Layer.BRONZE, "sales", "customers")
    compute.ensure_schema(bronze)
    compute.spark.range(1).write.format("delta").saveAsTable(compute.build_table_name(bronze))
    sneaky = with_sql(
        fct,
        fct.sql + f" AND o.customer_id IN (SELECT id FROM {compute.build_table_name(bronze)})",
    )

    with pytest.raises(GoldIsolationError, match="outside its declared inputs"):
        run_gold(compute, sneaky, env, NOW)


def test_run_gold_checks_output_schema(compute: LocalSparkDelta, env: str) -> None:
    fct = load_gold("sales", "fct_orders")
    write_silver(compute, env, fct)

    with pytest.raises(GoldSchemaError, match="undeclared column extra"):
        run_gold(compute, with_sql(fct, fct.sql.replace("SELECT", "SELECT 1 AS extra,", 1)), env)


def test_run_gold_blocks_on_failed_rule(compute: LocalSparkDelta, env: str) -> None:
    fct = load_gold("sales", "fct_orders")
    write_silver(compute, env, fct)
    duplicated = with_sql(fct, f"{fct.sql} UNION ALL {fct.sql}")

    with pytest.raises(QualityGateError, match=r"unique\(order_id\)"):
        run_gold(compute, duplicated, env, NOW)
