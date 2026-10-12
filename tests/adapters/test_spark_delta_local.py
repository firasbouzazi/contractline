import pytest

from contractline.adapters.compute.spark_delta_local import LocalSparkDelta, build_physical_name
from contractline.core.naming import Layer, build_dataset_name


def test_build_physical_name() -> None:
    assert build_physical_name("prod_silver.sales.customers") == "prod_silver__sales.customers"


@pytest.mark.parametrize("name", ["sales.customers", "a.b.c.d"])
def test_build_physical_name_rejects_other_shapes(name: str) -> None:
    with pytest.raises(ValueError, match=r"expected <catalog>\.<schema>\.<table>"):
        build_physical_name(name)


def test_writes_and_reads_delta_tables(compute: LocalSparkDelta, env: str) -> None:
    logical = build_dataset_name(env, Layer.BRONZE, "sales", "customers")
    compute.ensure_schema(logical)
    table = compute.build_table_name(logical)

    compute.spark.range(3).write.format("delta").saveAsTable(table)

    assert compute.spark.table(table).count() == 3
    detail = compute.spark.sql(f"DESCRIBE DETAIL {table}").first()
    assert detail is not None
    assert detail["format"] == "delta"


def test_checkpoint_paths_are_per_dataset(compute: LocalSparkDelta) -> None:
    path = compute.build_checkpoint_path("pr1_bronze.sales.customers")

    assert path.endswith("checkpoints/pr1_bronze.sales.customers")
