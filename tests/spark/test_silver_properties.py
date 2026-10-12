"""Property: SCD2 history does not depend on how events are batched or ordered."""

from typing import Any

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from contractline.adapters.compute.spark_delta_local import LocalSparkDelta
from contractline.core.naming import Layer
from contractline.core.validation import load_contract
from contractline.spark.checks import find_scd2_violations
from tests.spark.test_silver import EXAMPLES, Pipeline, at, customer

CUSTOMERS = load_contract(EXAMPLES / "sales" / "customers.yaml")
_RUNS = iter(range(10_000, 20_000))

# One event: (customer id, minute, kind, segment). Minutes are distinct per run.
events_strategy = st.lists(
    st.tuples(
        st.integers(1, 2),
        st.sampled_from(["upsert", "upsert", "delete"]),
        st.sampled_from(["consumer", "business", "partner"]),
    ),
    min_size=1,
    max_size=6,
)


def build_records(pipeline: Pipeline, events: list[tuple[int, str, str]]) -> list[dict[str, Any]]:
    records = []
    for minute, (customer_id, kind, segment) in enumerate(events):
        payload = customer(customer_id, minute, segment=segment)
        if kind == "delete":
            records.append(pipeline.event("delete", payload, source_at=at(minute)))
        else:
            records.append(pipeline.event("update", payload, source_at=at(minute)))
    return records


def final_state(pipeline: Pipeline) -> list[tuple[Any, ...]]:
    rows = pipeline.table(Layer.SILVER).select(
        "customer_id", "segment", "_valid_from", "_valid_to", "_is_current"
    )
    return sorted(tuple(r) for r in rows.collect())


@settings(
    max_examples=6,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(events=events_strategy, data=st.data())
def test_batching_and_arrival_order_do_not_change_history(
    compute: LocalSparkDelta, events: list[tuple[int, str, str]], data: st.DataObject
) -> None:
    reference = Pipeline(compute, CUSTOMERS, f"pr{next(_RUNS)}")
    records = build_records(reference, events)
    reference.run(records)

    shuffled = Pipeline(compute, CUSTOMERS, f"pr{next(_RUNS)}")
    order = data.draw(st.permutations(records))
    cut = data.draw(st.integers(0, len(order)))
    for batch in (order[:cut], order[cut:]):
        if batch:
            shuffled.run(list(batch))

    if reference.compute.spark.catalog.tableExists(
        reference.compute.build_table_name(f"{reference.env}_silver.sales.customers")
    ):
        assert final_state(shuffled) == final_state(reference)
        assert find_scd2_violations(shuffled.table(Layer.SILVER), ["customer_id"]) == []
