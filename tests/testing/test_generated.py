from pathlib import Path

from contractline.adapters.compute.spark_delta_local import LocalSparkDelta
from contractline.core.project import load_project
from contractline.core.validation import load_contract
from contractline.testing.generated import (
    EphemeralSecrets,
    check_conformance,
    check_quality,
    find_raw_pii,
    format_report,
    run_generated_tests,
)

ROOT = Path(__file__).parent.parent.parent / "examples"


def test_ephemeral_secrets_are_random_but_stable() -> None:
    secrets = EphemeralSecrets()

    assert secrets.get_secret("pii_hash") == secrets.get_secret("pii_hash")
    assert secrets.get_secret("pii_hash") != EphemeralSecrets().get_secret("pii_hash")
    assert len(secrets.get_secret("pii_token")) == 32


def test_find_raw_pii_flags_leaks(compute: LocalSparkDelta) -> None:
    df = compute.spark.createDataFrame(
        [("ok", "a@example.invalid", 1)], "note string, contact string, n int"
    )

    assert find_raw_pii(df, {"a@example.invalid"}) == ["raw PII found: a@example.invalid"]
    assert find_raw_pii(df, {"b@example.invalid"}) == []


def test_check_conformance_flags_schema_differences(compute: LocalSparkDelta) -> None:
    contract = load_contract(ROOT / "contracts" / "sales" / "orders.yaml")
    df = compute.spark.createDataFrame([("o", "x")], "order_id string, extra string")

    problems = check_conformance(df, contract)

    assert "unexpected column extra" in problems
    assert "missing column total_amount" in problems


def test_check_quality_flags_rule_violations(compute: LocalSparkDelta) -> None:
    contract = load_contract(ROOT / "contracts" / "sales" / "orders.yaml")
    df = compute.spark.createDataFrame(
        [("o-1", 1, "lost", -5.0, "EUR")],
        "order_id string, customer_id long, status string, total_amount double, "
        "currency_code string",
    )

    problems = check_quality(df, contract)

    assert problems == [
        "quarantine: range(total_amount) failed, quarantine: accepted_values(status) failed"
    ]


def test_generated_tests_pass_on_the_examples(compute: LocalSparkDelta, env: str) -> None:
    report = run_generated_tests(compute, load_project([ROOT]), env)

    assert report.passed, format_report(report)
    checks = {(r.target, r.check) for r in report.results}
    assert ("sales.customers", "no raw PII after bronze") in checks
    assert ("sales.customers", "history invariants") in checks
    assert ("sales.agg_daily_revenue", "no raw PII in gold") in checks


def test_generated_tests_fail_on_a_broken_model(
    compute: LocalSparkDelta, env: str, tmp_path: Path
) -> None:
    for source in (ROOT / "contracts").rglob("*.yaml"):
        target = tmp_path / source.relative_to(ROOT)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(source.read_text())
    gold = tmp_path / "pipelines" / "sales" / "gold"
    gold.mkdir(parents=True)
    original = ROOT / "pipelines" / "sales" / "gold"
    (gold / "fct_orders.yaml").write_text((original / "fct_orders.yaml").read_text())
    (gold / "fct_orders.sql").write_text(
        (original / "fct_orders.sql").read_text().replace("WHERE o.status <> 'cancelled'", "")
    )

    report = run_generated_tests(compute, load_project([tmp_path]), env)

    failed = [r for r in report.results if not r.passed]
    assert [r.check for r in failed] == [
        "model test: cancelled orders are excluded and segments come from the current version"
    ]
