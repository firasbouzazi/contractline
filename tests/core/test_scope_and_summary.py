import pytest

from contractline.core.ci_summary import build_ci_summary
from contractline.core.scope import find_out_of_scope

AGENT = "agent/DATA-1-sales-returns"


@pytest.mark.parametrize(
    ("path", "allowed"),
    [
        ("contracts/sales/returns.yaml", True),
        ("pipelines/sales/gold/fct_returns.sql", True),
        ("pipelines/sales/gold/fct_returns.yaml", True),
        ("infra/airflow/dags/sales__returns__bronze.py", True),
        ("evaluation/logs/S01.jsonl", True),
        ("src/contractline/spark/quality.py", False),
        ("tests/spark/test_quality.py", False),
        (".github/workflows/ci.yml", False),
        ("contractline.yaml", False),
        ("docs/adr/0001-record-architecture-decisions.md", False),
        ("contracts/sales/nested/returns.yaml", False),
    ],
)
def test_agent_scope(path: str, allowed: bool) -> None:
    assert (find_out_of_scope(AGENT, [path]) == []) is allowed


def test_humans_are_not_restricted() -> None:
    assert find_out_of_scope("feat/x", ["src/contractline/cli.py"]) == []


DIFF = {
    "old_version": "1.0.0",
    "new_version": "1.1.0",
    "required_bump": "minor",
    "relaxes_guarantees": True,
    "version_problem": None,
    "changes": [
        {
            "location": "sla.freshness",
            "message": "changed from PT1H to P1D",
            "bump": "minor",
            "relaxes": True,
        }
    ],
}
REPORT = {
    "env": "pr12",
    "passed": False,
    "results": [
        {"target": "sales.returns", "check": "pipeline runs", "passed": True, "details": []},
        {
            "target": "sales.returns",
            "check": "history invariants",
            "passed": False,
            "details": ["x"],
        },
    ],
}


def test_summary_flags_relaxing_changes() -> None:
    text = build_ci_summary({"contracts/sales/customers.yaml": DIFF}, None, [], [])

    assert "⚠️ relaxes a guarantee: needs the data owner's approval" in text
    assert "`sla.freshness`: changed from PT1H to P1D ⚠️ **relaxes**" in text


def test_summary_reports_failures_first() -> None:
    text = build_ci_summary({}, REPORT, ["src/contractline/cli.py"], ["branch: bad"])

    assert "**Status:** ❌ blocked" in text
    assert "- `src/contractline/cli.py`" in text
    assert "### Generated tests (pr12): 1/2" in text
    assert "- ❌ sales.returns: history invariants" in text


def test_summary_when_all_is_well() -> None:
    passing = {**REPORT, "passed": True, "results": REPORT["results"][:1]}  # type: ignore[index]

    text = build_ci_summary({"contracts/sales/returns.yaml": {"new": True}}, passing, [], [])

    assert "**Status:** ✅ ready for review" in text
    assert "`contracts/sales/returns.yaml`: new contract" in text
    assert "All checks passed." in text
