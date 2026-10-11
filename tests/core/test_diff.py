import copy
from pathlib import Path
from typing import Any

import pytest

from contractline.core.contract import Contract
from contractline.core.diff import (
    Bump,
    Change,
    build_report,
    compute_bump,
    compute_duration_seconds,
    diff_contracts,
    is_widening,
)
from contractline.core.validation import validate_document
from contractline.core.yaml_loader import load_yaml


@pytest.fixture
def base(examples_dir: Path) -> dict[str, Any]:
    doc: dict[str, Any] = load_yaml((examples_dir / "sales" / "customers.yaml").read_text())
    return doc


def build(doc: dict[str, Any]) -> Contract:
    contract, errors = validate_document(doc)
    assert contract is not None, errors
    return contract


def changes_between(base: dict[str, Any], mutate: Any) -> list[Change]:
    new = copy.deepcopy(base)
    mutate(new)
    return list(diff_contracts(build(base), build(new)).changes)


def column(doc: dict[str, Any], name: str) -> dict[str, Any]:
    found: dict[str, Any] = next(c for c in doc["schema"]["columns"] if c["name"] == name)
    return found


def rule(doc: dict[str, Any], rule_type: str) -> dict[str, Any]:
    found: dict[str, Any] = next(r for r in doc["quality"] if r["type"] == rule_type)
    return found


def test_identical_contracts_have_no_changes(base: dict[str, Any]) -> None:
    result = diff_contracts(build(base), build(copy.deepcopy(base)))

    assert result.changes == ()
    assert result.required_bump is Bump.NONE
    assert result.version_problem is None


@pytest.mark.parametrize(
    ("mutate", "expected"),
    [
        pytest.param(
            lambda d: d["schema"]["columns"].remove(column(d, "created_at")),
            Change("schema.columns.created_at", "column removed", Bump.MAJOR),
            id="column-removed",
        ),
        pytest.param(
            lambda d: d["schema"]["columns"].append(
                {"name": "phone_country", "type": "string", "nullable": True, "pii": "none"}
            ),
            Change("schema.columns.phone_country", "nullable column added", Bump.MINOR),
            id="nullable-column-added",
        ),
        pytest.param(
            lambda d: d["schema"]["columns"].append(
                {"name": "phone_country", "type": "string", "nullable": False, "pii": "none"}
            ),
            Change(
                "schema.columns.phone_country",
                "non-nullable column added (existing rows have no value)",
                Bump.MAJOR,
            ),
            id="non-nullable-column-added",
        ),
        pytest.param(
            lambda d: column(d, "customer_id").update(type="string"),
            Change("schema.columns.customer_id", "type changed from long to string", Bump.MAJOR),
            id="type-narrowed",
        ),
        pytest.param(
            lambda d: column(d, "segment").update(nullable=True),
            Change("schema.columns.segment", "became nullable", Bump.MAJOR, relaxes=True),
            id="became-nullable",
        ),
        pytest.param(
            lambda d: column(d, "full_name").update(nullable=False),
            Change("schema.columns.full_name", "became non-nullable", Bump.MINOR),
            id="became-non-nullable",
        ),
        pytest.param(
            lambda d: column(d, "birth_date").update(pii="none"),
            Change(
                "schema.columns.birth_date",
                "PII class changed from indirect to none",
                Bump.MAJOR,
                relaxes=True,
            ),
            id="pii-downgraded",
        ),
        pytest.param(
            lambda d: (
                column(d, "birth_date").update(pii="direct")
                or d["privacy"]["masking"].update(birth_date={"strategy": "drop"})
            ),
            Change(
                "schema.columns.birth_date", "PII class changed from indirect to direct", Bump.MINOR
            ),
            id="pii-upgraded",
        ),
        pytest.param(
            lambda d: column(d, "email").update(description="Login email"),
            Change("schema.columns.email", "description changed", Bump.PATCH),
            id="description",
        ),
        pytest.param(
            lambda d: d["identity"].update(owner="crm-team@example.com"),
            Change("identity.owner", "changed", Bump.PATCH),
            id="owner",
        ),
        pytest.param(
            lambda d: d["identity"].update(name="clients"),
            Change(
                "identity.name", "changed from customers to clients (a new dataset)", Bump.MAJOR
            ),
            id="renamed",
        ),
        pytest.param(
            lambda d: d["source"].update(location="cdc.crm.public.clients"),
            Change(
                "source.location",
                "changed from cdc.crm.public.customers to cdc.crm.public.clients",
                Bump.MAJOR,
            ),
            id="source-moved",
        ),
        pytest.param(
            lambda d: d["schema"].update(drift_policy="add_columns"),
            Change(
                "schema.drift_policy",
                "changed from quarantine to add_columns",
                Bump.MINOR,
                relaxes=True,
            ),
            id="drift-loosened",
        ),
        pytest.param(
            lambda d: d["schema"].update(drift_policy="fail"),
            Change("schema.drift_policy", "changed from quarantine to fail", Bump.MINOR),
            id="drift-tightened",
        ),
        pytest.param(
            lambda d: d["privacy"]["masking"].update(email={"strategy": "redact"}),
            Change("privacy.masking.email", "strategy changed from hash to redact", Bump.MAJOR),
            id="masking-strengthened",
        ),
        pytest.param(
            lambda d: d["privacy"]["masking"].update(
                birth_date={"strategy": "keep", "justification": "age checks"}
            ),
            Change(
                "privacy.masking.birth_date",
                "strategy changed from generalize to keep",
                Bump.MAJOR,
                relaxes=True,
            ),
            id="masking-weakened",
        ),
        pytest.param(
            lambda d: d["privacy"]["masking"]["birth_date"].update(params={"to": "month"}),
            Change(
                "privacy.masking.birth_date", "masking parameters changed", Bump.MAJOR, relaxes=True
            ),
            id="masking-params",
        ),
        pytest.param(
            lambda d: d["privacy"]["retention"].update(period="P10Y"),
            Change(
                "privacy.retention.period", "changed from P3Y to P10Y", Bump.MINOR, relaxes=True
            ),
            id="retention-longer",
        ),
        pytest.param(
            lambda d: d["privacy"]["retention"].update(period="P1Y"),
            Change("privacy.retention.period", "changed from P3Y to P1Y", Bump.MINOR),
            id="retention-shorter",
        ),
        pytest.param(
            lambda d: d["privacy"].update(legal_basis="legitimate_interests"),
            Change(
                "privacy.legal_basis",
                "changed from contract to legitimate_interests",
                Bump.MINOR,
                relaxes=True,
            ),
            id="legal-basis",
        ),
        pytest.param(
            lambda d: d["history"].update(tracked_columns=["segment"]),
            Change(
                "history.tracked_columns",
                "no longer tracked: ['email', 'country_code']",
                Bump.MAJOR,
                relaxes=True,
            ),
            id="untracked",
        ),
        pytest.param(
            lambda d: d["history"]["tracked_columns"].append("birth_date"),
            Change("history.tracked_columns", "now tracked: ['birth_date']", Bump.MINOR),
            id="tracked",
        ),
        pytest.param(
            lambda d: d["history"].update(type="scd1"),
            Change("history.type", "changed from scd2 to scd1", Bump.MAJOR),
            id="history-type",
        ),
        pytest.param(
            lambda d: d["quality"].pop(0),
            Change("quality", "rule removed: unique (customer_id)", Bump.MINOR, relaxes=True),
            id="rule-removed",
        ),
        pytest.param(
            lambda d: d["quality"].append(
                {"type": "not_null", "column": "full_name", "severity": "warn"}
            ),
            Change("quality", "rule added: not_null (full_name)", Bump.MINOR),
            id="rule-added",
        ),
        pytest.param(
            lambda d: rule(d, "unique").update(severity="warn"),
            Change(
                "quality.unique (customer_id)",
                "severity changed from block to warn",
                Bump.MINOR,
                relaxes=True,
            ),
            id="severity-lowered",
        ),
        pytest.param(
            lambda d: rule(d, "row_count_anomaly").update(severity="block"),
            Change("quality.row_count_anomaly", "severity changed from warn to block", Bump.MINOR),
            id="severity-raised",
        ),
        pytest.param(
            lambda d: rule(d, "accepted_values")["values"].append("reseller"),
            Change(
                "quality.accepted_values (segment)",
                "accepts new values ['reseller']",
                Bump.MINOR,
                relaxes=True,
            ),
            id="accepted-values-extended",
        ),
        pytest.param(
            lambda d: rule(d, "accepted_values")["values"].remove("partner"),
            Change(
                "quality.accepted_values (segment)", "no longer accepts ['partner']", Bump.MINOR
            ),
            id="accepted-values-reduced",
        ),
        pytest.param(
            lambda d: rule(d, "range").update(min="1800-01-01"),
            Change(
                "quality.range (birth_date)",
                "min changed from 1900-01-01 to 1800-01-01",
                Bump.MINOR,
                relaxes=True,
            ),
            id="range-widened",
        ),
        pytest.param(
            lambda d: rule(d, "range").update(max="2020-01-01"),
            Change("quality.range (birth_date)", "max bound added (2020-01-01)", Bump.MINOR),
            id="range-bound-added",
        ),
        pytest.param(
            lambda d: rule(d, "row_count_anomaly").update(max_change_ratio=0.9),
            Change(
                "quality.row_count_anomaly",
                "max_change_ratio changed from 0.5 to 0.9",
                Bump.MINOR,
                relaxes=True,
            ),
            id="anomaly-loosened",
        ),
        pytest.param(
            lambda d: d["sla"].update(freshness="P1D"),
            Change("sla.freshness", "changed from PT1H to P1D", Bump.MINOR, relaxes=True),
            id="sla-loosened",
        ),
        pytest.param(
            lambda d: d["sla"].update(freshness="PT30M"),
            Change("sla.freshness", "changed from PT1H to PT30M", Bump.MINOR),
            id="sla-tightened",
        ),
        pytest.param(
            lambda d: d["tags"].update(cost_center="marketing"),
            Change("tags", "changed", Bump.PATCH),
            id="tags",
        ),
    ],
)
def test_change_classification(base: dict[str, Any], mutate: Any, expected: Change) -> None:
    assert expected in changes_between(base, mutate)


def test_privacy_section_removed_relaxes(base: dict[str, Any]) -> None:
    def drop_pii(doc: dict[str, Any]) -> None:
        for c in doc["schema"]["columns"]:
            c["pii"] = "none"
        del doc["privacy"]

    changes = changes_between(base, drop_pii)

    assert Change("privacy", "privacy section removed", Bump.MAJOR, relaxes=True) in changes


@pytest.mark.parametrize(
    ("old", "new", "required", "problem"),
    [
        pytest.param("1.0.0", "2.0.0", Bump.MAJOR, None, id="major-for-breaking"),
        pytest.param(
            "1.0.0",
            "1.1.0",
            Bump.MAJOR,
            "changes need a major version bump, but 1.0.0 -> 1.1.0 is a minor bump",
            id="minor-for-breaking",
        ),
        pytest.param(
            "1.0.0",
            "1.0.0",
            Bump.MAJOR,
            "version must increase (was 1.0.0, now 1.0.0)",
            id="unchanged-version",
        ),
        pytest.param(
            "1.0.0",
            "0.9.0",
            Bump.MAJOR,
            "version must increase (was 1.0.0, now 0.9.0)",
            id="lower-version",
        ),
    ],
)
def test_version_problem_for_breaking_change(
    base: dict[str, Any], old: str, new: str, required: Bump, problem: str | None
) -> None:
    base["identity"]["version"] = old
    changed = copy.deepcopy(base)
    changed["identity"]["version"] = new
    changed["schema"]["columns"].remove(column(changed, "created_at"))

    result = diff_contracts(build(base), build(changed))

    assert result.required_bump is required
    assert result.version_problem == problem


def test_version_bump_without_changes_is_a_problem(base: dict[str, Any]) -> None:
    changed = copy.deepcopy(base)
    changed["identity"]["version"] = "1.0.1"

    result = diff_contracts(build(base), build(changed))

    assert result.version_problem == "version changed to 1.0.1 but the contract did not change"


def test_patch_change_accepts_any_higher_version(base: dict[str, Any]) -> None:
    changed = copy.deepcopy(base)
    changed["identity"]["owner"] = "someone-else@example.com"
    changed["identity"]["version"] = "1.1.0"

    assert diff_contracts(build(base), build(changed)).version_problem is None


def test_report_is_json_friendly(base: dict[str, Any]) -> None:
    changed = copy.deepcopy(base)
    changed["identity"]["version"] = "1.1.0"
    changed["sla"]["freshness"] = "P1D"

    report = build_report(diff_contracts(build(base), build(changed)))

    assert report == {
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


@pytest.mark.parametrize(
    ("old", "new", "expected"),
    [
        ("integer", "long", True),
        ("long", "integer", False),
        ("decimal(10,2)", "decimal(12,2)", True),
        ("decimal(10,2)", "decimal(11,3)", True),
        ("decimal(10,2)", "decimal(10,3)", False),
        ("decimal(10,2)", "decimal(12,1)", False),
        ("string", "long", False),
    ],
)
def test_is_widening(old: str, new: str, expected: bool) -> None:
    assert is_widening(old, new) is expected


@pytest.mark.parametrize(
    ("old", "new", "expected"),
    [
        ("1.0.0", "2.0.0", Bump.MAJOR),
        ("1.0.0", "1.1.0", Bump.MINOR),
        ("1.0.0", "1.0.1", Bump.PATCH),
        ("1.2.3", "1.2.3", Bump.NONE),
        ("1.2.3", "1.2.0", Bump.NONE),
    ],
)
def test_compute_bump(old: str, new: str, expected: Bump) -> None:
    assert compute_bump(old, new) is expected


@pytest.mark.parametrize(
    ("duration", "seconds"),
    [("PT1H", 3600), ("P1D", 86400), ("P1W", 604800), ("PT1H30M", 5400), ("P1Y", 31536000)],
)
def test_compute_duration_seconds(duration: str, seconds: int) -> None:
    assert compute_duration_seconds(duration) == seconds


def test_compute_duration_seconds_rejects_garbage() -> None:
    with pytest.raises(ValueError, match="not an ISO 8601 duration"):
        compute_duration_seconds("1 hour")
