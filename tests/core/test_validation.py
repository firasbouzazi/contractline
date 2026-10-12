import copy
from pathlib import Path
from typing import Any

import pytest

from contractline.core.contract import HistoryType, IngestMode, PiiClass
from contractline.core.validation import (
    InvalidContractError,
    load_contract,
    parse_text,
    validate_document,
)
from contractline.core.yaml_loader import load_yaml


@pytest.fixture
def customers(examples_dir: Path) -> dict[str, Any]:
    doc: dict[str, Any] = load_yaml((examples_dir / "sales" / "customers.yaml").read_text())
    return doc


def errors_of(doc: dict[str, Any]) -> list[str]:
    _, errors = validate_document(doc)
    return [str(e) for e in errors]


def column(doc: dict[str, Any], name: str) -> dict[str, Any]:
    found: dict[str, Any] = next(c for c in doc["schema"]["columns"] if c["name"] == name)
    return found


@pytest.mark.parametrize("name", ["customers", "orders"])
def test_examples_are_valid(examples_dir: Path, name: str) -> None:
    contract = load_contract(examples_dir / "sales" / f"{name}.yaml")
    assert contract.identity.name == name


def test_model_is_built_from_document(customers: dict[str, Any]) -> None:
    contract, errors = validate_document(customers)

    assert errors == []
    assert contract is not None
    assert contract.source.mode is IngestMode.CHANGE_STREAM
    assert contract.history.type is HistoryType.SCD2
    assert contract.schema.column("email") is not None
    assert contract.schema.column("email").pii is PiiClass.DIRECT  # type: ignore[union-attr]
    assert contract.has_pii
    assert contract.quality[1].columns == ("segment",)
    assert contract.quality[1].params == {"values": ["consumer", "business", "partner"]}


def test_reports_all_errors_not_only_the_first(customers: dict[str, Any]) -> None:
    del customers["identity"]["owner"]
    customers["sla"]["freshness"] = "soon"

    assert errors_of(customers) == [
        "identity: 'owner' is a required property",
        "sla.freshness: 'soon' does not match '^P(?!$)([0-9]+Y)?([0-9]+M)?([0-9]+W)?([0-9]+D)?"
        "(T(?=[0-9])([0-9]+H)?([0-9]+M)?([0-9]+S)?)?$'",
    ]


@pytest.mark.parametrize(
    ("mutate", "expected"),
    [
        pytest.param(
            lambda d: d.update(api_version="contractline/v9"),
            "api_version: 'contractline/v0' was expected",
            id="unknown-format-version",
        ),
        pytest.param(
            lambda d: d.update(unexpected=1),
            "<root>: Additional properties are not allowed ('unexpected' was unexpected)",
            id="unknown-top-level-key",
        ),
        pytest.param(
            lambda d: d["identity"].update(version="1.0"),
            "identity.version: '1.0' does not match",
            id="version-not-semver",
        ),
        pytest.param(
            lambda d: d["identity"].update(domain="Sales"),
            "identity.domain: 'Sales' does not match",
            id="identifier-not-snake-case",
        ),
        pytest.param(
            lambda d: d["source"].update(format="csv"),
            "source.format: 'csv' is not one of ['json', 'avro', 'debezium_json']",
            id="format-incompatible-with-adapter",
        ),
        pytest.param(
            lambda d: d["source"].update(adapter="files", mode="incremental", format="csv"),
            "source: 'watermark_column' is a required property",
            id="incremental-without-watermark",
        ),
        pytest.param(
            lambda d: d["source"].update(mode="full", format="json"),
            "source.mode: 'full' is not one of ['change_stream', 'append_stream']",
            id="kafka-full-snapshot",
        ),
        pytest.param(
            lambda d: d["source"].update(adapter="files", mode="change_stream", format="csv"),
            "source.mode: 'change_stream' is not one of ['full', 'incremental', 'append_stream']",
            id="files-change-stream",
        ),
        pytest.param(
            lambda d: d["source"].update(mode="append_stream"),
            "source.mode: 'change_stream' was expected",
            id="debezium-must-be-change-stream",
        ),
        pytest.param(
            lambda d: d["schema"]["columns"][0].update(type="varchar"),
            "schema.columns[0].type: 'varchar' does not match",
            id="unknown-type",
        ),
        pytest.param(
            lambda d: d["schema"]["columns"][0].pop("pii"),
            "schema.columns[0]: 'pii' is a required property",
            id="pii-class-is-mandatory",
        ),
        pytest.param(
            lambda d: d["history"].pop("sequence_column"),
            "history: 'sequence_column' is a required property",
            id="scd2-needs-sequence-column",
        ),
        pytest.param(
            lambda d: d["privacy"]["masking"].update(birth_date={"strategy": "keep"}),
            "privacy.masking.birth_date: 'justification' is a required property",
            id="keep-needs-justification",
        ),
        pytest.param(
            lambda d: d["quality"].append({"type": "not_null", "severity": "block"}),
            "quality[4]: 'column' is a required property",
            id="rule-missing-parameter",
        ),
        pytest.param(
            lambda d: d["quality"].append(
                {"type": "not_null", "column": "email", "severity": "block", "values": [1]}
            ),
            "quality[4]: Unevaluated properties are not allowed ('values' was unexpected)",
            id="rule-unknown-parameter",
        ),
        pytest.param(
            lambda d: d["quality"].append({"type": "range", "column": "email", "severity": "warn"}),
            "quality[4]: {'type': 'range', 'column': 'email', 'severity': 'warn'} "
            "is not valid under any of the given schemas",
            id="range-without-bounds",
        ),
        pytest.param(
            lambda d: d["sla"].update(freshness="1 hour"),
            "sla.freshness: '1 hour' does not match",
            id="freshness-not-iso-duration",
        ),
    ],
)
def test_schema_errors(customers: dict[str, Any], mutate: Any, expected: str) -> None:
    mutate(customers)

    errors = errors_of(customers)

    assert any(e.startswith(expected) for e in errors), errors


@pytest.mark.parametrize(
    ("mutate", "expected"),
    [
        pytest.param(
            lambda d: d["history"].update(type="snapshot"),
            "history.type: snapshot history does not fit a change_stream source",
            id="snapshot-from-change-stream",
        ),
        pytest.param(
            lambda d: d["schema"]["columns"].append(copy.deepcopy(d["schema"]["columns"][0])),
            "schema.columns[8].name: duplicate column 'customer_id'",
            id="duplicate-column",
        ),
        pytest.param(
            lambda d: d["schema"]["columns"][0].update(name="_op"),
            "schema.columns[0].name: '_op': names starting with '_' are reserved",
            id="reserved-prefix",
        ),
        pytest.param(
            lambda d: d["schema"]["columns"].append(
                {"name": "amount", "type": "decimal(40,2)", "nullable": True, "pii": "none"}
            ),
            "schema.columns[8].type: decimal precision must be between 1 and 38",
            id="decimal-precision",
        ),
        pytest.param(
            lambda d: d["schema"]["columns"].append(
                {"name": "amount", "type": "decimal(2,4)", "nullable": True, "pii": "none"}
            ),
            "schema.columns[8].type: decimal scale cannot exceed its precision",
            id="decimal-scale",
        ),
        pytest.param(
            lambda d: d["schema"].update(primary_key=["customer_key"]),
            "schema.primary_key: 'customer_key' is not a column declared in schema.columns",
            id="unknown-primary-key",
        ),
        pytest.param(
            lambda d: column(d, "customer_id").update(nullable=True),
            "schema.primary_key: key column 'customer_id' must not be nullable",
            id="nullable-primary-key",
        ),
        pytest.param(
            lambda d: d["schema"].pop("primary_key"),
            "schema.primary_key: a change_stream source needs a primary_key",
            id="change-stream-without-key",
        ),
        pytest.param(
            lambda d: d["source"].update(
                adapter="files", mode="incremental", format="csv", watermark_column="segment"
            ),
            "source.watermark_column: watermark column must be a date, timestamp or number, "
            "not string",
            id="watermark-not-orderable",
        ),
        pytest.param(
            lambda d: d["history"].update(tracked_columns=["customer_id", "segment"]),
            "history.tracked_columns: 'customer_id' is part of the business key",
            id="tracked-business-key",
        ),
        pytest.param(
            lambda d: d["history"].update(tracked_columns=["segment", "updated_at"]),
            "history.tracked_columns: the sequence column cannot be a tracked column",
            id="tracked-sequence-column",
        ),
        pytest.param(
            lambda d: d["history"].update(sequence_column="segment"),
            "history.sequence_column: sequence column must be a date, timestamp or number",
            id="sequence-not-orderable",
        ),
        pytest.param(
            lambda d: d["history"].update(type="append", tracked_columns=["segment"]),
            "history.tracked_columns: tracked_columns only apply to scd2 history",
            id="tracked-on-append",
        ),
        pytest.param(
            lambda d: d.pop("privacy"),
            "privacy: required because these columns hold personal data: "
            "email, full_name, birth_date",
            id="pii-without-privacy",
        ),
        pytest.param(
            lambda d: d["privacy"]["masking"].pop("email"),
            "privacy.masking.email: column 'email' is direct PII and has no masking",
            id="pii-column-not-masked",
        ),
        pytest.param(
            lambda d: d["privacy"]["masking"].update(
                email={"strategy": "keep", "justification": "needed for marketing"}
            ),
            "privacy.masking.email: direct PII cannot be kept unmasked after bronze",
            id="direct-pii-kept",
        ),
        pytest.param(
            lambda d: (
                column(d, "segment").update(pii="special")
                or d["privacy"]["masking"].update(
                    segment={"strategy": "keep", "justification": "x"}
                )
            ),
            "privacy.masking.segment: special PII cannot be kept unmasked after bronze",
            id="special-pii-kept",
        ),
        pytest.param(
            lambda d: d["privacy"]["masking"].update(phone={"strategy": "redact"}),
            "privacy.masking.phone: 'phone' is not a column declared in schema.columns",
            id="masking-unknown-column",
        ),
        pytest.param(
            lambda d: d["privacy"]["masking"].update(customer_id={"strategy": "drop"}),
            "privacy.masking.customer_id: the erasure key cannot use 'drop'",
            id="erasure-key-dropped",
        ),
        pytest.param(
            lambda d: d["privacy"].update(erasure_key="customer_ref"),
            "privacy.erasure_key: 'customer_ref' is not a column declared in schema.columns",
            id="unknown-erasure-key",
        ),
        pytest.param(
            lambda d: d["privacy"]["retention"].update(column="segment"),
            "privacy.retention.column: retention counts from a date or timestamp column, "
            "not string",
            id="retention-not-temporal",
        ),
        pytest.param(
            lambda d: d["quality"].append(
                {"type": "not_null", "column": "phone", "severity": "block"}
            ),
            "quality[4]: 'phone' is not a column declared in schema.columns",
            id="rule-unknown-column",
        ),
        pytest.param(
            lambda d: d["quality"][3].update(severity="quarantine"),
            "quality[3].severity: row_count_anomaly checks the whole dataset, "
            "so it cannot quarantine individual records",
            id="dataset-rule-quarantine",
        ),
        pytest.param(
            lambda d: d["privacy"]["masking"]["birth_date"].update(params={"to": "day"}),
            "privacy.masking.birth_date.params: generalize on a date or timestamp takes",
            id="generalize-date-unit",
        ),
        pytest.param(
            lambda d: d["privacy"]["masking"].update(
                full_name={"strategy": "generalize", "params": {"bucket": 3}}
            ),
            "privacy.masking.full_name.params: generalize on a string needs 'keep_chars",
            id="generalize-string-params",
        ),
        pytest.param(
            lambda d: d["privacy"]["masking"].update(
                email={"strategy": "redact", "params": {"key": "x"}}
            ),
            "privacy.masking.email.params: 'key' names a secret",
            id="key-on-redact",
        ),
        pytest.param(
            lambda d: d["privacy"]["masking"].update(
                email={"strategy": "hash", "params": {"rounds": 3}}
            ),
            "privacy.masking.email.params: hash takes no parameters: rounds",
            id="unknown-hash-param",
        ),
        pytest.param(
            lambda d: d["quality"][0].update(severity="quarantine"),
            "quality[0].severity: unique on scd2 history is checked on the current rows",
            id="unique-quarantine-keyed",
        ),
        pytest.param(
            lambda d: d["quality"].append(
                {"type": "range", "column": "customer_id", "min": 10, "max": 1, "severity": "warn"}
            ),
            "quality[4]: min (10) is greater than max (1)",
            id="range-min-above-max",
        ),
        pytest.param(
            lambda d: d["quality"].append(
                {"type": "freshness", "column": "segment", "max_age": "PT1H", "severity": "warn"}
            ),
            "quality[4]: freshness needs a date or timestamp column, not string",
            id="freshness-not-temporal",
        ),
    ],
)
def test_semantic_errors(customers: dict[str, Any], mutate: Any, expected: str) -> None:
    mutate(customers)

    errors = errors_of(customers)

    assert any(e.startswith(expected) for e in errors), errors


def test_privacy_is_optional_without_pii(examples_dir: Path) -> None:
    contract = load_contract(examples_dir / "sales" / "orders.yaml")

    assert contract.privacy is None
    assert not contract.has_pii


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("a: [1, 2", "line 1: invalid YAML"),
        ("a: 1\na: 2\n", "line 2: invalid YAML: found duplicate key 'a'"),
        ("- 1\n- 2\n", "<root>: [1, 2] is not of type 'object'"),
    ],
)
def test_parse_text_reports_yaml_problems(text: str, expected: str) -> None:
    contract, errors = parse_text(text)

    assert contract is None
    assert str(errors[0]).startswith(expected)


def test_load_contract_raises_with_every_error(tmp_path: Path) -> None:
    path = tmp_path / "bad.yaml"
    path.write_text("api_version: contractline/v0\n")

    with pytest.raises(InvalidContractError) as excinfo:
        load_contract(path)

    assert len(excinfo.value.errors) == 6
    assert "identity' is a required property" in str(excinfo.value)
