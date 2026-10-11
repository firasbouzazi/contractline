import copy
from pathlib import Path
from typing import Any

import pytest

from contractline.core.contract import Contract
from contractline.core.lint import (
    find_abbreviations,
    lint_branch,
    lint_commit_message,
    lint_contract,
    lint_pull_request_title,
)
from contractline.core.project_config import ProjectConfig
from contractline.core.validation import validate_document
from contractline.core.yaml_loader import load_yaml

CONFIG = ProjectConfig(
    domains=frozenset({"sales", "finance"}),
    glossary={"id": "identifier", "ts": "allowed here for the test"},
    banned_abbreviations={"cust": "customer", "amt": "amount", "ts": "timestamp"},
)
GOOD_PATH = Path("contracts/sales/customers.yaml")


@pytest.fixture
def customers(examples_dir: Path) -> dict[str, Any]:
    doc: dict[str, Any] = load_yaml((examples_dir / "sales" / "customers.yaml").read_text())
    return doc


def build(doc: dict[str, Any]) -> Contract:
    contract, errors = validate_document(doc)
    assert contract is not None, errors
    return contract


def lint_messages(doc: dict[str, Any], path: Path = GOOD_PATH) -> list[str]:
    return [
        f"{i.location}: {i.message} [{i.rule}]" for i in lint_contract(path, build(doc), CONFIG)
    ]


def add_column(doc: dict[str, Any], name: str, column_type: str) -> None:
    doc["schema"]["columns"].append(
        {"name": name, "type": column_type, "nullable": True, "pii": "none"}
    )


def test_example_contract_is_clean(customers: dict[str, Any]) -> None:
    assert lint_messages(customers) == []


def test_find_abbreviations_respects_glossary() -> None:
    assert find_abbreviations("cust_amt_ts_id", CONFIG) == ["cust", "amt"]


@pytest.mark.parametrize(
    ("path", "expected"),
    [
        (
            Path("pipelines/customers.yaml"),
            ": contracts live at contracts/<domain>/<entity>.yaml [naming/contract-path]",
        ),
        (
            Path("contracts/finance/customers.yaml"),
            ": path implies finance/customers but the contract declares sales/customers; "
            "move it to contracts/sales/customers.yaml [naming/contract-path]",
        ),
        (
            Path("contracts/sales/customers.yml"),
            ": contract files use the .yaml extension [naming/contract-path]",
        ),
        (
            Path("contracts/sales/extra/customers.yaml"),
            ": contracts live at contracts/<domain>/<entity>.yaml [naming/contract-path]",
        ),
    ],
)
def test_contract_path(customers: dict[str, Any], path: Path, expected: str) -> None:
    assert lint_messages(customers, path) == [expected]


def test_nested_contracts_directory_is_accepted(customers: dict[str, Any]) -> None:
    assert lint_messages(customers, Path("examples/contracts/sales/customers.yaml")) == []


@pytest.mark.parametrize(
    ("mutate", "expected"),
    [
        pytest.param(
            lambda d: d["identity"].update(domain="hr"),
            "identity.domain: 'hr' is not a known domain (finance, sales) [naming/domain]",
            id="unknown-domain",
        ),
        pytest.param(
            lambda d: add_column(d, "cust_name", "string"),
            "schema.columns[8].name: 'cust_name' uses the abbreviation 'cust'; write 'customer' "
            "or add 'cust' to the glossary [naming/abbreviation]",
            id="abbreviation",
        ),
        pytest.param(
            lambda d: add_column(d, "deleted", "timestamp"),
            "schema.columns[8].name: timestamp column 'deleted' must end in _at "
            "[naming/timestamp-suffix]",
            id="timestamp-without-suffix",
        ),
        pytest.param(
            lambda d: add_column(d, "deleted_at", "date"),
            "schema.columns[8].name: 'deleted_at' ends in _at but is date "
            "[naming/timestamp-suffix]",
            id="suffix-without-timestamp",
        ),
        pytest.param(
            lambda d: add_column(d, "signup_day", "date"),
            "schema.columns[8].name: date column 'signup_day' must end in _date "
            "[naming/date-suffix]",
            id="date-without-suffix",
        ),
        pytest.param(
            lambda d: add_column(d, "signup_date", "string"),
            "schema.columns[8].name: 'signup_date' ends in _date but is string "
            "[naming/date-suffix]",
            id="suffix-without-date",
        ),
        pytest.param(
            lambda d: add_column(d, "active", "boolean"),
            "schema.columns[8].name: boolean column 'active' must start with is_ or has_ "
            "[naming/boolean-prefix]",
            id="boolean-without-prefix",
        ),
        pytest.param(
            lambda d: add_column(d, "has_orders", "integer"),
            "schema.columns[8].name: 'has_orders' reads as a boolean but is integer "
            "[naming/boolean-prefix]",
            id="prefix-without-boolean",
        ),
        pytest.param(
            lambda d: d["source"].update(location="crm.customers"),
            "source.location: CDC topic 'crm.customers' must be "
            "cdc.<source_system>.<schema>.<table> [naming/topic]",
            id="cdc-topic",
        ),
        pytest.param(
            lambda d: d["source"].update(format="json", mode="append_stream", location="orders"),
            "source.location: topic 'orders' must be <domain>.<entity>.v<major> [naming/topic]",
            id="business-topic",
        ),
        pytest.param(
            lambda d: d["tags"].update(pii="false"),
            "tags.pii: 'pii' is set by the harness from the contract and cannot be overridden "
            "[naming/reserved-tag]",
            id="reserved-tag",
        ),
    ],
)
def test_contract_rules(customers: dict[str, Any], mutate: Any, expected: str) -> None:
    doc = copy.deepcopy(customers)
    mutate(doc)

    assert expected in lint_messages(doc)


def test_glossary_word_is_allowed(customers: dict[str, Any]) -> None:
    add_column(customers, "source_ts_id", "string")

    assert lint_messages(customers) == []


def test_files_source_has_no_topic_rule(customers: dict[str, Any]) -> None:
    customers["source"].update(adapter="files", format="csv", mode="full", location="/data/x")

    assert lint_messages(customers) == []


@pytest.mark.parametrize(
    "branch",
    [
        "main",
        "agent/DATA-231-sales-customers",
        "agent/FIN-7-finance-exchange_rates",
        "feat/contract-diff",
        "fix/null-handling",
        "chore/bump-ruff",
        "docs/adr-0009",
        "dependabot/pip/ruff-0.7.0",
    ],
)
def test_valid_branches(branch: str) -> None:
    assert lint_branch(branch, CONFIG) == []


@pytest.mark.parametrize(
    ("branch", "message"),
    [
        ("my-branch", "must be agent/<TICKET>-<domain>-<entity>"),
        ("feature/x", "must be agent/<TICKET>-<domain>-<entity>"),
        ("agent/customers", "must be agent/<TICKET>-<domain>-<entity>"),
        ("agent/DATA-231-crm-customers", "name a known domain and an entity"),
        ("agent/DATA-231-sales", "name a known domain and an entity"),
    ],
)
def test_invalid_branches(branch: str, message: str) -> None:
    [issue] = lint_branch(branch, CONFIG)

    assert message in issue.message


@pytest.mark.parametrize(
    ("title", "branch"),
    [
        ("[sales/customers] add SCD2 on segment", "agent/DATA-231-sales-customers"),
        ("[sales/customers] add SCD2 on segment", None),
        ("feat(core): add contract diff", "feat/contract-diff"),
        ("fix: handle empty files", None),
    ],
)
def test_valid_pull_request_titles(title: str, branch: str | None) -> None:
    assert lint_pull_request_title(title, branch, CONFIG) == []


@pytest.mark.parametrize(
    ("title", "branch", "message"),
    [
        ("Add customers", None, "must be '[<domain>/<entity>] <summary>' or a Conventional"),
        ("feat: add customers", "agent/DATA-231-sales-customers", "agent PR titles must be"),
        ("[crm/customers] add", None, "'crm' is not a known domain"),
        (
            "[sales/orders] add",
            "agent/DATA-231-sales-customers",
            "title names sales/orders but the branch is 'agent/DATA-231-sales-customers'",
        ),
    ],
)
def test_invalid_pull_request_titles(title: str, branch: str | None, message: str) -> None:
    [issue] = lint_pull_request_title(title, branch, CONFIG)

    assert message in issue.message


@pytest.mark.parametrize(
    "message",
    [
        "feat(sales): add customers contract",
        "fix!: change watermark handling\n\nBREAKING CHANGE: ...",
        "# comment from git\nchore: bump ruff",
        "Merge branch 'main' into feat/x",
        "fixup! feat: add x",
    ],
)
def test_valid_commit_messages(message: str) -> None:
    assert lint_commit_message(message) == []


@pytest.mark.parametrize("message", ["Add customers", "feat:missing space", "feature: x", ""])
def test_invalid_commit_messages(message: str) -> None:
    [issue] = lint_commit_message(message)

    assert issue.rule == "naming/commit"
