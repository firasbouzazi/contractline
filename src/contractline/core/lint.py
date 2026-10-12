"""Naming convention checks behind ``contractline lint`` (ADR-0007).

Each check returns ``LintIssue`` objects; nothing is fixed automatically and
nothing can be silenced per file. Conventions change by changing
``contractline.yaml`` or this module, both owned by the harness maintainer.
"""

import re
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

from contractline.core.contract import Contract, SourceAdapter
from contractline.core.naming import RESERVED_TAGS
from contractline.core.project_config import ProjectConfig

CONTRACTS_DIR = "contracts"

_TOPIC = re.compile(r"^(?P<domain>[a-z][a-z0-9_]*)\.(?P<entity>[a-z][a-z0-9_]*)\.v[1-9][0-9]*$")
_CDC_TOPIC = re.compile(r"^cdc\.[a-z][a-z0-9_]*\.[a-z][a-z0-9_]*\.[a-z][a-z0-9_]*$")
_TICKET = r"[A-Z][A-Z0-9]*-[0-9]+"
_AGENT_BRANCH = re.compile(rf"^agent/(?P<ticket>{_TICKET})-(?P<rest>[a-z][a-z0-9_-]*)$")
_HUMAN_BRANCH = re.compile(r"^(feat|fix|chore|docs)/[a-z0-9][a-z0-9._-]*$")
_TOOL_BRANCH = re.compile(r"^dependabot/")
_COMMIT_TYPES = "feat|fix|chore|docs|refactor|test|ci|build|perf|revert"
_CONVENTIONAL = re.compile(rf"^({_COMMIT_TYPES})(\([a-z0-9][a-z0-9_/-]*\))?!?: \S.*$")
_PIPELINE_TITLE = re.compile(r"^\[(?P<domain>[a-z][a-z0-9_]*)/(?P<entity>[a-z][a-z0-9_]*)\] \S.*$")


@dataclass(frozen=True)
class LintIssue:
    subject: str
    """File path, or what was checked (``branch``, ``pr-title``, ``commit``)."""
    location: str
    rule: str
    message: str

    def __str__(self) -> str:
        where = f"{self.subject}: {self.location}" if self.location else self.subject
        return f"{where}: {self.message} [{self.rule}]"


def find_abbreviations(name: str, config: ProjectConfig) -> list[str]:
    """Words of a snake_case name that are banned abbreviations not in the glossary."""
    return [
        word
        for word in name.split("_")
        if word in config.banned_abbreviations and word not in config.glossary
    ]


def _check_word_list(
    subject: str, location: str, name: str, config: ProjectConfig
) -> Iterable[LintIssue]:
    for word in find_abbreviations(name, config):
        yield LintIssue(
            subject,
            location,
            "naming/abbreviation",
            f"'{name}' uses the abbreviation '{word}'; write "
            f"'{config.banned_abbreviations[word]}' or add '{word}' to the glossary",
        )


def find_expected_path(path: Path) -> tuple[str, str] | None:
    """The (domain, entity) a contract path implies: ``.../contracts/<domain>/<entity>.yaml``."""
    parts = path.parts
    if CONTRACTS_DIR not in parts:
        return None
    index = len(parts) - 1 - parts[::-1].index(CONTRACTS_DIR)
    rest = parts[index + 1 :]
    if len(rest) != 2:
        return None
    return rest[0], Path(rest[1]).stem


def lint_contract(path: Path, contract: Contract, config: ProjectConfig) -> list[LintIssue]:
    subject = str(path)
    issues: list[LintIssue] = []

    def issue(location: str, rule: str, message: str) -> None:
        issues.append(LintIssue(subject, location, rule, message))

    domain, entity = contract.identity.domain, contract.identity.name

    if path.suffix != ".yaml":
        issue("", "naming/contract-path", "contract files use the .yaml extension")
    expected = find_expected_path(path)
    if expected is None:
        issue("", "naming/contract-path", "contracts live at contracts/<domain>/<entity>.yaml")
    elif expected != (domain, entity):
        issue(
            "",
            "naming/contract-path",
            f"path implies {expected[0]}/{expected[1]} but the contract declares "
            f"{domain}/{entity}; move it to contracts/{domain}/{entity}.yaml",
        )

    if domain not in config.domains:
        allowed = ", ".join(sorted(config.domains))
        issue("identity.domain", "naming/domain", f"'{domain}' is not a known domain ({allowed})")
    issues.extend(_check_word_list(subject, "identity.domain", domain, config))
    issues.extend(_check_word_list(subject, "identity.name", entity, config))

    for i, column in enumerate(contract.schema.columns):
        location = f"schema.columns[{i}].name"
        name = column.name
        issues.extend(_check_word_list(subject, location, name, config))
        if column.type == "timestamp" and not name.endswith("_at"):
            issue(location, "naming/timestamp-suffix", f"timestamp column '{name}' must end in _at")
        if column.type != "timestamp" and name.endswith("_at"):
            issue(location, "naming/timestamp-suffix", f"'{name}' ends in _at but is {column.type}")
        if column.type == "date" and not name.endswith("_date"):
            issue(location, "naming/date-suffix", f"date column '{name}' must end in _date")
        if column.type != "date" and name.endswith("_date"):
            issue(location, "naming/date-suffix", f"'{name}' ends in _date but is {column.type}")
        is_boolean_name = name.startswith(("is_", "has_"))
        if column.type == "boolean" and not is_boolean_name:
            issue(
                location,
                "naming/boolean-prefix",
                f"boolean column '{name}' must start with is_ or has_",
            )
        if column.type != "boolean" and is_boolean_name:
            issue(
                location,
                "naming/boolean-prefix",
                f"'{name}' reads as a boolean but is {column.type}",
            )

    if contract.source.adapter is SourceAdapter.KAFKA:
        location = contract.source.location
        if contract.source.format == "debezium_json":
            if not _CDC_TOPIC.match(location):
                issue(
                    "source.location",
                    "naming/topic",
                    f"CDC topic '{location}' must be cdc.<source_system>.<schema>.<table>",
                )
        elif not _TOPIC.match(location):
            issue(
                "source.location",
                "naming/topic",
                f"topic '{location}' must be <domain>.<entity>.v<major>",
            )

    for key in contract.tags or {}:
        if key in RESERVED_TAGS:
            issue(
                f"tags.{key}",
                "naming/reserved-tag",
                f"'{key}' is set by the harness from the contract and cannot be overridden",
            )
    return issues


def lint_branch(branch: str, config: ProjectConfig) -> list[LintIssue]:
    if branch == "main" or _HUMAN_BRANCH.match(branch) or _TOOL_BRANCH.match(branch):
        return []
    agent = _AGENT_BRANCH.match(branch)
    if agent is None:
        return [
            LintIssue(
                "branch",
                "",
                "naming/branch",
                f"'{branch}' must be agent/<TICKET>-<domain>-<entity>, "
                "feat/<topic>, fix/<topic>, chore/<topic> or docs/<topic>",
            )
        ]
    domain, _, entity = agent["rest"].partition("-")
    if domain not in config.domains or not entity:
        return [
            LintIssue(
                "branch",
                "",
                "naming/branch",
                f"'{branch}': after the ticket, agent branches name a known domain and an "
                "entity, e.g. agent/DATA-231-sales-customers",
            )
        ]
    return []


def lint_pull_request_title(
    title: str, branch: str | None, config: ProjectConfig
) -> list[LintIssue]:
    """Pipeline PRs: ``[<domain>/<entity>] summary``. Harness PRs: Conventional Commit form."""
    pipeline = _PIPELINE_TITLE.match(title)
    is_agent = branch is not None and branch.startswith("agent/")
    if pipeline is None:
        if is_agent:
            return [
                LintIssue(
                    "pr-title",
                    "",
                    "naming/pr-title",
                    f"'{title}': agent PR titles must be '[<domain>/<entity>] <summary>'",
                )
            ]
        if _CONVENTIONAL.match(title):
            return []
        return [
            LintIssue(
                "pr-title",
                "",
                "naming/pr-title",
                f"'{title}' must be '[<domain>/<entity>] <summary>' or a Conventional Commit "
                "header such as 'feat(core): add contract diff'",
            )
        ]
    issues: list[LintIssue] = []
    if pipeline["domain"] not in config.domains:
        issues.append(
            LintIssue(
                "pr-title", "", "naming/pr-title", f"'{pipeline['domain']}' is not a known domain"
            )
        )
    if is_agent and branch is not None:
        expected = f"-{pipeline['domain']}-{pipeline['entity']}"
        if not branch.endswith(expected):
            issues.append(
                LintIssue(
                    "pr-title",
                    "",
                    "naming/pr-title",
                    f"title names {pipeline['domain']}/{pipeline['entity']} but the branch "
                    f"is '{branch}'",
                )
            )
    return issues


def lint_commit_message(message: str) -> list[LintIssue]:
    lines = [line for line in message.splitlines() if not line.startswith("#")]
    header = lines[0] if lines else ""
    if header.startswith(("Merge ", 'Revert "', "fixup! ", "squash! ")):
        return []
    if _CONVENTIONAL.match(header):
        return []
    return [
        LintIssue(
            "commit",
            "",
            "naming/commit",
            f"'{header}' is not a Conventional Commit header (<type>(<scope>): <summary>, "
            f"type one of {_COMMIT_TYPES.replace('|', ', ')})",
        )
    ]
