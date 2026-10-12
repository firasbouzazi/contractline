"""Contract validation: JSON Schema (structure) then semantic checks.

The JSON Schema in ``schemas/contract-v0.json`` checks shape and allowed
values. Rules that need to look across sections (a key must name a declared
column, PII must be masked, ...) live in ``check_semantics``. Every problem is
reported, not only the first, each with a dotted location such as
``schema.columns[2].type``.
"""

import json
import re
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from functools import cache
from importlib.resources import files
from pathlib import Path
from typing import Any

import yaml
from jsonschema import Draft202012Validator

from contractline.core.contract import (
    Contract,
    HistoryType,
    IngestMode,
    MaskingStrategy,
    PiiClass,
    RuleType,
    Severity,
    build_contract,
)
from contractline.core.yaml_loader import load_yaml

SCHEMA_RESOURCE = "contract-v0.json"
RESERVED_PREFIX = "_"
MAX_DECIMAL_PRECISION = 38

_DECIMAL = re.compile(r"^decimal\((\d+),(\d+)\)$")
_ORDERABLE_TYPES = {"integer", "long", "date", "timestamp"}
_TEMPORAL_TYPES = {"date", "timestamp"}
_DATASET_LEVEL_RULES = {RuleType.ROW_COUNT_ANOMALY, RuleType.FRESHNESS}
_IRREVERSIBLE_FOR_ERASURE = {
    MaskingStrategy.GENERALIZE,
    MaskingStrategy.REDACT,
    MaskingStrategy.DROP,
}


@dataclass(frozen=True)
class ContractError:
    location: str
    message: str

    def __str__(self) -> str:
        return f"{self.location}: {self.message}"


class InvalidContractError(ValueError):
    def __init__(self, source: str, errors: Sequence[ContractError]) -> None:
        self.source = source
        self.errors = tuple(errors)
        details = "\n".join(f"  {e}" for e in self.errors)
        super().__init__(f"{source} is not a valid contract:\n{details}")


@cache
def get_json_schema() -> dict[str, Any]:
    text = files("contractline.core.schemas").joinpath(SCHEMA_RESOURCE).read_text("utf-8")
    schema: dict[str, Any] = json.loads(text)
    return schema


def format_location(path: Iterable[str | int]) -> str:
    location = ""
    for part in path:
        if isinstance(part, int):
            location += f"[{part}]"
        else:
            location += f".{part}" if location else part
    return location or "<root>"


def check_schema(doc: Any) -> list[ContractError]:
    validator = Draft202012Validator(get_json_schema())
    errors = sorted(validator.iter_errors(doc), key=lambda e: (list(map(str, e.path)), e.message))
    return [ContractError(format_location(e.path), e.message) for e in errors]


def _is_orderable(column_type: str) -> bool:
    return column_type in _ORDERABLE_TYPES or bool(_DECIMAL.match(column_type))


def check_semantics(contract: Contract) -> list[ContractError]:
    """Cross-section rules the JSON Schema cannot express."""
    errors: list[ContractError] = []

    def error(location: str, message: str) -> None:
        errors.append(ContractError(location, message))

    columns = contract.schema.columns
    types = {c.name: c.type for c in columns}

    seen: set[str] = set()
    for i, column in enumerate(columns):
        location = f"schema.columns[{i}]"
        if column.name in seen:
            error(f"{location}.name", f"duplicate column '{column.name}'")
        seen.add(column.name)
        if column.name.startswith(RESERVED_PREFIX):
            error(
                f"{location}.name",
                f"'{column.name}': names starting with '{RESERVED_PREFIX}' are reserved "
                "for harness metadata columns",
            )
        decimal = _DECIMAL.match(column.type)
        if decimal:
            precision, scale = int(decimal.group(1)), int(decimal.group(2))
            if not 1 <= precision <= MAX_DECIMAL_PRECISION:
                error(
                    f"{location}.type",
                    f"decimal precision must be between 1 and {MAX_DECIMAL_PRECISION}",
                )
            if scale > precision:
                error(f"{location}.type", "decimal scale cannot exceed its precision")

    def check_refs(location: str, names: Iterable[str]) -> None:
        for name in names:
            if name not in types:
                error(location, f"'{name}' is not a column declared in schema.columns")

    def check_not_nullable(location: str, names: Iterable[str]) -> None:
        for name in names:
            column = contract.schema.column(name)
            if column is not None and column.nullable:
                error(location, f"key column '{name}' must not be nullable")

    # Keys and ordering columns.
    check_refs("schema.primary_key", contract.schema.primary_key)
    check_not_nullable("schema.primary_key", contract.schema.primary_key)

    source = contract.source
    if source.watermark_column is not None:
        check_refs("source.watermark_column", [source.watermark_column])
        wm_type = types.get(source.watermark_column)
        if wm_type is not None and not _is_orderable(wm_type):
            error(
                "source.watermark_column",
                f"watermark column must be a date, timestamp or number, not {wm_type}",
            )
    if source.mode is IngestMode.CHANGE_STREAM and not contract.schema.primary_key:
        error("schema.primary_key", "a change_stream source needs a primary_key")

    history = contract.history
    check_refs("history.business_key", history.business_key)
    check_not_nullable("history.business_key", history.business_key)
    check_refs("history.tracked_columns", history.tracked_columns)
    for name in set(history.tracked_columns) & set(history.business_key):
        error("history.tracked_columns", f"'{name}' is part of the business key")
    if history.sequence_column is not None:
        check_refs("history.sequence_column", [history.sequence_column])
        seq_type = types.get(history.sequence_column)
        if seq_type is not None and not _is_orderable(seq_type):
            error(
                "history.sequence_column",
                f"sequence column must be a date, timestamp or number, not {seq_type}",
            )
        if history.sequence_column in history.tracked_columns:
            error("history.tracked_columns", "the sequence column cannot be a tracked column")
    if history.type is HistoryType.APPEND and history.tracked_columns:
        error("history.tracked_columns", "tracked_columns only apply to scd2 history")

    # Privacy.
    privacy = contract.privacy
    pii_columns = [c for c in columns if c.pii is not PiiClass.NONE]
    if pii_columns and privacy is None:
        names = ", ".join(c.name for c in pii_columns)
        error("privacy", f"required because these columns hold personal data: {names}")
    if privacy is not None:
        check_refs("privacy.erasure_key", [privacy.erasure_key])
        check_refs("privacy.retention.column", [privacy.retention.column])
        retention_type = types.get(privacy.retention.column)
        if retention_type is not None and retention_type not in _TEMPORAL_TYPES:
            error(
                "privacy.retention.column",
                f"retention counts from a date or timestamp column, not {retention_type}",
            )
        for name in privacy.masking:
            check_refs(f"privacy.masking.{name}", [name])
        for column in pii_columns:
            masking = privacy.masking.get(column.name)
            location = f"privacy.masking.{column.name}"
            if masking is None:
                error(location, f"column '{column.name}' is {column.pii} PII and has no masking")
            elif masking.strategy is MaskingStrategy.KEEP and column.pii in (
                PiiClass.DIRECT,
                PiiClass.SPECIAL,
            ):
                error(
                    location,
                    f"{column.pii} PII cannot be kept unmasked after bronze; "
                    "use hash, tokenize, redact or drop",
                )
        erasure_masking = privacy.masking.get(privacy.erasure_key)
        if erasure_masking is not None and erasure_masking.strategy in _IRREVERSIBLE_FOR_ERASURE:
            error(
                f"privacy.masking.{privacy.erasure_key}",
                f"the erasure key cannot use '{erasure_masking.strategy}': erasure requests "
                "must still find the subject's rows (use hash, tokenize or keep)",
            )

    # Quality rules.
    for i, rule in enumerate(contract.quality):
        location = f"quality[{i}]"
        check_refs(location, rule.columns)
        if rule.type in _DATASET_LEVEL_RULES and rule.severity is Severity.QUARANTINE:
            error(
                f"{location}.severity",
                f"{rule.type} checks the whole dataset, so it cannot quarantine "
                "individual records; use warn or block",
            )
        if rule.type is RuleType.RANGE:
            low, high = rule.params.get("min"), rule.params.get("max")
            if low is not None and high is not None and type(low) is type(high) and low > high:
                error(location, f"min ({low}) is greater than max ({high})")
        if rule.type is RuleType.FRESHNESS:
            for name in rule.columns:
                if name in types and types[name] not in _TEMPORAL_TYPES:
                    error(
                        location, f"freshness needs a date or timestamp column, not {types[name]}"
                    )

    return errors


def validate_document(doc: Any) -> tuple[Contract | None, list[ContractError]]:
    """Validate a parsed document. Returns the contract only when there are no errors."""
    errors = check_schema(doc)
    if errors:
        return None, errors
    contract = build_contract(doc)
    errors = check_semantics(contract)
    return (None, errors) if errors else (contract, [])


def parse_text(text: str) -> tuple[Contract | None, list[ContractError]]:
    try:
        doc = load_yaml(text)
    except yaml.YAMLError as exc:
        mark = getattr(exc, "problem_mark", None)
        location = f"line {mark.line + 1}" if mark is not None else "<yaml>"
        problem = getattr(exc, "problem", None) or str(exc)
        return None, [ContractError(location, f"invalid YAML: {problem}")]
    return validate_document(doc)


def load_contract(path: Path) -> Contract:
    """Load and validate a contract file, raising ``InvalidContractError`` on any problem."""
    contract, errors = parse_text(path.read_text("utf-8"))
    if contract is None:
        raise InvalidContractError(str(path), errors)
    return contract
