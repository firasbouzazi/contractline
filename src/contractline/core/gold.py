"""Gold models: custom SQL under an output contract with mandatory tests.

A gold model is two files side by side, ``<name>.sql`` and ``<name>.yaml``
(ADR-0013). The SQL reads its inputs only through ``{{ ref('<layer>.<domain>.<entity>') }}``
placeholders, and only the inputs the YAML declares.
"""

import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from functools import cache
from importlib.resources import files
from pathlib import Path
from typing import Any

import yaml
from jsonschema import Draft202012Validator
from referencing import Registry, Resource

from contractline.core.contract import (
    Column,
    Contract,
    PiiClass,
    QualityRule,
    RuleType,
    Severity,
    build_contract,
)
from contractline.core.validation import (
    ContractError,
    InvalidContractError,
    format_location,
    get_json_schema,
)
from contractline.core.yaml_loader import load_yaml

GOLD_SCHEMA_RESOURCE = "gold-v0.json"
REF_PATTERN = re.compile(r"\{\{\s*ref\(\s*'([a-z_]+\.[a-z0-9_]+\.[a-z0-9_]+)'\s*\)\s*\}\}")


@dataclass(frozen=True)
class InputRef:
    layer: str
    domain: str
    entity: str

    @classmethod
    def parse(cls, text: str) -> "InputRef":
        layer, domain, entity = text.split(".")
        return cls(layer, domain, entity)

    def __str__(self) -> str:
        return f"{self.layer}.{self.domain}.{self.entity}"


@dataclass(frozen=True)
class GoldTest:
    name: str
    given: Mapping[str, list[dict[str, Any]]]
    expect: list[dict[str, Any]]


@dataclass(frozen=True)
class GoldModel:
    name: str
    domain: str
    owner: str
    version: str
    description: str
    inputs: tuple[InputRef, ...]
    columns: tuple[Column, ...]
    primary_key: tuple[str, ...]
    quality: tuple[QualityRule, ...]
    tests: tuple[GoldTest, ...]
    sql: str
    contract: Contract
    """The output contract as a data contract, so quality blocks can be reused."""

    def column(self, name: str) -> Column | None:
        return next((c for c in self.columns if c.name == name), None)


@cache
def _gold_validator() -> Draft202012Validator:
    contract_schema = get_json_schema()
    gold_text = files("contractline.core.schemas").joinpath(GOLD_SCHEMA_RESOURCE).read_text()
    gold_schema = json.loads(gold_text)
    registry: Registry[Any] = Registry().with_resources(
        [
            (contract_schema["$id"], Resource.from_contents(contract_schema)),
            (gold_schema["$id"], Resource.from_contents(gold_schema)),
        ]
    )
    return Draft202012Validator(gold_schema, registry=registry)


def find_refs(sql: str) -> list[str]:
    return REF_PATTERN.findall(sql)


def check_gold_semantics(model: GoldModel) -> list[ContractError]:
    errors: list[ContractError] = []
    names = [c.name for c in model.columns]
    for i, column in enumerate(model.columns):
        if names.count(column.name) > 1:
            errors.append(ContractError(f"schema.columns[{i}].name", "duplicate column"))
        if column.name.startswith("_"):
            errors.append(
                ContractError(f"schema.columns[{i}].name", "names starting with '_' are reserved")
            )
        if column.pii in (PiiClass.DIRECT, PiiClass.SPECIAL):
            errors.append(
                ContractError(
                    f"schema.columns[{i}].pii",
                    f"gold cannot expose {column.pii} PII; it reads masked silver data",
                )
            )
    for name in model.primary_key:
        if name not in names:
            errors.append(ContractError("schema.primary_key", f"'{name}' is not a declared column"))
    for i, rule in enumerate(model.quality):
        for name in rule.columns:
            if name not in names:
                errors.append(ContractError(f"quality[{i}]", f"'{name}' is not a declared column"))
        if rule.severity is Severity.QUARANTINE:
            errors.append(
                ContractError(
                    f"quality[{i}].severity",
                    "gold has no quarantine: a model's output passes (warn) or fails (block)",
                )
            )
        if rule.type is RuleType.REFERENTIAL_INTEGRITY:
            errors.append(ContractError(f"quality[{i}]", "referential_integrity is silver-only"))

    declared = {str(i) for i in model.inputs}
    for i, ref in enumerate(model.inputs):
        if ref.layer == "gold" and ref.domain != model.domain:
            errors.append(
                ContractError(f"inputs[{i}]", "gold models may only read gold from their domain")
            )
        if ref.layer == "gold" and ref.entity == model.name:
            errors.append(ContractError(f"inputs[{i}]", "a model cannot read itself"))
    used = set(find_refs(model.sql))
    for name in sorted(used - declared):
        errors.append(ContractError("sql", f"reads {name}, which is not declared in inputs"))
    for name in sorted(declared - used):
        errors.append(ContractError("inputs", f"{name} is declared but the SQL never reads it"))
    for i, test in enumerate(model.tests):
        for name in test.given:
            if name not in declared:
                errors.append(ContractError(f"tests[{i}].given", f"{name} is not a declared input"))
        if len({tuple(sorted(row)) for row in test.expect}) > 1:
            errors.append(
                ContractError(f"tests[{i}].expect", "every expected row must list the same columns")
            )
        for j, row in enumerate(test.expect):
            for key in row:
                if key not in names:
                    errors.append(
                        ContractError(f"tests[{i}].expect[{j}]", f"'{key}' is not an output column")
                    )
    return errors


def build_gold_model(doc: Mapping[str, Any], sql: str) -> GoldModel:
    # Express the output contract as a data contract (append history: every
    # output row is checked on its own) to reuse its model and quality blocks.
    identity = doc["identity"]
    as_contract = {
        "identity": dict(identity),
        "source": {"adapter": "files", "mode": "full", "location": "gold", "format": "parquet"},
        "schema": {
            "columns": doc["schema"]["columns"],
            "drift_policy": "fail",
            **(
                {"primary_key": doc["schema"]["primary_key"]}
                if "primary_key" in doc["schema"]
                else {}
            ),
        },
        "history": {"type": "append"},
        "quality": doc["quality"],
        "sla": {"freshness": "P1D"},
    }
    parts = build_contract(as_contract)
    return GoldModel(
        name=identity["name"],
        domain=identity["domain"],
        owner=identity["owner"],
        version=identity["version"],
        description=identity["description"],
        inputs=tuple(InputRef.parse(i) for i in doc["inputs"]),
        columns=parts.schema.columns,
        primary_key=tuple(doc["schema"].get("primary_key", ())),
        quality=parts.quality,
        tests=tuple(GoldTest(t["name"], t["given"], t["expect"]) for t in doc["tests"]),
        sql=sql,
        contract=parts,
    )


def parse_gold(text: str, sql: str) -> tuple[GoldModel | None, list[ContractError]]:
    try:
        doc = load_yaml(text)
    except yaml.YAMLError as exc:
        return None, [ContractError("<yaml>", f"invalid YAML: {exc}")]
    schema_errors = sorted(
        _gold_validator().iter_errors(doc), key=lambda e: (list(map(str, e.path)), e.message)
    )
    if schema_errors:
        return None, [ContractError(format_location(e.path), e.message) for e in schema_errors]
    model = build_gold_model(doc, sql)
    errors = check_gold_semantics(model)
    return (None, errors) if errors else (model, [])


def load_gold_model(yaml_path: Path) -> GoldModel:
    sql_path = yaml_path.with_suffix(".sql")
    if not sql_path.is_file():
        raise InvalidContractError(
            str(yaml_path), [ContractError("<file>", f"missing SQL file {sql_path.name}")]
        )
    model, errors = parse_gold(yaml_path.read_text("utf-8"), sql_path.read_text("utf-8"))
    if model is None:
        raise InvalidContractError(str(yaml_path), errors)
    return model
