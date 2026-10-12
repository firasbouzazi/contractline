"""Find a project's contracts and gold models and order them for running."""

from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path

from contractline.core.contract import Contract, RuleType
from contractline.core.gold import GoldModel, load_gold_model
from contractline.core.validation import load_contract
from contractline.core.yaml_loader import load_yaml

GOLD_API_VERSION = "contractline/gold/v0"


class ProjectError(ValueError):
    pass


@dataclass(frozen=True)
class Project:
    contracts: tuple[Contract, ...]
    gold_models: tuple[GoldModel, ...]

    def find_contract(self, domain: str, entity: str) -> Contract:
        for contract in self.contracts:
            if (contract.identity.domain, contract.identity.name) == (domain, entity):
                return contract
        raise ProjectError(f"no contract for {domain}.{entity}")

    def find_gold(self, domain: str, name: str) -> GoldModel:
        for model in self.gold_models:
            if (model.domain, model.name) == (domain, name):
                return model
        raise ProjectError(f"no gold model {domain}.{name}")


# Folders of generated platform files (Dags, bundles): never contracts.
GENERATED_FOLDERS = frozenset({"airflow", "databricks"})


def find_project_files(paths: Iterable[Path]) -> list[Path]:
    """YAML files under ``paths`` (explicit files are always included)."""
    found: list[Path] = []
    for path in paths:
        if path.is_dir():
            found.extend(
                sorted(
                    p
                    for p in path.rglob("*")
                    if p.suffix in (".yaml", ".yml")
                    and not GENERATED_FOLDERS & set(p.relative_to(path).parts[:-1])
                )
            )
        else:
            found.append(path)
    return found


def load_project(paths: Iterable[Path]) -> Project:
    """Load every contract and gold model under ``paths`` (raises on invalid files)."""
    contracts: list[Contract] = []
    models: list[GoldModel] = []
    for file in find_project_files(paths):
        doc = load_yaml(file.read_text("utf-8"))
        if isinstance(doc, dict) and doc.get("api_version") == GOLD_API_VERSION:
            models.append(load_gold_model(file))
        else:
            contracts.append(load_contract(file))
    names = [(c.identity.domain, c.identity.name) for c in contracts]
    duplicates = {n for n in names if names.count(n) > 1}
    if duplicates:
        raise ProjectError(f"several contracts define {sorted(duplicates)}")
    return Project(tuple(order_contracts(contracts)), tuple(order_gold_models(models)))


def order_contracts(contracts: list[Contract]) -> list[Contract]:
    """Referenced datasets first, so referential integrity sees fresh data."""
    by_name = {f"{c.identity.domain}.{c.identity.name}": c for c in contracts}

    def dependencies(contract: Contract) -> list[str]:
        refs = [
            ".".join(rule.params["references"].split(".")[:2])
            for rule in contract.quality
            if rule.type is RuleType.REFERENTIAL_INTEGRITY
        ]
        return [
            r
            for r in refs
            if r in by_name and r != f"{contract.identity.domain}.{contract.identity.name}"
        ]

    ordered = _topological_order(by_name, lambda name: dependencies(by_name[name]))
    return [by_name[n] for n in ordered]


def order_gold_models(models: list[GoldModel]) -> list[GoldModel]:
    by_name = {f"{m.domain}.{m.name}": m for m in models}

    def dependencies(name: str) -> list[str]:
        model = by_name[name]
        return [
            f"{i.domain}.{i.entity}"
            for i in model.inputs
            if i.layer == "gold" and f"{i.domain}.{i.entity}" in by_name
        ]

    return [by_name[n] for n in _topological_order(by_name, dependencies)]


def _topological_order(
    nodes: Mapping[str, object], dependencies: Callable[[str], list[str]]
) -> list[str]:
    ordered: list[str] = []
    state: dict[str, str] = {}

    def visit(name: str, path: list[str]) -> None:
        if state.get(name) == "done":
            return
        if state.get(name) == "visiting":
            raise ProjectError("dependency cycle: " + " -> ".join([*path, name]))
        state[name] = "visiting"
        for dependency in dependencies(name):
            visit(dependency, [*path, name])
        state[name] = "done"
        ordered.append(name)

    for name in sorted(nodes):
        visit(name, [])
    return ordered
