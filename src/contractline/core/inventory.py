"""PII inventory: which personal data the project holds, where, and how it is
protected. Generated from contracts and gold models, never written by hand."""

from dataclasses import asdict, dataclass, field
from typing import Any

from contractline.core.contract import MaskingStrategy, PiiClass
from contractline.core.project import Project

RAW_LAYERS = ("bronze", "quarantine")


@dataclass(frozen=True)
class PiiColumn:
    column: str
    pii: str
    masking: str
    in_silver: bool
    """False when the column is dropped before silver."""


@dataclass(frozen=True)
class DatasetEntry:
    dataset: str
    owner: str
    contract_version: str
    legal_basis: str | None
    retention: str | None
    retention_column: str | None
    erasure_key: str | None
    raw_layers: tuple[str, ...]
    columns: tuple[PiiColumn, ...]
    read_by_gold: tuple[str, ...] = field(default_factory=tuple)


def build_inventory(project: Project) -> list[DatasetEntry]:
    readers: dict[str, list[str]] = {}
    for model in project.gold_models:
        for ref in model.inputs:
            if ref.layer == "silver":
                readers.setdefault(f"{ref.domain}.{ref.entity}", []).append(
                    f"{model.domain}.{model.name}"
                )
    entries = []
    for contract in project.contracts:
        pii = [c for c in contract.schema.columns if c.pii is not PiiClass.NONE]
        if not pii:
            continue
        privacy = contract.privacy
        masking = privacy.masking if privacy else {}
        name = f"{contract.identity.domain}.{contract.identity.name}"
        entries.append(
            DatasetEntry(
                dataset=name,
                owner=contract.identity.owner,
                contract_version=contract.identity.version,
                legal_basis=privacy.legal_basis if privacy else None,
                retention=privacy.retention.period if privacy else None,
                retention_column=privacy.retention.column if privacy else None,
                erasure_key=privacy.erasure_key if privacy else None,
                raw_layers=RAW_LAYERS,
                columns=tuple(
                    PiiColumn(
                        column=c.name,
                        pii=str(c.pii),
                        masking=str(masking[c.name].strategy) if c.name in masking else "none",
                        in_silver=not (
                            c.name in masking and masking[c.name].strategy is MaskingStrategy.DROP
                        ),
                    )
                    for c in pii
                ),
                read_by_gold=tuple(sorted(readers.get(name, []))),
            )
        )
    return entries


def inventory_as_dict(entries: list[DatasetEntry]) -> dict[str, Any]:
    return {"datasets": [asdict(e) for e in entries]}


def format_inventory(entries: list[DatasetEntry]) -> str:
    lines = [
        "| Dataset | Column | PII | Masking | In silver | Legal basis | Retention | Erasure key |"
    ]
    lines.append("| --- | --- | --- | --- | --- | --- | --- | --- |")
    for entry in entries:
        for column in entry.columns:
            lines.append(
                f"| {entry.dataset} | {column.column} | {column.pii} | {column.masking} | "
                f"{'yes' if column.in_silver else 'no'} | {entry.legal_basis} | "
                f"{entry.retention} from {entry.retention_column} | {entry.erasure_key} |"
            )
    return "\n".join(lines)
