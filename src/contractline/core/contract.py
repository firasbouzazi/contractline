"""Typed, immutable view of a validated contract.

Instances are only built from documents that already passed the JSON Schema
(see ``validation.py``), so this module does no input checking of its own.
"""

from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Any


class SourceAdapter(StrEnum):
    KAFKA = "kafka"
    FILES = "files"


class IngestMode(StrEnum):
    FULL = "full"
    INCREMENTAL = "incremental"
    CHANGE_STREAM = "change_stream"
    APPEND_STREAM = "append_stream"


class DriftPolicy(StrEnum):
    FAIL = "fail"
    QUARANTINE = "quarantine"
    ADD_COLUMNS = "add_columns"


class PiiClass(StrEnum):
    NONE = "none"
    INDIRECT = "indirect"
    DIRECT = "direct"
    SPECIAL = "special"


class MaskingStrategy(StrEnum):
    HASH = "hash"
    TOKENIZE = "tokenize"
    GENERALIZE = "generalize"
    REDACT = "redact"
    DROP = "drop"
    KEEP = "keep"


class HistoryType(StrEnum):
    APPEND = "append"
    SNAPSHOT = "snapshot"
    SCD1 = "scd1"
    SCD2 = "scd2"


class RuleType(StrEnum):
    NOT_NULL = "not_null"
    UNIQUE = "unique"
    ACCEPTED_VALUES = "accepted_values"
    RANGE = "range"
    REFERENTIAL_INTEGRITY = "referential_integrity"
    ROW_COUNT_ANOMALY = "row_count_anomaly"
    FRESHNESS = "freshness"


class Severity(StrEnum):
    WARN = "warn"
    QUARANTINE = "quarantine"
    BLOCK = "block"


@dataclass(frozen=True)
class Identity:
    name: str
    domain: str
    owner: str
    version: str
    description: str | None = None


@dataclass(frozen=True)
class Source:
    adapter: SourceAdapter
    mode: IngestMode
    location: str
    format: str
    watermark_column: str | None = None
    options: Mapping[str, Any] | None = None


@dataclass(frozen=True)
class Column:
    name: str
    type: str
    nullable: bool
    pii: PiiClass
    description: str | None = None


@dataclass(frozen=True)
class Schema:
    columns: tuple[Column, ...]
    drift_policy: DriftPolicy
    primary_key: tuple[str, ...] = ()

    def column(self, name: str) -> Column | None:
        return next((c for c in self.columns if c.name == name), None)


@dataclass(frozen=True)
class Masking:
    strategy: MaskingStrategy
    params: Mapping[str, Any] | None = None
    justification: str | None = None


@dataclass(frozen=True)
class Retention:
    period: str
    column: str


@dataclass(frozen=True)
class Privacy:
    legal_basis: str
    retention: Retention
    erasure_key: str
    masking: Mapping[str, Masking]


@dataclass(frozen=True)
class History:
    type: HistoryType
    business_key: tuple[str, ...] = ()
    tracked_columns: tuple[str, ...] = ()
    sequence_column: str | None = None


@dataclass(frozen=True)
class QualityRule:
    type: RuleType
    severity: Severity
    columns: tuple[str, ...]
    """Columns the rule reads (``column`` and ``columns`` are normalised here)."""
    params: Mapping[str, Any]
    """Rule-specific settings such as ``values``, ``min`` or ``references``."""


@dataclass(frozen=True)
class Sla:
    freshness: str


@dataclass(frozen=True)
class Contract:
    identity: Identity
    source: Source
    schema: Schema
    history: History
    quality: tuple[QualityRule, ...]
    sla: Sla
    privacy: Privacy | None = None
    tags: Mapping[str, str] | None = None

    @property
    def has_pii(self) -> bool:
        return any(c.pii is not PiiClass.NONE for c in self.schema.columns)


_RULE_BASE_KEYS = {"type", "severity", "description", "column", "columns"}


def _build_rule(raw: Mapping[str, Any]) -> QualityRule:
    columns = tuple(raw["columns"]) if "columns" in raw else ()
    if "column" in raw:
        columns = (raw["column"],)
    return QualityRule(
        type=RuleType(raw["type"]),
        severity=Severity(raw["severity"]),
        columns=columns,
        params={k: v for k, v in raw.items() if k not in _RULE_BASE_KEYS},
    )


def build_contract(doc: Mapping[str, Any]) -> Contract:
    """Build a ``Contract`` from a document that passed the JSON Schema."""
    identity = doc["identity"]
    source = doc["source"]
    schema = doc["schema"]
    history = doc["history"]
    privacy = doc.get("privacy")
    return Contract(
        identity=Identity(**identity),
        source=Source(
            adapter=SourceAdapter(source["adapter"]),
            mode=IngestMode(source["mode"]),
            location=source["location"],
            format=source["format"],
            watermark_column=source.get("watermark_column"),
            options=source.get("options"),
        ),
        schema=Schema(
            columns=tuple(
                Column(
                    name=c["name"],
                    type=c["type"],
                    nullable=c["nullable"],
                    pii=PiiClass(c["pii"]),
                    description=c.get("description"),
                )
                for c in schema["columns"]
            ),
            drift_policy=DriftPolicy(schema["drift_policy"]),
            primary_key=tuple(schema.get("primary_key", ())),
        ),
        history=History(
            type=HistoryType(history["type"]),
            business_key=tuple(history.get("business_key", ())),
            tracked_columns=tuple(history.get("tracked_columns", ())),
            sequence_column=history.get("sequence_column"),
        ),
        quality=tuple(_build_rule(r) for r in doc["quality"]),
        sla=Sla(freshness=doc["sla"]["freshness"]),
        privacy=None
        if privacy is None
        else Privacy(
            legal_basis=privacy["legal_basis"],
            retention=Retention(**privacy["retention"]),
            erasure_key=privacy["erasure_key"],
            masking={
                name: Masking(
                    strategy=MaskingStrategy(m["strategy"]),
                    params=m.get("params"),
                    justification=m.get("justification"),
                )
                for name, m in privacy["masking"].items()
            },
        ),
        tags=doc.get("tags"),
    )
