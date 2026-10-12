"""Synthetic source data generated from a contract (ADR-0014).

PII-safe by construction: every value is invented (``@example.invalid``
addresses, numbered names). The data deliberately contains the cases pipelines
get wrong: nulls, duplicates, late and out-of-order changes, deletes and
re-inserts, values of the wrong type, values breaking quarantine rules, and
undeclared columns when the drift policy tolerates them.
"""

import contextlib
import json
import random
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any

from contractline.core.contract import (
    Column,
    Contract,
    DriftPolicy,
    HistoryType,
    IngestMode,
    PiiClass,
    RuleType,
    Severity,
    SourceAdapter,
)

START = datetime(2026, 1, 1, tzinfo=UTC)
_DECIMAL_TYPE = "decimal("


@dataclass(frozen=True)
class Message:
    """One source message, as a Kafka record (key and value as text)."""

    key: str | None
    value: str | None


@dataclass
class SyntheticData:
    contract: Contract
    batches: list[list[Message]] = field(default_factory=list)
    raw_pii: set[str] = field(default_factory=set)
    """Every raw direct or special PII value generated: none may reach silver."""
    invalid_count: int = 0
    """Messages built to fail a schema or quarantine rule."""
    deleted_keys: set[tuple[Any, ...]] = field(default_factory=set)
    """Business keys whose last change is a delete."""
    key_values: dict[str, list[Any]] = field(default_factory=dict)
    """Values generated per column, so other datasets can reference them."""
    now: datetime = START
    """A clock just after the last event, for freshness rules."""
    batch_times: list[datetime] = field(default_factory=list)
    """When each batch was produced (a file's modification time)."""


class _Values:
    """Valid values for one contract, driven by its types and quality rules."""

    def __init__(self, contract: Contract, rng: random.Random, references: Mapping[str, list[Any]]):
        self.contract = contract
        self.rng = rng
        self.accepted: dict[str, list[Any]] = {}
        self.ranges: dict[str, tuple[Any, Any]] = {}
        self.references: dict[str, list[Any]] = {}
        for rule in contract.quality:
            if rule.severity is Severity.WARN and rule.type is not RuleType.REFERENTIAL_INTEGRITY:
                continue
            if rule.type is RuleType.ACCEPTED_VALUES:
                self.accepted[rule.columns[0]] = list(rule.params["values"])
            elif rule.type is RuleType.RANGE:
                self.ranges[rule.columns[0]] = (rule.params.get("min"), rule.params.get("max"))
            elif rule.type is RuleType.REFERENTIAL_INTEGRITY:
                values = references.get(rule.params["references"])
                if values:
                    self.references[rule.columns[0]] = values
        self.not_null = {
            r.columns[0]
            for r in contract.quality
            if r.type is RuleType.NOT_NULL and r.severity is not Severity.WARN
        }

    def make(self, column: Column, entity_number: int, version: int, at: datetime) -> Any:
        name, kind = column.name, column.type
        if column.nullable and name not in self.not_null and self.rng.random() < 0.1:
            return None
        if name in self.accepted:
            return self.rng.choice(self.accepted[name])
        if name in self.references:
            return self.rng.choice(self.references[name])
        if column.pii in (PiiClass.DIRECT, PiiClass.SPECIAL) and kind == "string":
            if "email" in name:
                return f"person{entity_number}.v{version}@example.invalid"
            return f"Synthetic Person {entity_number} v{version}"
        low, high = self.ranges.get(name, (None, None))
        if kind in ("integer", "long"):
            low_i = int(low) if isinstance(low, int | float) else 0
            high_i = int(high) if isinstance(high, int | float) else low_i + 10_000
            return self.rng.randint(low_i, high_i)
        if kind == "double":
            return round(self.rng.uniform(float(low or 0), float(high or 1000)), 4)
        if kind.startswith(_DECIMAL_TYPE):
            precision, scale = (int(x) for x in kind[len(_DECIMAL_TYPE) : -1].split(","))
            ceiling = min(
                float(high) if isinstance(high, int | float) else 10_000,
                10 ** (precision - scale) - 1,
            )
            floor = float(low) if isinstance(low, int | float) else 0.0
            return str(Decimal(str(round(self.rng.uniform(floor, ceiling), scale))))
        if kind == "boolean":
            return self.rng.random() < 0.5
        if kind == "date":
            if isinstance(low, str):
                base = date.fromisoformat(low)
                return (base + timedelta(days=self.rng.randint(0, 20_000))).isoformat()
            return (date(1950, 1, 1) + timedelta(days=self.rng.randint(0, 20_000))).isoformat()
        if kind == "timestamp":
            return at.strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"
        return f"{name}-{entity_number}-{self.rng.randint(0, 3)}"


def _wrong_type_value(column: Column) -> Any:
    return "not-a-number" if column.type != "string" else None


def _debezium_value(contract: Contract, values: dict[str, Any] | None) -> dict[str, Any] | None:
    """Encode values the way Debezium does: dates as days since epoch."""
    if values is None:
        return None
    encoded = dict(values)
    for column in contract.schema.columns:
        value = encoded.get(column.name)
        if column.type == "date" and isinstance(value, str):
            with contextlib.suppress(ValueError):  # deliberately bad values stay as they are
                encoded[column.name] = (date.fromisoformat(value) - date(1970, 1, 1)).days
    return encoded


def _key_text(contract: Contract, values: Mapping[str, Any]) -> str:
    keys = contract.history.business_key or contract.schema.primary_key
    return json.dumps({k: values.get(k) for k in keys})


def generate(
    contract: Contract,
    seed: int = 7,
    entities: int = 20,
    references: Mapping[str, list[Any]] | None = None,
) -> SyntheticData:
    """Synthetic messages for one contract, in three arrival batches."""
    rng = random.Random(f"{seed}:{contract.identity.domain}.{contract.identity.name}")  # noqa: S311 - test data
    values = _Values(contract, rng, references or {})
    data = SyntheticData(contract)
    columns = contract.schema.columns
    keys = list(contract.history.business_key or contract.schema.primary_key)
    sequence_column = contract.history.sequence_column
    cdc = contract.source.format == "debezium_json"
    mode = contract.source.mode
    changes = mode in (IngestMode.CHANGE_STREAM, IngestMode.INCREMENTAL, IngestMode.FULL)
    deletes = mode in (IngestMode.CHANGE_STREAM, IngestMode.FULL)
    files = contract.source.adapter is SourceAdapter.FILES
    as_text = contract.source.format == "csv"

    # Each event: (event time, op, values or None for delete, before-image).
    events: list[tuple[datetime, str, dict[str, Any] | None, dict[str, Any] | None]] = []
    for number in range(1, entities + 1):
        at = START + timedelta(minutes=number)
        row = {c.name: values.make(c, number, 0, at) for c in columns}
        for key in keys:
            column = contract.schema.column(key)
            if column is not None and column.type in ("integer", "long"):
                row[key] = number
            elif column is not None:
                row[key] = f"{contract.identity.name}-{number}"
        if sequence_column:
            row[sequence_column] = at.strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"
        events.append((at, "r" if changes else "c", row, None))
        if not changes:
            continue
        current = row
        for version in range(1, rng.randint(1, 3) + 1):
            at = at + timedelta(hours=version)
            updated = dict(current)
            for column in columns:
                if (
                    column.name not in keys
                    and column.name != sequence_column
                    and rng.random() < 0.5
                ):
                    updated[column.name] = values.make(column, number, version, at)
            if sequence_column:
                updated[sequence_column] = at.strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"
            events.append((at, "u", updated, current))
            current = updated
        if deletes and number % 7 == 0:  # delete; every other deleted key comes back
            at = at + timedelta(hours=1)
            events.append((at, "d", None, current))
            if number % 14 == 0:
                at = at + timedelta(hours=1)
                again = dict(current)
                if sequence_column:
                    again[sequence_column] = at.strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"
                events.append((at, "c", again, None))
            else:
                data.deleted_keys.add(tuple(current[k] for k in keys))

    for _, _, after, previous in events:
        for image in (after, previous):
            if image is None:
                continue
            for column in columns:
                value = image.get(column.name)
                if column.pii in (PiiClass.DIRECT, PiiClass.SPECIAL) and isinstance(value, str):
                    data.raw_pii.add(value)
                data.key_values.setdefault(column.name, [])
                if value is not None and value not in data.key_values[column.name]:
                    data.key_values[column.name].append(value)

    def encode(
        op: str, row: dict[str, Any] | None, before: dict[str, Any] | None, at: datetime, lsn: int
    ) -> Message:
        image = row if row is not None else before
        if image is None:
            raise ValueError("an event needs an after or a before image")
        key = _key_text(contract, image)
        if files:
            if row is None:
                raise ValueError("files sources carry no deletes")
            text = {k: _as_csv_text(v) for k, v in row.items()} if as_text else row
            return Message(None, json.dumps(text))
        if cdc:
            value = {
                "before": _debezium_value(contract, before),
                "after": _debezium_value(contract, row),
                "source": {"lsn": lsn, "ts_ms": int(at.timestamp() * 1000)},
                "op": op,
            }
            return Message(key, json.dumps(value))
        return Message(key, json.dumps(row))

    if files:
        # Files carry rows, not change events: deletes exist only as absence
        # from a full snapshot.
        events.sort(key=lambda e: e[0])
    third = max(1, len([e for e in events if e[2] is not None or not files]) // 3)
    messages = [
        encode(op, row, before, at, lsn)
        for lsn, (at, op, row, before) in enumerate(events, 1)
        if row is not None or not files
    ]

    # Edge cases appended to the stream.
    template = events[0][2] or {}
    extras: list[Message] = []
    typed = [c for c in columns if c.type != "string" and c.name not in keys]
    if typed:
        bad = {
            **template,
            **{k: f"bad-{k}" if isinstance(template.get(k), str) else 9999 for k in keys},
        }
        bad[typed[0].name] = _wrong_type_value(typed[0])
        extras.append(encode("c", bad, None, START, len(messages) + 1))
        data.invalid_count += 1
    for rule in contract.quality:
        if rule.type is RuleType.ACCEPTED_VALUES and rule.severity is Severity.QUARANTINE:
            bad = {
                **template,
                **{k: 8888 if not isinstance(template.get(k), str) else "bad-av" for k in keys},
            }
            bad[rule.columns[0]] = "__not_accepted__"
            extras.append(encode("c", bad, None, START, len(messages) + 2))
            data.invalid_count += 1
            break
    if contract.schema.drift_policy is DriftPolicy.ADD_COLUMNS:
        drifted = {**(events[1][2] or template), "synthetic_new_column": "drift"}
        extras.append(
            encode("u" if changes else "c", drifted, events[1][2], events[1][0], len(messages) + 3)
        )
    duplicate = messages[len(messages) // 2]
    if contract.history.type is not HistoryType.APPEND:
        extras.append(duplicate)  # the same change delivered twice

    # Arrival: three batches; some changes arrive late, after newer ones.
    first, second, rest = messages[:third], messages[third : 2 * third], messages[2 * third :]
    late = [m for i, m in enumerate(second) if i % 5 == 0]
    second = [m for m in second if m not in late]
    data.batches = [first, second + extras, rest + late]
    ordered = sorted(at for at, *_ in events)
    data.batch_times = [ordered[min(len(ordered) - 1, (i + 1) * third - 1)] for i in range(3)]
    if mode is IngestMode.FULL:
        data.batches, data.deleted_keys, data.batch_times = _build_snapshots(
            events, extras, keys, encode
        )
    data.now = max(at for at, *_ in events) + timedelta(minutes=1)
    data.batch_times = [t for t, b in zip(data.batch_times, data.batches, strict=True) if b]
    data.batches = [b for b in data.batches if b]
    return data


def _as_csv_text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _build_snapshots(
    events: list[tuple[datetime, str, dict[str, Any] | None, dict[str, Any] | None]],
    extras: list[Message],
    keys: list[str],
    encode: Any,
) -> tuple[list[list[Message]], set[tuple[Any, ...]], list[datetime]]:
    """Three full snapshots of the live rows, taken a third of the way through,
    two thirds, and at the end. Keys gone from the last snapshot were deleted."""
    live: dict[tuple[Any, ...], dict[str, Any]] = {}
    ever: set[tuple[Any, ...]] = set()
    snapshots: list[list[Message]] = []
    times: list[datetime] = []
    cuts = {len(events) // 3, 2 * len(events) // 3, len(events)}
    for index, (at, op, row, before) in enumerate(events, 1):
        image = row if row is not None else before
        if image is None:
            continue
        key = tuple(image[k] for k in keys)
        ever.add(key)
        if op == "d":
            live.pop(key, None)
        else:
            live[key] = row or image
        if index in cuts:
            snapshot = [encode("r", r, None, at, index) for r in live.values()]
            if len(snapshots) == 1:
                snapshot += extras
            snapshots.append(snapshot)
            times.append(at)
    return snapshots, ever - set(live), times
