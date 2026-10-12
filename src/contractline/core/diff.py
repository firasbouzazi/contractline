"""Compare two versions of a contract.

Each change gets the semantic-version bump it needs (major, minor or patch)
and a flag saying whether it *relaxes* a guarantee: less masking, a weaker
quality rule, a looser SLA. Relaxing changes are what a pipeline author
(human or agent) could use to make generated tests pass by weakening them,
so CI routes them to the data owner for review (ADR-0008).
"""

import re
from collections.abc import Iterable
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Any

from contractline.core.contract import (
    Column,
    Contract,
    DriftPolicy,
    MaskingStrategy,
    PiiClass,
    QualityRule,
    RuleType,
    Severity,
)


class Bump(IntEnum):
    NONE = 0
    PATCH = 1
    MINOR = 2
    MAJOR = 3


@dataclass(frozen=True)
class Change:
    location: str
    message: str
    bump: Bump
    relaxes: bool = False

    def __str__(self) -> str:
        tags = self.bump.name.lower() + (", relaxes a guarantee" if self.relaxes else "")
        return f"{self.location}: {self.message} [{tags}]"


@dataclass(frozen=True)
class ContractDiff:
    old_version: str
    new_version: str
    changes: tuple[Change, ...] = field(default_factory=tuple)

    @property
    def required_bump(self) -> Bump:
        return max((c.bump for c in self.changes), default=Bump.NONE)

    @property
    def actual_bump(self) -> Bump:
        return compute_bump(self.old_version, self.new_version)

    @property
    def relaxes_guarantees(self) -> bool:
        return any(c.relaxes for c in self.changes)

    @property
    def version_problem(self) -> str | None:
        """Why the new version number is wrong for these changes, or ``None``."""
        old, new = parse_version(self.old_version), parse_version(self.new_version)
        if self.changes and new <= old:
            return f"version must increase (was {self.old_version}, now {self.new_version})"
        if not self.changes and new != old:
            return f"version changed to {self.new_version} but the contract did not change"
        if self.actual_bump < self.required_bump:
            return (
                f"changes need a {self.required_bump.name.lower()} version bump, "
                f"but {self.old_version} -> {self.new_version} is a "
                f"{self.actual_bump.name.lower()} bump"
            )
        return None


def parse_version(version: str) -> tuple[int, int, int]:
    major, minor, patch = (int(p) for p in version.split("."))
    return major, minor, patch


def compute_bump(old: str, new: str) -> Bump:
    o, n = parse_version(old), parse_version(new)
    if n <= o:
        return Bump.NONE
    if n[0] > o[0]:
        return Bump.MAJOR
    if n[1] > o[1]:
        return Bump.MINOR
    return Bump.PATCH


_DURATION = re.compile(
    r"^P(?:(\d+)Y)?(?:(\d+)M)?(?:(\d+)W)?(?:(\d+)D)?(?:T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?)?$"
)
# Calendar units are approximated; durations are only compared, never added to dates.
_DURATION_SECONDS = (365 * 86400, 30 * 86400, 7 * 86400, 86400, 3600, 60, 1)


def compute_duration_seconds(duration: str) -> int:
    match = _DURATION.match(duration)
    if match is None:
        raise ValueError(f"not an ISO 8601 duration: {duration!r}")
    return sum(int(v or 0) * s for v, s in zip(match.groups(), _DURATION_SECONDS, strict=True))


_PII_RANK = {PiiClass.NONE: 0, PiiClass.INDIRECT: 1, PiiClass.DIRECT: 2, PiiClass.SPECIAL: 3}
# How much of the original value survives the masking (lower is stronger).
_MASKING_EXPOSURE = {
    MaskingStrategy.DROP: 0,
    MaskingStrategy.REDACT: 1,
    MaskingStrategy.HASH: 2,
    MaskingStrategy.TOKENIZE: 2,
    MaskingStrategy.GENERALIZE: 3,
    MaskingStrategy.KEEP: 4,
}
_SEVERITY_RANK = {Severity.WARN: 0, Severity.QUARANTINE: 1, Severity.BLOCK: 2}
_DRIFT_STRICTNESS = {DriftPolicy.FAIL: 2, DriftPolicy.QUARANTINE: 1, DriftPolicy.ADD_COLUMNS: 0}
_DECIMAL = re.compile(r"^decimal\((\d+),(\d+)\)$")


def is_widening(old_type: str, new_type: str) -> bool:
    """True when every value of ``old_type`` fits ``new_type`` unchanged."""
    if (old_type, new_type) == ("integer", "long"):
        return True
    old_dec, new_dec = _DECIMAL.match(old_type), _DECIMAL.match(new_type)
    if old_dec and new_dec:
        old_p, old_s = int(old_dec[1]), int(old_dec[2])
        new_p, new_s = int(new_dec[1]), int(new_dec[2])
        return new_s >= old_s and new_p - new_s >= old_p - old_s
    return False


def _rule_key(rule: QualityRule) -> tuple[str, tuple[str, ...]]:
    return rule.type.value, rule.columns


def _rule_label(rule: QualityRule) -> str:
    columns = ", ".join(rule.columns)
    return f"{rule.type} ({columns})" if columns else str(rule.type)


def _compare_rule_params(old: QualityRule, new: QualityRule) -> list[tuple[str, bool]]:
    """Return (message, relaxes) for each parameter change of a matched rule."""
    found: list[tuple[str, bool]] = []
    o, n = old.params, new.params
    if old.type is RuleType.ACCEPTED_VALUES:
        added = [v for v in n["values"] if v not in o["values"]]
        removed = [v for v in o["values"] if v not in n["values"]]
        if added:
            found.append((f"accepts new values {added}", True))
        if removed:
            found.append((f"no longer accepts {removed}", False))
    elif old.type is RuleType.RANGE:
        for bound in ("min", "max"):
            a, b = o.get(bound), n.get(bound)
            if a == b:
                continue
            if b is None:
                found.append((f"{bound} bound removed", True))
            elif a is None:
                found.append((f"{bound} bound added ({b})", False))
            else:
                # Mixed types cannot be ordered, so treat the change as relaxing.
                looser = b < a if bound == "min" else b > a
                relaxes = type(a) is not type(b) or bool(looser)
                found.append((f"{bound} changed from {a} to {b}", relaxes))
    elif old.type is RuleType.ROW_COUNT_ANOMALY:
        a, b = o["max_change_ratio"], n["max_change_ratio"]
        if a != b:
            found.append((f"max_change_ratio changed from {a} to {b}", b > a))
    elif old.type is RuleType.FRESHNESS:
        a, b = o["max_age"], n["max_age"]
        if a != b:
            relaxes = compute_duration_seconds(b) > compute_duration_seconds(a)
            found.append((f"max_age changed from {a} to {b}", relaxes))
    elif old.type is RuleType.REFERENTIAL_INTEGRITY and o["references"] != n["references"]:
        found.append((f"references changed to {n['references']}", True))
    return found


class _Collector:
    def __init__(self) -> None:
        self.changes: list[Change] = []

    def add(self, location: str, message: str, bump: Bump, *, relaxes: bool = False) -> None:
        self.changes.append(Change(location, message, bump, relaxes))


def _diff_columns(old: Contract, new: Contract, out: _Collector) -> None:
    old_cols = {c.name: c for c in old.schema.columns}
    new_cols = {c.name: c for c in new.schema.columns}
    for name in old_cols.keys() - new_cols.keys():
        out.add(f"schema.columns.{name}", "column removed", Bump.MAJOR)
    for name in sorted(new_cols.keys() - old_cols.keys()):
        column = new_cols[name]
        if column.nullable:
            out.add(f"schema.columns.{name}", "nullable column added", Bump.MINOR)
        else:
            out.add(
                f"schema.columns.{name}",
                "non-nullable column added (existing rows have no value)",
                Bump.MAJOR,
            )
    for name in sorted(old_cols.keys() & new_cols.keys()):
        _diff_column(old_cols[name], new_cols[name], out)


def _diff_column(o: Column, n: Column, out: _Collector) -> None:
    location = f"schema.columns.{o.name}"
    if o.type != n.type:
        bump = Bump.MINOR if is_widening(o.type, n.type) else Bump.MAJOR
        out.add(location, f"type changed from {o.type} to {n.type}", bump)
    if o.nullable != n.nullable:
        if n.nullable:
            out.add(location, "became nullable", Bump.MAJOR, relaxes=True)
        else:
            out.add(location, "became non-nullable", Bump.MINOR)
    if o.pii != n.pii:
        lowered = _PII_RANK[n.pii] < _PII_RANK[o.pii]
        out.add(
            location,
            f"PII class changed from {o.pii} to {n.pii}",
            Bump.MAJOR if lowered else Bump.MINOR,
            relaxes=lowered,
        )
    if o.description != n.description:
        out.add(location, "description changed", Bump.PATCH)


def _diff_lists(location: str, label: str, old: Iterable[str], new: Iterable[str]) -> str | None:
    old_t, new_t = tuple(old), tuple(new)
    if old_t == new_t:
        return None
    return f"{label} changed from {list(old_t)} to {list(new_t)}"


def _diff_privacy(old: Contract, new: Contract, out: _Collector) -> None:
    o, n = old.privacy, new.privacy
    if o is None and n is None:
        return
    if o is None:
        out.add("privacy", "privacy section added", Bump.MINOR)
        return
    if n is None:
        out.add("privacy", "privacy section removed", Bump.MAJOR, relaxes=True)
        return
    if o.legal_basis != n.legal_basis:
        out.add(
            "privacy.legal_basis",
            f"changed from {o.legal_basis} to {n.legal_basis}",
            Bump.MINOR,
            relaxes=True,
        )
    if o.erasure_key != n.erasure_key:
        out.add(
            "privacy.erasure_key",
            f"changed from {o.erasure_key} to {n.erasure_key}",
            Bump.MAJOR,
            relaxes=True,
        )
    if o.retention.period != n.retention.period:
        longer = compute_duration_seconds(n.retention.period) > compute_duration_seconds(
            o.retention.period
        )
        out.add(
            "privacy.retention.period",
            f"changed from {o.retention.period} to {n.retention.period}",
            Bump.MINOR,
            relaxes=longer,
        )
    if o.retention.column != n.retention.column:
        out.add(
            "privacy.retention.column",
            f"changed from {o.retention.column} to {n.retention.column}",
            Bump.MINOR,
            relaxes=True,
        )
    for name in sorted(o.masking.keys() | n.masking.keys()):
        location = f"privacy.masking.{name}"
        om, nm = o.masking.get(name), n.masking.get(name)
        if om is None and nm is not None:
            out.add(location, f"masking added ({nm.strategy})", Bump.MAJOR)
        elif om is not None and nm is None:
            out.add(location, f"masking removed (was {om.strategy})", Bump.MAJOR, relaxes=True)
        elif om is not None and nm is not None and om != nm:
            if om.strategy != nm.strategy:
                message = f"strategy changed from {om.strategy} to {nm.strategy}"
                weaker = _MASKING_EXPOSURE[nm.strategy] > _MASKING_EXPOSURE[om.strategy]
            else:
                # Parameter strength is strategy-specific; let the data owner judge it.
                message = "masking parameters changed"
                weaker = True
            out.add(location, message, Bump.MAJOR, relaxes=weaker)


def _diff_quality(old: Contract, new: Contract, out: _Collector) -> None:
    old_rules = {_rule_key(r): r for r in old.quality}
    new_rules = {_rule_key(r): r for r in new.quality}
    for key in sorted(old_rules.keys() - new_rules.keys()):
        out.add("quality", f"rule removed: {_rule_label(old_rules[key])}", Bump.MINOR, relaxes=True)
    for key in sorted(new_rules.keys() - old_rules.keys()):
        out.add("quality", f"rule added: {_rule_label(new_rules[key])}", Bump.MINOR)
    for key in sorted(old_rules.keys() & new_rules.keys()):
        o, n = old_rules[key], new_rules[key]
        location = f"quality.{_rule_label(o)}"
        if o.severity != n.severity:
            lowered = _SEVERITY_RANK[n.severity] < _SEVERITY_RANK[o.severity]
            out.add(
                location,
                f"severity changed from {o.severity} to {n.severity}",
                Bump.MINOR,
                relaxes=lowered,
            )
        for message, relaxes in _compare_rule_params(o, n):
            out.add(location, message, Bump.MINOR, relaxes=relaxes)


def diff_contracts(old: Contract, new: Contract) -> ContractDiff:
    """List every change from ``old`` to ``new`` with its required version bump."""
    out = _Collector()

    for attr in ("name", "domain"):
        a, b = getattr(old.identity, attr), getattr(new.identity, attr)
        if a != b:
            out.add(f"identity.{attr}", f"changed from {a} to {b} (a new dataset)", Bump.MAJOR)
    for attr in ("owner", "description"):
        if getattr(old.identity, attr) != getattr(new.identity, attr):
            out.add(f"identity.{attr}", "changed", Bump.PATCH)

    # Moving a pipeline to another source or mode means re-ingesting history.
    for attr in ("adapter", "mode", "location", "format", "watermark_column"):
        a, b = getattr(old.source, attr), getattr(new.source, attr)
        if a != b:
            out.add(f"source.{attr}", f"changed from {a} to {b}", Bump.MAJOR)
    if old.source.options != new.source.options:
        out.add("source.options", "changed", Bump.PATCH)

    _diff_columns(old, new, out)
    message = _diff_lists(
        "schema.primary_key", "primary key", old.schema.primary_key, new.schema.primary_key
    )
    if message:
        out.add("schema.primary_key", message, Bump.MAJOR)
    o_drift, n_drift = old.schema.drift_policy, new.schema.drift_policy
    if o_drift != n_drift:
        looser = _DRIFT_STRICTNESS[n_drift] < _DRIFT_STRICTNESS[o_drift]
        out.add(
            "schema.drift_policy",
            f"changed from {o_drift} to {n_drift}",
            Bump.MINOR,
            relaxes=looser,
        )

    _diff_privacy(old, new, out)

    oh, nh = old.history, new.history
    if oh.type != nh.type:
        out.add("history.type", f"changed from {oh.type} to {nh.type}", Bump.MAJOR)
    message = _diff_lists("history.business_key", "business key", oh.business_key, nh.business_key)
    if message:
        out.add("history.business_key", message, Bump.MAJOR)
    removed = [c for c in oh.tracked_columns if c not in nh.tracked_columns]
    added = [c for c in nh.tracked_columns if c not in oh.tracked_columns]
    if removed:
        out.add(
            "history.tracked_columns", f"no longer tracked: {removed}", Bump.MAJOR, relaxes=True
        )
    if added:
        out.add("history.tracked_columns", f"now tracked: {added}", Bump.MINOR)
    if oh.sequence_column != nh.sequence_column:
        out.add(
            "history.sequence_column",
            f"changed from {oh.sequence_column} to {nh.sequence_column}",
            Bump.MAJOR,
        )

    _diff_quality(old, new, out)

    if old.sla.freshness != new.sla.freshness:
        looser = compute_duration_seconds(new.sla.freshness) > compute_duration_seconds(
            old.sla.freshness
        )
        out.add(
            "sla.freshness",
            f"changed from {old.sla.freshness} to {new.sla.freshness}",
            Bump.MINOR,
            relaxes=looser,
        )
    if dict(old.tags or {}) != dict(new.tags or {}):
        out.add("tags", "changed", Bump.PATCH)

    return ContractDiff(old.identity.version, new.identity.version, tuple(out.changes))


def build_report(diff: ContractDiff) -> dict[str, Any]:
    """Machine-readable form of a diff, used by CI."""
    return {
        "old_version": diff.old_version,
        "new_version": diff.new_version,
        "required_bump": diff.required_bump.name.lower(),
        "relaxes_guarantees": diff.relaxes_guarantees,
        "version_problem": diff.version_problem,
        "changes": [
            {
                "location": c.location,
                "message": c.message,
                "bump": c.bump.name.lower(),
                "relaxes": c.relaxes,
            }
            for c in diff.changes
        ],
    }
