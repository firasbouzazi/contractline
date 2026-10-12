"""Turn evaluation logs into the thesis metrics (see evaluation/protocol.md).

Logs are JSON lines, one file per scenario run, one event per line:
``{"scenario": "S01", "run": "r1", "at": "<ISO 8601>", "event": "<type>", ...}``.
"""

import json
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from statistics import median
from typing import Any

EVENT_TYPES = {
    "ticket_received": "an agent starts a scenario (fields: agent, model)",
    "iteration": "a full check run (fields: n, ci_passed, failures)",
    "pr_opened": "the PR is opened (fields: pr)",
    "review": "a human review (fields: reviewer, decision, minutes)",
    "human_edit": "a human changed the PR's files (fields: files, lines)",
    "merged": "the PR is merged",
    "abandoned": "the scenario was given up (fields: reason)",
    "defect": "a problem found after merge (fields: description, severity)",
    "incident": "an incident in a simulated run (fields: description, minutes_to_resolve)",
    "adapter_added": "a new source adapter was added (fields: name, hours)",
}


class LogError(ValueError):
    pass


@dataclass
class ScenarioRun:
    scenario: str
    run: str
    events: list[dict[str, Any]] = field(default_factory=list)

    def first(self, event: str) -> dict[str, Any] | None:
        return next((e for e in self.events if e["event"] == event), None)

    def all(self, event: str) -> list[dict[str, Any]]:
        return [e for e in self.events if e["event"] == event]


@dataclass(frozen=True)
class RunMetrics:
    scenario: str
    run: str
    status: str
    lead_time_hours: float | None
    iterations_to_green: int | None
    review_minutes: float
    human_edits: int
    defects: int
    incidents: int


def load_runs(paths: Iterable[Path]) -> list[ScenarioRun]:
    runs: dict[tuple[str, str], ScenarioRun] = {}
    for path in paths:
        files = sorted(path.glob("*.jsonl")) if path.is_dir() else [path]
        for file in files:
            for number, line in enumerate(file.read_text("utf-8").splitlines(), start=1):
                if not line.strip():
                    continue
                try:
                    event = json.loads(line)
                except json.JSONDecodeError as exc:
                    raise LogError(f"{file}:{number}: not JSON: {exc}") from exc
                missing = {"scenario", "run", "at", "event"} - event.keys()
                if missing:
                    raise LogError(f"{file}:{number}: missing {', '.join(sorted(missing))}")
                if event["event"] not in EVENT_TYPES:
                    raise LogError(f"{file}:{number}: unknown event {event['event']!r}")
                key = (event["scenario"], event["run"])
                runs.setdefault(key, ScenarioRun(*key)).events.append(event)
    for run in runs.values():
        run.events.sort(key=lambda e: e["at"])
    return sorted(runs.values(), key=lambda r: (r.scenario, r.run))


def _hours(start: str, end: str) -> float:
    return (datetime.fromisoformat(end) - datetime.fromisoformat(start)).total_seconds() / 3600


def compute_run_metrics(run: ScenarioRun) -> RunMetrics:
    started, merged = run.first("ticket_received"), run.first("merged")
    status = "merged" if merged else "abandoned" if run.first("abandoned") else "in progress"
    iterations = run.all("iteration")
    to_green = next((i + 1 for i, e in enumerate(iterations) if e.get("ci_passed")), None)
    return RunMetrics(
        scenario=run.scenario,
        run=run.run,
        status=status,
        lead_time_hours=_hours(started["at"], merged["at"]) if started and merged else None,
        iterations_to_green=to_green,
        review_minutes=sum(float(e.get("minutes", 0)) for e in run.all("review")),
        human_edits=len(run.all("human_edit")),
        defects=len(run.all("defect")),
        incidents=len(run.all("incident")),
    )


def summarize(metrics: list[RunMetrics], runs: list[ScenarioRun]) -> dict[str, Any]:
    merged = [m for m in metrics if m.status == "merged"]
    lead = [m.lead_time_hours for m in merged if m.lead_time_hours is not None]
    green = [m.iterations_to_green for m in metrics if m.iterations_to_green is not None]
    adapters = [e for r in runs for e in r.all("adapter_added")]
    incidents = [e for r in runs for e in r.all("incident")]
    return {
        "runs": len(metrics),
        "merged": len(merged),
        "abandoned": sum(m.status == "abandoned" for m in metrics),
        "median_lead_time_hours": median(lead) if lead else None,
        "median_iterations_to_green": median(green) if green else None,
        "total_review_minutes": sum(m.review_minutes for m in metrics),
        "share_merged_without_human_edits": (
            sum(m.human_edits == 0 for m in merged) / len(merged) if merged else None
        ),
        "defects_after_merge": sum(m.defects for m in metrics),
        "incidents": len(incidents),
        "median_minutes_to_resolve_incident": (
            median(float(e["minutes_to_resolve"]) for e in incidents) if incidents else None
        ),
        "adapter_hours": {e["name"]: e["hours"] for e in adapters},
    }


def compare_to_baseline(
    metrics: list[RunMetrics], baseline: dict[str, float]
) -> list[tuple[str, float | None, float | None]]:
    """(scenario, agent lead time, hand-built estimate) in hours."""
    by_scenario: dict[str, list[float]] = defaultdict(list)
    for m in metrics:
        if m.lead_time_hours is not None:
            by_scenario[m.scenario].append(m.lead_time_hours)
    return [
        (scenario, median(by_scenario[scenario]) if by_scenario[scenario] else None, estimate)
        for scenario, estimate in sorted(baseline.items())
    ]
