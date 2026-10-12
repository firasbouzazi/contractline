"""The evaluation report: results, comparison with the baseline, and where the
thesis fails. Missing data is reported as missing, never filled in."""

from typing import Any

from contractline.evaluation.metrics import RunMetrics


def _fmt(value: float | None, unit: str = "") -> str:
    if value is None:
        return "no data"
    return f"{value:.1f}{unit}" if isinstance(value, float) else f"{value}{unit}"


def build_report(
    metrics: list[RunMetrics],
    summary: dict[str, Any],
    baseline: list[tuple[str, float | None, float | None]],
    scenario_count: int,
) -> str:
    lines = ["# Thesis evaluation: results", ""]
    lines += [
        f"Scenario runs logged: **{summary['runs']}** "
        f"(of {scenario_count} scenarios in the backlog); merged: {summary['merged']}, "
        f"abandoned: {summary['abandoned']}.",
        "",
    ]
    if summary["runs"] < scenario_count:
        lines += [
            f"> ⚠️ Only {summary['runs']} of {scenario_count} scenarios have been run. "
            "The numbers below are not yet evidence for or against the thesis.",
            "",
        ]
    share = summary["share_merged_without_human_edits"]
    rows = [
        ("Median lead time, ticket → merged PR", _fmt(summary["median_lead_time_hours"], " h")),
        ("Median agent iterations until CI is green", _fmt(summary["median_iterations_to_green"])),
        ("Total human review time", _fmt(float(summary["total_review_minutes"]), " min")),
        ("PRs merged with no human code edits", "no data" if share is None else f"{share:.0%}"),
        ("Defects found after merge", str(summary["defects_after_merge"])),
        ("Incidents in simulated runs", str(summary["incidents"])),
        (
            "Median minutes to resolve an incident",
            _fmt(summary["median_minutes_to_resolve_incident"]),
        ),
    ]
    lines += ["## Headline metrics", "", "| Metric | Value |", "| --- | --- |"]
    lines += [f"| {name} | {value} |" for name, value in rows]
    adapters = summary["adapter_hours"]
    adapter_text = ", ".join(f"{k}: {v} h" for k, v in adapters.items()) or "no data"
    lines += [f"| Time to add a source adapter | {adapter_text} |", ""]

    lines += ["## Per scenario", ""]
    lines += [
        "| Scenario | Run | Status | Lead time | Iterations to green | Review "
        "| Human edits | Defects |",
        "| --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for m in metrics:
        lines.append(
            f"| {m.scenario} | {m.run} | {m.status} | {_fmt(m.lead_time_hours, ' h')} | "
            f"{_fmt(m.iterations_to_green)} | {m.review_minutes:.0f} min | {m.human_edits} | "
            f"{m.defects} |"
        )
    lines.append("")

    lines += ["## Against the baseline (hand-built estimates)", ""]
    lines += ["| Scenario | Agent + harness | Hand-built estimate |", "| --- | --- | --- |"]
    for scenario, agent, estimate in baseline:
        lines.append(f"| {scenario} | {_fmt(agent, ' h')} | {_fmt(estimate, ' h')} |")
    lines.append("")

    lines += ["## Where the thesis fails (so far)", ""]
    failures = []
    for m in metrics:
        if m.status == "abandoned":
            failures.append(f"- {m.scenario} ({m.run}) was abandoned.")
        if m.human_edits:
            failures.append(f"- {m.scenario} ({m.run}) needed {m.human_edits} human code edit(s).")
        if m.defects:
            failures.append(f"- {m.scenario} ({m.run}) had {m.defects} defect(s) after merge.")
    slower = [
        s for s, agent, est in baseline if agent is not None and est is not None and agent > est
    ]
    failures += [f"- {s}: slower than the hand-built estimate." for s in slower]
    lines += failures or ["- Nothing recorded yet. Absence of evidence is not evidence."]
    lines += [
        "",
        "## Threats to validity",
        "",
        "- The harness author also wrote the scenarios and the baseline estimates.",
        "- Runs where the agent that built the harness also delivers the scenario are biased "
        "towards success; such runs are marked in their logs (`agent` field).",
        "- Baseline figures are estimates unless a hand-built control run is logged.",
        "",
    ]
    return "\n".join(lines)
