import json
from pathlib import Path
from typing import Any

import pytest

from contractline.evaluation.metrics import (
    LogError,
    compare_to_baseline,
    compute_run_metrics,
    load_runs,
    summarize,
)
from contractline.evaluation.report import build_report


def write_log(folder: Path, name: str, events: list[dict[str, Any]]) -> None:
    folder.mkdir(parents=True, exist_ok=True)
    (folder / f"{name}.jsonl").write_text("\n".join(json.dumps(e) for e in events) + "\n")


def event(scenario: str, at: str, kind: str, **fields: Any) -> dict[str, Any]:
    return {"scenario": scenario, "run": "r1", "at": at, "event": kind, **fields}


@pytest.fixture
def logs(tmp_path: Path) -> Path:
    write_log(
        tmp_path,
        "S01-r1",
        [
            event("S01", "2026-10-12T10:00:00", "ticket_received", agent="a"),
            event("S01", "2026-10-12T10:20:00", "iteration", n=1, ci_passed=False),
            event("S01", "2026-10-12T10:40:00", "iteration", n=2, ci_passed=True),
            event("S01", "2026-10-12T10:45:00", "pr_opened", pr=1),
            event("S01", "2026-10-12T11:00:00", "review", minutes=15, decision="approved"),
            event("S01", "2026-10-12T12:00:00", "merged"),
        ],
    )
    write_log(
        tmp_path,
        "S02-r1",
        [
            event("S02", "2026-10-12T09:00:00", "ticket_received", agent="a"),
            event("S02", "2026-10-12T09:30:00", "iteration", n=1, ci_passed=True),
            event("S02", "2026-10-12T10:00:00", "review", minutes=30, decision="changes"),
            event("S02", "2026-10-12T10:10:00", "human_edit", files=["x"], lines=3),
            event("S02", "2026-10-12T13:00:00", "merged"),
            event("S02", "2026-10-13T09:00:00", "defect", description="d", severity="low"),
        ],
    )
    write_log(tmp_path, "S07-r1", [event("S07", "2026-10-12T09:00:00", "abandoned", reason="r")])
    return tmp_path


def test_run_metrics(logs: Path) -> None:
    metrics = {m.scenario: m for m in map(compute_run_metrics, load_runs([logs]))}

    assert metrics["S01"].status == "merged"
    assert metrics["S01"].lead_time_hours == 2.0
    assert metrics["S01"].iterations_to_green == 2
    assert metrics["S01"].review_minutes == 15
    assert metrics["S02"].human_edits == 1
    assert metrics["S02"].defects == 1
    assert metrics["S07"].status == "abandoned"


def test_summary_and_report(logs: Path) -> None:
    runs = load_runs([logs])
    metrics = [compute_run_metrics(r) for r in runs]
    summary = summarize(metrics, runs)
    baseline = compare_to_baseline(metrics, {"S01": 16.0, "S02": 2.0})

    assert summary["merged"] == 2
    assert summary["share_merged_without_human_edits"] == 0.5
    assert summary["median_lead_time_hours"] == 3.0
    assert baseline == [("S01", 2.0, 16.0), ("S02", 4.0, 2.0)]

    text = build_report(metrics, summary, baseline, scenario_count=14)
    assert "Only 3 of 14 scenarios have been run" in text
    assert "- S07 (r1) was abandoned." in text
    assert "- S02 (r1) needed 1 human code edit(s)." in text
    assert "- S02: slower than the hand-built estimate." in text
    assert "| PRs merged with no human code edits | 50% |" in text


@pytest.mark.parametrize(
    ("line", "message"),
    [
        ("not json", "not JSON"),
        ('{"scenario": "S01", "run": "r", "at": "2026-01-01"}', "missing event"),
        ('{"scenario": "S01", "run": "r", "at": "2026-01-01", "event": "lunch"}', "unknown event"),
    ],
)
def test_bad_logs_are_rejected(tmp_path: Path, line: str, message: str) -> None:
    (tmp_path / "x.jsonl").write_text(line + "\n")

    with pytest.raises(LogError, match=message):
        load_runs([tmp_path])
