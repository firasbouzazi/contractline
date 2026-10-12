"""The pull request comment CI posts: what changed, what it means, what passed."""

from typing import Any


def build_ci_summary(
    diffs: dict[str, dict[str, Any]],
    test_report: dict[str, Any] | None,
    out_of_scope: list[str],
    lint_problems: list[str],
) -> str:
    """Markdown summary. ``diffs`` maps a contract path to ``contractline diff --format json``."""
    lines = ["## contractline", ""]
    status = "✅ ready for review"
    if out_of_scope or lint_problems:
        status = "❌ blocked"
    elif test_report is not None and not test_report.get("passed", False):
        status = "❌ generated tests failed"
    elif any(d.get("version_problem") for d in diffs.values()):
        status = "❌ version number does not match the changes"
    elif any(d.get("relaxes_guarantees") for d in diffs.values()):
        status = "⚠️ relaxes a guarantee: needs the data owner's approval"
    lines += [f"**Status:** {status}", ""]

    if out_of_scope:
        lines += ["### Files an agent may not change", ""]
        lines += [f"- `{path}`" for path in out_of_scope]
        lines.append("")
    if lint_problems:
        lines += ["### Naming", ""] + [f"- {p}" for p in lint_problems] + [""]

    if diffs:
        lines += ["### Contract changes", ""]
        for path, diff in sorted(diffs.items()):
            if diff.get("new"):
                lines.append(f"- `{path}`: new contract")
                continue
            lines.append(
                f"- `{path}`: {diff['old_version']} → {diff['new_version']} "
                f"(needs a {diff['required_bump']} bump)"
            )
            for change in diff["changes"]:
                flag = " ⚠️ **relaxes**" if change["relaxes"] else ""
                lines.append(f"  - `{change['location']}`: {change['message']}{flag}")
            if diff.get("version_problem"):
                lines.append(f"  - ❌ {diff['version_problem']}")
        lines.append("")

    if test_report is not None:
        results = test_report.get("results", [])
        passed = sum(1 for r in results if r["passed"])
        lines += [f"### Generated tests ({test_report.get('env')}): {passed}/{len(results)}", ""]
        failed = [r for r in results if not r["passed"]]
        for result in failed:
            lines.append(f"- ❌ {result['target']}: {result['check']}")
            lines += [f"  - {d}" for d in result["details"]]
        if not failed:
            lines.append("All checks passed.")
        lines.append("")
    return "\n".join(lines)
