"""Command-line entry point.

Commands are added one per change as they are implemented
(validate, lint, plan, test, run, new-dataset).
"""

import json
import subprocess
import tempfile
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Annotated

import typer
import yaml

from contractline import __version__
from contractline.adapters.orchestration.airflow import build_dags, find_stale_dags, write_dags
from contractline.core.ci_summary import build_ci_summary
from contractline.core.contract import Contract
from contractline.core.diff import build_report, diff_contracts
from contractline.core.gold import GoldModel, parse_gold
from contractline.core.inventory import build_inventory, format_inventory, inventory_as_dict
from contractline.core.lint import (
    LintIssue,
    lint_branch,
    lint_commit_message,
    lint_contract,
    lint_gold_model,
    lint_pull_request_title,
)
from contractline.core.naming import is_valid_environment
from contractline.core.plan import build_plan, format_plan
from contractline.core.project import Project, ProjectError, load_project
from contractline.core.project_config import ConfigError, find_config, load_config
from contractline.core.scaffold import build_contract_skeleton
from contractline.core.scope import find_out_of_scope
from contractline.core.secrets import EnvSecretProvider
from contractline.core.validation import (
    ContractError,
    InvalidContractError,
    load_contract,
    parse_text,
)
from contractline.core.yaml_loader import load_yaml
from contractline.evaluation.metrics import (
    EVENT_TYPES,
    compare_to_baseline,
    compute_run_metrics,
    load_runs,
)
from contractline.evaluation.metrics import summarize as summarize_runs
from contractline.evaluation.report import build_report as build_eval_report

app = typer.Typer(no_args_is_help=True, add_completion=False)

CONTRACT_SUFFIXES = (".yaml", ".yml")


def _print_version(value: bool) -> None:
    if value:
        typer.echo(f"contractline {__version__}")
        raise typer.Exit


@app.callback()
def main(
    version: Annotated[
        bool,
        typer.Option(
            "--version", callback=_print_version, is_eager=True, help="Show the version and exit."
        ),
    ] = False,
) -> None:
    """Turn data contracts into governed data pipelines."""


GOLD_API_VERSION = "contractline/gold/v0"


def parse_file(path: Path) -> tuple[Contract | GoldModel | None, list[ContractError]]:
    """Validate a data contract or a gold model (chosen by its ``api_version``)."""
    text = path.read_text("utf-8")
    try:
        doc = load_yaml(text)
    except yaml.YAMLError:
        doc = None  # parse_text reports the YAML error with its location
    if isinstance(doc, dict) and doc.get("api_version") == GOLD_API_VERSION:
        sql_path = path.with_suffix(".sql")
        if not sql_path.is_file():
            return None, [ContractError("<file>", f"missing SQL file {sql_path.name}")]
        return parse_gold(text, sql_path.read_text("utf-8"))
    return parse_text(text)


def find_contract_files(paths: list[Path]) -> list[Path]:
    found: list[Path] = []
    for path in paths:
        if path.is_dir():
            found.extend(sorted(p for p in path.rglob("*") if p.suffix in CONTRACT_SUFFIXES))
        else:
            found.append(path)
    return found


@app.command()
def validate(
    paths: Annotated[
        list[Path],
        typer.Argument(
            exists=True, help="Contract files, or directories searched for *.yaml files."
        ),
    ],
) -> None:
    """Validate data contracts and report every problem found."""
    files = find_contract_files(paths)
    if not files:
        typer.echo("No contract files found.", err=True)
        raise typer.Exit(code=1)
    failed = 0
    for file in files:
        _, errors = parse_file(file)
        if errors:
            failed += 1
            for error in errors:
                typer.echo(f"{file}: {error}", err=True)
    if failed:
        typer.echo(f"{failed} of {len(files)} contract(s) invalid.", err=True)
        raise typer.Exit(code=1)
    typer.echo(f"{len(files)} contract(s) valid.")


class OutputFormat(StrEnum):
    TEXT = "text"
    JSON = "json"


@app.command()
def diff(
    old: Annotated[Path, typer.Argument(exists=True, dir_okay=False, help="Previous contract.")],
    new: Annotated[Path, typer.Argument(exists=True, dir_okay=False, help="Changed contract.")],
    output: Annotated[
        OutputFormat, typer.Option("--format", help="Output format.")
    ] = OutputFormat.TEXT,
    fail_on_relax: Annotated[
        bool,
        typer.Option(
            "--fail-on-relax", help="Exit with an error if any change relaxes a guarantee."
        ),
    ] = False,
) -> None:
    """Classify the changes between two contract versions.

    Fails when the new version number does not match the changes
    (for example a breaking change without a major bump).
    """
    try:
        result = diff_contracts(load_contract(old), load_contract(new))
    except InvalidContractError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=1) from exc

    if output is OutputFormat.JSON:
        typer.echo(json.dumps(build_report(result), indent=2))
    elif not result.changes:
        typer.echo("No changes.")
    else:
        for change in result.changes:
            typer.echo(str(change))
        typer.echo(f"Required version bump: {result.required_bump.name.lower()}")
        if result.relaxes_guarantees:
            typer.echo("Some changes relax a guarantee and need the data owner's review.")

    if result.version_problem:
        typer.echo(f"Version error: {result.version_problem}", err=True)
        raise typer.Exit(code=1)
    if fail_on_relax and result.relaxes_guarantees:
        raise typer.Exit(code=1)


@app.command()
def lint(
    paths: Annotated[
        list[Path] | None,
        typer.Argument(
            exists=True, help="Contract files, or directories searched for *.yaml files."
        ),
    ] = None,
    branch: Annotated[str | None, typer.Option(help="Branch name to check.")] = None,
    pr_title: Annotated[str | None, typer.Option(help="Pull request title to check.")] = None,
    commit_message_file: Annotated[
        Path | None,
        typer.Option(exists=True, dir_okay=False, help="File holding a commit message."),
    ] = None,
    config: Annotated[
        Path | None,
        typer.Option(
            exists=True, dir_okay=False, help="Defaults to the nearest contractline.yaml."
        ),
    ] = None,
) -> None:
    """Check naming conventions of contracts, branches, PR titles and commits."""
    if not (paths or branch or pr_title or commit_message_file):
        typer.echo("Nothing to lint: pass contract paths or a git option.", err=True)
        raise typer.Exit(code=2)
    config_path = config or find_config(Path.cwd())
    if config_path is None:
        typer.echo("No contractline.yaml found in this directory or its parents.", err=True)
        raise typer.Exit(code=2)
    try:
        project = load_config(config_path)
    except ConfigError as exc:
        typer.echo(f"Invalid configuration: {exc}", err=True)
        raise typer.Exit(code=2) from exc

    issues: list[LintIssue | str] = []
    for file in find_contract_files(paths or []):
        parsed, errors = parse_file(file)
        if parsed is None:
            issues.extend(f"{file}: {error}" for error in errors)
        elif isinstance(parsed, GoldModel):
            issues.extend(lint_gold_model(file, parsed, project))
        else:
            issues.extend(lint_contract(file, parsed, project))
    if branch:
        issues.extend(lint_branch(branch, project))
    if pr_title:
        issues.extend(lint_pull_request_title(pr_title, branch, project))
    if commit_message_file:
        issues.extend(lint_commit_message(commit_message_file.read_text("utf-8")))

    for found in issues:
        typer.echo(str(found), err=True)
    if issues:
        typer.echo(f"{len(issues)} problem(s) found.", err=True)
        raise typer.Exit(code=1)
    typer.echo("No problems found.")


def _load_project_or_exit(paths: list[Path]) -> Project:
    try:
        return load_project(paths)
    except (InvalidContractError, ProjectError) as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=1) from exc


PathsArgument = Annotated[
    list[Path],
    typer.Argument(exists=True, help="Contract and gold model files, or directories."),
]
EnvOption = Annotated[str, typer.Option("--env", help="local, dev, staging, prod or pr<number>.")]


@app.command()
def plan(paths: PathsArgument, env: EnvOption = "dev") -> None:
    """Show the steps a run would take, in order, with the tables they touch."""
    if not is_valid_environment(env):
        typer.echo(f"Unknown environment {env!r}.", err=True)
        raise typer.Exit(code=2)
    typer.echo(format_plan(build_plan(_load_project_or_exit(paths), env)))


@app.command("test")
def run_tests(
    paths: PathsArgument,
    env: EnvOption = "local",
    seed: Annotated[int, typer.Option(help="Seed for the synthetic data.")] = 7,
    report: Annotated[
        Path | None, typer.Option("--report", help="Also write the results as JSON.")
    ] = None,
    workdir: Annotated[
        Path | None, typer.Option(help="Where tables are written (default: a temporary folder).")
    ] = None,
) -> None:
    """Run the generated tests: synthetic data through the real pipeline."""
    # Spark is imported here, not at the top, so validate and lint work without Java.
    from contractline.adapters.compute.spark_delta_local import LocalSparkDelta
    from contractline.testing.generated import format_report, run_generated_tests

    if not is_valid_environment(env):
        typer.echo(f"Unknown environment {env!r}.", err=True)
        raise typer.Exit(code=2)
    project = _load_project_or_exit(paths)
    root = workdir or Path(tempfile.mkdtemp(prefix="contractline-test-"))
    results = run_generated_tests(
        LocalSparkDelta(root), project, env, seed, progress=lambda m: typer.echo(f"... {m}")
    )
    typer.echo(format_report(results))
    if report:
        report.write_text(results.to_json(), "utf-8")
    if not results.passed:
        raise typer.Exit(code=1)


@app.command()
def run(
    paths: PathsArgument,
    env: EnvOption = "local",
    kafka_bootstrap_servers: Annotated[
        str | None, typer.Option(help="Kafka brokers, e.g. localhost:29092.")
    ] = None,
    workdir: Annotated[Path, typer.Option(help="Local warehouse and checkpoints.")] = Path(
        ".contractline"
    ),
) -> None:
    """Run every pipeline once: ingest new data, update silver, rebuild gold."""
    from contractline.adapters.compute.spark_delta_local import LocalSparkDelta
    from contractline.runner import build_source_factory, run_project

    if env != "local" and not env.startswith("pr"):
        typer.echo("This command runs locally; shared environments are deployed by CD.", err=True)
        raise typer.Exit(code=2)
    project = _load_project_or_exit(paths)
    summary = run_project(
        LocalSparkDelta(workdir),
        project,
        env,
        build_source_factory(kafka_bootstrap_servers),
        EnvSecretProvider(),
        progress=lambda m: typer.echo(f"... {m}"),
    )
    for report in summary.silver:
        typer.echo(
            f"silver {report.dataset}: {report.records_in} in, {report.records_valid} valid, "
            f"{report.records_quarantined} quarantined"
        )
        for warning, count in report.warnings.items():
            typer.echo(f"  warning: {warning} ({count})")
    for gold in summary.gold:
        typer.echo(f"gold {gold.model}: {gold.rows} rows")


@app.command("new-dataset")
def new_dataset(
    domain: Annotated[str, typer.Option(help="One of the domains in contractline.yaml.")],
    entity: Annotated[str, typer.Option(help="snake_case entity name, e.g. customers.")],
    source: Annotated[str, typer.Option(help="cdc, events or files.")] = "cdc",
    owner: Annotated[str, typer.Option(help="Accountable team or person.")] = "TODO-owner",
    root: Annotated[Path, typer.Option(help="Folder holding contracts/.")] = Path(),
) -> None:
    """Create contracts/<domain>/<entity>.yaml from a template."""
    config_path = find_config(root.resolve())
    if config_path is None:
        typer.echo("No contractline.yaml found.", err=True)
        raise typer.Exit(code=2)
    project_config = load_config(config_path)
    if domain not in project_config.domains:
        allowed = ", ".join(sorted(project_config.domains))
        typer.echo(f"Unknown domain {domain!r}: use one of {allowed}.", err=True)
        raise typer.Exit(code=2)
    target = root / "contracts" / domain / f"{entity}.yaml"
    if target.exists():
        typer.echo(f"{target} already exists.", err=True)
        raise typer.Exit(code=1)
    try:
        text = build_contract_skeleton(domain, entity, source, owner)
    except ValueError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=2) from exc
    _, errors = parse_text(text)
    if errors:  # the template itself is broken: a harness bug, not the user's
        typer.echo("\n".join(map(str, errors)), err=True)
        raise typer.Exit(code=1)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(text, "utf-8")
    typer.echo(f"Created {target}. Fill in the TODOs, then run contractline validate.")


@app.command("pii-inventory")
def pii_inventory(
    paths: PathsArgument,
    output: Annotated[
        OutputFormat, typer.Option("--format", help="json (machine-readable) or text (table).")
    ] = OutputFormat.JSON,
) -> None:
    """List every personal-data column: class, masking, legal basis, retention, readers."""
    entries = build_inventory(_load_project_or_exit(paths))
    if output is OutputFormat.JSON:
        typer.echo(json.dumps(inventory_as_dict(entries), indent=2))
    else:
        typer.echo(format_inventory(entries))


@app.command()
def erase(
    paths: PathsArgument,
    dataset: Annotated[str, typer.Option(help="<domain>.<entity> holding the subject.")],
    key: Annotated[str, typer.Option(help="The subject's erasure key value, as text.")],
    request_id: Annotated[str, typer.Option(help="Ticket or request reference, for the audit.")],
    env: EnvOption = "local",
    vacuum_hours: Annotated[
        int | None, typer.Option(help="Also VACUUM, keeping this many hours of history.")
    ] = None,
    workdir: Annotated[Path, typer.Option(help="Local warehouse.")] = Path(".contractline"),
) -> None:
    """Right to erasure: delete one subject from bronze, quarantine, silver and gold."""
    from contractline.adapters.compute.spark_delta_local import LocalSparkDelta
    from contractline.spark.gdpr import ErasureError, erase_subject

    project = _load_project_or_exit(paths)
    try:
        record = erase_subject(
            LocalSparkDelta(workdir),
            project,
            env,
            dataset,
            key,
            request_id,
            EnvSecretProvider(),
            vacuum_hours,
        )
    except (ErasureError, ProjectError) as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=1) from exc
    deleted = ", ".join(f"{layer}: {n}" for layer, n in record.rows_deleted.items())
    typer.echo(f"Erased from {record.dataset} ({deleted}); gold rebuilt: {record.gold_rebuilt}")
    if not record.vacuumed:
        typer.echo("Old file versions keep the data until VACUUM runs (see --vacuum-hours).")


@app.command()
def purge(
    paths: PathsArgument,
    env: EnvOption = "local",
    vacuum_hours: Annotated[
        int | None, typer.Option(help="Also VACUUM, keeping this many hours of history.")
    ] = None,
    workdir: Annotated[Path, typer.Option(help="Local warehouse.")] = Path(".contractline"),
) -> None:
    """Retention: delete records older than each contract's retention period."""
    from contractline.adapters.compute.spark_delta_local import LocalSparkDelta
    from contractline.spark.gdpr import purge_expired

    project = _load_project_or_exit(paths)
    for record in purge_expired(LocalSparkDelta(workdir), project, env, vacuum_hours=vacuum_hours):
        deleted = ", ".join(f"{layer}: {n}" for layer, n in record.rows_deleted.items())
        typer.echo(f"{record.dataset}: {deleted}")


@app.command("run-step")
def run_step_command(
    step: Annotated[str, typer.Argument(help="A step id from `contractline plan`.")],
    project: Annotated[
        list[Path],
        typer.Option("--project", envvar="CONTRACTLINE_PROJECT", help="Contract and gold folders."),
    ],
    env: EnvOption = "local",
    kafka_bootstrap_servers: Annotated[
        str | None, typer.Option(envvar="CONTRACTLINE_KAFKA_BOOTSTRAP_SERVERS")
    ] = None,
    workdir: Annotated[
        Path, typer.Option(envvar="CONTRACTLINE_WORKDIR", help="Local warehouse.")
    ] = Path(".contractline"),
) -> None:
    """Run a single plan step: what each generated Airflow task calls."""
    from contractline.adapters.compute.spark_delta_local import LocalSparkDelta
    from contractline.runner import (
        UnknownStepError,
        build_source_factory,
        run_step,
    )

    loaded = _load_project_or_exit(project)
    try:
        summary = run_step(
            LocalSparkDelta(workdir),
            loaded,
            env,
            step,
            build_source_factory(kafka_bootstrap_servers),
            EnvSecretProvider(),
        )
    except UnknownStepError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=2) from exc
    typer.echo(summary)


@app.command()
def dags(
    paths: PathsArgument,
    out: Annotated[Path, typer.Option(help="Airflow Dags folder.")] = Path("infra/airflow/dags"),
    env: EnvOption = "dev",
    check: Annotated[
        bool, typer.Option("--check", help="Fail if the folder is not up to date; write nothing.")
    ] = False,
) -> None:
    """Generate one Airflow Dag per plan step."""
    generated = build_dags(build_plan(_load_project_or_exit(paths), env), env)
    if check:
        problems = find_stale_dags(generated, out)
        for problem in problems:
            typer.echo(problem, err=True)
        if problems:
            typer.echo("Run `contractline dags` to regenerate.", err=True)
            raise typer.Exit(code=1)
        typer.echo(f"{len(generated)} Dag(s) up to date.")
        return
    for change in write_dags(generated, out) or ["no changes"]:
        typer.echo(change)


def _git(*args: str) -> str:
    result = subprocess.run(["git", *args], capture_output=True, text=True, check=False)  # noqa: S603, S607
    if result.returncode != 0:
        raise RuntimeError(f"git {' '.join(args)}: {result.stderr.strip()}")
    return result.stdout


@app.command()
def ci(
    base: Annotated[str, typer.Option(help="Base ref the PR merges into.")] = "origin/main",
    branch: Annotated[str, typer.Option(envvar="CONTRACTLINE_BRANCH", help="PR branch.")] = "",
    pr_title: Annotated[str, typer.Option(envvar="CONTRACTLINE_PR_TITLE")] = "",
    env: EnvOption = "local",
    project: Annotated[
        list[Path] | None, typer.Option("--project", help="Default: contracts/ and pipelines/.")
    ] = None,
    summary: Annotated[
        Path | None, typer.Option(help="Write the PR comment (Markdown) here.")
    ] = None,
    run_tests: Annotated[bool, typer.Option("--tests/--no-tests")] = True,
) -> None:
    """Everything a pipeline PR must pass: scope, naming, contract diffs, generated tests."""
    changed = [p for p in _git("diff", "--name-only", f"{base}...HEAD").splitlines() if p]
    out_of_scope = find_out_of_scope(branch, changed)

    lint_problems: list[str] = []
    config_path = find_config(Path.cwd())
    project_config = load_config(config_path) if config_path else None
    if project_config is not None:
        if branch:
            lint_problems += [str(i) for i in lint_branch(branch, project_config)]
        if pr_title:
            lint_problems += [
                str(i) for i in lint_pull_request_title(pr_title, branch, project_config)
            ]

    diffs: dict[str, dict[str, object]] = {}
    for path in changed:
        file = Path(path)
        if file.suffix != ".yaml" or not file.exists():
            continue
        parsed, errors = parse_file(file)
        if errors:
            lint_problems += [f"{path}: {e}" for e in errors]
            continue
        if isinstance(parsed, GoldModel):
            if project_config:
                lint_problems += [str(i) for i in lint_gold_model(file, parsed, project_config)]
            continue
        if parsed is None:
            continue
        if project_config:
            lint_problems += [str(i) for i in lint_contract(file, parsed, project_config)]
        try:
            old_text = _git("show", f"{base}:{path}")
        except RuntimeError:
            diffs[path] = {"new": True}
            continue
        old, old_errors = parse_text(old_text)
        if old is None:
            diffs[path] = {"new": True, "previous_invalid": [str(e) for e in old_errors]}
            continue
        diffs[path] = build_report(diff_contracts(old, parsed))

    paths = project or [p for p in (Path("contracts"), Path("pipelines")) if p.exists()]
    report_dict = None
    if run_tests and paths and not out_of_scope:
        from contractline.adapters.compute.spark_delta_local import (
            LocalSparkDelta,
        )
        from contractline.testing.generated import run_generated_tests

        results = run_generated_tests(
            LocalSparkDelta(Path(tempfile.mkdtemp(prefix="contractline-ci-"))),
            _load_project_or_exit(paths),
            env,
            progress=lambda m: typer.echo(f"... {m}"),
        )
        report_dict = json.loads(results.to_json())

    text = build_ci_summary(diffs, report_dict, out_of_scope, lint_problems)
    typer.echo(text)
    if summary:
        summary.write_text(text, "utf-8")
    failed = (
        out_of_scope
        or lint_problems
        or any(d.get("version_problem") for d in diffs.values())
        or (report_dict is not None and not report_dict["passed"])
    )
    if failed:
        raise typer.Exit(code=1)


@app.command("eval-log")
def eval_log(
    scenario: Annotated[str, typer.Argument(help="Scenario id, e.g. S01.")],
    event: Annotated[str, typer.Argument(help="Event type (see evaluation/protocol.md).")],
    run_id: Annotated[str, typer.Option("--run", help="Run id, e.g. 2026-10-12-claude.")],
    data: Annotated[str, typer.Option(help="Extra fields as a JSON object.")] = "{}",
    logs: Annotated[Path, typer.Option(help="Log folder.")] = Path("evaluation/logs"),
) -> None:
    """Append one event to a scenario run's log."""
    if event not in EVENT_TYPES:
        typer.echo(f"Unknown event {event!r}; use one of {', '.join(EVENT_TYPES)}.", err=True)
        raise typer.Exit(code=2)
    fields = json.loads(data)
    record = {
        "scenario": scenario,
        "run": run_id,
        "at": datetime.now(UTC).isoformat(timespec="seconds"),
        "event": event,
        **fields,
    }
    logs.mkdir(parents=True, exist_ok=True)
    with (logs / f"{scenario}-{run_id}.jsonl").open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record) + "\n")
    typer.echo(f"logged {event} for {scenario}/{run_id}")


@app.command("eval-report")
def eval_report(
    logs: Annotated[Path, typer.Option(help="Log folder.")] = Path("evaluation/logs"),
    scenarios: Annotated[Path, typer.Option(help="Scenario folder.")] = Path(
        "evaluation/scenarios"
    ),
    baseline: Annotated[Path, typer.Option(help="Hand-built estimates (YAML).")] = Path(
        "evaluation/baseline.yaml"
    ),
    out: Annotated[Path | None, typer.Option(help="Write the report here.")] = None,
) -> None:
    """Turn the evaluation logs into the results report."""
    runs = load_runs([logs]) if logs.exists() else []
    metrics = [compute_run_metrics(r) for r in runs]
    estimates = load_yaml(baseline.read_text("utf-8")) if baseline.exists() else {}
    estimates = {k: float(v["hours"]) for k, v in (estimates or {}).get("scenarios", {}).items()}
    count = len(list(scenarios.glob("S*.md"))) if scenarios.exists() else 0
    text = build_eval_report(
        metrics, summarize_runs(metrics, runs), compare_to_baseline(metrics, estimates), count
    )
    if out:
        out.write_text(text, "utf-8")
    typer.echo(text)
