"""Command-line entry point.

Commands are added one per change as they are implemented
(validate, lint, plan, test, run, new-dataset).
"""

import json
from enum import StrEnum
from pathlib import Path
from typing import Annotated

import typer

from contractline import __version__
from contractline.core.diff import build_report, diff_contracts
from contractline.core.lint import (
    LintIssue,
    lint_branch,
    lint_commit_message,
    lint_contract,
    lint_pull_request_title,
)
from contractline.core.project_config import ConfigError, find_config, load_config
from contractline.core.validation import InvalidContractError, load_contract, parse_text

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
        _, errors = parse_text(file.read_text("utf-8"))
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
        contract, errors = parse_text(file.read_text("utf-8"))
        if contract is None:
            issues.extend(f"{file}: {error}" for error in errors)
        else:
            issues.extend(lint_contract(file, contract, project))
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
