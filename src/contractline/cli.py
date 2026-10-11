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
