"""Command-line entry point.

Commands are added one per change as they are implemented
(validate, lint, plan, test, run, new-dataset).
"""

from pathlib import Path

import typer

from contractline import __version__
from contractline.core.validation import parse_text

app = typer.Typer(no_args_is_help=True, add_completion=False)

CONTRACT_SUFFIXES = (".yaml", ".yml")


def _print_version(value: bool) -> None:
    if value:
        typer.echo(f"contractline {__version__}")
        raise typer.Exit


@app.callback()
def main(
    version: bool = typer.Option(
        False,
        "--version",
        callback=_print_version,
        is_eager=True,
        help="Show the version and exit.",
    ),
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
    paths: list[Path] = typer.Argument(  # noqa: B008
        ..., exists=True, help="Contract files, or directories searched for *.yaml files."
    ),
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
