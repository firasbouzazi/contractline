from pathlib import Path

from typer.testing import CliRunner

from contractline import __version__
from contractline.cli import app

runner = CliRunner()


def test_version_flag_prints_version() -> None:
    result = runner.invoke(app, ["--version"])

    assert result.exit_code == 0
    assert result.stdout.strip() == f"contractline {__version__}"


def test_no_arguments_shows_help() -> None:
    result = runner.invoke(app, [])

    assert "Usage" in result.output


def test_validate_accepts_valid_contracts(examples_dir: Path) -> None:
    result = runner.invoke(app, ["validate", str(examples_dir)])

    assert result.exit_code == 0
    assert result.stdout.strip() == "2 contract(s) valid."


def test_validate_reports_errors_and_fails(tmp_path: Path, examples_dir: Path) -> None:
    bad = tmp_path / "bad.yaml"
    bad.write_text((examples_dir / "sales" / "orders.yaml").read_text().replace("pii: none", ""))

    result = runner.invoke(app, ["validate", str(bad), str(examples_dir / "sales")])

    assert result.exit_code == 1
    assert f"{bad}: schema.columns[0]: 'pii' is a required property" in result.stderr
    assert "1 of 3 contract(s) invalid." in result.stderr


def test_validate_fails_when_no_contracts_found(tmp_path: Path) -> None:
    result = runner.invoke(app, ["validate", str(tmp_path)])

    assert result.exit_code == 1
    assert "No contract files found." in result.stderr
