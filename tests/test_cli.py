import json
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


def _write_changed(tmp_path: Path, examples_dir: Path, version: str) -> Path:
    text = (examples_dir / "sales" / "customers.yaml").read_text()
    text = text.replace("version: 1.0.0", f"version: {version}")
    text = text.replace("freshness: PT1H", "freshness: P1D")
    path = tmp_path / "customers.yaml"
    path.write_text(text)
    return path


def test_diff_reports_changes(tmp_path: Path, examples_dir: Path) -> None:
    new = _write_changed(tmp_path, examples_dir, "1.1.0")

    result = runner.invoke(app, ["diff", str(examples_dir / "sales" / "customers.yaml"), str(new)])

    assert result.exit_code == 0
    assert result.stdout.splitlines() == [
        "sla.freshness: changed from PT1H to P1D [minor, relaxes a guarantee]",
        "Required version bump: minor",
        "Some changes relax a guarantee and need the data owner's review.",
    ]


def test_diff_fails_on_wrong_version(tmp_path: Path, examples_dir: Path) -> None:
    new = _write_changed(tmp_path, examples_dir, "1.0.1")

    result = runner.invoke(app, ["diff", str(examples_dir / "sales" / "customers.yaml"), str(new)])

    assert result.exit_code == 1
    assert "Version error: changes need a minor version bump" in result.stderr


def test_diff_fail_on_relax(tmp_path: Path, examples_dir: Path) -> None:
    new = _write_changed(tmp_path, examples_dir, "1.1.0")
    old = str(examples_dir / "sales" / "customers.yaml")

    result = runner.invoke(app, ["diff", old, str(new), "--fail-on-relax", "--format", "json"])

    assert result.exit_code == 1
    assert json.loads(result.stdout)["relaxes_guarantees"] is True


def test_diff_no_changes(examples_dir: Path) -> None:
    path = str(examples_dir / "sales" / "customers.yaml")

    result = runner.invoke(app, ["diff", path, path])

    assert result.exit_code == 0
    assert result.stdout.strip() == "No changes."


def test_diff_rejects_invalid_contract(tmp_path: Path, examples_dir: Path) -> None:
    bad = tmp_path / "bad.yaml"
    bad.write_text("api_version: contractline/v0\n")

    result = runner.invoke(app, ["diff", str(examples_dir / "sales" / "orders.yaml"), str(bad)])

    assert result.exit_code == 1
    assert "is not a valid contract" in result.stderr
