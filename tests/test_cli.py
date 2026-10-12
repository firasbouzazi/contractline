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


REPO_CONFIG = Path(__file__).parent.parent / "contractline.yaml"


def test_lint_passes_on_examples(examples_dir: Path) -> None:
    result = runner.invoke(
        app,
        [
            "lint",
            str(examples_dir),
            "--config",
            str(REPO_CONFIG),
            "--branch",
            "agent/DATA-1-sales-customers",
            "--pr-title",
            "[sales/customers] add contract",
        ],
    )

    assert result.exit_code == 0, result.output
    assert result.stdout.strip() == "No problems found."


def test_lint_reports_problems(tmp_path: Path, examples_dir: Path) -> None:
    contract = tmp_path / "contracts" / "sales" / "clients.yaml"
    contract.parent.mkdir(parents=True)
    contract.write_text((examples_dir / "sales" / "customers.yaml").read_text())
    message = tmp_path / "COMMIT_EDITMSG"
    message.write_text("added stuff\n")

    result = runner.invoke(
        app,
        [
            "lint",
            str(contract),
            "--config",
            str(REPO_CONFIG),
            "--branch",
            "wip",
            "--commit-message-file",
            str(message),
        ],
    )

    assert result.exit_code == 1
    assert "path implies sales/clients" in result.stderr
    assert "branch: 'wip' must be" in result.stderr
    assert "commit: 'added stuff' is not a Conventional Commit header" in result.stderr
    assert "3 problem(s) found." in result.stderr


def test_lint_reports_invalid_contracts(tmp_path: Path) -> None:
    bad = tmp_path / "bad.yaml"
    bad.write_text("api_version: contractline/v0\n")

    result = runner.invoke(app, ["lint", str(bad), "--config", str(REPO_CONFIG)])

    assert result.exit_code == 1
    assert "'identity' is a required property" in result.stderr


def test_lint_needs_something_to_check() -> None:
    result = runner.invoke(app, ["lint", "--config", str(REPO_CONFIG)])

    assert result.exit_code == 2
    assert "Nothing to lint" in result.stderr


def test_lint_rejects_invalid_config(tmp_path: Path) -> None:
    config = tmp_path / "contractline.yaml"
    config.write_text("domains: []\n")

    result = runner.invoke(app, ["lint", "--branch", "main", "--config", str(config)])

    assert result.exit_code == 2
    assert "Invalid configuration" in result.stderr


def test_validate_and_lint_accept_gold_models(examples_dir: Path) -> None:
    pipelines = str(examples_dir.parent / "pipelines")

    validated = runner.invoke(app, ["validate", pipelines])
    linted = runner.invoke(app, ["lint", pipelines, "--config", str(REPO_CONFIG)])

    assert validated.exit_code == 0, validated.output
    assert validated.stdout.strip() == "2 contract(s) valid."
    assert linted.exit_code == 0, linted.output


def test_lint_reports_misplaced_gold_model(tmp_path: Path, examples_dir: Path) -> None:
    gold = examples_dir.parent / "pipelines" / "sales" / "gold"
    target = tmp_path / "models"
    target.mkdir()
    for suffix in (".yaml", ".sql"):
        (target / f"fct_orders{suffix}").write_text((gold / f"fct_orders{suffix}").read_text())

    result = runner.invoke(app, ["lint", str(target), "--config", str(REPO_CONFIG)])

    assert result.exit_code == 1
    assert "gold models live at pipelines/sales/gold/fct_orders.yaml" in result.stderr


def test_validate_reports_gold_without_sql(tmp_path: Path, examples_dir: Path) -> None:
    gold = examples_dir.parent / "pipelines" / "sales" / "gold"
    (tmp_path / "fct_orders.yaml").write_text((gold / "fct_orders.yaml").read_text())

    result = runner.invoke(app, ["validate", str(tmp_path)])

    assert result.exit_code == 1
    assert "missing SQL file fct_orders.sql" in result.stderr


def test_plan_command(examples_dir: Path) -> None:
    result = runner.invoke(app, ["plan", str(examples_dir.parent), "--env", "pr3"])

    assert result.exit_code == 0, result.output
    assert "pr3_gold.sales.agg_daily_revenue" in result.stdout


def test_plan_rejects_unknown_env(examples_dir: Path) -> None:
    result = runner.invoke(app, ["plan", str(examples_dir.parent), "--env", "production"])

    assert result.exit_code == 2


def test_new_dataset_creates_a_valid_contract(tmp_path: Path) -> None:
    (tmp_path / "contractline.yaml").write_text(REPO_CONFIG.read_text())

    created = runner.invoke(
        app, ["new-dataset", "--domain", "sales", "--entity", "returns", "--root", str(tmp_path)]
    )
    again = runner.invoke(
        app, ["new-dataset", "--domain", "sales", "--entity", "returns", "--root", str(tmp_path)]
    )
    unknown = runner.invoke(
        app, ["new-dataset", "--domain", "crm", "--entity", "x", "--root", str(tmp_path)]
    )

    assert created.exit_code == 0, created.output
    assert runner.invoke(app, ["validate", str(tmp_path / "contracts")]).exit_code == 0
    assert again.exit_code == 1
    assert unknown.exit_code == 2
