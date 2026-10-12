from pathlib import Path
from typing import Any

import pytest

from contractline.core.project_config import (
    ConfigError,
    find_config,
    load_config,
    parse_config,
)

REPO_ROOT = Path(__file__).parent.parent.parent


def test_repository_config_is_valid() -> None:
    config = load_config(REPO_ROOT / "contractline.yaml")

    assert "sales" in config.domains
    assert config.glossary["id"] == "identifier"
    assert config.banned_abbreviations["cust"] == "customer"


def test_find_config_searches_parents(tmp_path: Path) -> None:
    (tmp_path / "contractline.yaml").write_text("domains: [sales]\n")
    nested = tmp_path / "a" / "b"
    nested.mkdir(parents=True)

    assert find_config(nested) == tmp_path / "contractline.yaml"


@pytest.mark.parametrize(
    ("raw", "message"),
    [
        ([], "must be a mapping"),
        ({"domains": []}, "'domains' must be a non-empty list"),
        ({"domains": ["Sales"]}, "'domains' must be a non-empty list"),
        ({"domains": ["sales"], "extra": 1}, "unknown keys: extra"),
        ({"domains": ["sales"], "glossary": ["id"]}, "'glossary' must map"),
        ({"domains": ["sales"], "banned_abbreviations": {"x": 1}}, "'banned_abbreviations'"),
    ],
)
def test_invalid_config(raw: Any, message: str) -> None:
    with pytest.raises(ConfigError, match=message):
        parse_config(raw)
