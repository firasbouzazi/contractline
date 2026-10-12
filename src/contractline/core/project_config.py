"""Project configuration (``contractline.yaml``): domains and abbreviations."""

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from contractline.core.yaml_loader import load_yaml

CONFIG_FILE = "contractline.yaml"
_IDENTIFIER_CHARS = set("abcdefghijklmnopqrstuvwxyz0123456789_")


class ConfigError(ValueError):
    pass


@dataclass(frozen=True)
class ProjectConfig:
    domains: frozenset[str]
    glossary: Mapping[str, str]
    banned_abbreviations: Mapping[str, str]


def _read_word_map(raw: Mapping[str, Any], key: str) -> dict[str, str]:
    value = raw.get(key, {})
    if not isinstance(value, dict) or not all(
        isinstance(k, str) and isinstance(v, str) for k, v in value.items()
    ):
        raise ConfigError(f"'{key}' must map each abbreviation to what it stands for")
    return value


def parse_config(raw: Any) -> ProjectConfig:
    if not isinstance(raw, dict):
        raise ConfigError("the configuration must be a mapping")
    unknown = raw.keys() - {"domains", "glossary", "banned_abbreviations"}
    if unknown:
        raise ConfigError(f"unknown keys: {', '.join(sorted(unknown))}")
    domains = raw.get("domains")
    if (
        not isinstance(domains, list)
        or not domains
        or not all(isinstance(d, str) and d and set(d) <= _IDENTIFIER_CHARS for d in domains)
    ):
        raise ConfigError("'domains' must be a non-empty list of snake_case names")
    return ProjectConfig(
        domains=frozenset(domains),
        glossary=_read_word_map(raw, "glossary"),
        banned_abbreviations=_read_word_map(raw, "banned_abbreviations"),
    )


def find_config(start: Path) -> Path | None:
    """Look for ``contractline.yaml`` in ``start`` and its parents."""
    for directory in (start, *start.parents):
        candidate = directory / CONFIG_FILE
        if candidate.is_file():
            return candidate
    return None


def load_config(path: Path) -> ProjectConfig:
    try:
        return parse_config(load_yaml(path.read_text("utf-8")))
    except ConfigError as exc:
        raise ConfigError(f"{path}: {exc}") from exc
