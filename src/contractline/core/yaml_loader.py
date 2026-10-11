"""Strict YAML loading for contracts.

PyYAML's default behaviour silently accepts duplicate keys (the last one wins)
and follows YAML 1.1, where ``no``, ``off`` and ``y`` become booleans and
``2024-01-01`` becomes a date. In a contract both are bugs waiting to happen
(``accepted_values: [yes, no]``), so this loader rejects duplicates and only
treats ``true``/``false`` as booleans and never parses timestamps.
"""

import re
from typing import Any

import yaml

_YAML12_BOOL = re.compile(r"^(?:true|True|TRUE|false|False|FALSE)$")


class DuplicateKeyError(yaml.MarkedYAMLError):
    pass


class _StrictLoader(yaml.SafeLoader):
    pass


def _build_resolvers() -> dict[str, list[tuple[str, re.Pattern[str]]]]:
    resolvers: dict[str, list[tuple[str, re.Pattern[str]]]] = {}
    for first_char, entries in yaml.SafeLoader.yaml_implicit_resolvers.items():
        kept = [
            (tag, regexp)
            for tag, regexp in entries
            if tag not in ("tag:yaml.org,2002:bool", "tag:yaml.org,2002:timestamp")
        ]
        if kept:
            resolvers[first_char] = kept
    for first_char in "tTfF":
        resolvers.setdefault(first_char, []).append(("tag:yaml.org,2002:bool", _YAML12_BOOL))
    return resolvers


_StrictLoader.yaml_implicit_resolvers = _build_resolvers()


def _construct_mapping(loader: _StrictLoader, node: yaml.MappingNode) -> dict[Any, Any]:
    loader.flatten_mapping(node)
    mapping: dict[Any, Any] = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=True)
        if key in mapping:
            raise DuplicateKeyError(
                "while constructing a mapping",
                node.start_mark,
                f"found duplicate key {key!r}",
                key_node.start_mark,
            )
        mapping[key] = loader.construct_object(value_node, deep=True)
    return mapping


_StrictLoader.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, _construct_mapping)


def load_yaml(text: str) -> Any:
    """Parse one YAML document strictly. Raises ``yaml.YAMLError`` on bad input."""
    return yaml.load(text, Loader=_StrictLoader)  # noqa: S506 - safe loader subclass
