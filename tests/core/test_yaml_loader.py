import pytest
import yaml

from contractline.core.yaml_loader import DuplicateKeyError, load_yaml


def test_rejects_duplicate_keys() -> None:
    with pytest.raises(DuplicateKeyError, match="duplicate key 'name'"):
        load_yaml("name: a\nname: b\n")


@pytest.mark.parametrize("word", ["yes", "no", "on", "off", "y", "n", "Yes", "NO"])
def test_yaml_11_booleans_stay_strings(word: str) -> None:
    assert load_yaml(f"value: {word}") == {"value": word}


@pytest.mark.parametrize(("text", "expected"), [("true", True), ("False", False)])
def test_yaml_12_booleans_are_booleans(text: str, expected: bool) -> None:
    assert load_yaml(f"value: {text}") == {"value": expected}


def test_dates_are_not_parsed() -> None:
    assert load_yaml("value: 2024-01-31") == {"value": "2024-01-31"}


def test_numbers_are_still_numbers() -> None:
    assert load_yaml("a: 1\nb: 0.5") == {"a": 1, "b": 0.5}


def test_rejects_unsafe_tags() -> None:
    with pytest.raises(yaml.YAMLError):
        load_yaml("value: !!python/object/apply:os.system ['true']")
