"""Property-based tests: the validator must never crash, whatever it is given."""

from typing import Any

from hypothesis import given, settings
from hypothesis import strategies as st
from jsonschema import Draft202012Validator

from contractline.core.validation import get_json_schema, parse_text, validate_document

json_values: st.SearchStrategy[Any] = st.recursive(
    st.none() | st.booleans() | st.integers() | st.floats(allow_nan=False) | st.text(),
    lambda children: (
        st.lists(children, max_size=4) | st.dictionaries(st.text(max_size=12), children, max_size=4)
    ),
    max_leaves=20,
)


def test_json_schema_is_valid_draft_2020_12() -> None:
    Draft202012Validator.check_schema(get_json_schema())


@settings(max_examples=300)
@given(json_values)
def test_validate_document_returns_errors_or_contract(doc: Any) -> None:
    contract, errors = validate_document(doc)

    assert (contract is None) == bool(errors)


@settings(max_examples=300)
@given(st.text())
def test_parse_text_never_raises(text: str) -> None:
    contract, errors = parse_text(text)

    assert contract is None or errors == []
