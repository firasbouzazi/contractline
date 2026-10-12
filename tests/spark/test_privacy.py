import hashlib
import hmac
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from contractline.adapters.compute.spark_delta_local import LocalSparkDelta
from contractline.core.contract import Column, Contract, Masking, MaskingStrategy, PiiClass
from contractline.core.secrets import MissingSecretError
from contractline.core.validation import load_contract
from contractline.spark.privacy import (
    MaskingConfigError,
    apply_masking,
    build_hmac_sha256,
    build_masked_column,
)
from tests.spark.secrets_fixtures import HASH_KEY, TOKEN_KEY, FakeSecrets

EXAMPLES = Path(__file__).parent.parent.parent / "examples" / "contracts"

# Building Spark column expressions needs an active session.
pytestmark = pytest.mark.usefixtures("compute")


def expected_hmac(key: bytes, text: str) -> str:
    return hmac.new(key, text.encode(), hashlib.sha256).hexdigest()


def mask_one(
    compute: LocalSparkDelta, value: Any, spark_type: str, column: Column, masking: Masking
) -> Any:
    df = compute.spark.createDataFrame([(value,)], f"`{column.name}` {spark_type}")
    expression = build_masked_column(column, masking, FakeSecrets())
    assert expression is not None
    return df.select(expression.alias("v")).first()["v"]  # type: ignore[index]


@settings(max_examples=25, deadline=None)
@given(text=st.text(max_size=40), key=st.binary(min_size=1, max_size=100))
def test_hmac_matches_python(compute: LocalSparkDelta, text: str, key: bytes) -> None:
    df = compute.spark.createDataFrame([(text,)], "v string")

    result = df.select(build_hmac_sha256(key, df["v"]).alias("h")).first()

    assert result is not None
    assert result["h"] == expected_hmac(key, text)


def column(name: str, column_type: str) -> Column:
    return Column(name=name, type=column_type, nullable=True, pii=PiiClass.DIRECT)


@pytest.mark.parametrize(
    ("value", "spark_type", "col", "masking", "expected"),
    [
        (
            "ada@example.com",
            "string",
            column("email", "string"),
            Masking(MaskingStrategy.HASH),
            expected_hmac(HASH_KEY, "ada@example.com"),
        ),
        (
            42,
            "long",
            column("customer_id", "long"),
            Masking(MaskingStrategy.HASH),
            expected_hmac(HASH_KEY, "42"),
        ),
        (
            "ada@example.com",
            "string",
            column("email", "string"),
            Masking(MaskingStrategy.TOKENIZE),
            "tok_" + expected_hmac(TOKEN_KEY, "ada@example.com")[:32],
        ),
        (None, "string", column("email", "string"), Masking(MaskingStrategy.HASH), None),
        (
            "Ada",
            "string",
            column("full_name", "string"),
            Masking(MaskingStrategy.REDACT),
            "[REDACTED]",
        ),
        (5, "integer", column("age", "integer"), Masking(MaskingStrategy.REDACT), None),
        (
            date(1985, 4, 12),
            "date",
            column("birth_date", "date"),
            Masking(MaskingStrategy.GENERALIZE),
            date(1985, 1, 1),
        ),
        (
            date(1985, 4, 12),
            "date",
            column("birth_date", "date"),
            Masking(MaskingStrategy.GENERALIZE, params={"to": "month"}),
            date(1985, 4, 1),
        ),
        (
            datetime(2026, 5, 17, 10, 30),
            "timestamp",
            column("seen_at", "timestamp"),
            Masking(MaskingStrategy.GENERALIZE),
            datetime(2026, 1, 1),
        ),
        (
            37,
            "integer",
            column("age", "integer"),
            Masking(MaskingStrategy.GENERALIZE, params={"bucket": 10}),
            30,
        ),
        (
            "75011",
            "string",
            column("postal_code", "string"),
            Masking(MaskingStrategy.GENERALIZE, params={"keep_chars": 2}),
            "75",
        ),
        (
            "ada@example.com",
            "string",
            column("email", "string"),
            Masking(MaskingStrategy.KEEP, justification="test"),
            "ada@example.com",
        ),
    ],
)
def test_strategies(
    compute: LocalSparkDelta,
    value: Any,
    spark_type: str,
    col: Column,
    masking: Masking,
    expected: Any,
) -> None:
    assert mask_one(compute, value, spark_type, col, masking) == expected


@pytest.mark.parametrize(
    ("col", "masking", "message"),
    [
        (column("d", "date"), Masking(MaskingStrategy.GENERALIZE, {"to": "day"}), "year or month"),
        (column("n", "integer"), Masking(MaskingStrategy.GENERALIZE), "positive 'bucket'"),
        (column("s", "string"), Masking(MaskingStrategy.GENERALIZE), "positive 'keep_chars'"),
        (column("b", "boolean"), Masking(MaskingStrategy.GENERALIZE), "does not apply"),
    ],
)
def test_generalize_config_errors(col: Column, masking: Masking, message: str) -> None:
    with pytest.raises(MaskingConfigError, match=message):
        build_masked_column(col, masking, FakeSecrets())


def test_missing_key_fails_before_any_data_is_processed() -> None:
    with pytest.raises(MissingSecretError):
        build_masked_column(column("e", "string"), Masking(MaskingStrategy.HASH), FakeSecrets({}))


def test_custom_secret_name() -> None:
    masking = Masking(MaskingStrategy.HASH, params={"key": "crm_hash"})
    secrets = FakeSecrets({"crm_hash": b"another-key-0123456789"})

    assert build_masked_column(column("e", "string"), masking, secrets) is not None


def test_apply_masking_on_example_contract(compute: LocalSparkDelta) -> None:
    contract: Contract = load_contract(EXAMPLES / "sales" / "customers.yaml")
    df = compute.spark.createDataFrame(
        [(1, "ada@example.com", "Ada Example", date(1985, 4, 12), "FR", "x")],
        "customer_id long, email string, full_name string, birth_date date, "
        "country_code string, _op string",
    )

    row = apply_masking(df, contract, FakeSecrets()).first()

    assert row is not None
    assert row.asDict() == {
        "customer_id": 1,
        "email": expected_hmac(HASH_KEY, "ada@example.com"),
        "birth_date": date(1985, 1, 1),
        "country_code": "FR",
        "_op": "x",
    }
