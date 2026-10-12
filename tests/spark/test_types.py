import pytest
from pyspark.sql import types as T

from contractline.spark.types import to_spark_type


@pytest.mark.parametrize(
    ("logical", "expected"),
    [
        ("string", T.StringType()),
        ("integer", T.IntegerType()),
        ("long", T.LongType()),
        ("double", T.DoubleType()),
        ("boolean", T.BooleanType()),
        ("date", T.DateType()),
        ("timestamp", T.TimestampType()),
        ("decimal(12,2)", T.DecimalType(12, 2)),
    ],
)
def test_to_spark_type(logical: str, expected: T.DataType) -> None:
    assert to_spark_type(logical) == expected


def test_unknown_type() -> None:
    with pytest.raises(ValueError, match="unknown logical type"):
        to_spark_type("varchar")
