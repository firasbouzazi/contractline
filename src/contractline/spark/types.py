"""Logical contract types to Spark SQL types."""

import re

from pyspark.sql import types as T

_DECIMAL = re.compile(r"^decimal\((\d+),(\d+)\)$")

_SIMPLE: dict[str, T.DataType] = {
    "string": T.StringType(),
    "integer": T.IntegerType(),
    "long": T.LongType(),
    "double": T.DoubleType(),
    "boolean": T.BooleanType(),
    "date": T.DateType(),
    "timestamp": T.TimestampType(),
}


def to_spark_type(logical_type: str) -> T.DataType:
    if logical_type in _SIMPLE:
        return _SIMPLE[logical_type]
    match = _DECIMAL.match(logical_type)
    if match:
        return T.DecimalType(int(match[1]), int(match[2]))
    raise ValueError(f"unknown logical type {logical_type!r}")
