"""Invariant checks on silver data, used by generated tests (ADR-0013) and the
harness's own tests. Each returns human-readable violations; empty means OK."""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from contractline.spark.history import IS_CURRENT, VALID_FROM, VALID_TO

SAMPLE = 5


def find_scd2_violations(df: DataFrame, keys: list[str]) -> list[str]:
    """One current row per key, no overlapping or inverted validity windows,
    and current rows (only) have an open ``_valid_to``."""
    violations: list[str] = []
    several_current = df.where(F.col(IS_CURRENT)).groupBy(*keys).count().where(F.col("count") > 1)
    for row in several_current.limit(SAMPLE).collect():
        violations.append(f"{[row[k] for k in keys]}: {row['count']} current rows")

    order = Window.partitionBy(*keys).orderBy(VALID_FROM)
    ordered = df.withColumn("_next_from", F.lead(VALID_FROM).over(order))
    overlapping = ordered.where(
        F.col("_next_from").isNotNull()
        & (F.col(VALID_TO).isNull() | (F.col(VALID_TO) > F.col("_next_from")))
    )
    for row in overlapping.limit(SAMPLE).collect():
        violations.append(f"{[row[k] for k in keys]}: version from {row[VALID_FROM]} overlaps")

    inverted = df.where(F.col(VALID_TO).isNotNull() & (F.col(VALID_TO) < F.col(VALID_FROM)))
    for row in inverted.limit(SAMPLE).collect():
        violations.append(f"{[row[k] for k in keys]}: _valid_to before _valid_from")

    flag_mismatch = df.where(F.col(IS_CURRENT) != F.col(VALID_TO).isNull())
    for row in flag_mismatch.limit(SAMPLE).collect():
        violations.append(f"{[row[k] for k in keys]}: _is_current disagrees with _valid_to")
    return violations


def find_duplicate_keys(df: DataFrame, keys: list[str]) -> list[str]:
    duplicates = df.groupBy(*keys).count().where(F.col("count") > 1)
    return [
        f"{[row[k] for k in keys]}: {row['count']} rows"
        for row in duplicates.limit(SAMPLE).collect()
    ]
