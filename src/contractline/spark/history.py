"""History block: append, snapshot, SCD1 and SCD2 (ADR-0012).

Keyed histories (SCD1, SCD2) are rebuilt per key rather than patched: for
every key touched by a batch, the stored rows are turned back into change
events, merged with the new events, ordered, and the key's history is computed
again from scratch. That makes late and out-of-order records, deletes and
re-runs of the same batch all the same case, and keeps the result
deterministic. The rebuilt rows replace the old ones in one Delta ``MERGE``.

Event order is ``(_event_at, _sequence, _source_offset)``. ``_event_at`` is
the contract's ``sequence_column`` for inserts and updates, and the source
timestamp for deletes (a delete's before-image carries the old value).
"""

from collections.abc import Sequence

from delta.tables import DeltaTable
from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.window import WindowSpec

from contractline.core.contract import Contract, HistoryType
from contractline.spark.bronze import OP_DELETE

VALID_FROM = "_valid_from"
VALID_TO = "_valid_to"
IS_CURRENT = "_is_current"
HASH_DIFF = "_hash_diff"
EVENT_AT = "_event_at"
LINEAGE_COLUMNS = ("_sequence", "_source_offset", "_ingested_at")
SCD2_COLUMNS = (VALID_FROM, VALID_TO, IS_CURRENT, HASH_DIFF)
_MAX_SEQUENCE = 2**63 - 1
REBUILT_OFFSET = "rebuilt:"
_NULL_MARK = "\u0000"
_SEPARATOR = "\u001f"


def compute_hash_diff(columns: Sequence[str]) -> Column:
    """SHA-256 over the columns' text, telling null apart from an empty string."""
    parts = [F.coalesce(F.col(c).cast("string"), F.lit(_NULL_MARK)) for c in columns]
    return F.sha2(F.concat_ws(_SEPARATOR, *parts), 256)


def get_business_key(contract: Contract) -> list[str]:
    return list(contract.history.business_key or contract.schema.primary_key)


def get_value_columns(df: DataFrame, contract: Contract) -> list[str]:
    """Contract columns still present after masking (dropped ones are gone)."""
    return [c.name for c in contract.schema.columns if c.name in df.columns]


def _event_time(contract: Contract) -> Column:
    sequence_column = contract.history.sequence_column
    upsert_time = F.col(sequence_column) if sequence_column else F.col("_source_at")
    return F.when(F.col("_op") == OP_DELETE, F.col("_source_at")).otherwise(
        F.coalesce(upsert_time.cast("timestamp"), F.col("_source_at"))
    )


def build_incoming_events(valid: DataFrame, contract: Contract) -> DataFrame:
    """Masked, valid silver records as history events."""
    values = get_value_columns(valid, contract)
    return valid.select(
        *values,
        F.col("_op"),
        _event_time(contract).alias(EVENT_AT),
        *LINEAGE_COLUMNS,
    )


def _order(keys: list[str]) -> WindowSpec:
    return Window.partitionBy(*keys).orderBy(EVENT_AT, "_sequence", "_source_offset")


def rebuild_scd2_events(existing: DataFrame, keys: list[str], values: list[str]) -> DataFrame:
    """Stored SCD2 rows back to events: one upsert per version, plus a delete
    at ``_valid_to`` when a key's last version is closed."""
    # Rebuilt events get their own offsets: the stored lineage belongs to the
    # version's latest event, which may also come back from bronze.
    upserts = existing.select(
        *values,
        F.lit("update").alias("_op"),
        F.col(VALID_FROM).alias(EVENT_AT),
        F.col("_sequence"),
        F.concat(F.lit(REBUILT_OFFSET), F.col(VALID_FROM).cast("string")).alias("_source_offset"),
        F.col("_ingested_at"),
    )
    last = Window.partitionBy(*keys).orderBy(F.col(VALID_FROM).desc())
    deletes = (
        existing.withColumn("_rank", F.row_number().over(last))
        .where((F.col("_rank") == 1) & ~F.col(IS_CURRENT))
        .select(
            *[F.col(c) if c in keys else F.lit(None).alias(c) for c in values],
            F.lit(OP_DELETE).alias("_op"),
            F.col(VALID_TO).alias(EVENT_AT),
            F.lit(_MAX_SEQUENCE).cast("long").alias("_sequence"),
            F.lit(REBUILT_OFFSET + "delete").alias("_source_offset"),
            F.col("_ingested_at"),
        )
    )
    return upserts.unionByName(deletes)


def compute_scd2(events: DataFrame, contract: Contract) -> DataFrame:
    """Compute SCD2 rows from all events of the affected keys."""
    keys = get_business_key(contract)
    values = [c for c in events.columns if c not in ("_op", EVENT_AT, *LINEAGE_COLUMNS)]
    tracked = list(contract.history.tracked_columns)
    tracked = [c for c in tracked if c in values]
    untracked = [c for c in values if c not in keys and c not in tracked]
    order = _order(keys)
    is_delete = F.col("_op") == OP_DELETE

    marked = (
        events.dropDuplicates([*keys, "_source_offset"])
        .withColumn(HASH_DIFF, F.when(~is_delete, compute_hash_diff(tracked)))
        .withColumn("_prev_delete", F.lag(is_delete).over(order))
        .withColumn("_prev_hash", F.lag(HASH_DIFF).over(order))
        .withColumn(
            "_starts",
            is_delete
            | F.col("_prev_delete").isNull()
            | F.col("_prev_delete")
            | (F.col("_prev_hash") != F.col(HASH_DIFF)),
        )
        .withColumn(
            "_group",
            F.sum(F.col("_starts").cast("int")).over(
                order.rowsBetween(Window.unboundedPreceding, Window.currentRow)
            ),
        )
    )
    # Real events win over rebuilt ones at the same instant, for lineage and values.
    ordering = F.struct(EVENT_AT, ~F.col("_source_offset").startswith(REBUILT_OFFSET), "_sequence")
    first_of = [c for c in [*keys, *tracked, HASH_DIFF] if c not in keys]
    grouped = marked.groupBy(*keys, "_group").agg(
        F.min(EVENT_AT).alias(VALID_FROM),
        F.max(is_delete.cast("int")).cast("boolean").alias("_is_delete"),
        *[F.min_by(c, ordering).alias(c) for c in first_of],
        *[F.max_by(c, ordering).alias(c) for c in [*untracked, *LINEAGE_COLUMNS]],
    )
    by_group = Window.partitionBy(*keys).orderBy("_group")
    timeline = grouped.withColumn(VALID_TO, F.lead(VALID_FROM).over(by_group)).withColumn(
        IS_CURRENT, F.col(VALID_TO).isNull()
    )
    return timeline.where(~F.col("_is_delete")).select(
        *values, VALID_FROM, VALID_TO, IS_CURRENT, HASH_DIFF, *LINEAGE_COLUMNS
    )


def compute_scd1(events: DataFrame, contract: Contract) -> DataFrame:
    """Latest event per key; keys whose latest event is a delete disappear."""
    keys = get_business_key(contract)
    values = [c for c in events.columns if c not in ("_op", EVENT_AT, *LINEAGE_COLUMNS)]
    latest = Window.partitionBy(*keys).orderBy(
        F.col(EVENT_AT).desc(), F.col("_sequence").desc(), F.col("_source_offset").desc()
    )
    return (
        events.withColumn("_rank", F.row_number().over(latest))
        .where((F.col("_rank") == 1) & (F.col("_op") != OP_DELETE))
        .select(*values, EVENT_AT, *LINEAGE_COLUMNS)
    )


def rebuild_scd1_events(existing: DataFrame, values: list[str]) -> DataFrame:
    return existing.select(*values, F.lit("update").alias("_op"), EVENT_AT, *LINEAGE_COLUMNS)


def merge_rebuilt_keys(
    target: DeltaTable,
    target_df: DataFrame,
    rebuilt: DataFrame,
    affected_keys: DataFrame,
    match_columns: list[str],
) -> None:
    """Replace the affected keys' rows with ``rebuilt`` in one atomic MERGE.

    Rows of affected keys that the rebuild no longer produces are deleted in the
    same transaction, so a crash can never leave a half-applied history.
    """
    keys = affected_keys.columns
    stale = (
        target_df.join(affected_keys, on=keys, how="left_semi")
        .join(rebuilt.select(*match_columns), on=match_columns, how="left_anti")
        .withColumn("_action", F.lit("delete"))
    )
    source = rebuilt.withColumn("_action", F.lit("upsert")).unionByName(stale)
    condition = " AND ".join(f"t.`{c}` <=> s.`{c}`" for c in match_columns)
    assignments: dict[str, str | Column] = {f"`{c}`": f"s.`{c}`" for c in rebuilt.columns}
    (
        target.alias("t")
        .merge(source.alias("s"), condition)
        .whenMatchedDelete(condition="s._action = 'delete'")
        .whenMatchedUpdate(condition="s._action = 'upsert'", set=assignments)
        .whenNotMatchedInsert(condition="s._action = 'upsert'", values=assignments)
        .execute()
    )


def is_keyed(contract: Contract) -> bool:
    return contract.history.type in (HistoryType.SCD1, HistoryType.SCD2)
