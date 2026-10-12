# 0015. Files source and ingest modes

- Status: Accepted
- Date: 2026-10-12

## Context

The brief asks for four ingest modes (`full`, `incremental`, `change_stream`,
`append_stream`) on two source adapters (Kafka, files). Not every combination
means something, and each mode must be idempotent and quarantine bad records.

## Decision

**Allowed combinations** (enforced by the contract schema and validator):

| Mode | Kafka | Files | Histories |
| --- | --- | --- | --- |
| `change_stream` | Debezium CDC | — (files carry no ops) | scd1, scd2, append |
| `append_stream` | JSON events | rows appended | append, scd1, scd2 |
| `incremental` | — | files of changed rows, ordered by `watermark_column` | scd1, scd2, append |
| `full` | — | each file is the complete dataset | snapshot, scd1, scd2 |

**Files adapter** (`contractline.adapters.sources.files`): Spark's file source
with an `availableNow` trigger and a checkpoint, so each run reads only new
files. CSV (header row, read as text and typed in silver), JSON lines, Parquet.
Rows are identified by file and content hash: identical rows in one file land once.

**Full snapshots**: each file is read as its own batch (`maxFilesPerTrigger=1`).
For keyed histories, a key that was current but is missing from the newest
snapshot gets a delete event at the file's time: SCD2 closes its version,
SCD1 removes it. `snapshot` history replaces the table.

**Incremental**: `_sequence` is the watermark column, so a late file with an
older change cannot overwrite a newer state.

On Databricks the same mapper is fed by Auto Loader (milestone 5).

## Consequences

- A full-snapshot file that is truncated by mistake looks like mass deletion.
  The `row_count_anomaly` rule (with `block`) is the guard; contracts for full
  sources should declare it.
- Two identical rows in one append file count once; sources that send
  legitimate exact duplicates need a distinguishing column.
