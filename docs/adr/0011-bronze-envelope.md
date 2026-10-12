# 0011. Bronze envelope and ingestion

- Status: Accepted
- Date: 2026-10-12

## Context

Principle 7: every source lands in bronze in one standard envelope, so silver
never knows where data came from. Ingestion must be idempotent, never lose a
record, and quarantine bad records instead of failing the whole run.

## Options

1. **Typed bronze**: parse into contract columns at landing. Bad records fail
   or vanish at the door, and schema drift breaks ingestion.
2. **Envelope bronze**: land the record's values as a JSON object plus
   standard metadata; parsing, typing and validation happen in silver.

## Decision

Option 2. Columns: `_key`, `_payload`, `_op`, `_source_system`,
`_source_location`, `_source_offset`, `_sequence`, `_source_at`,
`_ingested_at`, `_batch_id` (documented in `contractline.spark.bronze`).

- **Bronze never rejects.** An unreadable message lands with `_op = unknown`
  and its raw text as payload; silver quarantines it.
- **Ops** are normalised to `insert`, `update`, `delete`, `snapshot`.
  Deletes carry the before-image.
- **Source encodings stay in the mapper.** Debezium sends dates as days since
  epoch and some timestamps as microseconds; the Debezium mapper rewrites
  those as ISO text using the contract types. The connector must use
  `decimal.handling.mode=string` so decimals arrive exactly.
- **Payload values are not re-encoded** for plain JSON sources, because Spark's
  generic JSON parsing turns large decimals into doubles.
- **Exactly once.** Sources are read with Structured Streaming and an
  `availableNow` trigger; each micro-batch is appended with Delta's
  idempotent-write options (`txnAppId`, `txnVersion`), so a retried batch is a
  no-op. Silver also drops duplicate `_source_offset` values, which covers a
  replay from a lost checkpoint.
- Tombstones (null Kafka values after a delete) are dropped.

## Consequences

- Bronze holds raw personal data. It is the only layer allowed to, and the
  erasure and retention operations (milestone 3) must cover it.
- Silver needs a parsing step that detects values which do not fit the
  contract type (see ADR-0012).
- Avro on Kafka is allowed by the contract schema but not implemented yet;
  the Kafka adapter rejects it with a clear error.
