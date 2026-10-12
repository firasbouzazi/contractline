# 0012. Silver blocks: standardize, quality, privacy, history

- Status: Accepted
- Date: 2026-10-12

## Context

Silver is where the harness's guarantees are made: typed data, quality rules
enforced, no raw direct PII, correct history. Every contract gets exactly the
same code path (principle 2), so the semantics must be precise.

## Decision

Each bronze micro-batch goes through, in order (`contractline.spark.silver`):

1. **Drift check.** Undeclared payload columns: `fail` stops the run,
   `quarantine` quarantines the records, `add_columns` ignores the extra
   columns (they appear in silver only when the contract adds them) and
   reports them.
2. **Standardize.** Payload parsed to contract types. A value present but not
   fitting its type is an error, never a silent null. Decimals are parsed from
   the JSON text, so precision is kept; a value with more digits than the
   declared precision is an error. Duplicate `_source_offset`s are dropped.
3. **Row quality rules.** `not_null`, `accepted_values`, `range`,
   `referential_integrity` (against current rows of the referenced silver
   dataset in the same environment; a missing reference fails the rule) and,
   for append-only data, `unique`. Schema errors always quarantine. Deletes are
   exempt from row rules so a delete always goes through.
4. **Block check.** Any failing `block` rule stops the batch before anything
   is written; the next run retries it.
5. **Privacy.** HMAC-SHA256 for `hash`; deterministic `tok_` pseudonyms with a
   separate key for `tokenize` (no reversible vault in the MVP); truncation or
   bucketing for `generalize`; `redact`; `drop`. Keys come from the
   `SecretProvider` (`pii_hash`, `pii_token`, or `params.key`). Parameters are
   validated with the contract, before any data moves.
6. **History.**
   - `append`: insert new events, keyed on `_source_offset` (replays are no-ops).
   - `snapshot`: the latest batch replaces the table.
   - `scd1` / `scd2`: for every key touched by the batch, stored rows are
     turned back into events, merged with the new ones, and the key's history
     is rebuilt; the result replaces the key's rows in one Delta `MERGE`
     (rows that disappear are deleted in the same transaction).
     Order is `(_event_at, _sequence, _source_offset)`: the contract's
     `sequence_column` for upserts, the source timestamp for deletes. SCD2
     opens a version when tracked columns change (`_hash_diff`); untracked
     columns take their latest value within a version. Deletes close the
     current version at the delete time.
7. **Dataset rules** on the candidate current rows, before the write:
   `unique` for keyed history, `row_count_anomaly` (against the previous
   run's input count), `freshness`.
8. **Write** quarantine (keyed on `_source_offset`), then silver; append a run
   record to `<env>_harness.contractline.runs`.

Silver reads bronze with Structured Streaming (`availableNow`,
`skipChangeCommits` so erasure deletes in bronze are not treated as new data).

## Options considered for SCD2

1. Patch-style `MERGE` (close current row, insert new) — simple for in-order
   data, wrong for late records and fragile on re-runs.
2. Recompute the whole table each run — always correct, cost grows with history.
3. **Rebuild only the affected keys** (chosen) — correct for late,
   out-of-order and replayed data; cost grows with the number of keys changed.

## Consequences

- Re-running a batch, or the same events split differently into batches,
  produces the same silver table; a property-based test checks this.
- A key with very many versions is rebuilt entirely when it changes;
  acceptable for dimension-like data, revisit for high-churn keys.
- Hashed values are pseudonymous, still personal data under GDPR: erasure must
  cover silver too (milestone 3).
- `unique` on keyed history cannot quarantine (it judges the whole current
  state); the validator rejects that combination.
