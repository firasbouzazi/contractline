# 0016. GDPR operations: inventory, retention, erasure

- Status: Accepted
- Date: 2026-10-12

## Context

Principle 5 (compliance by construction) covers data in motion. Personal data
at rest also has obligations: know what is held (record of processing), keep it
no longer than needed (storage limitation), and erase a person's data on
request (Article 17), with evidence that it was done.

## Decision

**PII inventory** (`contractline pii-inventory`, JSON or table): generated from
the contracts and gold models: per dataset, each PII column with its class,
masking, whether it reaches silver, the legal basis, retention, erasure key,
the raw layers (bronze, quarantine) and the gold models that read it.

**Retention** (`contractline purge`): for each contract, delete records whose
`retention.column` is older than `retention.period` from bronze (read from the
payload), quarantine and silver (every SCD2 version), then rebuild the gold
models that read them. One audit row per dataset in
`<env>_harness.contractline.purges`, with the cutoff used.

**Erasure** (`contractline erase --dataset --key --request-id`): delete every
record whose erasure key matches, from bronze, quarantine and silver (all
history; the key is matched after its masking, so a hashed key is found by its
HMAC), then rebuild dependent gold models. The audit row in
`<env>_harness.contractline.erasures` stores the request id, rows deleted per
layer, models rebuilt and the subject's **keyed hash, never the key itself**.

**No resurrection**: silver drops incoming records whose erasure key hashes to
an erased subject, so replaying a topic (a reset checkpoint, a re-ingestion)
cannot bring them back.

**Time travel**: Delta keeps deleted rows in old files until `VACUUM`.
`--vacuum-hours` runs VACUUM on the touched tables as part of the operation and
the audit row says whether it ran. Deployments must run VACUUM within the
erasure deadline (one month under GDPR) and set
`delta.deletedFileRetentionDuration` accordingly.

## What this does not cover (by design, documented)

- **Kafka topics** keep messages until topic retention expires; a harness
  cannot delete single messages. Topic retention must be shorter than the
  erasure deadline; compacted CDC topics get a tombstone per deleted key from
  Debezium.
- **One subject across datasets**: erasure runs per dataset. A request for a
  person who appears in several datasets lists each of them (the PII inventory
  shows where to look). Linking subjects across datasets automatically needs a
  subject registry: future work.
- **Gold derived from pseudonymous identifiers** (for example a `customer_id`
  kept in facts) is rebuilt from silver; facts that keep a subject's id after
  their customer row is erased still hold that id. Erase those datasets too, or
  model the id as indirect PII with masking.

## Consequences

- Erasure and retention are tested end to end, including gold rebuild, the
  replay guard and VACUUM removing time-travel access.
- Approximate calendar arithmetic (365-day years) can keep data up to a few
  days longer than the period; the cutoff is recorded.
