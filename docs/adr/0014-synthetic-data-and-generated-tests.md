# 0014. Synthetic data and generated tests

- Status: Accepted
- Date: 2026-10-12

## Context

Principle 6: tests are owned by the harness, so whoever writes a pipeline
cannot weaken them. Tests must run on a laptop and in CI without production
data, and must exercise the cases pipelines usually get wrong.

## Decision

**Synthetic data** (`contractline.testing.synthetic`) is generated from the
contract alone, deterministically from a seed:

- values follow the column types and the `accepted_values`, `range` and
  `referential_integrity` rules (references use keys generated for the
  referenced dataset);
- PII-safe: invented values only (`@example.invalid` emails, numbered names);
  every raw direct or special PII value is recorded;
- edge cases: nulls in nullable columns, snapshots, updates, deletes,
  re-inserts after delete, duplicate deliveries, changes arriving late (out of
  order, in a later batch), a value of the wrong type, a value breaking a
  quarantine rule, and an undeclared column when the drift policy tolerates it;
- encoded the way the source sends it (Debezium envelopes with day-number
  dates, plain JSON events) and pushed through the real source mapper.

**Generated tests** (`contractline.testing.generated`, `contractline test`)
run the whole project in an ephemeral environment (`pr<number>` in CI), with
random throw-away masking keys, and check for every dataset:

| Check | Meaning |
| --- | --- |
| pipeline runs | ingest and silver complete without error |
| contract conformance | silver columns and types are exactly what the contract implies |
| bad records quarantined | every deliberately bad record ended up in quarantine |
| quality rules hold | no current silver row breaks a quarantine or block rule (unmasked columns) |
| no raw PII after bronze | no raw direct/special PII value appears in any silver string column |
| history invariants | SCD2: one current row per key, no overlapping or inverted windows; SCD1/snapshot: unique keys; deleted keys have no current row |
| dropped columns absent | columns masked with `drop` are not in silver |

and for every gold model: its own example tests, a build on the synthetic
silver data (output schema, quality rules, input isolation), and no raw PII.

The quarantine table holds raw payloads so records can be repaired; it is
treated as part of the bronze zone (restricted, covered by erasure and retention).

## Consequences

- A pipeline author can change a contract (and the diff tool flags relaxing
  changes, ADR-0008) but cannot change what "passing" means.
- Synthetic data catches mechanical errors, not business ones: a wrong gold
  definition passes if its own example tests are wrong. That is what human
  review of the model's YAML is for.
- The suite runs the real Spark pipeline: about three minutes for the examples.
