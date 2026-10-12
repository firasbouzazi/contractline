# 0013. Gold models: Spark SQL with output contracts and mandatory tests

- Status: Accepted
- Date: 2026-10-12

## Context

Gold is the only place for custom code (principle 3). Each model needs an
output contract and mandatory tests, may only read silver (or other gold of its
domain, so facts can feed aggregates), and must never expose raw PII.

## Options

1. **dbt** (dbt-spark locally, dbt-databricks in the cloud).
   - Familiar to analytics engineers; lineage graph and docs for free.
   - Locally needs a Spark Thrift server or a dbt-spark session setup; a
     second project format next to contracts; dbt tests can be weakened by the
     same person who writes the model, which is what principle 6 forbids.
2. **Plain Spark SQL files + a YAML output contract**, run by the harness.
   - Nothing new to install; same Spark session as silver, locally and on
     Databricks.
   - We build the small parts dbt would give us (ref resolution, tests).

## Decision

Option 2.

- A model is `pipelines/<domain>/gold/<kind>_<name>.sql` plus `.yaml`;
  `kind` is `fct`, `dim` or `agg`. Logical table: `<env>_gold.<domain>.<kind>_<name>`.
- The YAML (`api_version: contractline/gold/v0`) declares a business
  `description` (required), `inputs`, output `schema` (with PII class),
  `quality` rules and at least one test.
- The SQL reads inputs only through `{{ ref('<layer>.<domain>.<entity>') }}`.
  The validator rejects undeclared or unused inputs. At run time the harness
  also compares the files Spark actually read with the declared inputs' files,
  so a hard-coded table name (for example bronze) fails the run.
- Gold columns cannot be `direct` or `special` PII; gold reads masked silver.
- **Tests** are example-based: `given` rows per input, `expect` rows; the
  model must return exactly those rows (as a multiset, on the columns listed).
  Input rows are typed with the input's silver schema, so a test cannot feed
  data silver could never contain.
- Output columns and types must match the YAML exactly; quality rules run on
  every build; `block` stops the write, `warn` is reported. Gold has no
  quarantine.
- Each run fully rebuilds the model (`overwrite`). Incremental gold is out of
  scope for the MVP.

## Consequences

- Model tests live in the model's YAML and are reviewed with it; the harness
  decides what "passing" means.
- The isolation check relies on Spark reporting the files a query reads
  (`DataFrame.inputFiles`), which holds for Delta tables.
- Full rebuilds are simple and correct; large fact tables will need an
  incremental strategy later (an ADR when a scenario needs it).
