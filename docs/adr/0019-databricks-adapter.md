# 0019. Databricks adapter and Unity Catalog governance

- Status: Accepted
- Date: 2026-10-12

## Context

Milestone 5: the same contracts run on Databricks with Unity Catalog, deployed
with Terraform and Asset Bundles, with a test proving identical outputs
locally and on Databricks.

## Decision

**Compute** (`adapters.compute.databricks.DatabricksCompute`): the logical name
*is* the Unity Catalog name (`prod_silver.sales.customers`); schemas are
created on first use; checkpoints live in the `<env>_harness.contractline.checkpoints`
volume. The session is the job's, or Databricks Connect from outside.
**All blocks are unchanged** (ADR-0010).

**Sources**: Kafka uses the same adapter (the connector ships with Databricks
Runtime). Files use Auto Loader (`adapters.sources.autoloader`) feeding the
same `map_files`, so bronze rows are identical.

**Secrets**: `DatabricksSecretProvider` reads the `contractline` secret scope.

**Governance** (`core.governance` → adapters): a governance plan per table
(tags: domain, owner, contract version, PII, layer, raw PII; column tags: PII
class and masking; readers). Unity Catalog gets `SET TAGS` and `GRANT SELECT`
statements (`contractline governance`): bronze and quarantine of PII datasets
are readable only by `<domain>_pii_readers`. Locally the same tags become Delta
table properties (`--apply-local`).

**Deployment**:
- `infra/terraform/databricks`: catalogs `<env>_<layer>`, harness schema and
  volumes, domain groups, catalog grants, secret scope. Validated with
  `terraform validate`.
- `infra/databricks/databricks.yml`: the bundle; `contractline bundle`
  generates one serverless job per plan step. Ingest jobs use the SLA schedule
  (as Quartz cron); silver and gold jobs use table-update triggers on their
  inputs, mirroring the Airflow assets. Schedules are paused outside prod.
- `.github/workflows/databricks.yml`: deploys the bundle and runs the parity test.

**Parity test** (`tests_databricks/`): the same synthetic data, with the same
masking keys, through bronze, silver and gold on both platforms; every table
must hold the same rows (ignoring ingestion lineage).

## Consequences

- Airflow remains the cross-system orchestrator; on Databricks the jobs can
  also run on their own triggers. Using both for the same steps would double-run;
  a deployment picks one.
- **Not yet run against a real workspace**: no workspace is connected to this
  repository. The adapter, bundle, Terraform and parity test are written and
  checked offline (unit tests, YAML parsing, `terraform validate`, the local
  half of the parity test); the Databricks half runs when the workflow gets
  credentials. Until then, "runs on Databricks" is unproven.
- `databricks-connect` replaces pyspark, so it lives in its own environment.
