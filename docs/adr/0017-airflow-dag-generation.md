# 0017. Airflow Dags generated from the plan

- Status: Accepted
- Date: 2026-10-12

## Context

Airflow orchestrates across systems; pipeline logic runs in the compute layer.
Dags must be generated from contracts, never hand-written. The brief proposes
Dag ids `<domain>__<entity>__<layer>`, which means several Dags per dataset.

## Options

1. **One Dag per dataset** with a task group per layer. Fewer Dags; but a gold
   model that reads several datasets has no natural home, and cross-dataset
   dependencies become cross-Dag sensors.
2. **One Dag per plan step**, linked by Airflow 3 assets. Matches the naming
   convention; each step can be scheduled, retried and owned on its own; gold
   models depend on their inputs through assets, across domains.

## Decision

Option 2. `contractline dags` turns the plan into one file per step
(`infra/airflow/dags/<dag_id>.py` for the project, `examples/airflow/dags/` for the examples; header "Generated ... do not edit"):

- **Ingest** Dags run on a cron derived from the freshness SLA: the slowest
  preset interval that is at most half the SLA (15 min floor), so one failed
  run can be caught up within the SLA.
- **Silver** Dags are triggered by their own bronze asset. Referential-integrity
  checks read whatever the referenced dataset currently holds; waiting for it
  (asset AND) would couple unrelated schedules.
- **Gold** Dags are triggered when **any** input asset changes (asset OR).
- Assets are `contractline://<logical table>`. Quarantine tables are not assets.
- Each Dag has one task: `contractline run-step <dag_id> --env <env>`. Project
  location, Kafka brokers and the warehouse come from `CONTRACTLINE_*`
  environment variables of the worker image. The Databricks adapter will swap
  the task for a Databricks job run (milestone 5).
- Tags carry `domain`, `owner`, `contract_version`, `pii`, `layer`, `env`.
- CI fails when the committed Dags differ from what the contracts generate
  (`contractline dags --check`), and the stack workflow loads them in Airflow
  and requires zero import errors.

## Consequences

- Hand edits to generated Dags are overwritten; files without the header are
  never touched or removed.
- The local Airflow container can load the Dags but cannot run them unless the
  worker image has contractline installed; that image is a deployment concern.
