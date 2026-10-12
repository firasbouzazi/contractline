# 0009. Local stack

- Status: Accepted
- Date: 2026-10-11

## Context

Principle 8: any contributor runs the full stack and the test suite on a
laptop, with Docker and no cloud account. The README promises setup in under
ten minutes. Kafka, Debezium, Postgres, Spark and Airflow together are heavy,
and most work (contracts, blocks, generated tests) needs none of them.

## Options

1. **One Compose file that starts everything**, including a Spark cluster.
   Simple to explain; 8 to 10 GB of RAM and slow to start, so contributors
   skip it.
2. **Compose profiles, Spark in-process.** The test suite needs no Docker:
   PySpark with Delta Lake runs in local mode inside pytest (Java required).
   Services are grouped in profiles and started only when needed.

## Decision

Option 2. `infra/docker-compose.yml` with profiles:

| Profile | Services | Measured memory |
| --- | --- | --- |
| `cdc` | Postgres 16 (demo CRM, `wal_level=logical`), Kafka 3.9 (KRaft, single node), Debezium Connect 3.0 | about 1 GB |
| `orchestration` | Airflow 3.1 `standalone` (SQLite, no login locally) | about 1 GB |

- `make up` starts `cdc` and registers the Debezium connectors (idempotent
  `PUT`), `make up-all` adds Airflow, `make smoke` / `make smoke-all` check
  them end to end, `make clean` removes everything including data.
- The CDC connector publishes to `cdc.crm.public.customers` (ADR-0007) with
  plain JSON envelopes (`schemas.enable=false`): the contract is the schema.
- Image versions are pinned. Credentials in the Compose file are local-only.
- A separate CI workflow runs the stack and the smoke test whenever `infra/`
  changes.

## Consequences

- Spark behaviour locally is Spark local mode, not a cluster. Cluster-only
  behaviour is covered by the Databricks adapter tests (milestone 5).
- Debezium encodes `DATE` as days since epoch and timestamps as ISO strings
  (`adaptive_time_microseconds`); the Kafka CDC mapper in milestone 2 must
  convert them using the contract types.
- Airflow `standalone` is for local use only and not a deployment model.
