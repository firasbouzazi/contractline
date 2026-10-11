# 0006. Agnostic core and adapter boundary

- Status: Accepted
- Date: 2026-10-11

## Context

Principle 4: contracts and core logic must never reference a specific engine,
orchestrator or ingestion tool. The MVP uses Kafka, Debezium, Spark with
Delta Lake, Airflow and later Databricks with Unity Catalog. If those names
leak into contracts or core logic, adding a second engine means rewriting
the core, and the "same contract, same results on any platform" claim fails.

## Options

1. **Convention only**: document the rule and rely on review.
2. **Package layout plus an enforced import rule**: tool-specific code lives
   under `contractline.adapters`, and a static check fails CI if
   `contractline.core` or `contractline.blocks` imports an adapter or a
   tool library (`pyspark`, `delta`, `confluent_kafka`, `airflow`, `databricks`).

## Decision

Option 2.

- `contractline.core`: contract model, validator, compiler (contract to
  pipeline graph), and the adapter **interfaces** (Python `Protocol`s).
- `contractline.blocks`: block logic expressed against those interfaces.
- `contractline.adapters.<kind>.<tool>`: implementations
  (`sources/kafka`, `sources/files`, `compute/spark_delta_local`,
  `compute/databricks`, `orchestration/airflow`, `governance/local`,
  `governance/unity_catalog`).
- Contracts name adapters only by an abstract kind (`source.adapter: kafka`
  selects an implementation; it does not configure Kafka internals beyond an
  opaque `options` map validated by the adapter).
- Tool libraries are optional dependencies (extras such as
  `contractline[spark]`), so installing the core never pulls in Spark.
- The import rule is enforced with `import-linter` from the first change
  that adds an adapter package.

## Consequences

- Interfaces are designed before the second implementation exists. This is
  the one place we accept a speculative abstraction, because the principle
  requires it; we keep the interfaces as small as milestone 2 needs.
- Engine-specific behaviour (for example Delta `MERGE`) is reached through
  an interface method, so the core describes *what* (an SCD2 upsert) and the
  adapter decides *how*.
