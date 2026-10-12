# 0010. Blocks target the Spark engine family

- Status: Accepted (refines ADR-0006)
- Date: 2026-10-12

## Context

ADR-0006 keeps the core free of tool names and says blocks are expressed
against interfaces. Writing every block (SCD2 merge, masking, quality) against
an engine-neutral abstraction means building a query layer before a second
engine exists, which the brief tells us not to do. The MVP has exactly one
engine family: Apache Spark with Delta Lake, locally and on Databricks.

## Options

1. **Engine-neutral blocks** through a dataframe abstraction (Ibis, or our own
   SQL generator). Portable in theory; a large abstraction with one user, and
   Delta-specific features (MERGE, idempotent writes, time travel) leak anyway.
2. **Blocks written for Spark**, shared by every Spark-based platform adapter,
   with the engine-neutral part (contract, plan, naming, validation, secrets)
   kept in `contractline.core`.

## Decision

Option 2.

- `contractline.core`: contract model, validation, diff, naming, lint, the
  pipeline plan and the secret provider interface. No Spark.
- `contractline.spark`: blocks as plain functions over Spark DataFrames, plus
  the `SparkCompute` interface that platform adapters implement.
- `contractline.adapters.compute.spark_delta_local` (and later `databricks`):
  sessions, table naming, storage locations. Blocks never import adapters.
- Versions: PySpark 4.0 with delta-spark 4.0 (Scala 2.13, Java 17+); the Kafka
  source is `spark-sql-kafka-0-10` 4.0.1. Spark is an optional extra
  (`contractline[spark]`) and part of the dev environment.
- `import-linter` enforces both boundaries in CI and pre-commit.

## Consequences

- A second engine family (for example a warehouse running SQL) would get its
  own block package behind the same plan; nothing in `core` changes.
- Databricks reuses all blocks; its adapter only differs in sessions, names
  (Unity Catalog three-level names) and storage.
- Tests that touch blocks start a local Spark session once per test run
  (about 15 seconds, plus a one-time Maven download of the Delta and Kafka jars).
