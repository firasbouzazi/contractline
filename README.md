# contractline

An open-source **data pipeline harness**: a library of pre-tested, versioned
building blocks that turns a declarative **data contract** into a governed
pipeline (ingest → bronze → silver → gold). Privacy controls, SCD history and
data quality are enforced by the harness, not re-implemented in each pipeline.

The project also tests a thesis: with a well-designed harness, one senior data
engineer plus coding agents can deliver and operate the pipeline volume that
normally needs a team. The repository will contain an `evaluation/` area that
measures whether this holds, and reports where it does not.

## Status: pre-alpha

| Area | State |
| --- | --- |
| Repository scaffold, tooling, CI | ✅ works |
| Contract JSON Schema + validator (`contractline validate`) | ✅ works |
| Contract diff: version bumps and relaxed guarantees (`contractline diff`) | ✅ works |
| Naming conventions (`contractline lint`, pre-commit, CI) | ✅ works |
| Local Docker stack: Postgres → Debezium → Kafka, Airflow (`make up`, `make smoke`) | ✅ works |
| Bronze envelope, Kafka and Debezium CDC source | ✅ works |
| Silver: standardize, quality with quarantine, masking, SCD1/SCD2/append/snapshot | ✅ works |
| Gold models: Spark SQL + output contract + mandatory tests, input isolation | ✅ works |
| Synthetic data + generated tests (`contractline test`) | ✅ works |
| `contractline plan`, `run` (local), `new-dataset` | ✅ works |
| End to end on the Docker stack (`make e2e`) | ✅ works: Postgres → Debezium → Kafka → bronze → silver → gold |
| Files source (CSV, JSON lines, Parquet); `full`, `incremental`, `append_stream` modes | ✅ works |
| GDPR operations, Airflow Dags | ❌ not started |
| Evaluation | ❌ not started |

Today a contract and a gold model are enough to run a governed pipeline locally, from Kafka or Postgres CDC to gold, and to test it on synthetic data.

## Run it locally

Requirements: Python 3.11+, [uv](https://docs.astral.sh/uv/), Java 17+ (for Spark).

```bash
git clone https://github.com/firasbouzazi/contractline.git
cd contractline
uv sync
uv run contractline validate examples   # contracts and gold models
uv run contractline lint examples       # naming conventions
uv run contractline plan examples       # what a run would do, in order
uv run contractline test examples       # synthetic data through the real pipeline (~3 min)
uv run pytest
```

To run the example pipelines against real sources (needs Docker):

```bash
make e2e   # starts the stack, changes Postgres rows, sends Kafka events,
           # runs contractline end to end and checks silver and gold
```

Or by hand: `make up`, then

```bash
export CONTRACTLINE_SECRET_PII_HASH=$(openssl rand -hex 32)
export CONTRACTLINE_SECRET_PII_TOKEN=$(openssl rand -hex 32)
uv run contractline run examples/contracts/sales examples/pipelines \
  --env local --kafka-bootstrap-servers localhost:29092
```

### Local stack (optional)

Needs Docker with Compose. About 1 GB of RAM for the CDC services, plus about
1 GB for Airflow.

```bash
make up        # Postgres (demo CRM) -> Debezium -> Kafka, connector registered
make smoke     # inserts a row in Postgres, checks it arrives on cdc.crm.public.customers
make up-all    # also starts Airflow on http://localhost:8080
make clean     # stops everything and deletes the data
```

The test suite does not need Docker. See [ADR-0009](docs/adr/0009-local-stack.md).

## A contract in brief

See [`examples/contracts/sales/customers.yaml`](examples/contracts/sales/customers.yaml).
A contract has these sections:

| Section | Holds |
| --- | --- |
| `identity` | name, domain, owner, semver version |
| `source` | adapter (`kafka`, `files`), ingest mode, location, format |
| `schema` | columns (logical type, nullability, **PII class**), primary key, drift policy |
| `privacy` | legal basis, retention, erasure key, masking per PII column |
| `history` | `append`, `snapshot`, `scd1` or `scd2` with business key and tracked columns |
| `quality` | rules with severity `warn`, `quarantine` or `block` |
| `sla` | freshness |

The JSON Schema is in
[`src/contractline/core/schemas/contract-v0.json`](src/contractline/core/schemas/contract-v0.json);
the reasoning is in [ADR-0005](docs/adr/0005-contract-format.md).

## Principles

1. **Declare, don't code.** Bronze and silver pipelines are pure configuration.
2. **Same code everywhere.** One versioned block library, pinned per pipeline.
3. **Custom code only in gold**, under an output contract with mandatory tests.
4. **Agnostic core.** Engines, orchestrators and ingestion tools live behind adapters.
5. **Compliance by construction.** No path to gold skips validation, PII masking or quality gates.
6. **Tests are owned by the harness**, generated from the contract.
7. **Uniform landing.** Every source lands in bronze in one standard envelope.
8. **Runs locally**, no cloud account required.

Design decisions are recorded in [`docs/adr/`](docs/adr/).

## License

Apache License 2.0, see [LICENSE](LICENSE).
