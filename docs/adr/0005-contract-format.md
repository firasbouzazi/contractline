# 0005. Contract format

- Status: Accepted
- Date: 2026-10-11

## Context

The contract is the only thing a pipeline author writes below gold. It drives
the compiler, the generated tests and the PII inventory, so it must be strict
enough that a machine can check it and generate everything from it.

The brief asks us to evaluate the
[Open Data Contract Standard](https://bitol-io.github.io/open-data-contract-standard/) (ODCS v3).

## Options

1. **Adopt ODCS v3 as the authoring format.**
   - A recognised standard; tooling and catalogs may read it directly.
   - It describes *what a dataset is*, not *how to build it*: no ingest mode,
     SCD type, sequence column, drift policy or masking strategy. All of these
     would live in free-form `customProperties`, which ODCS does not validate.
   - Its quality section is permissive (free text, SQL or vendor rules), so
     "tests generated from the contract" would depend on unchecked fields.
   - Verbose: authors (human or agent) would edit far more YAML per dataset.
2. **Extend ODCS** with our own required sections. Keeps the ODCS envelope,
   but the strictness comes from our extension anyway, and we inherit every
   ODCS change.
3. **Own small, strict format, mapped to ODCS.** Our schema is the source of
   truth; an export command produces ODCS for catalogs and other tools.

## Decision

Option 3.

- Contracts are YAML, `api_version: contractline/v0`, validated in two passes:
  1. **JSON Schema** (`src/contractline/core/schemas/contract-v0.json`, draft
     2020-12): shape, types and allowed values. Usable by editors for
     autocompletion.
  2. **Semantic checks** (`contractline.core.validation`): rules across
     sections, for example that keys name declared columns, that every PII
     column has a masking strategy, and that the erasure key stays findable.
- Every column must declare `pii: none | indirect | direct | special`. There is
  no default, so nobody forgets to classify a column.
- Direct and special-category PII cannot use `keep` (principle 5). `keep`
  on indirect PII needs a written `justification`.
- Types are engine-agnostic logical types (`string`, `long`, `decimal(p,s)`,
  `timestamp`, ...); adapters map them to engine types.
- YAML is parsed strictly: duplicate keys are errors, and only `true`/`false`
  are booleans (so `accepted_values: [yes, no]` stays two strings).
- All errors are reported at once, each with a dotted location.
- `contractline export --odcs` will be added when a consumer needs it; the
  field names (`domain`, `owner`, `version`, `schema`, `quality`, `sla`) are
  chosen so the mapping is direct.

## Consequences

- We maintain the ODCS mapping ourselves and must follow ODCS releases.
- The format is versioned (`contractline/v0`); a breaking change to the format
  means a new `api_version` and a migration command, never a silent change.
- v0 deliberately leaves out things not yet needed (multiple sources per
  contract, column-level lineage, partitioning). They are added when a
  scenario needs them.
