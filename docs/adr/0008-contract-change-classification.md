# 0008. Contract change classification

- Status: Accepted
- Date: 2026-10-11

## Context

Tests are generated from the contract, so the harness owns them (principle 6).
But pipeline authors, including agents, are allowed to edit contracts. An
agent whose pipeline fails a quality rule could "fix" it by removing the rule,
lowering its severity, widening accepted values or weakening masking. The
generated tests would then pass while the guarantee is gone.

Separately, consumers of a dataset need to know when a change breaks them,
and contract versions are semver.

## Options

1. **Detect only breaking schema changes** (removed column, narrowed type).
   Standard, but misses every weakening that is not a schema change.
2. **Forbid relaxing changes outright.** Safe, but some relaxations are
   legitimate (a new accepted status value, a longer retention required by law).
3. **Classify every change** by the version bump it needs *and* whether it
   relaxes a guarantee, and route relaxing changes to the data owner.

## Decision

Option 3, implemented in `contractline.core.diff` and `contractline diff`.

- Each change needs a **major**, **minor** or **patch** bump:
  - Major: removed or renamed things, narrowed types, changed keys, history type
    or masking strategy, a source move, a column becoming nullable.
  - Minor: added nullable columns, widened types, quality and SLA changes.
  - Patch: descriptions, owner, tags, adapter options.
- `contractline diff` fails when the new version number is lower than the
  changes need, or when the version changes without any change.
- A change **relaxes a guarantee** when it lowers a PII class, weakens or removes
  masking, removes the privacy section, changes the legal basis or erasure key,
  lengthens retention, stops tracking a column's history, loosens the drift
  policy, makes a column nullable, removes a quality rule, lowers its severity
  or loosens its parameters, or loosens the freshness SLA.
- Masking *parameter* changes are always treated as relaxing, because their
  strength depends on the strategy; the data owner judges them.
- Relaxing changes do not fail on their own; `--fail-on-relax` and the JSON
  output let CI require the data owner's approval for them.

## Consequences

- CI (milestone 4) runs `contractline diff --format json` against the base
  branch for every changed contract and requests the owner's review when
  `relaxes_guarantees` is true.
- The classification is conservative: when unsure, a change is called relaxing.
  False alarms cost a review; misses cost a silent loss of a guarantee.
- New contract fields must be added to the diff together with the schema;
  a test asserting every field is covered should be added once the format
  stabilises.
