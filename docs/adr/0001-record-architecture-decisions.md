# 0001. Record architecture decisions

- Status: Accepted
- Date: 2026-10-11

## Context

The project tests whether one engineer plus coding agents can run a harness
at team scale. Agents and future contributors need the *why* behind the design,
not only the code, or they will "fix" deliberate choices.

## Options

1. No formal records; rely on PR descriptions. Cheap, but scattered and hard to find.
2. A wiki. Lives outside the repository, drifts from the code, invisible to agents.
3. Short ADRs in `docs/adr/`, reviewed in PRs like code.

## Decision

Option 3, using the template in `0000-template.md`
(context, options, decision, consequences).

## Consequences

- Every significant choice gets an ADR in the same PR that implements it.
- Accepted ADRs are immutable; a new ADR supersedes an old one.
- Agents are pointed at `docs/adr/` from `AGENTS.md`.
