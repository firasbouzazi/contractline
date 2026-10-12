# 0003. Trunk-based development

- Status: Accepted
- Date: 2026-10-11

## Context

Most changes will come from coding agents working on many small, independent
dataset requests. We need a branching model where every change is reviewed,
tested in isolation and deployable, without long-lived branches drifting apart.

## Options

1. **`dev` → `main` (environment branches).** Work merges into `dev`, which is
   periodically merged into `main` for production.
   - Familiar to many teams.
   - Two places to review, merge conflicts between `dev` and `main`, and what
     runs in production is a merge result nobody tested as such.
   - Environments are coupled to branches, so promoting means merging, not
     redeploying a known artifact.
2. **Trunk-based.** `main` is the only long-lived branch and is always deployable.
   Short-lived branches merge into `main` through a PR. Continuous delivery
   promotes the *same built artifact* through `dev` → `staging` → `prod`.
   - Every PR is tested against what will actually ship.
   - Needs a strong PR gate (CI, ephemeral environment, review), because
     nothing sits between a merge and the first deployment.

## Decision

Trunk-based (option 2).

- `main` is the only long-lived branch.
- Each PR gets an ephemeral environment named `pr<number>_…` where CI deploys
  the change and runs the generated tests; it is destroyed when the PR closes.
- After merge, CD builds one artifact and promotes it `dev` → `staging`
  (smoke tests on realistic data) → `prod` (manual approval).
- Library releases are tagged with semver (`v1.4.0`); pipelines pin the
  version they use.

## Consequences

- The PR gate carries all the weight: required checks, code owner review and
  the ephemeral environment are not optional (see ADR-0004).
- Incomplete work must not reach `main` as half-wired behaviour; a contract that
  is not ready is simply not merged.
- Rollback is "redeploy the previous artifact", not "revert a branch merge".
