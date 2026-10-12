# 0004. Branch naming and the agent pull request flow

- Status: Accepted
- Date: 2026-10-11

## Context

Agents and humans both open pull requests to `main` (ADR-0003). We need to tell
their work apart, keep agents from approving or merging their own work, and
make sure harness internals are only changed with the maintainer's review.

GitHub does not let a PR author approve their own PR. If agents push and open
PRs with the maintainer's account, the maintainer can never approve them and
the "one human approval" rule is unreachable.

## Options

1. **Agents use the maintainer's account.** No setup, but approvals become
   impossible (or must be disabled), and the history cannot tell agent work
   from human work, which the thesis evaluation needs.
2. **Agents use a separate identity** (a GitHub App or a machine user) with
   write access to branches but no admin rights and no production credentials.

## Decision

Option 2.

**Branch names**

| Who | Pattern | Example |
| --- | --- | --- |
| Agent | `agent/<ticket>-<domain>-<entity>` | `agent/DATA-231-sales-customers` |
| Human | `feat/<topic>`, `fix/<topic>`, `chore/<topic>` | `feat/contract-diff` |

**Agent rules**

- Starts every task from a fresh branch off `main`.
- Pushes only to its own branch; opens a PR to `main`.
- Never pushes to `main`, never merges its own PR, never force-pushes over
  someone else's work, never holds production credentials.

**Commits and PR titles**

- Commits follow Conventional Commits: `feat(sales): add customers contract`.
- Pipeline PR titles: `[<domain>/<entity>] <summary>`. Harness PRs use the
  Conventional Commit form.

**Protection on `main`** (applied by `scripts/configure-branch-protection.sh`)

- Pull request required; at least one approval; stale approvals dismissed on push.
- Code owner review required (`.github/CODEOWNERS`).
- Required status checks: the CI jobs, branch up to date before merge.
- No force pushes, no deletion, linear history (squash merge).
- Administrators are **not** subject to the rules, so that a single maintainer
  can merge their own harness changes. This is a deliberate trade-off while the
  project has one maintainer; revisit when a second maintainer joins.

## Consequences

- The maintainer must create the agent identity and give it write access.
- Every agent PR needs the maintainer's approval, which is exactly the review
  time the evaluation measures.
- Branch naming and PR titles are checked by `contractline lint` in CI
  (ADR-0007), not left to documentation.
- Admin bypass means the maintainer's own changes are only as safe as their
  discipline: the bypass must not be used for pipeline (contract) changes.
