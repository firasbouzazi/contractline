# 0018. Agent workflow, pipeline PR checks and thesis evaluation

- Status: Accepted
- Date: 2026-10-12

## Context

The thesis is that agents can deliver pipelines while a human designs the
system, reviews intent and owns data meaning. That only holds if agents cannot
weaken the harness, if review focuses on meaning rather than mechanics, and if
the claim is measured rather than asserted.

## Decision

**Scope.** On `agent/` branches, `contractline ci` fails when the PR changes
anything outside contracts, gold models, generated Dags and the agent's own
evaluation log (`contractline.core.scope`). Blocks, adapters, tests, CI,
conventions and ADRs are harness changes, reviewed by the maintainer through
CODEOWNERS. Human branches are not scope-restricted; CODEOWNERS applies.

**One command for "would this PR pass".** `contractline ci` runs scope, naming
(branch, title, files), contract diffs against the base branch (version bumps,
relaxing changes) and the generated tests in an ephemeral environment named
after the PR (`pr<number>`). Agents run it locally; the *Pipeline PR* workflow
runs it on every dataset PR and posts the summary as a PR comment.

**Relaxing changes** are not failures but are flagged at the top of the
comment; CODEOWNERS routes each domain's contracts to its data owner.

**Deployment** happens only in the *Deploy* workflow after merge: one built
artifact promoted dev → staging (smoke test on synthetic data) → prod, gated by
the `prod` environment's required reviewers. Agents hold no credentials.

**Evaluation** (`evaluation/`): 14 scenario tickets, a written protocol, a
baseline of hand-built estimates written before any run, JSON-lines event logs
(`contractline eval-log`) and a report (`contractline eval-report`) that states
how much data it rests on, lists where the thesis fails, and names its threats
to validity.

## Consequences

- An agent that needs a harness change must stop and say so; that is a
  measured outcome (`abandoned` or a human edit), not a workaround.
- The baseline is an estimate until control runs are logged; the report says so.
- Running the generated tests on every dataset PR takes minutes; acceptable for
  the safety it buys, and the job only triggers on dataset paths.
