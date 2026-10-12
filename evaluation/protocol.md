# Evaluation protocol

The thesis: *with a well-designed harness, one senior data engineer plus coding
agents can deliver and operate the pipeline volume that normally needs a team.*
This protocol says how a scenario is run and what is measured, so results can
be compared and repeated.

## Roles

- **Agent**: a coding agent in a fresh session, given the repository and the
  ticket text, nothing else. It follows `AGENTS.md`.
- **Reviewer**: the senior data engineer. Reviews the PR, approves or requests
  changes, and owns data meaning. Does not write the agent's code unless the
  scenario is recorded as needing a human edit.

## Running a scenario

1. Pick a scenario from `scenarios/`. Choose a run id: `<date>-<agent>`.
2. Log `ticket_received` with the agent and model:
   `contractline eval-log S01 ticket_received --run 2026-10-12-claude --data '{"agent": "...", "model": "..."}'`
3. The agent works on `agent/<ticket>-<domain>-<entity>`. Each time it runs the
   full check (`contractline ci`, or CI on the PR), it logs an `iteration` with
   `ci_passed` and the failing checks.
4. The agent opens the PR (`pr_opened`). From here the reviewer logs each
   `review` with the minutes spent and the decision.
5. If the reviewer changes the PR's files themselves, log `human_edit` with the
   files and lines changed. Any edit counts, however small.
6. On merge, log `merged`. If the scenario is given up, log `abandoned` with
   the reason; abandoned runs stay in the results.
7. After merge, problems found later (by another scenario, a simulated run or
   review of a later PR) are logged as `defect` against the scenario that
   introduced them.

## Simulated operation

After a batch of scenarios is merged, run the project in a simulated week:
`contractline test` with several seeds, plus `make e2e` with injected faults
(a late file, a type change at the source, a stale topic). Every event that
needs a human to act is an `incident`, with `minutes_to_resolve`.

## Metrics (computed by `contractline eval-report`)

| Metric | From |
| --- | --- |
| Lead time, ticket → merged PR | `ticket_received` → `merged` |
| Agent iterations until CI is green | `iteration` events until the first `ci_passed: true` |
| Human review time | sum of `review.minutes` |
| Share of PRs merged with no human code edits | merged runs without `human_edit` |
| Defects found after merge | `defect` events |
| Incidents in a simulated run | `incident` events, median `minutes_to_resolve` |
| Time to add a new source adapter | `adapter_added.hours` (scenario S14) |

## Baseline

`baseline.yaml` holds, per scenario, the estimated hours for a data engineer to
build the same pipeline by hand (code, tests, review, deployment) without the
harness. The estimates are written **before** any agent run and are not changed
afterwards. A stronger baseline is a hand-built control set: run 2–3 scenarios
by hand on a plain Spark + Airflow project, log them with the same events, and
use those numbers instead of estimates.

## Rules that keep the results honest

- Every run is logged, including failures and abandoned runs.
- The report states how many scenarios have been run; numbers from a few runs
  are not presented as evidence.
- Runs where the agent also built the harness are marked by the `agent` field
  and reported separately when there are enough independent runs.
- Logs never contain personal data.
