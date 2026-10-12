# Thesis evaluation: results

Scenario runs logged: **0** (of 14 scenarios in the backlog); merged: 0, abandoned: 0.

> ⚠️ Only 0 of 14 scenarios have been run. The numbers below are not yet evidence for or against the thesis.

## Headline metrics

| Metric | Value |
| --- | --- |
| Median lead time, ticket → merged PR | no data |
| Median agent iterations until CI is green | no data |
| Total human review time | 0.0 min |
| PRs merged with no human code edits | no data |
| Defects found after merge | 0 |
| Incidents in simulated runs | 0 |
| Median minutes to resolve an incident | no data |
| Time to add a source adapter | no data |

## Per scenario

| Scenario | Run | Status | Lead time | Iterations to green | Review | Human edits | Defects |
| --- | --- | --- | --- | --- | --- | --- | --- |

## Against the baseline (hand-built estimates)

| Scenario | Agent + harness | Hand-built estimate |
| --- | --- | --- |
| S01 | no data | 16.0 h |
| S02 | no data | 10.0 h |
| S03 | no data | 14.0 h |
| S04 | no data | 12.0 h |
| S05 | no data | 24.0 h |
| S06 | no data | 6.0 h |
| S07 | no data | 10.0 h |
| S08 | no data | 20.0 h |
| S09 | no data | 16.0 h |
| S10 | no data | 6.0 h |
| S11 | no data | 12.0 h |
| S12 | no data | 12.0 h |
| S13 | no data | 8.0 h |
| S14 | no data | 24.0 h |

## Where the thesis fails (so far)

- Nothing recorded yet. Absence of evidence is not evidence.

## Threats to validity

- The harness author also wrote the scenarios and the baseline estimates.
- Runs where the agent that built the harness also delivers the scenario are biased towards success; such runs are marked in their logs (`agent` field).
- Baseline figures are estimates unless a hand-built control run is logged.
