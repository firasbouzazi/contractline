# Instructions for coding agents

You deliver datasets: a **contract** (and, for business logic, a **gold
model**). The harness builds, masks, historizes, tests and deploys them. You
never write pipeline code, and you cannot change what "passing" means.

## You may change (enforced by `contractline ci` on `agent/` branches)

| Path | What |
| --- | --- |
| `contracts/<domain>/<entity>.yaml` | data contracts |
| `pipelines/<domain>/gold/<kind>_<name>.yaml` and `.sql` | gold models |
| `infra/airflow/dags/*.py` | only by running `contractline dags` (generated) |
| `evaluation/logs/*.jsonl` | your own work log, when running an evaluation scenario |

## You must not change

Anything else: `src/` (blocks, adapters, core), `tests/`, `.github/`,
`contractline.yaml` (domains, glossary), `docs/adr/`, `infra/` except generated
Dags, this file. If the task seems to need one of these, stop and say so in the
PR description: that is a harness change for the maintainer.

## Workflow

1. Start from `main`: `git checkout -b agent/<TICKET>-<domain>-<entity>`.
2. New dataset: `uv run contractline new-dataset --domain <d> --entity <e> --source cdc|events|files`.
   Fill every TODO. Classify **every** column's `pii`. When unsure whether a
   column is personal data, classify it as PII and say so in the PR.
3. Check as you go:
   - `uv run contractline validate contracts pipelines`
   - `uv run contractline lint contracts pipelines`
   - changed contract: `uv run contractline diff <old> <new>`; bump the version as it says
4. Gold model: write the SQL and its YAML with a business `description` and at
   least one test whose expected rows you derived from the ticket, not from
   running your SQL.
5. Regenerate Dags: `uv run contractline dags contracts pipelines`.
6. Run everything CI will run: `uv run contractline ci --base origin/main --branch <your branch> --pr-title "<title>"`.
   It runs the generated tests (synthetic data through the real pipeline).
7. Commit with Conventional Commits: `feat(<domain>): add <entity> contract`.
8. Push your branch and open a PR to `main` titled `[<domain>/<entity>] <summary>`.
   Describe the business meaning and every assumption you made.

## Never

- Push to `main`, merge a PR, or force-push over someone else's work.
- Weaken a contract to make tests pass (removing a rule, lowering a severity,
  widening accepted values, weakening masking). `contractline diff` flags these
  as **relaxing** and they go to the data owner. If a rule seems wrong, explain
  why in the PR and leave it.
- Put real personal data in a contract, test or log. Use `@example.invalid`.
- Use production credentials. You have none, and you need none.

## Reference

- Contract format: `docs/adr/0005-contract-format.md`, schema in
  `src/contractline/core/schemas/contract-v0.json`, examples in `examples/`.
- Naming: `docs/adr/0007-naming-conventions.md`.
- Silver semantics (quality, masking, history): `docs/adr/0012-silver-blocks.md`.
- Gold models: `docs/adr/0013-gold-models.md`.
