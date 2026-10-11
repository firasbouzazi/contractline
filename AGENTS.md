# Instructions for coding agents

This file will define exactly what an agent may change in a pipeline PR.
The full rules land with the agent workflow milestone; until then:

## You may change

- Contracts under `contracts/`.
- Pipeline configuration and gold models under `pipelines/`.

## You must not change (in a pipeline PR)

- Harness code under `src/` (blocks, core, adapters).
- Generated tests, CI configuration (`.github/`), ADRs.

## Always

- Branch from `main` as `agent/<ticket>-<domain>-<entity>`.
- Push only to your own branch and open a PR to `main`.
- Never push to `main`, merge your own PR or force-push over others' work.
- Run `uv run pytest` and the linters before pushing.
- Never weaken a test to make it pass; explain in the PR instead.
