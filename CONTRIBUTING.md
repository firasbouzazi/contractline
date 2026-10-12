# Contributing

## Setup

```bash
uv sync
uv run pre-commit install
```

## Checks (the same ones CI runs)

```bash
uv run ruff check .
uv run ruff format --check .
uv run mypy
uv run pytest
```

## Workflow

- `main` is the only long-lived branch (trunk-based; see `docs/adr/`).
- Work on a short-lived branch: `feat/…`, `fix/…`, `chore/…` for humans,
  `agent/<ticket>-<domain>-<entity>` for coding agents.
- Commits follow [Conventional Commits](https://www.conventionalcommits.org/); the
  commit-msg hook checks it. Naming rules are in `docs/adr/0007-naming-conventions.md`.
- One concern per pull request, with tests.
- Never weaken a test to make it pass. If a test looks wrong, say why in the PR.
- Significant design choices get an ADR in `docs/adr/` (copy `0000-template.md`).

## Repository setup (maintainers)

Branch protection for `main` is applied by a script rather than by hand, so it
is reviewable and repeatable. With `gh` authenticated as a repository admin:

```bash
scripts/configure-branch-protection.sh firasbouzazi/contractline
```

See `docs/adr/0004-branches-and-agent-pull-requests.md` for the rules and why.
