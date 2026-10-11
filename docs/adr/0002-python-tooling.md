# 0002. Python tooling

- Status: Accepted
- Date: 2026-10-11

## Context

Contributors must be able to set up and run the test suite in minutes, and CI
must run exactly the same checks. The brief allows `uv` or `poetry`.

## Options

1. **poetry**: mature, widely known; slower resolver, its own lockfile format,
   Python versions installed separately.
2. **uv**: fast, installs Python itself, standard `pyproject.toml`
   (`[dependency-groups]`), a single lockfile; younger project.

## Decision

- Packaging and environments: **uv**, build backend **hatchling**, `src/` layout.
- Lint and format: **ruff**. Types: **mypy --strict**.
- Tests: **pytest** with **hypothesis** for property-based tests.
- CLI: **Typer** (thin layer over Click, typed signatures).
- Hooks: **pre-commit**, running the same tools as CI.
- Supported Python: 3.11 and 3.12, both tested in CI.

## Consequences

- One command (`uv sync`) sets up a development environment.
- `uv.lock` is committed; CI installs with `--locked`, so dependency drift fails loudly.
- Strict typing from day one costs some speed now and avoids a painful retrofit later.
