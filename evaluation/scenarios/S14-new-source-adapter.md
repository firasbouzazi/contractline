# S14 · New source adapter: HTTP API (harness change, human)

**Type:** harness change · **Difficulty:** large · **Who:** the harness maintainer, with or without an agent

A partner exposes orders through a paginated REST API with an `updated_since`
parameter. Add a source adapter that lands those records in bronze using the
standard envelope, for the `incremental` mode.

This measures "time to add a new source adapter". Agents may not change
adapters in a pipeline PR, so this ticket is a harness PR reviewed under CODEOWNERS.
