"""Unity Catalog governance: the governance plan as SQL (tags, column tags, grants).

Generated, reviewed in PRs (``contractline governance``), applied by CD.
"""

from contractline.core.governance import TablePolicy


def _quote(logical_name: str) -> str:
    return ".".join(f"`{part}`" for part in logical_name.split("."))


def _tag_list(tags: dict[str, str]) -> str:
    return ", ".join(f"'{k}' = '{v}'" for k, v in sorted(tags.items()))


def build_statements(policies: list[TablePolicy]) -> list[str]:
    statements: list[str] = []
    for policy in policies:
        table = _quote(policy.table)
        statements.append(f"ALTER TABLE {table} SET TAGS ({_tag_list(policy.tags)});")
        for column, tags in sorted(policy.column_tags.items()):
            statements.append(
                f"ALTER TABLE {table} ALTER COLUMN `{column}` SET TAGS ({_tag_list(tags)});"
            )
        for group in policy.readers:
            statements.append(f"GRANT SELECT ON TABLE {table} TO `{group}`;")
    return statements
