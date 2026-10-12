"""Local governance: the governance plan as Delta table properties, so local
tables carry the same tags as Unity Catalog ones (access rules have no local
equivalent and are not applied)."""

from contractline.core.governance import TablePolicy
from contractline.spark.interfaces import SparkCompute

TAG_PREFIX = "contractline.tag."


def apply_table_tags(compute: SparkCompute, policies: list[TablePolicy]) -> list[str]:
    """Set tags on every existing table; return the tables tagged."""
    tagged = []
    for policy in policies:
        name = compute.build_table_name(policy.table)
        if not compute.spark.catalog.tableExists(name):
            continue
        properties = {f"{TAG_PREFIX}{k}": v for k, v in policy.tags.items()}
        for column, tags in policy.column_tags.items():
            properties.update({f"{TAG_PREFIX}{column}.{k}": v for k, v in tags.items()})
        assignments = ", ".join(f"'{k}' = '{v}'" for k, v in sorted(properties.items()))
        compute.spark.sql(f"ALTER TABLE {name} SET TBLPROPERTIES ({assignments})")
        tagged.append(policy.table)
    return tagged
