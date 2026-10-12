"""``contractline new-dataset``: a contract skeleton that validates as generated,
with TODOs where only a person can know the answer."""

from contractline.core.naming import build_cdc_topic_name, build_topic_name

TEMPLATE = """\
# {domain}/{entity}: TODO describe the dataset in one sentence.
# Fill in every TODO, then run: contractline validate {path} && contractline lint {path}
api_version: contractline/v0

identity:
  name: {entity}
  domain: {domain}
  owner: {owner}
  version: 0.1.0
  description: TODO what one row means.

source:
  adapter: {adapter}
  mode: {mode}
  location: {location}
  format: {format}

schema:
  drift_policy: quarantine
  primary_key: [{entity_key}]
  columns:
    - name: {entity_key}
      type: long
      nullable: false
      pii: none
    - name: status
      type: string
      nullable: true
      pii: none
    - name: updated_at
      type: timestamp
      nullable: false
      pii: none
    # TODO add the other columns. Every column needs pii: none | indirect | direct | special.

history:
  type: {history}
{history_extra}
quality:
  - type: unique
    columns: [{entity_key}]
    severity: {unique_severity}

sla:
  freshness: P1D
"""

_SOURCES = {
    "cdc": ("kafka", "change_stream", "debezium_json", "scd2"),
    "events": ("kafka", "append_stream", "json", "append"),
    "files": ("files", "full", "csv", "snapshot"),
}


def build_contract_skeleton(
    domain: str, entity: str, kind: str, owner: str, source_system: str = "TODO_SYSTEM"
) -> str:
    if kind not in _SOURCES:
        raise ValueError(f"source kind must be one of {', '.join(_SOURCES)}")
    adapter, mode, format_, history = _SOURCES[kind]
    key = f"{entity[:-1] if entity.endswith('s') else entity}_id"
    if kind == "cdc":
        location = build_cdc_topic_name(source_system.lower(), "public", entity)
    elif kind == "events":
        location = build_topic_name(domain, entity, 1)
    else:
        location = f"/data/landing/{domain}/{entity}/"
    history_extra = (
        f"  business_key: [{key}]\n  tracked_columns: [status]\n  sequence_column: updated_at\n"
        if history == "scd2"
        else f"  business_key: [{key}]\n"
        if history == "snapshot"
        else ""
    )
    return TEMPLATE.format(
        domain=domain,
        entity=entity,
        owner=owner,
        adapter=adapter,
        mode=mode,
        location=location,
        format=format_,
        entity_key=key,
        history=history,
        history_extra=history_extra,
        unique_severity="block" if history != "append" else "quarantine",
        path=f"contracts/{domain}/{entity}.yaml",
    )
