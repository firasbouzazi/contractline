"""Privacy block: apply each column's masking strategy (ADR-0012).

==============  ==============================================================
strategy        result
==============  ==============================================================
``hash``        HMAC-SHA256 of the value, hex. Key: secret ``pii_hash``.
``tokenize``    ``tok_`` + 32 hex characters of an HMAC with a separate key
                (secret ``pii_token``). Deterministic, so joins still work;
                not reversible (no token vault in the MVP).
``generalize``  dates and timestamps truncated (``to: year|month``, default
                year); numbers bucketed (``bucket: <width>``); strings cut
                (``keep_chars: <n>``).
``redact``      ``[REDACTED]`` for strings, null otherwise.
``drop``        column removed.
``keep``        unchanged (only allowed for indirect PII, with a justification).
==============  ==============================================================

Masking keys come from the ``SecretProvider``; ``params.key`` names a different
secret. Values are hashed in their canonical text form (``cast(value as
string)``), so the erasure workflow can hash a subject's key the same way.
"""

import hashlib
from collections.abc import Mapping
from typing import Any

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from contractline.core.contract import Column as ContractColumn
from contractline.core.contract import Contract, Masking, MaskingStrategy
from contractline.core.secrets import SecretProvider
from contractline.spark.types import to_spark_type

# Names of secrets in the SecretProvider, not secret values.
HASH_SECRET = "pii_hash"  # noqa: S105
TOKEN_SECRET = "pii_token"  # noqa: S105
REDACTED = "[REDACTED]"
TOKEN_PREFIX = "tok_"  # noqa: S105
TOKEN_HEX_LENGTH = 32
_BLOCK_SIZE = 64  # SHA-256 block size in bytes


class MaskingConfigError(ValueError):
    pass


def build_hmac_sha256(key: bytes, value: Column) -> Column:
    """HMAC-SHA256 (RFC 2104) of ``value``'s UTF-8 text, as lowercase hex.

    Spark has no HMAC function, so it is built from ``sha2``: the padded keys are
    constants computed here, once, on the driver.
    """
    if len(key) > _BLOCK_SIZE:
        key = hashlib.sha256(key).digest()
    key = key.ljust(_BLOCK_SIZE, b"\x00")
    inner_pad = bytes(b ^ 0x36 for b in key)
    outer_pad = bytes(b ^ 0x5C for b in key)
    message = F.encode(value, "UTF-8")
    inner = F.unhex(F.sha2(F.concat(F.lit(inner_pad), message), 256))
    return F.sha2(F.concat(F.lit(outer_pad), inner), 256)


def _param(masking: Masking, name: str, default: Any = None) -> Any:
    return (masking.params or {}).get(name, default)


def _generalize(value: Column, column: ContractColumn, masking: Masking) -> Column:
    if column.type in ("date", "timestamp"):
        unit = _param(masking, "to", "year")
        if unit not in ("year", "month"):
            raise MaskingConfigError(f"{column.name}: generalize 'to' must be year or month")
        truncated = F.date_trunc(unit, value)
        return truncated.cast("date") if column.type == "date" else truncated
    if column.type in ("integer", "long", "double") or column.type.startswith("decimal"):
        width = _param(masking, "bucket")
        if not isinstance(width, int | float) or width <= 0:
            raise MaskingConfigError(f"{column.name}: generalize needs a positive 'bucket' width")
        return (F.floor(value / F.lit(width)) * F.lit(width)).cast(to_spark_type(column.type))
    if column.type == "string":
        keep = _param(masking, "keep_chars")
        if not isinstance(keep, int) or keep < 1:
            raise MaskingConfigError(f"{column.name}: generalize needs a positive 'keep_chars'")
        return F.substring(value, 1, keep)
    raise MaskingConfigError(f"{column.name}: generalize does not apply to {column.type}")


def build_masked_column(
    column: ContractColumn, masking: Masking, secrets: SecretProvider
) -> Column | None:
    """The masked expression for one column, or ``None`` to drop it."""
    value = F.col(column.name)
    text = value.cast("string")
    strategy = masking.strategy
    if strategy is MaskingStrategy.KEEP:
        return value
    if strategy is MaskingStrategy.DROP:
        return None
    if strategy is MaskingStrategy.HASH:
        key = secrets.get_secret(_param(masking, "key", HASH_SECRET))
        return F.when(value.isNotNull(), build_hmac_sha256(key, text))
    if strategy is MaskingStrategy.TOKENIZE:
        key = secrets.get_secret(_param(masking, "key", TOKEN_SECRET))
        digest = F.substring(build_hmac_sha256(key, text), 1, TOKEN_HEX_LENGTH)
        return F.when(value.isNotNull(), F.concat(F.lit(TOKEN_PREFIX), digest))
    if strategy is MaskingStrategy.REDACT:
        if column.type == "string":
            return F.when(value.isNotNull(), F.lit(REDACTED))
        return F.lit(None).cast(to_spark_type(column.type))
    if strategy is MaskingStrategy.GENERALIZE:
        return _generalize(value, column, masking)
    raise MaskingConfigError(f"unknown masking strategy {strategy}")


def apply_masking(df: DataFrame, contract: Contract, secrets: SecretProvider) -> DataFrame:
    """Mask every column with a masking entry; other columns pass through."""
    masking: Mapping[str, Masking] = contract.privacy.masking if contract.privacy else {}
    selected: list[Column] = []
    for name in df.columns:
        column = contract.schema.column(name)
        if column is None or name not in masking:
            selected.append(F.col(name))
            continue
        masked = build_masked_column(column, masking[name], secrets)
        if masked is not None:
            selected.append(masked.alias(name))
    return df.select(*selected)
