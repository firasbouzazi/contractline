"""Masking keys from a Databricks secret scope (``dbutils.secrets``)."""

from dataclasses import dataclass
from typing import Any

from contractline.core.secrets import MissingSecretError


@dataclass
class DatabricksSecretProvider:
    scope: str
    dbutils: Any = None
    """Injected for tests; found in the job's globals on Databricks."""

    def _utils(self) -> Any:
        if self.dbutils is not None:
            return self.dbutils
        try:
            from databricks.sdk.runtime import dbutils
        except ImportError as exc:
            raise MissingSecretError("dbutils is only available on Databricks") from exc
        return dbutils

    def get_secret(self, name: str) -> bytes:
        try:
            value = self._utils().secrets.get(scope=self.scope, key=name)
        except Exception as exc:  # dbutils raises its own exception types
            raise MissingSecretError(f"secret {name!r} not found in scope {self.scope!r}") from exc
        if not value:
            raise MissingSecretError(f"secret {name!r} in scope {self.scope!r} is empty")
        return str(value).encode("utf-8")
