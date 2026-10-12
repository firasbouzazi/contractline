"""Secret provider interface.

Masking keys never come from code or contracts: blocks ask a provider by name.
Locally the provider reads environment variables; on Databricks it reads a
secret scope (milestone 5).
"""

import os
from typing import Protocol


class MissingSecretError(LookupError):
    pass


class SecretProvider(Protocol):
    def get_secret(self, name: str) -> bytes:
        """Return the secret called ``name`` or raise ``MissingSecretError``."""
        ...


class EnvSecretProvider:
    """Reads ``CONTRACTLINE_SECRET_<NAME>`` environment variables."""

    PREFIX = "CONTRACTLINE_SECRET_"
    MIN_LENGTH = 16

    def get_secret(self, name: str) -> bytes:
        variable = self.PREFIX + name.upper()
        value = os.environ.get(variable)
        if not value:
            raise MissingSecretError(f"secret {name!r} is not set; export {variable}")
        if len(value) < self.MIN_LENGTH:
            raise MissingSecretError(
                f"secret {name!r} is too short: use at least {self.MIN_LENGTH} characters"
            )
        return value.encode("utf-8")
