import pytest

from contractline.core.secrets import EnvSecretProvider, MissingSecretError


def test_reads_secret_from_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CONTRACTLINE_SECRET_PII_HASH", "a" * 32)

    assert EnvSecretProvider().get_secret("pii_hash") == b"a" * 32


def test_missing_secret(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("CONTRACTLINE_SECRET_PII_HASH", raising=False)

    with pytest.raises(MissingSecretError, match="export CONTRACTLINE_SECRET_PII_HASH"):
        EnvSecretProvider().get_secret("pii_hash")


def test_short_secret_is_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CONTRACTLINE_SECRET_PII_HASH", "short")

    with pytest.raises(MissingSecretError, match="too short"):
        EnvSecretProvider().get_secret("pii_hash")
