from contractline.core.secrets import MissingSecretError

HASH_KEY = b"test-hash-key-0123456789abcdef"
TOKEN_KEY = b"test-token-key-0123456789abcdef"


class FakeSecrets:
    def __init__(self, secrets: dict[str, bytes] | None = None) -> None:
        self.secrets = (
            {"pii_hash": HASH_KEY, "pii_token": TOKEN_KEY} if secrets is None else secrets
        )

    def get_secret(self, name: str) -> bytes:
        try:
            return self.secrets[name]
        except KeyError:
            raise MissingSecretError(name) from None
