from dataclasses import dataclass


@dataclass(frozen=True)
class ClientConfig:
    redis_url: str = "redis://localhost:6379/0"
    key_prefix: str = "escrowmint"
    idempotency_ttl_ms: int = 86_400_000


class Client:
    """Minimal placeholder client until the Redis-backed implementation lands."""

    def __init__(self, config: ClientConfig) -> None:
        self.config = config

    @classmethod
    def from_url(cls, redis_url: str) -> "Client":
        return cls(ClientConfig(redis_url=redis_url))
