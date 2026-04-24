import hashlib
import json
import uuid
from dataclasses import dataclass

from redis import Redis
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import ResponseError

from ._lua import TRY_CONSUME
from .errors import BackendUnavailable, DuplicateIdempotencyConflict, InvalidAmount
from .models import ConsumeResult, ResourceState


@dataclass(frozen=True)
class ClientConfig:
    redis_url: str = "redis://localhost:6379/0"
    key_prefix: str = "escrowmint"
    idempotency_ttl_ms: int = 86_400_000
    socket_timeout_s: float = 5.0


class Client:
    def __init__(self, config: ClientConfig, *, redis_client: Redis | None = None) -> None:
        self.config = config
        self._redis = redis_client or Redis.from_url(
            config.redis_url,
            decode_responses=True,
            socket_timeout=config.socket_timeout_s,
        )
        self._try_consume_script = self._redis.register_script(TRY_CONSUME)

    @classmethod
    def from_url(cls, redis_url: str) -> "Client":
        return cls(ClientConfig(redis_url=redis_url))

    def try_consume(
        self,
        resource: str,
        amount: int,
        *,
        idempotency_key: str | None = None,
    ) -> ConsumeResult:
        if amount <= 0:
            raise InvalidAmount("amount must be a positive integer")

        operation_id = str(uuid.uuid4())
        state_key = self._state_key(resource)
        idem_key = self._idempotency_key(resource, idempotency_key)
        fingerprint = self._fingerprint(resource=resource, amount=amount)

        try:
            raw_result = self._try_consume_script(
                keys=[state_key, idem_key],
                args=[
                    amount,
                    operation_id,
                    self.config.idempotency_ttl_ms,
                    fingerprint,
                ],
            )
        except ResponseError as exc:
            if "INVALID_AMOUNT" in str(exc):
                raise InvalidAmount("amount must be a positive integer") from exc
            if "DUPLICATE_IDEMPOTENCY_CONFLICT" in str(exc):
                raise DuplicateIdempotencyConflict(
                    "idempotency key was reused for a different request"
                ) from exc
            raise
        except RedisConnectionError as exc:
            raise BackendUnavailable("redis is unavailable") from exc

        payload = json.loads(raw_result)
        return ConsumeResult(
            applied=bool(payload["applied"]),
            remaining=int(payload["remaining"]),
            operation_id=str(payload["operation_id"]),
        )

    def get_state(self, resource: str) -> ResourceState:
        try:
            available, reserved, version = self._redis.hmget(
                self._state_key(resource),
                "available",
                "reserved",
                "version",
            )
        except RedisConnectionError as exc:
            raise BackendUnavailable("redis is unavailable") from exc

        return ResourceState(
            resource=resource,
            available=int(available or 0),
            reserved=int(reserved or 0),
            version=int(version or 0),
        )

    def seed_available(self, resource: str, amount: int) -> None:
        if amount < 0:
            raise InvalidAmount("seed amount must be zero or greater")
        self._redis.hset(
            self._state_key(resource),
            mapping={
                "available": amount,
                "reserved": 0,
                "version": 0,
            },
        )

    def _state_key(self, resource: str) -> str:
        return f"{self.config.key_prefix}:{{{resource}}}:state"

    def _idempotency_key(self, resource: str, idempotency_key: str | None) -> str:
        if not idempotency_key:
            return ""
        return f"{self.config.key_prefix}:{{{resource}}}:idem:{idempotency_key}"

    @staticmethod
    def _fingerprint(*, resource: str, amount: int) -> str:
        digest = hashlib.sha256(f"{resource}:{amount}".encode("utf-8")).hexdigest()
        return digest
