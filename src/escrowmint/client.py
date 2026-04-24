import hashlib
import json
import uuid
from dataclasses import dataclass

from redis import Redis
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import ResponseError

from ._lua import CANCEL, COMMIT, GET_STATE, RESERVE, TRY_CONSUME
from .errors import (
    BackendUnavailable,
    DuplicateIdempotencyConflict,
    InsufficientQuota,
    InvalidAmount,
    InvalidTTL,
    ReservationAlreadyCommitted,
    ReservationExpired,
    ReservationNotFound,
)
from .models import ConsumeResult, Reservation, ResourceState


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
        self._reserve_script = self._redis.register_script(RESERVE)
        self._commit_script = self._redis.register_script(COMMIT)
        self._cancel_script = self._redis.register_script(CANCEL)
        self._get_state_script = self._redis.register_script(GET_STATE)

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
        reservations_key = self._reservations_key(resource)
        idem_key = self._idempotency_key(resource, idempotency_key)
        fingerprint = self._fingerprint(resource=resource, amount=amount)

        raw_result = self._run_script(
            self._try_consume_script,
            keys=[state_key, reservations_key, idem_key],
            args=[
                amount,
                operation_id,
                self.config.idempotency_ttl_ms,
                fingerprint,
            ],
        )

        payload = json.loads(raw_result)
        return ConsumeResult(
            applied=bool(payload["applied"]),
            remaining=int(payload["remaining"]),
            operation_id=str(payload["operation_id"]),
        )

    def reserve(
        self,
        resource: str,
        amount: int,
        *,
        ttl_ms: int,
        reservation_id: str | None = None,
    ) -> Reservation:
        if amount <= 0:
            raise InvalidAmount("amount must be a positive integer")
        if ttl_ms <= 0:
            raise InvalidTTL("ttl_ms must be a positive integer")

        reservation_id = reservation_id or str(uuid.uuid4())
        raw_result = self._run_script(
            self._reserve_script,
            keys=[self._state_key(resource), self._reservations_key(resource)],
            args=[amount, ttl_ms, reservation_id],
        )
        payload = json.loads(raw_result)
        return Reservation(
            reservation_id=str(payload["reservation_id"]),
            resource=str(payload["resource"]),
            amount=int(payload["amount"]),
            expires_at_ms=int(payload["expires_at_ms"]),
            status=str(payload["status"]),
        )

    def commit(self, resource: str, reservation_id: str) -> ConsumeResult:
        raw_result = self._run_script(
            self._commit_script,
            keys=[self._state_key(resource), self._reservations_key(resource)],
            args=[reservation_id, str(uuid.uuid4())],
        )
        payload = json.loads(raw_result)
        return ConsumeResult(
            applied=bool(payload["applied"]),
            remaining=int(payload["remaining"]),
            operation_id=str(payload["operation_id"]),
        )

    def cancel(self, resource: str, reservation_id: str) -> bool:
        raw_result = self._run_script(
            self._cancel_script,
            keys=[self._state_key(resource), self._reservations_key(resource)],
            args=[reservation_id],
        )
        payload = json.loads(raw_result)
        return bool(payload["canceled"])

    def get_state(self, resource: str) -> ResourceState:
        raw_result = self._run_script(
            self._get_state_script,
            keys=[self._state_key(resource), self._reservations_key(resource)],
            args=[],
        )
        payload = json.loads(raw_result)

        return ResourceState(
            resource=resource,
            available=int(payload["available"]),
            reserved=int(payload["reserved"]),
            version=int(payload["version"]),
        )

    def seed_available(self, resource: str, amount: int) -> None:
        if amount < 0:
            raise InvalidAmount("seed amount must be zero or greater")
        self._redis.delete(self._reservations_key(resource))
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

    def _reservations_key(self, resource: str) -> str:
        return f"{self.config.key_prefix}:{{{resource}}}:reservations"

    def _idempotency_key(self, resource: str, idempotency_key: str | None) -> str:
        if not idempotency_key:
            return ""
        return f"{self.config.key_prefix}:{{{resource}}}:idem:{idempotency_key}"

    @staticmethod
    def _fingerprint(*, resource: str, amount: int) -> str:
        digest = hashlib.sha256(f"{resource}:{amount}".encode("utf-8")).hexdigest()
        return digest

    @staticmethod
    def _raise_script_error(exc: ResponseError) -> None:
        text = str(exc)
        if "INVALID_AMOUNT" in text:
            raise InvalidAmount("amount must be a positive integer") from exc
        if "INVALID_TTL" in text:
            raise InvalidTTL("ttl_ms must be a positive integer") from exc
        if "DUPLICATE_IDEMPOTENCY_CONFLICT" in text:
            raise DuplicateIdempotencyConflict(
                "idempotency key was reused for a different request"
            ) from exc
        if "INSUFFICIENT_QUOTA" in text:
            raise InsufficientQuota("insufficient quota") from exc
        if "RESERVATION_NOT_FOUND" in text:
            raise ReservationNotFound("reservation was not found") from exc
        if "RESERVATION_EXPIRED" in text:
            raise ReservationExpired("reservation has expired") from exc
        if "RESERVATION_ALREADY_COMMITTED" in text:
            raise ReservationAlreadyCommitted("reservation already committed") from exc
        raise exc

    def _run_script(self, script: object, *, keys: list[str], args: list[object]) -> str:
        try:
            return script(keys=keys, args=args)
        except ResponseError as exc:
            self._raise_script_error(exc)
        except RedisConnectionError as exc:
            raise BackendUnavailable("redis is unavailable") from exc
