import hashlib
import json
import uuid
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from redis import Redis
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import RedisError
from redis.exceptions import ResponseError
from redis.exceptions import TimeoutError as RedisTimeoutError

from ._lua import (
    ALLOCATE_CHUNK,
    CANCEL,
    COMMIT,
    CONSUME_CHUNK,
    GET_CHUNK,
    GET_STATE,
    RELEASE_CHUNK,
    RENEW_CHUNK,
    RESERVE,
    TRY_CONSUME,
)
from .errors import (
    BackendUnavailable,
    CorruptState,
    DuplicateIdempotencyConflict,
    InsufficientQuota,
    InvalidAmount,
    InvalidOwner,
    InvalidTTL,
    LeaseAlreadyReleased,
    LeaseExpired,
    LeaseNotFound,
    LeaseOwnershipMismatch,
    ReservationAlreadyCommitted,
    ReservationExpired,
    ReservationNotFound,
)
from .models import ChunkConsumeResult, ChunkLease, ConsumeResult, Reservation, ResourceState


@dataclass(frozen=True)
class ClientConfig:
    redis_url: str = "redis://localhost:6379/0"
    key_prefix: str = "escrowmint"
    idempotency_ttl_ms: int = 86_400_000
    socket_timeout_s: float = 5.0


class Client:
    def __init__(self, config: ClientConfig, *, redis_client: Optional[Redis] = None) -> None:
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
        self._allocate_chunk_script = self._redis.register_script(ALLOCATE_CHUNK)
        self._consume_chunk_script = self._redis.register_script(CONSUME_CHUNK)
        self._renew_chunk_script = self._redis.register_script(RENEW_CHUNK)
        self._release_chunk_script = self._redis.register_script(RELEASE_CHUNK)
        self._get_chunk_script = self._redis.register_script(GET_CHUNK)

    @classmethod
    def from_url(cls, redis_url: str) -> "Client":
        return cls(ClientConfig(redis_url=redis_url))

    def try_consume(
        self,
        resource: str,
        amount: int,
        *,
        idempotency_key: Optional[str] = None,
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
            keys=[
                state_key,
                reservations_key,
                self._expiries_key(resource),
                self._chunk_leases_key(resource),
                self._chunk_expiries_key(resource),
                idem_key,
            ],
            args=[
                amount,
                operation_id,
                self.config.idempotency_ttl_ms,
                fingerprint,
                self.config.idempotency_ttl_ms,
            ],
        )

        payload = self._load_payload(raw_result)
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
        reservation_id: Optional[str] = None,
    ) -> Reservation:
        if amount <= 0:
            raise InvalidAmount("amount must be a positive integer")
        if ttl_ms <= 0:
            raise InvalidTTL("ttl_ms must be a positive integer")

        reservation_id = reservation_id or str(uuid.uuid4())
        raw_result = self._run_script(
            self._reserve_script,
            keys=[
                self._state_key(resource),
                self._reservations_key(resource),
                self._expiries_key(resource),
                self._chunk_leases_key(resource),
                self._chunk_expiries_key(resource),
            ],
            args=[amount, ttl_ms, reservation_id, self.config.idempotency_ttl_ms],
        )
        payload = self._load_payload(raw_result)
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
            keys=[
                self._state_key(resource),
                self._reservations_key(resource),
                self._expiries_key(resource),
                self._chunk_leases_key(resource),
                self._chunk_expiries_key(resource),
            ],
            args=[reservation_id, str(uuid.uuid4()), self.config.idempotency_ttl_ms],
        )
        payload = self._load_payload(raw_result)
        return ConsumeResult(
            applied=bool(payload["applied"]),
            remaining=int(payload["remaining"]),
            operation_id=str(payload["operation_id"]),
        )

    def cancel(self, resource: str, reservation_id: str) -> bool:
        raw_result = self._run_script(
            self._cancel_script,
            keys=[
                self._state_key(resource),
                self._reservations_key(resource),
                self._expiries_key(resource),
                self._chunk_leases_key(resource),
                self._chunk_expiries_key(resource),
            ],
            args=[reservation_id, self.config.idempotency_ttl_ms],
        )
        payload = self._load_payload(raw_result)
        return bool(payload["canceled"])

    def get_state(self, resource: str) -> ResourceState:
        raw_result = self._run_script(
            self._get_state_script,
            keys=[
                self._state_key(resource),
                self._reservations_key(resource),
                self._expiries_key(resource),
                self._chunk_leases_key(resource),
                self._chunk_expiries_key(resource),
            ],
            args=[self.config.idempotency_ttl_ms],
        )
        payload = self._load_payload(raw_result)

        return ResourceState(
            resource=resource,
            available=int(payload["available"]),
            reserved=int(payload["reserved"]),
            version=int(payload["version"]),
        )

    def allocate_chunk(
        self,
        resource: str,
        amount: int,
        *,
        owner_id: str,
        ttl_ms: int,
        lease_id: Optional[str] = None,
    ) -> ChunkLease:
        if amount <= 0:
            raise InvalidAmount("amount must be a positive integer")
        if ttl_ms <= 0:
            raise InvalidTTL("ttl_ms must be a positive integer")
        if not owner_id:
            raise InvalidOwner("owner_id must be a non-empty string")

        lease_id = lease_id or str(uuid.uuid4())
        raw_result = self._run_script(
            self._allocate_chunk_script,
            keys=self._resource_keys(resource),
            args=[amount, ttl_ms, owner_id, lease_id, self.config.idempotency_ttl_ms],
        )
        payload = self._load_payload(raw_result)
        return ChunkLease(
            lease_id=str(payload["lease_id"]),
            resource=str(payload["resource"]),
            owner_id=str(payload["owner_id"]),
            granted=int(payload["granted"]),
            remaining=int(payload["remaining"]),
            expires_at_ms=int(payload["expires_at_ms"]),
            status=str(payload["status"]),
        )

    def consume_chunk(
        self,
        resource: str,
        lease_id: str,
        amount: int,
        *,
        owner_id: str,
    ) -> ChunkConsumeResult:
        if amount <= 0:
            raise InvalidAmount("amount must be a positive integer")
        if not owner_id:
            raise InvalidOwner("owner_id must be a non-empty string")

        raw_result = self._run_script(
            self._consume_chunk_script,
            keys=self._resource_keys(resource),
            args=[lease_id, owner_id, amount, self.config.idempotency_ttl_ms],
        )
        payload = self._load_payload(raw_result)
        return ChunkConsumeResult(
            applied=bool(payload["applied"]),
            lease_id=str(payload["lease_id"]),
            remaining=int(payload["remaining"]),
            expires_at_ms=int(payload["expires_at_ms"]),
        )

    def renew_chunk(
        self,
        resource: str,
        lease_id: str,
        *,
        owner_id: str,
        ttl_ms: int,
    ) -> ChunkLease:
        if ttl_ms <= 0:
            raise InvalidTTL("ttl_ms must be a positive integer")
        if not owner_id:
            raise InvalidOwner("owner_id must be a non-empty string")

        raw_result = self._run_script(
            self._renew_chunk_script,
            keys=self._resource_keys(resource),
            args=[lease_id, owner_id, ttl_ms, self.config.idempotency_ttl_ms],
        )
        payload = self._load_payload(raw_result)
        return ChunkLease(
            lease_id=str(payload["lease_id"]),
            resource=str(payload["resource"]),
            owner_id=str(payload["owner_id"]),
            granted=int(payload["granted"]),
            remaining=int(payload["remaining"]),
            expires_at_ms=int(payload["expires_at_ms"]),
            status=str(payload["status"]),
        )

    def release_chunk(self, resource: str, lease_id: str, *, owner_id: str) -> ChunkLease:
        if not owner_id:
            raise InvalidOwner("owner_id must be a non-empty string")

        raw_result = self._run_script(
            self._release_chunk_script,
            keys=self._resource_keys(resource),
            args=[lease_id, owner_id, self.config.idempotency_ttl_ms],
        )
        payload = self._load_payload(raw_result)
        return ChunkLease(
            lease_id=str(payload["lease_id"]),
            resource=str(payload["resource"]),
            owner_id=str(payload["owner_id"]),
            granted=int(payload["granted"]),
            remaining=int(payload["remaining"]),
            expires_at_ms=int(payload["expires_at_ms"]),
            status=str(payload["status"]),
        )

    def get_chunk(self, resource: str, lease_id: str) -> ChunkLease:
        raw_result = self._run_script(
            self._get_chunk_script,
            keys=self._resource_keys(resource),
            args=[lease_id, self.config.idempotency_ttl_ms],
        )
        payload = self._load_payload(raw_result)
        return ChunkLease(
            lease_id=str(payload["lease_id"]),
            resource=str(payload["resource"]),
            owner_id=str(payload["owner_id"]),
            granted=int(payload["granted"]),
            remaining=int(payload["remaining"]),
            expires_at_ms=int(payload["expires_at_ms"]),
            status=str(payload["status"]),
        )

    def seed_available(self, resource: str, amount: int) -> None:
        if amount < 0:
            raise InvalidAmount("seed amount must be zero or greater")
        keys_to_delete = [
            self._reservations_key(resource),
            self._expiries_key(resource),
            self._chunk_leases_key(resource),
            self._chunk_expiries_key(resource),
        ]
        keys_to_delete.extend(self._resource_auxiliary_keys(resource))
        if keys_to_delete:
            self._redis.delete(*keys_to_delete)
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

    def _expiries_key(self, resource: str) -> str:
        return f"{self.config.key_prefix}:{{{resource}}}:reservation_expiries"

    def _chunk_leases_key(self, resource: str) -> str:
        return f"{self.config.key_prefix}:{{{resource}}}:chunk_leases"

    def _chunk_expiries_key(self, resource: str) -> str:
        return f"{self.config.key_prefix}:{{{resource}}}:chunk_lease_expiries"

    def _receipt_key(self, resource: str, reservation_id: str) -> str:
        return f"{self.config.key_prefix}:{{{resource}}}:receipt:{reservation_id}"

    def _chunk_receipt_key(self, resource: str, lease_id: str) -> str:
        return f"{self.config.key_prefix}:{{{resource}}}:chunk_receipt:{lease_id}"

    def _resource_keys(self, resource: str) -> List[str]:
        return [
            self._state_key(resource),
            self._reservations_key(resource),
            self._expiries_key(resource),
            self._chunk_leases_key(resource),
            self._chunk_expiries_key(resource),
        ]

    def _idempotency_key(self, resource: str, idempotency_key: Optional[str]) -> str:
        if not idempotency_key:
            return ""
        return f"{self.config.key_prefix}:{{{resource}}}:idem:{idempotency_key}"

    def _resource_auxiliary_keys(self, resource: str) -> List[str]:
        patterns = (
            f"{self.config.key_prefix}:{{{resource}}}:idem:*",
            f"{self.config.key_prefix}:{{{resource}}}:receipt:*",
            f"{self.config.key_prefix}:{{{resource}}}:chunk_receipt:*",
        )
        keys: List[str] = []
        for pattern in patterns:
            keys.extend(self._redis.scan_iter(match=pattern))
        return keys

    @staticmethod
    def _fingerprint(*, resource: str, amount: int) -> str:
        digest = hashlib.sha256(f"{resource}:{amount}".encode("utf-8")).hexdigest()
        return digest

    @staticmethod
    def _raise_script_error(exc: ResponseError) -> None:
        text = str(exc)
        if "CORRUPT_STATE" in text:
            raise CorruptState("escrowmint state is malformed") from exc
        if "INVALID_AMOUNT" in text:
            raise InvalidAmount("amount must be a positive integer") from exc
        if "INVALID_OWNER" in text:
            raise InvalidOwner("owner_id must be a non-empty string") from exc
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
        if "LEASE_NOT_FOUND" in text:
            raise LeaseNotFound("chunk lease was not found") from exc
        if "LEASE_EXPIRED" in text:
            raise LeaseExpired("chunk lease has expired") from exc
        if "LEASE_ALREADY_RELEASED" in text:
            raise LeaseAlreadyReleased("chunk lease has already been released") from exc
        if "OWNER_MISMATCH" in text:
            raise LeaseOwnershipMismatch("chunk lease is owned by another worker") from exc
        raise exc

    @staticmethod
    def _load_payload(raw_result: str) -> Dict[str, Any]:
        try:
            return json.loads(raw_result)
        except json.JSONDecodeError as exc:
            raise CorruptState("backend returned malformed result") from exc

    def _run_script(self, script: object, *, keys: List[str], args: List[object]) -> str:
        try:
            return script(keys=keys, args=args)
        except ResponseError as exc:
            self._raise_script_error(exc)
        except (RedisConnectionError, RedisTimeoutError, RedisError) as exc:
            raise BackendUnavailable("redis is unavailable") from exc
