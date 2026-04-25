import concurrent.futures
import time
from pathlib import Path
from typing import Dict, List, Optional

import pytest
from redis import Redis
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import ResponseError
from redis.exceptions import TimeoutError as RedisTimeoutError

from escrowmint import (
    BackendUnavailable,
    Client,
    ClientConfig,
    ChunkLease,
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
from escrowmint._lua import CANCEL, COMMIT, GET_STATE, RESERVE, TOP_UP, TRY_CONSUME


def test_client_from_url() -> None:
    client = Client.from_url("redis://localhost:6379/0")

    assert isinstance(client.config, ClientConfig)
    assert client.config.key_prefix == "escrowmint"


def test_try_consume_rejects_invalid_amount() -> None:
    client = Client.from_url("redis://localhost:6379/0")

    with pytest.raises(InvalidAmount):
        client.try_consume("wallet:1", 0)


def test_top_up_rejects_invalid_amount() -> None:
    client = Client.from_url("redis://localhost:6379/0")

    with pytest.raises(InvalidAmount):
        client.top_up("wallet:1", 0)


def test_reserve_rejects_invalid_inputs() -> None:
    client = Client.from_url("redis://localhost:6379/0")

    with pytest.raises(InvalidAmount):
        client.reserve("wallet:1", 0, ttl_ms=1000)

    with pytest.raises(InvalidTTL):
        client.reserve("wallet:1", 1, ttl_ms=0)

    with pytest.raises(InvalidOwner):
        client.allocate_chunk("wallet:1", 1, owner_id="", ttl_ms=1000)


def test_seed_available_rejects_negative_amount() -> None:
    client = Client.from_url("redis://localhost:6379/0")

    with pytest.raises(InvalidAmount):
        client.seed_available("wallet:1", -1)


def test_seed_available_clears_stale_replay_keys(redis_url: str) -> None:
    client = Client.from_url(redis_url)
    client.seed_available("wallet:stale", 10)
    client.try_consume("wallet:stale", 3, idempotency_key="req-1")

    receipt_key = client._receipt_key("wallet:stale", "res-1")
    chunk_receipt_key = client._chunk_receipt_key("wallet:stale", "lease-1")
    client._redis.set(receipt_key, "{}")
    client._redis.set(chunk_receipt_key, "{}")

    client.seed_available("wallet:stale", 20)

    assert not client._redis.exists(client._idempotency_key("wallet:stale", "req-1"))
    assert not client._redis.exists(receipt_key)
    assert not client._redis.exists(chunk_receipt_key)


def test_embedded_lua_matches_repo_scripts() -> None:
    def normalize(script: str) -> str:
        return "\n".join(
            line.rstrip()
            for line in script.strip().splitlines()
            if line.strip()
        )

    expectations = {
        "try_consume.lua": TRY_CONSUME,
        "top_up.lua": TOP_UP,
        "reserve.lua": RESERVE,
        "commit.lua": COMMIT,
        "cancel.lua": CANCEL,
        "get_state.lua": GET_STATE,
    }

    for filename, expected in expectations.items():
        repo_script = Path("scripts", filename).read_text(encoding="utf-8")
        assert normalize(expected) == normalize(repo_script)


def test_try_consume_applies_and_updates_state(redis_url: str) -> None:
    client = Client.from_url(redis_url)
    client.seed_available("wallet:123", 10)

    result = client.try_consume("wallet:123", 3)
    state = client.get_state("wallet:123")

    assert result.applied is True
    assert result.remaining == 7
    assert state.available == 7
    assert state.version == 1


def test_try_consume_returns_applied_false_on_insufficient_quota(redis_url: str) -> None:
    client = Client.from_url(redis_url)
    client.seed_available("wallet:124", 2)

    result = client.try_consume("wallet:124", 5)
    state = client.get_state("wallet:124")

    assert result.applied is False
    assert result.remaining == 2
    assert state.available == 2
    assert state.version == 0


def test_try_consume_is_idempotent_for_retries(redis_url: str) -> None:
    client = Client.from_url(redis_url)
    client.seed_available("wallet:125", 10)

    first = client.try_consume("wallet:125", 4, idempotency_key="req-1")
    second = client.try_consume("wallet:125", 4, idempotency_key="req-1")
    state = client.get_state("wallet:125")

    assert first == second
    assert state.available == 6
    assert state.version == 1


def test_try_consume_rejects_conflicting_idempotency_reuse(redis_url: str) -> None:
    client = Client.from_url(redis_url)
    client.seed_available("wallet:126", 10)
    client.try_consume("wallet:126", 4, idempotency_key="req-2")

    with pytest.raises(DuplicateIdempotencyConflict):
        client.try_consume("wallet:126", 5, idempotency_key="req-2")


def test_top_up_applies_and_updates_state(redis_url: str) -> None:
    client = Client.from_url(redis_url)
    client.seed_available("wallet:127", 10)

    result = client.top_up("wallet:127", 4)
    state = client.get_state("wallet:127")

    assert result.added == 4
    assert result.available == 14
    assert state.available == 14
    assert state.version == 1


def test_top_up_is_idempotent_for_retries(redis_url: str) -> None:
    client = Client.from_url(redis_url)
    client.seed_available("wallet:128", 10)

    first = client.top_up("wallet:128", 4, idempotency_key="top-up-1")
    second = client.top_up("wallet:128", 4, idempotency_key="top-up-1")
    state = client.get_state("wallet:128")

    assert first == second
    assert state.available == 14
    assert state.version == 1


def test_top_up_rejects_conflicting_idempotency_reuse(redis_url: str) -> None:
    client = Client.from_url(redis_url)
    client.seed_available("wallet:129", 10)
    client.top_up("wallet:129", 4, idempotency_key="top-up-2")

    with pytest.raises(DuplicateIdempotencyConflict):
        client.top_up("wallet:129", 5, idempotency_key="top-up-2")


def test_top_up_reclaims_expired_reservations_before_adding(redis_url: str) -> None:
    client = Client.from_url(redis_url)
    client.seed_available("wallet:130", 10)
    client.reserve("wallet:130", 4, ttl_ms=100, reservation_id="res-topup")

    time.sleep(0.15)

    result = client.top_up("wallet:130", 3)
    state = client.get_state("wallet:130")

    assert result.added == 3
    assert result.available == 13
    assert state.available == 13
    assert state.reserved == 0
    assert state.version == 3


def test_reserve_holds_quota_until_commit(redis_url: str) -> None:
    client = Client.from_url(redis_url)
    client.seed_available("wallet:200", 10)

    reservation = client.reserve("wallet:200", 4, ttl_ms=5_000)
    state = client.get_state("wallet:200")

    assert reservation.resource == "wallet:200"
    assert reservation.amount == 4
    assert reservation.status == "pending"
    assert state.available == 6
    assert state.reserved == 4
    assert state.version == 1


def test_reserve_rejects_when_quota_is_insufficient(redis_url: str) -> None:
    client = Client.from_url(redis_url)
    client.seed_available("wallet:201", 1)

    with pytest.raises(InsufficientQuota):
        client.reserve("wallet:201", 2, ttl_ms=5_000)


def test_reserve_is_idempotent_for_same_reservation_id(redis_url: str) -> None:
    client = Client.from_url(redis_url)
    client.seed_available("wallet:202", 10)

    first = client.reserve("wallet:202", 3, ttl_ms=5_000, reservation_id="res-1")
    second = client.reserve("wallet:202", 3, ttl_ms=5_000, reservation_id="res-1")
    state = client.get_state("wallet:202")

    assert first == second
    assert state.available == 7
    assert state.reserved == 3
    assert state.version == 1


def test_reserve_rejects_conflicting_reuse_of_reservation_id(redis_url: str) -> None:
    client = Client.from_url(redis_url)
    client.seed_available("wallet:203", 10)
    client.reserve("wallet:203", 3, ttl_ms=5_000, reservation_id="res-2")

    with pytest.raises(DuplicateIdempotencyConflict):
        client.reserve("wallet:203", 4, ttl_ms=5_000, reservation_id="res-2")


def test_commit_burns_reserved_quota(redis_url: str) -> None:
    client = Client.from_url(redis_url)
    redis_client = Redis.from_url(redis_url, decode_responses=True)
    client.seed_available("wallet:204", 10)
    reservation = client.reserve("wallet:204", 3, ttl_ms=5_000, reservation_id="res-3")

    result = client.commit("wallet:204", reservation.reservation_id)
    state = client.get_state("wallet:204")

    assert result.applied is True
    assert result.remaining == 7
    assert state.available == 7
    assert state.reserved == 0
    assert state.version == 2
    assert redis_client.hlen(client._reservations_key("wallet:204")) == 0
    assert redis_client.zcard(client._expiries_key("wallet:204")) == 0


def test_commit_is_idempotent(redis_url: str) -> None:
    client = Client.from_url(redis_url)
    client.seed_available("wallet:205", 10)
    reservation = client.reserve("wallet:205", 3, ttl_ms=5_000, reservation_id="res-4")

    first = client.commit("wallet:205", reservation.reservation_id)
    second = client.commit("wallet:205", reservation.reservation_id)

    assert first == second


def test_commit_missing_reservation_raises(redis_url: str) -> None:
    client = Client.from_url(redis_url)
    client.seed_available("wallet:206", 10)

    with pytest.raises(ReservationNotFound):
        client.commit("wallet:206", "missing")


def test_cancel_releases_reserved_quota(redis_url: str) -> None:
    client = Client.from_url(redis_url)
    redis_client = Redis.from_url(redis_url, decode_responses=True)
    client.seed_available("wallet:207", 10)
    reservation = client.reserve("wallet:207", 3, ttl_ms=5_000, reservation_id="res-5")

    canceled = client.cancel("wallet:207", reservation.reservation_id)
    state = client.get_state("wallet:207")

    assert canceled is True
    assert state.available == 10
    assert state.reserved == 0
    assert state.version == 2
    assert redis_client.hlen(client._reservations_key("wallet:207")) == 0
    assert redis_client.zcard(client._expiries_key("wallet:207")) == 0


def test_cancel_returns_false_after_commit(redis_url: str) -> None:
    client = Client.from_url(redis_url)
    client.seed_available("wallet:208", 10)
    reservation = client.reserve("wallet:208", 3, ttl_ms=5_000, reservation_id="res-6")
    client.commit("wallet:208", reservation.reservation_id)

    assert client.cancel("wallet:208", reservation.reservation_id) is False


def test_expired_reservation_releases_quota_on_get_state(redis_url: str) -> None:
    client = Client.from_url(redis_url)
    redis_client = Redis.from_url(redis_url, decode_responses=True)
    client.seed_available("wallet:209", 10)
    client.reserve("wallet:209", 4, ttl_ms=100, reservation_id="res-7")

    time.sleep(0.2)
    state = client.get_state("wallet:209")

    assert state.available == 10
    assert state.reserved == 0
    assert state.version == 2
    assert redis_client.hlen(client._reservations_key("wallet:209")) == 0
    assert redis_client.zcard(client._expiries_key("wallet:209")) == 0


def test_commit_expired_reservation_raises_and_releases(redis_url: str) -> None:
    client = Client.from_url(redis_url)
    client.seed_available("wallet:210", 10)
    client.reserve("wallet:210", 4, ttl_ms=100, reservation_id="res-8")

    time.sleep(0.2)
    with pytest.raises(ReservationExpired):
        client.commit("wallet:210", "res-8")

    state = client.get_state("wallet:210")
    assert state.available == 10
    assert state.reserved == 0


def test_reusing_expired_reservation_id_raises(redis_url: str) -> None:
    client = Client.from_url(redis_url)
    client.seed_available("wallet:211", 10)
    client.reserve("wallet:211", 2, ttl_ms=100, reservation_id="res-9")

    time.sleep(0.2)
    with pytest.raises(ReservationExpired):
        client.reserve("wallet:211", 2, ttl_ms=100, reservation_id="res-9")


def test_reserving_after_expiry_can_reuse_released_quota(redis_url: str) -> None:
    client = Client.from_url(redis_url)
    client.seed_available("wallet:212", 5)
    client.reserve("wallet:212", 5, ttl_ms=100, reservation_id="res-10")

    time.sleep(0.2)
    reservation = client.reserve("wallet:212", 5, ttl_ms=5_000, reservation_id="res-11")

    assert reservation.amount == 5
    state = client.get_state("wallet:212")
    assert state.available == 0
    assert state.reserved == 5


def test_get_state_reads_existing_redis_hash(redis_url: str) -> None:
    client = Client.from_url(redis_url)
    redis_client = Redis.from_url(redis_url, decode_responses=True)
    redis_client.hset(
        "escrowmint:{wallet:213}:state",
        mapping={"available": 11, "reserved": 3, "version": 9},
    )

    state = client.get_state("wallet:213")

    assert state.available == 11
    assert state.reserved == 3
    assert state.version == 9


def test_allocate_chunk_holds_quota_until_release(redis_url: str) -> None:
    client = Client.from_url(redis_url)
    client.seed_available("wallet:300", 20)

    lease = client.allocate_chunk("wallet:300", 6, owner_id="worker-a", ttl_ms=5_000)
    state = client.get_state("wallet:300")

    assert isinstance(lease, ChunkLease)
    assert lease.owner_id == "worker-a"
    assert lease.granted == 6
    assert lease.remaining == 6
    assert lease.status == "active"
    assert state.available == 14


def test_allocate_chunk_is_idempotent_for_same_lease_id(redis_url: str) -> None:
    client = Client.from_url(redis_url)
    client.seed_available("wallet:301", 20)

    first = client.allocate_chunk(
        "wallet:301",
        5,
        owner_id="worker-a",
        ttl_ms=5_000,
        lease_id="lease-1",
    )
    second = client.allocate_chunk(
        "wallet:301",
        5,
        owner_id="worker-a",
        ttl_ms=5_000,
        lease_id="lease-1",
    )

    assert first == second


def test_allocate_chunk_rejects_reuse_of_released_lease_id(redis_url: str) -> None:
    client = Client.from_url(redis_url)
    client.seed_available("wallet:302", 20)
    lease = client.allocate_chunk(
        "wallet:302",
        5,
        owner_id="worker-a",
        ttl_ms=5_000,
        lease_id="lease-2",
    )
    client.release_chunk("wallet:302", lease.lease_id, owner_id="worker-a")

    with pytest.raises(LeaseAlreadyReleased):
        client.allocate_chunk(
            "wallet:302",
            5,
            owner_id="worker-a",
            ttl_ms=5_000,
            lease_id="lease-2",
        )


def test_consume_chunk_updates_lease_remaining_without_touching_available(redis_url: str) -> None:
    client = Client.from_url(redis_url)
    client.seed_available("wallet:303", 20)
    lease = client.allocate_chunk("wallet:303", 8, owner_id="worker-a", ttl_ms=5_000)

    result = client.consume_chunk("wallet:303", lease.lease_id, 3, owner_id="worker-a")
    refreshed = client.get_chunk("wallet:303", lease.lease_id)
    state = client.get_state("wallet:303")

    assert result.applied is True
    assert result.remaining == 5
    assert refreshed.remaining == 5
    assert state.available == 12


def test_consume_chunk_returns_applied_false_when_lease_remaining_is_low(redis_url: str) -> None:
    client = Client.from_url(redis_url)
    client.seed_available("wallet:304", 20)
    lease = client.allocate_chunk("wallet:304", 4, owner_id="worker-a", ttl_ms=5_000)

    result = client.consume_chunk("wallet:304", lease.lease_id, 6, owner_id="worker-a")

    assert result.applied is False
    assert result.remaining == 4


def test_release_chunk_returns_unused_quota(redis_url: str) -> None:
    client = Client.from_url(redis_url)
    redis_client = Redis.from_url(redis_url, decode_responses=True)
    client.seed_available("wallet:305", 20)
    lease = client.allocate_chunk("wallet:305", 10, owner_id="worker-a", ttl_ms=5_000)
    client.consume_chunk("wallet:305", lease.lease_id, 4, owner_id="worker-a")

    released = client.release_chunk("wallet:305", lease.lease_id, owner_id="worker-a")
    state = client.get_state("wallet:305")

    assert released.status == "released"
    assert released.remaining == 6
    assert state.available == 16
    assert redis_client.hlen(client._chunk_leases_key("wallet:305")) == 0
    assert redis_client.zcard(client._chunk_expiries_key("wallet:305")) == 0


def test_release_chunk_is_idempotent(redis_url: str) -> None:
    client = Client.from_url(redis_url)
    client.seed_available("wallet:306", 20)
    lease = client.allocate_chunk("wallet:306", 5, owner_id="worker-a", ttl_ms=5_000)

    first = client.release_chunk("wallet:306", lease.lease_id, owner_id="worker-a")
    second = client.release_chunk("wallet:306", lease.lease_id, owner_id="worker-a")

    assert first == second


def test_consume_chunk_rejects_wrong_owner(redis_url: str) -> None:
    client = Client.from_url(redis_url)
    client.seed_available("wallet:307", 20)
    lease = client.allocate_chunk("wallet:307", 5, owner_id="worker-a", ttl_ms=5_000)

    with pytest.raises(LeaseOwnershipMismatch):
        client.consume_chunk("wallet:307", lease.lease_id, 1, owner_id="worker-b")


def test_renew_chunk_extends_expiry(redis_url: str) -> None:
    client = Client.from_url(redis_url)
    client.seed_available("wallet:308", 20)
    lease = client.allocate_chunk("wallet:308", 5, owner_id="worker-a", ttl_ms=150)

    time.sleep(0.05)
    renewed = client.renew_chunk("wallet:308", lease.lease_id, owner_id="worker-a", ttl_ms=500)

    assert renewed.expires_at_ms > lease.expires_at_ms

    time.sleep(0.2)
    result = client.consume_chunk("wallet:308", lease.lease_id, 2, owner_id="worker-a")
    assert result.applied is True


def test_expired_chunk_releases_remaining_on_get_state(redis_url: str) -> None:
    client = Client.from_url(redis_url)
    client.seed_available("wallet:309", 20)
    lease = client.allocate_chunk("wallet:309", 10, owner_id="worker-a", ttl_ms=100)
    client.consume_chunk("wallet:309", lease.lease_id, 4, owner_id="worker-a")

    time.sleep(0.2)
    state = client.get_state("wallet:309")

    assert state.available == 16
    with pytest.raises(LeaseExpired):
        client.consume_chunk("wallet:309", lease.lease_id, 1, owner_id="worker-a")


def test_get_chunk_returns_terminal_receipt_after_release(redis_url: str) -> None:
    client = Client.from_url(redis_url)
    client.seed_available("wallet:310", 20)
    lease = client.allocate_chunk("wallet:310", 5, owner_id="worker-a", ttl_ms=5_000)
    client.consume_chunk("wallet:310", lease.lease_id, 2, owner_id="worker-a")
    client.release_chunk("wallet:310", lease.lease_id, owner_id="worker-a")

    chunk = client.get_chunk("wallet:310", lease.lease_id)

    assert chunk.status == "released"
    assert chunk.remaining == 3


def test_get_chunk_missing_lease_raises(redis_url: str) -> None:
    client = Client.from_url(redis_url)
    client.seed_available("wallet:311", 20)

    with pytest.raises(LeaseNotFound):
        client.get_chunk("wallet:311", "missing")


def test_try_consume_handles_concurrent_requests_without_overspending(redis_url: str) -> None:
    base_client = Client.from_url(redis_url)
    base_client.seed_available("wallet:214", 50)

    clients = [Client.from_url(redis_url) for _ in range(10)]

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(client.try_consume, "wallet:214", 7) for client in clients]
        results = [future.result() for future in futures]

    applied_count = sum(1 for result in results if result.applied)
    state = base_client.get_state("wallet:214")

    assert applied_count == 7
    assert state.available == 1


class _ScriptStub:
    def __init__(self, result: str = "{}", exc: Optional[Exception] = None) -> None:
        self._result = result
        self._exc = exc

    def __call__(self, *, keys: List[str], args: List[object]) -> str:
        if self._exc is not None:
            raise self._exc
        return self._result


class _RedisStub:
    def __init__(self, script_stubs: Optional[List[_ScriptStub]] = None) -> None:
        self._script_stubs = list(script_stubs or [])

    def register_script(self, _: str) -> _ScriptStub:
        if self._script_stubs:
            return self._script_stubs.pop(0)
        return _ScriptStub()

    def delete(self, *keys: str) -> None:
        return None

    def hset(self, key: str, mapping: Dict[str, int]) -> None:
        return None


def test_try_consume_maps_invalid_amount_script_error() -> None:
    client = Client(
        ClientConfig(),
        redis_client=_RedisStub(
            script_stubs=[
                _ScriptStub(exc=ResponseError("INVALID_AMOUNT")),
                _ScriptStub(),
                _ScriptStub(),
                _ScriptStub(),
                _ScriptStub(),
            ]
        ),
    )

    with pytest.raises(InvalidAmount):
        client.try_consume("wallet:stub", 1)


def test_reserve_maps_insufficient_quota_script_error() -> None:
    client = Client(
        ClientConfig(),
        redis_client=_RedisStub(
            script_stubs=[
                _ScriptStub(),
                _ScriptStub(),
                _ScriptStub(exc=ResponseError("INSUFFICIENT_QUOTA")),
                _ScriptStub(),
                _ScriptStub(),
                _ScriptStub(),
            ]
        ),
    )

    with pytest.raises(InsufficientQuota):
        client.reserve("wallet:stub", 1, ttl_ms=1_000)


def test_commit_maps_already_committed_script_error() -> None:
    client = Client(
        ClientConfig(),
        redis_client=_RedisStub(
            script_stubs=[
                _ScriptStub(),
                _ScriptStub(),
                _ScriptStub(),
                _ScriptStub(exc=ResponseError("RESERVATION_ALREADY_COMMITTED")),
                _ScriptStub(),
                _ScriptStub(),
            ]
        ),
    )

    with pytest.raises(ReservationAlreadyCommitted):
        client.commit("wallet:stub", "res")


def test_get_state_maps_connection_error_to_backend_unavailable() -> None:
    client = Client(
        ClientConfig(),
        redis_client=_RedisStub(
            script_stubs=[
                _ScriptStub(),
                _ScriptStub(),
                _ScriptStub(),
                _ScriptStub(),
                _ScriptStub(),
                _ScriptStub(exc=RedisConnectionError("boom")),
            ]
        ),
    )

    with pytest.raises(BackendUnavailable):
        client.get_state("wallet:stub")


def test_get_state_maps_timeout_error_to_backend_unavailable() -> None:
    client = Client(
        ClientConfig(),
        redis_client=_RedisStub(
            script_stubs=[
                _ScriptStub(),
                _ScriptStub(),
                _ScriptStub(),
                _ScriptStub(),
                _ScriptStub(),
                _ScriptStub(exc=RedisTimeoutError("boom")),
            ]
        ),
    )

    with pytest.raises(BackendUnavailable):
        client.get_state("wallet:stub")


def test_try_consume_maps_corrupt_state_script_error() -> None:
    client = Client(
        ClientConfig(),
        redis_client=_RedisStub(
            script_stubs=[
                _ScriptStub(exc=ResponseError("CORRUPT_STATE")),
                _ScriptStub(),
                _ScriptStub(),
                _ScriptStub(),
                _ScriptStub(),
            ]
        ),
    )

    with pytest.raises(CorruptState):
        client.try_consume("wallet:stub", 1)


def test_get_state_raises_corrupt_state_for_malformed_reservation_payload(redis_url: str) -> None:
    client = Client.from_url(redis_url)
    redis_client = Redis.from_url(redis_url, decode_responses=True)
    client.seed_available("wallet:215", 5)
    redis_client.hset(client._reservations_key("wallet:215"), mapping={"bad": "{not-json"})
    redis_client.zadd(client._expiries_key("wallet:215"), {"bad": 1})

    with pytest.raises(CorruptState):
        client.get_state("wallet:215")


def test_commit_raises_corrupt_state_for_malformed_receipt(redis_url: str) -> None:
    client = Client.from_url(redis_url)
    redis_client = Redis.from_url(redis_url, decode_responses=True)
    client.seed_available("wallet:216", 5)
    redis_client.set(client._receipt_key("wallet:216", "res-bad"), "{not-json")

    with pytest.raises(CorruptState):
        client.commit("wallet:216", "res-bad")
