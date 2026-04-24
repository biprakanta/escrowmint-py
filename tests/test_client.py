from pathlib import Path

import pytest
from redis import Redis
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import ResponseError

from escrowmint import (
    BackendUnavailable,
    Client,
    ClientConfig,
    DuplicateIdempotencyConflict,
    InvalidAmount,
)


def test_client_from_url() -> None:
    client = Client.from_url("redis://localhost:6379/0")

    assert isinstance(client.config, ClientConfig)
    assert client.config.key_prefix == "escrowmint"


def test_try_consume_rejects_invalid_amount() -> None:
    client = Client.from_url("redis://localhost:6379/0")

    with pytest.raises(InvalidAmount):
        client.try_consume("wallet:1", 0)


def test_seed_available_rejects_negative_amount() -> None:
    client = Client.from_url("redis://localhost:6379/0")

    with pytest.raises(InvalidAmount):
        client.seed_available("wallet:1", -1)


def test_embedded_lua_matches_repo_script() -> None:
    from escrowmint._lua import TRY_CONSUME

    repo_script = Path("scripts/try_consume.lua").read_text(encoding="utf-8")
    normalized = "\n".join(line for line in repo_script.splitlines() if not line.startswith("--"))

    assert TRY_CONSUME.strip() == normalized.strip()


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


def test_seed_available_overwrites_existing_state(redis_url: str) -> None:
    client = Client.from_url(redis_url)
    client.seed_available("wallet:127", 2)
    client.try_consume("wallet:127", 1)

    client.seed_available("wallet:127", 9)
    state = client.get_state("wallet:127")

    assert state.available == 9
    assert state.reserved == 0
    assert state.version == 0


def test_try_consume_handles_concurrent_requests_without_overspending(redis_url: str) -> None:
    base_client = Client.from_url(redis_url)
    base_client.seed_available("wallet:128", 50)

    clients = [Client.from_url(redis_url) for _ in range(10)]
    results = []

    import concurrent.futures

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(client.try_consume, "wallet:128", 7) for client in clients]
        results = [future.result() for future in futures]

    applied_count = sum(1 for result in results if result.applied)
    state = base_client.get_state("wallet:128")

    assert applied_count == 7
    assert state.available == 1


def test_get_state_reads_existing_redis_hash(redis_url: str) -> None:
    client = Client.from_url(redis_url)
    redis_client = Redis.from_url(redis_url, decode_responses=True)
    redis_client.hset(
        "escrowmint:{wallet:129}:state",
        mapping={"available": 11, "reserved": 3, "version": 9},
    )

    state = client.get_state("wallet:129")

    assert state.available == 11
    assert state.reserved == 3
    assert state.version == 9


class _ScriptStub:
    def __init__(self, exc: Exception) -> None:
        self._exc = exc

    def __call__(self, *, keys: list[str], args: list[object]) -> str:
        raise self._exc


class _RedisStub:
    def __init__(self, script_exc: Exception | None = None, hmget_exc: Exception | None = None) -> None:
        self._script_exc = script_exc
        self._hmget_exc = hmget_exc

    def register_script(self, _: str) -> _ScriptStub:
        return _ScriptStub(self._script_exc or RuntimeError("missing script exception"))

    def hmget(self, key: str, *fields: str) -> list[str]:
        if self._hmget_exc is not None:
            raise self._hmget_exc
        return ["0", "0", "0"]

    def hset(self, key: str, mapping: dict[str, int]) -> None:
        return None


def test_try_consume_maps_invalid_amount_script_error() -> None:
    client = Client(
        ClientConfig(),
        redis_client=_RedisStub(script_exc=ResponseError("INVALID_AMOUNT")),
    )

    with pytest.raises(InvalidAmount):
        client.try_consume("wallet:stub", 1)


def test_try_consume_reraises_unknown_script_error() -> None:
    client = Client(
        ClientConfig(),
        redis_client=_RedisStub(script_exc=ResponseError("SOMETHING_ELSE")),
    )

    with pytest.raises(ResponseError):
        client.try_consume("wallet:stub", 1)


def test_try_consume_maps_connection_error_to_backend_unavailable() -> None:
    client = Client(
        ClientConfig(),
        redis_client=_RedisStub(script_exc=RedisConnectionError("boom")),
    )

    with pytest.raises(BackendUnavailable):
        client.try_consume("wallet:stub", 1)


def test_get_state_maps_connection_error_to_backend_unavailable() -> None:
    client = Client(
        ClientConfig(),
        redis_client=_RedisStub(hmget_exc=RedisConnectionError("boom")),
    )

    with pytest.raises(BackendUnavailable):
        client.get_state("wallet:stub")
