# EscrowMint Python

Exact, Redis-backed bounded consumption for shared quotas.

EscrowMint Python is for cases where many threads, processes, or services need to consume from the same global quota without letting it go below zero.

Good fits:

- prepaid credits
- inventory reservation
- budget caps
- worker permit pools
- campaign spend controls

## Why EscrowMint

EscrowMint is not a generic counter library. It is a quota and reservation library with application-level semantics:

- exact bounded decrement
- idempotent consume
- reservation with TTL
- commit and cancel flow
- crash recovery via lazy expiry reclaim

## Install

Until the first PyPI release, install from GitHub:

```bash
uv add git+https://github.com/biprakanta/escrowmint-py
```

or:

```bash
pip install git+https://github.com/biprakanta/escrowmint-py
```

## Quickstart

```python
from escrowmint import Client

client = Client.from_url("redis://localhost:6379/0")

result = client.try_consume(
    "wallet:123",
    5,
    idempotency_key="req-001",
)

print(result.applied)      # True
print(result.remaining)    # remaining global quota
```

## Crash-Safe Reservation

```python
from escrowmint import Client, ReservationExpired

client = Client.from_url("redis://localhost:6379/0")

reservation = client.reserve(
    "wallet:123",
    10,
    ttl_ms=30_000,
)

try:
    result = client.commit("wallet:123", reservation.reservation_id)
except ReservationExpired:
    # the hold expired and the quota was released
    ...
```

If a worker crashes after `reserve` but before `commit`, the held quota is released after TTL expiry on the next mutation or `get_state` call for that same resource.

## Current API

```python
client.try_consume(resource, amount, idempotency_key=None)
client.reserve(resource, amount, ttl_ms=..., reservation_id=None)
client.commit(resource, reservation_id)
client.cancel(resource, reservation_id)
client.get_state(resource)
```

## How It Works

- Redis remains the source of truth for each resource.
- Lua scripts make each operation atomic.
- Reservations move units from `available` to `reserved`.
- Expired reservations are reclaimed lazily on the next touch of that resource.

## V1 vs V2

Use the current v1 model for most workloads:

- exact correctness
- simple Redis-first deployment
- reservation lifecycle with crash recovery

Planned v2 is for very hot resources:

- escrow or chunk allocation per worker
- fewer Redis round trips on the hottest path
- more complexity in exchange for higher throughput

See [docs/V2_ESCROW.md](/Users/biprakantapal/Desktop/codex-plugins/escrowmint-py/docs/V2_ESCROW.md).

## Development

EscrowMint Python uses `uv`.

```bash
uv sync --dev
uv run ruff check
uv run pytest
uv build
```

Notes:

- Python version is pinned in [`.python-version`](/Users/biprakantapal/Desktop/codex-plugins/escrowmint-py/.python-version)
- dependencies are locked in [uv.lock](/Users/biprakantapal/Desktop/codex-plugins/escrowmint-py/uv.lock)
- tests use Docker-backed Redis integration cases
- coverage is enforced from [pyproject.toml](/Users/biprakantapal/Desktop/codex-plugins/escrowmint-py/pyproject.toml)

## Docs

- [V1 API](/Users/biprakantapal/Desktop/codex-plugins/escrowmint-py/docs/V1_API.md)
- [Architecture](/Users/biprakantapal/Desktop/codex-plugins/escrowmint-py/docs/ARCHITECTURE.md)
- [V2 Escrow Design](/Users/biprakantapal/Desktop/codex-plugins/escrowmint-py/docs/V2_ESCROW.md)
- [Lua Scripts](/Users/biprakantapal/Desktop/codex-plugins/escrowmint-py/scripts/README.md)
