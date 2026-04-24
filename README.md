<p align="center">
  <img src="logo-transparent.png" alt="EscrowMint logo" width="180">
</p>

<h1 align="center">EscrowMint Python</h1>

<p align="center">Exact, Redis-backed bounded consumption for shared quotas.</p>

<p align="center">
  <a href="https://github.com/biprakanta/escrowmint-py/actions/workflows/ci.yml"><img alt="CI" src="https://img.shields.io/github/actions/workflow/status/biprakanta/escrowmint-py/ci.yml?branch=main&label=CI"></a>
  <a href="https://github.com/biprakanta/escrowmint-py/releases"><img alt="GitHub Release" src="https://img.shields.io/github/v/release/biprakanta/escrowmint-py?display_name=tag"></a>
  <a href="https://github.com/biprakanta/escrowmint-py/blob/main/LICENSE"><img alt="License" src="https://img.shields.io/github/license/biprakanta/escrowmint-py"></a>
  <img alt="Python" src="https://img.shields.io/badge/python-3.13%2B-blue">
  <img alt="Coverage" src="https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/biprakanta/escrowmint-py/badges/coverage.json">
</p>

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

## Chunk Lease Path

EscrowMint also ships an explicit chunk-lease lifecycle for hot resources:

```python
lease = client.allocate_chunk(
    "wallet:123",
    100,
    owner_id="worker-a",
    ttl_ms=30_000,
)

result = client.consume_chunk("wallet:123", lease.lease_id, 5, owner_id="worker-a")
lease = client.renew_chunk("wallet:123", lease.lease_id, owner_id="worker-a", ttl_ms=30_000)
lease = client.release_chunk("wallet:123", lease.lease_id, owner_id="worker-a")
```

This is the authoritative distributed chunk path. It keeps chunk state in Redis and supports expiry reclaim, renew, release, and worker ownership checks.

## How It Works

- Redis remains the source of truth for each resource.
- Lua scripts make each operation atomic.
- Reservations move units from `available` to `reserved`.
- Pending reservations are indexed by expiry time in Redis.
- Expired reservations are reclaimed lazily in bounded batches on the next touch of that resource.
- Terminal reservation outcomes are moved into short-lived receipt keys so the hot reservation hash stays small.

## Direct Path and Chunk Lease Path

EscrowMint currently ships both models.

The direct path is the shared-resource path:

- `try_consume`, `reserve`, `commit`, and `cancel`
- exact bounded updates against the resource's shared state
- the simplest way to get correctness and crash recovery

The chunk lease path adds a worker-owned lease layer on top of that model:

- `allocate_chunk`, `consume_chunk`, `renew_chunk`, `release_chunk`, and `get_chunk`
- explicit escrow or chunk allocation per worker
- better control over hot-resource ownership, refill, expiry, and reclaim
- more operational complexity than the direct path

Choose the direct path when you want the simplest exact path.

Choose the chunk lease path when a resource benefits from explicit worker-level quota management.

The current chunk lease implementation is an authoritative Redis-backed lease lifecycle. It improves the state model for hot resources, but it does not automatically become a no-Redis local fast path. If you want fewer Redis round trips than the shipped chunk API provides, you can layer an in-process chunk consumer on top of the authoritative lease lifecycle.

See [docs/CHUNK_LEASES.md](docs/CHUNK_LEASES.md).

## Development

EscrowMint Python uses `uv`.

```bash
uv sync --dev
uv run ruff check
uv run pytest
uv build
```

Notes:

- Python version is pinned in [`.python-version`](.python-version)
- dependencies are locked in [uv.lock](uv.lock)
- tests use Docker-backed Redis integration cases
- coverage is enforced from [pyproject.toml](pyproject.toml)

## Support

- Python: `3.13+`
- Redis: intended for modern Redis deployments that support Lua scripting and standard key expiry semantics
- Stability: the package is approaching its first public `0.1.x` release; expect the core API to be much more stable than the surrounding release/docs tooling

## Release Process

EscrowMint Python uses Conventional Commits and Release Please for semantic versioning and release notes.

- `fix:` -> patch release
- `feat:` -> minor release
- `feat!:` or `BREAKING CHANGE:` -> major release

When releasable commits land on `main`, Release Please opens or updates a release PR. Merging that PR updates [CHANGELOG.md](CHANGELOG.md), creates the `vX.Y.Z` tag, creates the GitHub release notes, and then the existing tag workflow publishes the package to PyPI.

## Docs

- [Direct Path API](docs/DIRECT_API.md)
- [Architecture](docs/ARCHITECTURE.md)
- [Chunk Lease Design](docs/CHUNK_LEASES.md)
- [Lua Scripts](scripts/README.md)
- [Changelog](CHANGELOG.md)
- [Contributing](CONTRIBUTING.md)
