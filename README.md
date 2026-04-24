# EscrowMint Python

EscrowMint Python is the Python client library for exact, Redis-backed bounded consumption.

It is designed for cases where many threads, processes, or services need to consume from a shared global quota without allowing the value to go below zero.

Examples:

- prepaid credit consumption
- inventory reservation
- budget caps
- worker permit pools
- campaign spend controls

## Why This Exists

There are many good Redis libraries for locks, semaphores, and rate limiting. There are far fewer Python libraries that focus on this narrower contract:

- consume from a shared quota
- never overspend
- support reservations with expiry
- stay fast under distributed contention
- expose clean application-level semantics

That is the gap EscrowMint Python is intended to fill.

## Core Model

EscrowMint Python is not a generic counter library. It is a quota and reservation library.

The minimal primitives are:

- `try_consume(resource, amount, idempotency_key=None)`
- `reserve(resource, amount, ttl_ms, reservation_id=None)`
- `commit(resource, reservation_id, idempotency_key=None)`
- `cancel(resource, reservation_id)`
- `get_state(resource)`

These operations are executed atomically in Redis via Lua scripts or Redis Functions.

## Planned Package

The intended package name is `escrowmint`.

The Python client should expose:

- a small synchronous client first
- Redis-backed atomic operations via Lua scripts
- typed exceptions
- integration tests against a real Redis instance

## Proposed Repo Layout

```text
escrowmint-py/
  README.md
  pyproject.toml
  docs/
    ARCHITECTURE.md
    V1_API.md
  scripts/
    try_consume.lua
    reserve.lua
    commit.lua
    cancel.lua
  src/
    escrowmint/
  tests/
```

## V1 Priorities

1. Exact bounded decrement with idempotency
2. Reservation lifecycle with TTL
3. Clean errors and observability hooks
4. Redis Cluster-safe keying strategy
5. Python packaging and test automation

## Future Extensions

- async Python client
- background scavenging helpers
- metrics exporters
- Redis Functions as the default backend

See [docs/V1_API.md](/Users/biprakantapal/Desktop/codex-plugins/escrowmint-py/docs/V1_API.md) and [docs/ARCHITECTURE.md](/Users/biprakantapal/Desktop/codex-plugins/escrowmint-py/docs/ARCHITECTURE.md) for the initial design.
