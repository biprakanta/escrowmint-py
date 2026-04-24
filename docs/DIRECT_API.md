# Direct Path API

This document defines the direct-path public API for EscrowMint Python.

## Design Rules

- every mutating operation must be atomic
- every operation should have deterministic failure semantics
- idempotent retry should be first-class
- API names should reflect quota semantics, not Redis internals

## Domain Types

### Resource

A logical shared quota bucket, such as:

- `wallet:123`
- `campaign:456`
- `inventory:item-42`

### Amount

A positive integer unit to consume or reserve.

The direct path intentionally uses integer arithmetic only.

### Reservation

A temporary hold against available quota that expires automatically unless committed or canceled.

### Idempotency Key

A caller-provided stable key for retry-safe mutation requests.

## Python API

```python
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class ConsumeResult:
    applied: bool
    remaining: int
    operation_id: str


@dataclass(frozen=True)
class Reservation:
    reservation_id: str
    resource: str
    amount: int
    expires_at_ms: int
    status: str


@dataclass(frozen=True)
class ResourceState:
    resource: str
    available: int
    reserved: int
    version: int


class Client:
    def try_consume(
        self,
        resource: str,
        amount: int,
        *,
        idempotency_key: Optional[str] = None,
    ) -> ConsumeResult: ...

    def reserve(
        self,
        resource: str,
        amount: int,
        *,
        ttl_ms: int,
        reservation_id: Optional[str] = None,
    ) -> Reservation: ...

    def commit(
        self,
        resource: str,
        reservation_id: str,
    ) -> ConsumeResult: ...

    def cancel(self, resource: str, reservation_id: str) -> bool: ...

    def get_state(self, resource: str) -> ResourceState: ...
```

## Error Model

The direct path should use typed errors instead of string matching.

Common errors:

- `InsufficientQuota`
- `ReservationNotFound`
- `ReservationExpired`
- `ReservationAlreadyCommitted`
- `DuplicateIdempotencyConflict`
- `CorruptState`
- `InvalidAmount`
- `InvalidTTL`
- `BackendUnavailable`

## Semantics

### `try_consume`

- succeeds only if `available >= amount`
- permanently burns quota
- returns `applied=False` on insufficient quota
- if an idempotency key is reused with the same request, returns the original result
- if an idempotency key is reused with a conflicting request shape, returns `DuplicateIdempotencyConflict`

### `reserve`

- succeeds only if `available >= amount`
- moves units from available to reserved
- creates a reservation with expiry
- expired reservations are reclaimed lazily on the next mutation or `get_state`
- active reservations are indexed by expiry time so reclaim does not require a full reservation scan
- same reservation ID must be safe to retry

### `commit`

- turns a valid live reservation into permanent consumption
- must be idempotent
- expired reservations must not commit
- terminal results are preserved in short-lived receipt keys for retry safety

### `cancel`

- releases reserved units back to available
- must be safe to retry

### `get_state`

- returns current logical view for one resource
- performs bounded lazy expiry reclaim for that resource before returning
- the direct path does not promise a globally linearizable read across multiple resources

## Recommended Defaults

- idempotency TTL: 24 hours
- reservation TTL: caller-defined, with a sane minimum and maximum
- all resource operations isolated by resource key

## What The Direct Path Should Avoid

- floating-point amounts
- multi-resource atomic transactions
- implicit background workers as a hard requirement
- lock-based APIs as the main surface
