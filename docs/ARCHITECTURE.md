# Architecture

This document describes a Redis-first architecture for EscrowMint v1.

## Principle

Do not solve this with distributed locks between application nodes.

Instead:

- keep one authoritative quota state per resource
- execute each mutation atomically inside Redis
- use reservations for temporary holds
- use idempotency records for retry safety

## Redis Data Model

For a resource named `wallet:123`, use a stable hash tag so related keys stay in the same Redis Cluster slot.

Example prefix:

- `escrowmint:{wallet:123}:state`
- `escrowmint:{wallet:123}:reservations`
- `escrowmint:{wallet:123}:idem:{idempotency_key}`

### State Hash

Key: `escrowmint:{resource}:state`

Fields:

- `available`
- `reserved`
- `version`

### Reservation Hash

Key: `escrowmint:{resource}:reservations`

Each field maps `reservation_id -> packed reservation payload`

Payload should include:

- amount
- expires_at_ms
- status

### Idempotency Key

Key: `escrowmint:{resource}:idem:{idempotency_key}`

Value is a packed result payload with a Redis TTL.

Using one key per idempotency token keeps expiry simple and portable.

## Operation Flow

### Consume

1. validate amount
2. check idempotency record if provided
3. read `available`
4. reject if insufficient
5. decrement `available`
6. increment `version`
7. store idempotent result if needed
8. return remaining balance

### Reserve

1. validate amount and TTL
2. reclaim expired reservations for the target resource if needed
3. reject if `available < amount`
4. decrement `available`
5. increment `reserved`
6. create reservation payload
7. increment `version`

### Commit

1. find reservation
2. reject if missing or expired
3. ensure it is not already committed
4. decrement `reserved`
5. mark reservation as committed
6. increment `version`
7. store idempotent result if provided

### Cancel

1. find reservation
2. if missing, return false
3. if already canceled, return true
4. if committed, reject or return false based on final API choice
5. decrement `reserved`
6. increment `available`
7. mark reservation as canceled
8. increment `version`

## Expiry Strategy

V1 should use lazy expiration first.

That means expired reservations are reclaimed during normal operations on the same resource, instead of requiring a global background sweeper.

This keeps v1 simple while preserving correctness.

## Why Lua or Redis Functions

Redis scripts execute atomically, which makes them the natural home for:

- check then mutate
- state plus idempotency update
- state plus reservation lifecycle update

This avoids application-level races and extra lock coordination.

## Important Limits

### Single Resource Hot Spot

Atomic Redis scripting solves correctness, but it does not remove per-resource serialization. A single extremely hot resource will still serialize.

For very hot resources, a future v2 can add escrow or chunk allocation.

### Cross-Resource Transactions

V1 should not promise exact atomicity across multiple unrelated resources.

## Observability

Every client should emit:

- operation type
- outcome
- latency
- Redis round trips
- script cache miss count
- insufficient quota count
- expired reservation reclaim count

## Compatibility Notes

- all keys for one resource must share the same hash tag in Redis Cluster
- integer-only arithmetic avoids rounding bugs
- Redis server time is preferable for expiry checks to avoid client clock skew
