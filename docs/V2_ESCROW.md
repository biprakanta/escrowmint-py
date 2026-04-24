# V2 Escrow / Chunk Allocation

This document describes the shipped v2 chunk-lease model for very hot resources.

The current client now supports the authoritative lease lifecycle in Redis:

- `allocate_chunk`
- `consume_chunk`
- `renew_chunk`
- `release_chunk`
- `get_chunk`

## Why V2 Exists

The v1 model gives exact correctness for a single resource, but each resource is still serialized at Redis.

That is usually fine until one resource becomes extremely hot. At that point, the bottleneck is not correctness. The bottleneck is that every request still reaches the same authoritative resource state.

V2 addresses that by allocating chunks of quota to workers.

## Core Idea

Instead of asking Redis to approve every consume:

1. Redis remains the source of truth for total quota.
2. A worker reserves a chunk such as `100` units from the resource.
3. The worker consumes against that chunk instead of touching the resource-global availability on every operation.
4. When the local chunk runs low, the worker refills from Redis.
5. If the worker crashes, the unspent part of the chunk returns after lease expiry.

This is an escrow model.

## Expected Benefits

- higher throughput for very hot keys
- lower Redis round trips on the hot path
- preserved no-overspend guarantee
- bounded loss on worker crash, limited to leased chunk size until expiry

## Expected Tradeoffs

- more complex state machine
- temporary stranded quota while a worker lease is still active
- weaker per-request visibility into the exact remaining global `available` count
- more tuning around chunk size and refill thresholds

## Current Concepts

### Chunk Lease

A temporary worker-owned allocation from one resource.

Fields should include:

- `lease_id`
- `resource`
- `owner_id`
- `granted`
- `remaining`
- `expires_at_ms`
- `status`

### Worker Ownership

Each process or node should use a stable `owner_id`.

That allows:

- lease reuse on retry
- lease heartbeats
- lease reclaim on timeout

## Current V2 API

Python:

```python
lease = client.allocate_chunk(
    resource="wallet:123",
    amount=100,
    owner_id="worker-a",
    ttl_ms=30_000,
)

result = client.consume_chunk(
    resource="wallet:123",
    lease_id=lease.lease_id,
    amount=1,
    owner_id="worker-a",
)

client.renew_chunk(
    resource="wallet:123",
    lease_id=lease.lease_id,
    owner_id="worker-a",
    ttl_ms=30_000,
)

client.release_chunk(
    resource="wallet:123",
    lease_id=lease.lease_id,
    owner_id="worker-a",
)
```

Go:

```go
lease, err := client.AllocateChunk(ctx, "wallet:123", 100, escrowmint.AllocateChunkOptions{
    OwnerID: "worker-a",
    TTLMS:   30000,
})
```

## What This Implementation Does

- allocates worker-owned chunk leases from global `available`
- tracks per-lease `remaining` in Redis
- reclaims expired remaining quota back into `available`
- supports explicit renew and release
- keeps worker ownership checks on mutate paths

## What It Does Not Do Yet

- an in-process no-Redis fast path helper
- local crash-safe checkpointing of worker-side buffered chunk usage
- background chunk sweeper outside normal resource touches

## Redis Model

Resource-local keys:

- `escrowmint:{resource}:state`
- `escrowmint:{resource}:reservations`
- `escrowmint:{resource}:reservation_expiries`
- `escrowmint:{resource}:chunk_leases`
- `escrowmint:{resource}:chunk_lease_expiries`
- `escrowmint:{resource}:chunk_receipt:{lease_id}`

## Invariants

V2 must preserve:

- `available >= 0`
- `reserved >= 0`
- `sum(active lease remaining) <= sum(active lease granted)`
- `available + reserved + sum(active lease remaining) <= original tracked supply`

## Suggested Flow

### Allocate Chunk

1. reclaim expired reservations
2. reclaim expired leases
3. reject if available is insufficient
4. decrement available by chunk size
5. create lease payload
6. return lease

### Chunk Consume

1. validate lease ownership and expiry
2. decrement lease `remaining`
3. do not touch the resource-global `available` on each consume

### Lease Expiry

1. reclaim remaining lease amount
2. increment resource-global available
3. mark lease expired

## Rollout Guidance

- keep v1 APIs as the default path for ordinary resources
- use v2 only for explicitly hot resources
- choose a stable per-worker `owner_id`
- use renew or release to keep leases tidy when workers are healthy
- rely on expiry reclaim as the safety net for abandoned active leases
