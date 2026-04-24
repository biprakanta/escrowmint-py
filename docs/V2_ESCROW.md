# V2 Escrow / Chunk Allocation

This document describes the intended v2 design for very hot resources.

## Why V2 Exists

The v1 model gives exact correctness for a single resource, but each resource is still serialized at Redis.

That is usually fine until one resource becomes extremely hot. At that point, the bottleneck is not correctness. The bottleneck is that every request still reaches the same authoritative resource state.

V2 addresses that by allocating chunks of quota to workers.

## Core Idea

Instead of asking Redis to approve every consume:

1. Redis remains the source of truth for total quota.
2. A worker reserves a chunk such as `100` units from the resource.
3. The worker serves many local consumes from that chunk without another Redis round trip.
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

## Proposed New Concepts

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

## Proposed V2 API

Python-style sketch:

```python
lease = client.allocate_chunk(
    resource="wallet:123",
    amount=100,
    owner_id="worker-a",
    ttl_ms=30_000,
)

result = client.try_consume_from_chunk(
    resource="wallet:123",
    lease_id=lease.lease_id,
    amount=1,
)

client.renew_chunk(
    resource="wallet:123",
    lease_id=lease.lease_id,
    ttl_ms=30_000,
)
```

Go-style sketch:

```go
lease, err := client.AllocateChunk(ctx, "wallet:123", 100, escrowmint.AllocateChunkOptions{
    OwnerID: "worker-a",
    TTLMS:   30000,
})
```

## Redis Model

Suggested resource-local keys:

- `escrowmint:{resource}:state`
- `escrowmint:{resource}:reservations`
- `escrowmint:{resource}:leases`

Each active chunk lease should live in a lease hash keyed by `lease_id`.

## Invariants

V2 must preserve:

- `available >= 0`
- `reserved >= 0`
- `sum(active lease remaining) <= leased total`
- `available + reserved + leased_out <= original tracked supply`, depending on the exact accounting split

## Suggested Flow

### Allocate Chunk

1. reclaim expired reservations
2. reclaim expired leases
3. reject if available is insufficient
4. decrement available by chunk size
5. create lease payload
6. return lease

### Local Consume

1. validate lease ownership and expiry
2. decrement lease-local remaining
3. do not touch the resource-global available on each consume

### Lease Expiry

1. reclaim remaining lease amount
2. increment resource-global available
3. mark lease expired

## Suggested Rollout

Start with:

1. keep v1 APIs as the default path
2. add chunk allocation behind explicit APIs
3. document that v2 is only for very hot resources
4. keep ordinary resources on v1 for simplicity
