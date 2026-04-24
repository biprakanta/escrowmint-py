from dataclasses import dataclass


@dataclass(frozen=True)
class ConsumeResult:
    applied: bool
    remaining: int
    operation_id: str


@dataclass(frozen=True)
class ResourceState:
    resource: str
    available: int
    reserved: int
    version: int


@dataclass(frozen=True)
class Reservation:
    reservation_id: str
    resource: str
    amount: int
    expires_at_ms: int
    status: str


@dataclass(frozen=True)
class ChunkLease:
    lease_id: str
    resource: str
    owner_id: str
    granted: int
    remaining: int
    expires_at_ms: int
    status: str


@dataclass(frozen=True)
class ChunkConsumeResult:
    applied: bool
    lease_id: str
    remaining: int
    expires_at_ms: int
