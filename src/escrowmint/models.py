from dataclasses import dataclass


@dataclass(frozen=True)
class ConsumeResult:
    applied: bool
    remaining: int
    operation_id: str
