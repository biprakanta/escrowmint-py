class EscrowMintError(Exception):
    """Base exception for EscrowMint."""


class InsufficientQuota(EscrowMintError):
    """Raised when a consume or reserve operation cannot be applied."""


class InvalidAmount(EscrowMintError):
    """Raised when an operation amount is not a positive integer."""


class DuplicateIdempotencyConflict(EscrowMintError):
    """Raised when an idempotency key is reused for a different request."""


class BackendUnavailable(EscrowMintError):
    """Raised when Redis cannot service a request."""
