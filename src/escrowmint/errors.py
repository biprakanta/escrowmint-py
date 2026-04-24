class EscrowMintError(Exception):
    """Base exception for EscrowMint."""


class InsufficientQuota(EscrowMintError):
    """Raised when a consume or reserve operation cannot be applied."""


class InvalidAmount(EscrowMintError):
    """Raised when an operation amount is not a positive integer."""


class InvalidTTL(EscrowMintError):
    """Raised when a reservation TTL is not a positive integer."""


class InvalidOwner(EscrowMintError):
    """Raised when an owner ID is missing or invalid."""


class DuplicateIdempotencyConflict(EscrowMintError):
    """Raised when an idempotency key is reused for a different request."""


class ReservationNotFound(EscrowMintError):
    """Raised when a reservation does not exist."""


class ReservationExpired(EscrowMintError):
    """Raised when a reservation has already expired."""


class ReservationAlreadyCommitted(EscrowMintError):
    """Raised when a reservation has already been committed."""


class LeaseNotFound(EscrowMintError):
    """Raised when a chunk lease does not exist."""


class LeaseExpired(EscrowMintError):
    """Raised when a chunk lease has expired."""


class LeaseAlreadyReleased(EscrowMintError):
    """Raised when a chunk lease has already been released."""


class LeaseOwnershipMismatch(EscrowMintError):
    """Raised when a chunk lease is used by a different owner."""


class BackendUnavailable(EscrowMintError):
    """Raised when Redis cannot service a request."""


class CorruptState(EscrowMintError):
    """Raised when Redis contains malformed EscrowMint state."""
