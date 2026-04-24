class EscrowMintError(Exception):
    """Base exception for EscrowMint."""


class InsufficientQuota(EscrowMintError):
    """Raised when a consume or reserve operation cannot be applied."""
