class ConnectorError(Exception):
    """Base exception for connector failures."""


class InvalidCredentialsError(ConnectorError):
    """Raised when provider credentials are missing, invalid, or expired."""


class TemporaryError(ConnectorError):
    """Raised when sync can be retried later."""


class RateLimitError(TemporaryError):
    """Raised when provider throttles requests."""


class CaptchaRequiredError(ConnectorError):
    """Raised when user interaction is required to continue."""
