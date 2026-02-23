from __future__ import annotations


class CCloudApiError(Exception):
    """Raised when CCloud API returns an error response."""

    def __init__(self, status_code: int, message: str) -> None:
        self.status_code = status_code
        self.message = message
        super().__init__(f"CCloud API error {status_code}: {message}")


class CCloudConnectionError(Exception):
    """Raised when connection to CCloud fails (DNS, TCP, etc.)."""

    pass
