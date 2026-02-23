from __future__ import annotations

from dataclasses import dataclass

from pydantic import SecretStr


@dataclass
class CCloudConnection:
    """HTTP client for Confluent Cloud API."""

    api_key: str
    api_secret: SecretStr
    base_url: str = "https://api.confluent.cloud"
    timeout_seconds: int = 30
    max_retries: int = 5
    base_backoff_seconds: float = 2.0
