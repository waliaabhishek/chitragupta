from __future__ import annotations

import logging
import random
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Any, cast
from urllib import parse

import requests
from pydantic import SecretStr  # noqa: TC002 - runtime use in get_secret_value()
from requests.auth import HTTPBasicAuth

from plugins.confluent_cloud.exceptions import CCloudApiError, CCloudConnectionError

LOGGER = logging.getLogger(__name__)
DEFAULT_PAGE_SIZE = 500


@dataclass
class CCloudConnection:
    """HTTP client for Confluent Cloud API with connection pooling and throttling."""

    api_key: str
    api_secret: SecretStr
    base_url: str = "https://api.confluent.cloud"
    timeout_seconds: int = 30
    max_retries: int = 5
    base_backoff_seconds: float = 2.0
    request_interval_seconds: float = 0.1  # Proactive throttling: 100ms = 10 req/s max

    _session: requests.Session = field(init=False, repr=False, compare=False)
    _last_request_time: float = field(init=False, default=0.0, repr=False, compare=False)

    def __post_init__(self) -> None:
        self._session = requests.Session()
        self._session.auth = HTTPBasicAuth(self.api_key, self.api_secret.get_secret_value())

    def close(self) -> None:
        """Close the underlying HTTP session and release connections."""
        self._session.close()

    def get(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> Iterator[dict[str, Any]]:
        """GET with pagination. Yields each item from 'data' array."""
        url = f"{self.base_url.rstrip('/')}{path}"
        request_params: dict[str, Any] = {"page_size": DEFAULT_PAGE_SIZE, **(params or {})}

        while True:
            response = self._request("GET", url, params=request_params, **kwargs)
            data = response.get("data")

            if data:
                yield from data

            # Check for next page
            metadata = response.get("metadata", {})
            next_url = metadata.get("next")
            if not next_url:
                break

            # Parse next page token
            query = parse.parse_qs(parse.urlsplit(next_url).query)
            page_token = query.get("page_token", [""])[0]
            if not page_token:
                break

            request_params["page_token"] = page_token

    def get_raw(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """GET returning full JSON response without pagination.

        Use for endpoints that don't follow the standard
        {"data": [...], "metadata": {...}} envelope (e.g., connector list API).

        Returns {} on 404 (unlike get() which returns the standard empty envelope).
        This allows callers to safely iterate response.values() without special handling.
        """
        url = f"{self.base_url.rstrip('/')}{path}"
        result = self._request("GET", url, params=params or {}, **kwargs)
        # _request() returns {"data": [], "metadata": {}} on 404, which doesn't
        # make sense for non-standard endpoints. Return empty dict instead.
        # Use semantic check (empty data array with metadata present) to handle
        # variations in the exact envelope structure.
        if result.get("data") == [] and "metadata" in result:
            return {}
        return result

    def post(
        self,
        path: str,
        json: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """POST request. Returns JSON response."""
        url = f"{self.base_url.rstrip('/')}{path}"
        return self._request("POST", url, json=json, **kwargs)

    def _request(
        self,
        method: str,
        url: str,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Execute HTTP request with retry logic, rate limits, and proactive throttling."""
        kwargs.setdefault("timeout", self.timeout_seconds)

        # Proactive throttling: ensure minimum interval between requests
        if self.request_interval_seconds > 0:
            elapsed = time.time() - self._last_request_time
            if elapsed < self.request_interval_seconds:
                time.sleep(self.request_interval_seconds - elapsed)

        last_exception: Exception | None = None

        for attempt in range(self.max_retries + 1):
            try:
                resp = self._session.request(method, url, **kwargs)
            except requests.exceptions.Timeout as e:
                last_exception = CCloudApiError(408, f"Request timeout: {e}")
                wait = self._calculate_backoff(attempt)
                LOGGER.warning("Timeout on attempt %d, retrying in %.2fs", attempt + 1, wait)
                time.sleep(wait)
                continue
            except requests.exceptions.RequestException as e:
                raise CCloudConnectionError(str(e)) from e

            if resp.status_code == 200:
                self._last_request_time = time.time()
                return cast("dict[str, Any]", resp.json())
            elif resp.status_code == 404:
                LOGGER.info("Resource not found: %s", url)
                return {"data": [], "metadata": {}}
            elif resp.status_code == 429:
                last_exception = CCloudApiError(429, resp.text)
                wait = self._get_rate_limit_wait(resp, attempt)
                LOGGER.warning("Rate limited on attempt %d, retrying in %.2fs", attempt + 1, wait)
                time.sleep(wait)
                continue
            else:
                raise CCloudApiError(resp.status_code, resp.text)

        # Max retries exhausted — last_exception is always set since we only
        # reach here after timeout (sets last_exception) or 429 (sets last_exception)
        assert last_exception is not None  # unreachable: loop always sets last_exception
        raise last_exception

    def _calculate_backoff(self, attempt: int) -> float:
        """Calculate exponential backoff with jitter."""
        base: float = self.base_backoff_seconds * (2**attempt)
        jitter: float = random.uniform(0, 1)
        return base + jitter

    def _get_rate_limit_wait(self, response: requests.Response, attempt: int) -> float:
        """Get wait time from rate limit headers or fall back to backoff.

        Confluent Cloud API uses these headers (per docs):
        - Retry-After: seconds to wait (standard HTTP)
        - rateLimit-reset: relative seconds until window resets (NOT Unix timestamp)

        See: https://api.telemetry.confluent.cloud/docs
        """
        # Standard HTTP Retry-After header (seconds)
        if "Retry-After" in response.headers:
            wait = float(response.headers["Retry-After"])
        # Confluent-specific header: relative seconds until reset (lowercase)
        elif "rateLimit-reset" in response.headers:
            wait = float(response.headers["rateLimit-reset"])
        # Legacy/alternative header name (some Confluent APIs may use this)
        elif "RateLimit-Reset" in response.headers:
            wait = float(response.headers["RateLimit-Reset"])
        else:
            wait = self._calculate_backoff(attempt)

        # Add jitter (10-20%) to avoid thundering herd
        jitter_factor = 1.1 + 0.1 * random.random()
        return wait * jitter_factor
