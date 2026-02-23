from __future__ import annotations

import logging
import random
import time
from dataclasses import dataclass, field
from typing import Any, Iterator
from urllib import parse

import requests
from pydantic import SecretStr
from requests.auth import HTTPBasicAuth

from plugins.confluent_cloud.exceptions import CCloudApiError, CCloudConnectionError

LOGGER = logging.getLogger(__name__)
DEFAULT_PAGE_SIZE = 500


@dataclass
class CCloudConnection:
    """HTTP client for Confluent Cloud API."""

    api_key: str
    api_secret: SecretStr
    base_url: str = "https://api.confluent.cloud"
    timeout_seconds: int = 30
    max_retries: int = 5
    base_backoff_seconds: float = 2.0

    _auth: HTTPBasicAuth = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        self._auth = HTTPBasicAuth(self.api_key, self.api_secret.get_secret_value())

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
            page_token_list = query.get("page_token", [])
            if not page_token_list or not page_token_list[0]:
                break

            request_params["page_token"] = page_token_list[0]

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
        """Execute HTTP request with retry logic for rate limits and timeouts."""
        kwargs.setdefault("auth", self._auth)
        kwargs.setdefault("timeout", self.timeout_seconds)

        last_exception: Exception | None = None

        for attempt in range(self.max_retries + 1):
            try:
                resp = requests.request(method, url, **kwargs)
            except requests.exceptions.Timeout as e:
                last_exception = CCloudApiError(408, f"Request timeout: {e}")
                wait = self._calculate_backoff(attempt)
                LOGGER.warning(f"Timeout on attempt {attempt + 1}, retrying in {wait:.2f}s")
                time.sleep(wait)
                continue
            except requests.exceptions.RequestException as e:
                raise CCloudConnectionError(str(e)) from e

            if resp.status_code == 200:
                return resp.json()
            elif resp.status_code == 404:
                LOGGER.info(f"Resource not found: {url}")
                return {"data": [], "metadata": {}}
            elif resp.status_code == 429:
                last_exception = CCloudApiError(429, resp.text)
                wait = self._get_rate_limit_wait(resp, attempt)
                LOGGER.warning(f"Rate limited on attempt {attempt + 1}, retrying in {wait:.2f}s")
                time.sleep(wait)
                continue
            else:
                raise CCloudApiError(resp.status_code, resp.text)

        # Max retries exhausted
        if last_exception:
            raise last_exception
        raise CCloudApiError(0, "Max retries exhausted")

    def _calculate_backoff(self, attempt: int) -> float:
        """Calculate exponential backoff with jitter."""
        base = self.base_backoff_seconds * (2**attempt)
        jitter = random.uniform(0, 1)
        return base + jitter

    def _get_rate_limit_wait(self, response: requests.Response, attempt: int) -> float:
        """Get wait time from rate limit headers or fall back to backoff."""
        if "Retry-After" in response.headers:
            wait = float(response.headers["Retry-After"])
        elif "X-RateLimit-Reset" in response.headers:
            reset_time = float(response.headers["X-RateLimit-Reset"])
            wait = max(0.01, reset_time - time.time())
        else:
            wait = self._calculate_backoff(attempt)

        # Add jitter (10-20%)
        jitter_factor = 1.1 + 0.1 * random.random()
        return wait * jitter_factor
