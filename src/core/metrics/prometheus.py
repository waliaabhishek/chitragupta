from __future__ import annotations

import json
import logging
import random
import threading
import time
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Literal
from urllib.parse import urljoin

import httpx

from core.metrics.protocol import MetricsQueryError
from core.models.metrics import MetricQuery, MetricRow  # noqa: TC001 — used at runtime in _parse_response

LOGGER = logging.getLogger(__name__)

_TRANSIENT_STATUS = frozenset({408, 425, 429, 500, 502, 503, 504})


def _iso_utc(dt: datetime) -> str:
    """Convert datetime to ISO UTC string. Raises ValueError on naive datetimes."""
    if dt.tzinfo is None:
        raise ValueError(f"Naive datetime not allowed: {dt!r}")
    return dt.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S+00:00")


@dataclass
class AuthConfig:
    """Authentication configuration for Prometheus."""

    type: Literal["basic", "digest", "bearer"]
    username: str | None = None
    password: str | None = None
    token: str | None = None


@dataclass
class PrometheusConfig:
    """Configuration for PrometheusMetricsSource."""

    url: str
    auth: AuthConfig | None = None
    timeout: float = 30.0
    max_workers: int = 10
    cache_maxsize: int = 512
    cache_ttl_seconds: float = 300.0
    step_seconds: int = 3600
    """Fallback step (seconds) if caller-provided step resolves to ≤0.
    Protocol default timedelta(hours=1) takes precedence in normal operation.
    Rarely needed — exists for edge case protection only."""
    max_retries: int = 4
    base_delay: float = 1.0
    extra_headers: dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.max_workers < 1:
            raise ValueError(f"max_workers must be >= 1, got {self.max_workers}")


class PrometheusMetricsSource:
    """Ecosystem-agnostic Prometheus client implementing MetricsSource."""

    def __init__(
        self,
        config: PrometheusConfig,
        client: httpx.Client | None = None,
    ) -> None:
        self._config = config
        self._url_range = urljoin(config.url, "/api/v1/query_range")
        self._extra_headers = dict(config.extra_headers)
        self._auth = self._build_auth()
        if self._config.auth and self._config.auth.type == "bearer":
            self._extra_headers["Authorization"] = f"Bearer {self._config.auth.token or ''}"
        self._cache: dict[tuple[str, ...], tuple[float, str]] = {}
        self._cache_lock = threading.Lock()
        # Connection pooling via httpx.Client (injectable for testing)
        if client is not None:
            self._client = client
            self._owns_client = False
        else:
            self._client = httpx.Client(
                auth=self._auth,
                timeout=httpx.Timeout(self._config.timeout),
            )
            self._owns_client = True

    def close(self) -> None:
        """Close HTTP client and release pooled connections."""
        if self._owns_client:
            self._client.close()

    def query(
        self,
        queries: Sequence[MetricQuery],
        start: datetime,
        end: datetime,
        step: timedelta = timedelta(hours=1),
        resource_id_filter: str | None = None,
    ) -> dict[str, list[MetricRow]]:
        """Execute multiple metric queries in parallel, return results keyed by query key."""
        if not queries:
            return {}

        # Prepare expressions with resource filter injected
        prepared: list[tuple[MetricQuery, str]] = []
        for mq in queries:
            expr = _inject_resource_filter(mq.query_expression, mq.resource_label, resource_id_filter)
            prepared.append((mq, expr))

        workers = min(self._config.max_workers, len(prepared))
        results: dict[str, list[MetricRow]] = {}
        errors: list[MetricsQueryError] = []

        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(
                    self._execute_query,
                    expr,
                    start,
                    end,
                    step,
                ): mq
                for mq, expr in prepared
            }

            for future in futures:
                mq = futures[future]
                try:
                    response_text = future.result()
                    rows = self._parse_response(response_text, mq)
                    results[mq.key] = rows
                except MetricsQueryError as exc:
                    errors.append(exc)

        if errors:
            for extra in errors[1:]:
                LOGGER.error("Additional query error: %s", extra)
            raise errors[0]

        return results

    def _execute_query(
        self,
        query_expression: str,
        start: datetime,
        end: datetime,
        step: timedelta,
    ) -> str:
        """Execute a single query with caching. Returns raw response text."""
        start_str = _iso_utc(start)
        end_str = _iso_utc(end)

        step_seconds = int(step.total_seconds())
        if step_seconds <= 0:
            LOGGER.warning("Step %s is non-positive; defaulting to %ss", step, self._config.step_seconds)
            step_seconds = self._config.step_seconds
        step_str = str(step_seconds)

        cache_key = (self._url_range, query_expression, start_str, end_str, step_str)

        # Check cache (thread-safe)
        with self._cache_lock:
            cached = self._cache.get(cache_key)
            if cached is not None:
                expiry, text = cached
                if time.monotonic() < expiry:
                    return text

        data = {
            "query": query_expression,
            "start": start_str,
            "end": end_str,
            "step": step_str,
        }
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        headers.update(self._extra_headers)

        response_text = self._post_with_retry(self._url_range, data, headers)

        # Cache the result (thread-safe)
        with self._cache_lock:
            # Evict expired entries if cache is full
            if len(self._cache) >= self._config.cache_maxsize:
                now = time.monotonic()
                expired_keys = [k for k, (exp, _) in self._cache.items() if now >= exp]
                for k in expired_keys:
                    del self._cache[k]

            # If still at capacity after eviction, skip caching
            if len(self._cache) >= self._config.cache_maxsize:
                LOGGER.debug("Cache full with non-expired entries; skipping cache for query")
                return response_text

            expiry = time.monotonic() + self._config.cache_ttl_seconds
            self._cache[cache_key] = (expiry, response_text)

        return response_text

    def _post_with_retry(
        self,
        url: str,
        data: dict[str, str],
        headers: dict[str, str],
    ) -> str:
        """POST with exponential backoff + jitter on transient failures."""
        attempt = 0
        last_exc: httpx.RequestError | None = None
        while True:
            try:
                resp = self._client.post(
                    url,
                    data=data,
                    headers=headers,
                )
                if resp.status_code not in _TRANSIENT_STATUS:
                    if resp.status_code >= 400:
                        raise MetricsQueryError(
                            message=f"HTTP {resp.status_code}: {resp.text[:200]}",
                            query=data.get("query"),
                            status_code=resp.status_code,
                        )
                    return resp.text

                LOGGER.warning(
                    "Prometheus returned %s, attempt %s/%s",
                    resp.status_code,
                    attempt + 1,
                    self._config.max_retries,
                )
            except httpx.RequestError as exc:
                last_exc = exc
                LOGGER.warning(
                    "Request error: %s, attempt %s/%s",
                    exc,
                    attempt + 1,
                    self._config.max_retries,
                )

            attempt += 1
            if attempt >= self._config.max_retries:
                raise MetricsQueryError(
                    message=f"Exhausted {self._config.max_retries} retries for {url}",
                    query=data.get("query"),
                ) from last_exc

            sleep_for = self._config.base_delay * (2**attempt)
            sleep_for *= 0.8 + random.random() * 0.4  # ±20% jitter
            time.sleep(sleep_for)

    def _parse_response(self, response_text: str, query: MetricQuery) -> list[MetricRow]:
        """Parse Prometheus JSON response into MetricRow list."""
        payload = json.loads(response_text)

        if payload.get("status") == "error":
            error_msg = payload.get("error", "Unknown Prometheus error")
            error_type = payload.get("errorType", "")
            raise MetricsQueryError(
                message=f"{error_type}: {error_msg}",
                query=query.query_expression,
            )

        result = payload.get("data", {}).get("result", [])
        rows: list[MetricRow] = []

        for series in result:
            metric = series.get("metric", {})
            labels = {k: v for k, v in metric.items() if k in query.label_keys}

            values = series.get("values", [])
            if not values and "value" in series:
                values = [series["value"]]

            for ts_raw, val_raw in values:
                try:
                    ts = datetime.fromtimestamp(float(ts_raw), tz=UTC)
                    val = float(val_raw)
                except (ValueError, TypeError) as exc:
                    LOGGER.warning(
                        "Skipping malformed datapoint for %s: %s",
                        query.key,
                        exc,
                    )
                    continue

                rows.append(
                    MetricRow(
                        timestamp=ts,
                        metric_key=query.key,
                        value=val,
                        labels=labels,
                    )
                )

        return rows

    def _build_auth(self) -> httpx.BasicAuth | httpx.DigestAuth | None:
        """Build httpx auth object from config."""
        auth_cfg = self._config.auth
        if auth_cfg is None:
            return None

        if auth_cfg.type == "basic":
            return httpx.BasicAuth(auth_cfg.username or "", auth_cfg.password or "")
        if auth_cfg.type == "digest":
            return httpx.DigestAuth(auth_cfg.username or "", auth_cfg.password or "")
        if auth_cfg.type == "bearer":
            # Bearer is handled via extra headers in __init__, not httpx auth
            return None

        return None  # pragma: no cover


def _inject_resource_filter(
    expression: str,
    resource_label: str,
    resource_id_filter: str | None,
) -> str:
    """Inject resource_id_filter into a PromQL expression.

    - No filter: strip empty ``{}`` from expression
    - Filter with ``{}`` placeholder: replace with ``{resource_label="value"}``
    - Filter without ``{}``: log warning, return unchanged
    """
    if resource_id_filter is None:
        return expression.replace("{}", "", 1)

    if "{}" in expression:
        return expression.replace("{}", "{" + f'{resource_label}="{resource_id_filter}"' + "}", 1)

    LOGGER.warning(
        "Expression %r has no {} placeholder for resource filter %r",
        expression,
        resource_id_filter,
    )
    return expression
