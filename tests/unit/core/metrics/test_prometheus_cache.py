from __future__ import annotations

import json
import time
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import MagicMock

import httpx

from core.metrics.prometheus import PrometheusConfig, PrometheusMetricsSource
from core.models.metrics import MetricQuery

# ---------------------------------------------------------------------------
# Fixtures & constants
# ---------------------------------------------------------------------------

_START = datetime(2026, 1, 1, tzinfo=UTC)
_END = datetime(2026, 1, 2, tzinfo=UTC)
_STEP = timedelta(hours=1)

_QUERY = MetricQuery(
    key="bytes_in",
    query_expression="kafka_server_brokertopicmetrics_bytesinpersec_count{}",
    label_keys=("topic", "cluster_id"),
    resource_label="kafka_id",
)

_RANGE_RESPONSE = json.dumps(
    {
        "status": "success",
        "data": {
            "resultType": "matrix",
            "result": [
                {
                    "metric": {"topic": "my-topic", "cluster_id": "lkc-123"},
                    "values": [[1735689600.0, "100.5"]],
                }
            ],
        },
    }
)


def _make_config(**overrides: Any) -> PrometheusConfig:
    defaults: dict[str, Any] = {"url": "http://prom:9090/", "max_retries": 1, "base_delay": 0.0}
    defaults.update(overrides)
    return PrometheusConfig(**defaults)


def _mock_response(text: str, status_code: int = 200) -> MagicMock:
    resp = MagicMock()
    resp.text = text
    resp.status_code = status_code
    return resp


def _make_source_with_mock(mock_post: MagicMock, **config_overrides: Any) -> tuple[PrometheusMetricsSource, MagicMock]:
    """Create a PrometheusMetricsSource with a mocked httpx.Client."""
    client = MagicMock(spec=httpx.Client)
    client.post = mock_post
    src = PrometheusMetricsSource(_make_config(**config_overrides), client=client)
    return src, client


def _make_unique_query(key: str) -> MetricQuery:
    """Create a MetricQuery with a unique key (produces unique cache keys)."""
    return MetricQuery(
        key=key,
        query_expression=f"metric_{key}{{}}",
        label_keys=("topic",),
        resource_label="kafka_id",
    )


# ===========================================================================
# Verification 1: TTL=None (lifetime caching)
# ===========================================================================


class TestTTLNoneLifetime:
    """cache_ttl_seconds=None → entries never expire, second call is cache hit."""

    def test_ttl_none_caches_forever(self) -> None:
        mock_post = MagicMock(return_value=_mock_response(_RANGE_RESPONSE, 200))
        src, _ = _make_source_with_mock(mock_post, cache_ttl_seconds=None)

        src.query([_QUERY], _START, _END, _STEP)
        src.query([_QUERY], _START, _END, _STEP)

        # Only one HTTP call — second was a cache hit
        assert mock_post.call_count == 1


# ===========================================================================
# Verification 2: TTL expiry
# ===========================================================================


class TestTTLExpiry:
    """cache_ttl_seconds=0.01 → entry expires quickly, second call is cache miss."""

    def test_ttl_expires_triggers_new_http_call(self) -> None:
        mock_post = MagicMock(return_value=_mock_response(_RANGE_RESPONSE, 200))
        src, _ = _make_source_with_mock(mock_post, cache_ttl_seconds=0.01)

        src.query([_QUERY], _START, _END, _STEP)
        time.sleep(0.02)  # Wait for TTL to expire
        src.query([_QUERY], _START, _END, _STEP)

        # Two HTTP calls — second was a cache miss after expiry
        assert mock_post.call_count == 2


# ===========================================================================
# Verification 3: TTL hit within window
# ===========================================================================


class TestTTLHitWithinWindow:
    """cache_ttl_seconds=60 → immediate second call is a cache hit."""

    def test_ttl_hit_within_window(self) -> None:
        mock_post = MagicMock(return_value=_mock_response(_RANGE_RESPONSE, 200))
        src, _ = _make_source_with_mock(mock_post, cache_ttl_seconds=60)

        src.query([_QUERY], _START, _END, _STEP)
        src.query([_QUERY], _START, _END, _STEP)

        # Only one HTTP call — second was a cache hit
        assert mock_post.call_count == 1


# ===========================================================================
# Verification 4: LRU eviction
# ===========================================================================


class TestLRUEviction:
    """Fill cache to maxsize, insert one more → oldest entry evicted."""

    def test_lru_eviction_removes_oldest(self) -> None:
        mock_post = MagicMock(return_value=_mock_response(_RANGE_RESPONSE, 200))
        src, _ = _make_source_with_mock(mock_post, cache_maxsize=2, cache_ttl_seconds=3600)

        q_a = _make_unique_query("a")
        q_b = _make_unique_query("b")
        q_c = _make_unique_query("c")

        # Fill cache: entries a, b
        src.query([q_a], _START, _END, _STEP)
        src.query([q_b], _START, _END, _STEP)
        assert mock_post.call_count == 2

        # Insert c → should evict oldest (a) via LRU
        src.query([q_c], _START, _END, _STEP)
        assert mock_post.call_count == 3

        # c should be cached (hit)
        src.query([q_c], _START, _END, _STEP)
        assert mock_post.call_count == 3  # no new call

        # b should still be cached (it wasn't the oldest)
        src.query([q_b], _START, _END, _STEP)
        assert mock_post.call_count == 3  # no new call

        # a was evicted → cache miss → new HTTP call
        src.query([q_a], _START, _END, _STEP)
        assert mock_post.call_count == 4


# ===========================================================================
# Verification 5: LRU promotion
# ===========================================================================


class TestLRUPromotion:
    """Access oldest entry before overflow → it gets promoted, different entry evicted."""

    def test_lru_promotion_on_access(self) -> None:
        mock_post = MagicMock(return_value=_mock_response(_RANGE_RESPONSE, 200))
        src, _ = _make_source_with_mock(mock_post, cache_maxsize=2, cache_ttl_seconds=3600)

        q_a = _make_unique_query("a")
        q_b = _make_unique_query("b")
        q_c = _make_unique_query("c")

        # Fill cache: entries a (oldest), b (newest)
        src.query([q_a], _START, _END, _STEP)
        src.query([q_b], _START, _END, _STEP)
        assert mock_post.call_count == 2

        # Access a → promotes it; now LRU order is b (oldest), a (newest)
        src.query([q_a], _START, _END, _STEP)
        assert mock_post.call_count == 2  # cache hit, no HTTP call

        # Insert c → should evict b (now the LRU entry), not a
        src.query([q_c], _START, _END, _STEP)
        assert mock_post.call_count == 3

        # a should still be cached (it was promoted)
        src.query([q_a], _START, _END, _STEP)
        assert mock_post.call_count == 3  # cache hit

        # b was evicted → cache miss
        src.query([q_b], _START, _END, _STEP)
        assert mock_post.call_count == 4


# ===========================================================================
# Verification 6: Cross-run benefit
# ===========================================================================


class TestCrossRunBenefit:
    """Same source instance across runs — historical queries are cache hits."""

    def test_cross_run_cache_hit(self) -> None:
        mock_post = MagicMock(return_value=_mock_response(_RANGE_RESPONSE, 200))
        src, _ = _make_source_with_mock(mock_post, cache_ttl_seconds=3600)

        # "Run N" — query for a historical date
        historical_start = datetime(2026, 1, 1, tzinfo=UTC)
        historical_end = datetime(2026, 1, 2, tzinfo=UTC)
        src.query([_QUERY], historical_start, historical_end, _STEP)
        assert mock_post.call_count == 1

        # "Run N+1" — same source instance, same query params
        # Should be a cache hit (within 3600s TTL)
        src.query([_QUERY], historical_start, historical_end, _STEP)
        assert mock_post.call_count == 1  # no new HTTP call
