from __future__ import annotations

import json
import logging
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from core.metrics.prometheus import (
    AuthConfig,
    PrometheusConfig,
    PrometheusMetricsSource,
    _inject_resource_filter,
    _iso_utc,
)
from core.metrics.protocol import MetricsQueryError
from core.models.metrics import MetricQuery, MetricRow

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
                    "metric": {"topic": "my-topic", "cluster_id": "lkc-123", "extra": "ignored"},
                    "values": [
                        [1735689600.0, "100.5"],
                        [1735693200.0, "200.0"],
                    ],
                },
                {
                    "metric": {"topic": "other", "cluster_id": "lkc-456"},
                    "values": [[1735689600.0, "50"]],
                },
            ],
        },
    }
)

_INSTANT_RESPONSE = json.dumps(
    {
        "status": "success",
        "data": {
            "resultType": "vector",
            "result": [
                {
                    "metric": {"topic": "t1", "cluster_id": "c1"},
                    "value": [1735689600.0, "42.0"],
                }
            ],
        },
    }
)

_EMPTY_RESPONSE = json.dumps({"status": "success", "data": {"resultType": "matrix", "result": []}})

_ERROR_RESPONSE = json.dumps({"status": "error", "errorType": "bad_data", "error": "invalid query"})

_MALFORMED_RESPONSE = json.dumps(
    {
        "status": "success",
        "data": {
            "resultType": "matrix",
            "result": [
                {
                    "metric": {"topic": "t1"},
                    "values": [
                        [1735689600.0, "100"],
                        [1735693200.0, "not_a_number"],
                        ["bad_ts", "200"],
                    ],
                }
            ],
        },
    }
)


def _make_config(**overrides: object) -> PrometheusConfig:
    defaults: dict[str, object] = {"url": "http://prom:9090/", "max_retries": 1, "base_delay": 0.0}
    defaults.update(overrides)
    return PrometheusConfig(**defaults)  # type: ignore[arg-type]


def _mock_response(text: str, status_code: int = 200) -> MagicMock:
    resp = MagicMock()
    resp.text = text
    resp.status_code = status_code
    return resp


# ===========================================================================
# Config tests
# ===========================================================================


class TestPrometheusConfig:
    def test_defaults(self) -> None:
        cfg = PrometheusConfig(url="http://prom:9090/")
        assert cfg.timeout == 30.0
        assert cfg.max_workers == 10
        assert cfg.cache_maxsize == 512
        assert cfg.cache_ttl_seconds == 300.0
        assert cfg.step_seconds == 3600
        assert cfg.max_retries == 4
        assert cfg.base_delay == 1.0
        assert cfg.extra_headers == {}
        assert cfg.auth is None

    def test_custom_values(self) -> None:
        auth = AuthConfig(type="basic", username="u", password="p")
        cfg = PrometheusConfig(
            url="http://prom:9090/",
            auth=auth,
            timeout=10.0,
            max_workers=5,
            cache_maxsize=100,
            cache_ttl_seconds=60.0,
            step_seconds=300,
            max_retries=2,
            base_delay=0.5,
            extra_headers={"X-Custom": "val"},
        )
        assert cfg.auth is auth
        assert cfg.max_workers == 5
        assert cfg.extra_headers == {"X-Custom": "val"}

    def test_max_workers_validation(self) -> None:
        with pytest.raises(ValueError, match="max_workers must be >= 1"):
            PrometheusConfig(url="http://prom:9090/", max_workers=0)


class TestAuthConfig:
    def test_basic(self) -> None:
        auth = AuthConfig(type="basic", username="u", password="p")
        assert auth.type == "basic"

    def test_bearer(self) -> None:
        auth = AuthConfig(type="bearer", token="tok")
        assert auth.token == "tok"
        assert auth.username is None


# ===========================================================================
# _iso_utc tests
# ===========================================================================


class TestIsoUtc:
    def test_aware_datetime(self) -> None:
        dt = datetime(2026, 1, 15, 12, 0, 0, tzinfo=UTC)
        assert _iso_utc(dt) == "2026-01-15T12:00:00+00:00"

    def test_naive_raises(self) -> None:
        with pytest.raises(ValueError, match="Naive datetime"):
            _iso_utc(datetime(2026, 1, 15, 12, 0, 0))


# ===========================================================================
# _inject_resource_filter tests
# ===========================================================================


class TestInjectResourceFilter:
    def test_with_filter_and_placeholder(self) -> None:
        expr = "sum(rate(metric_total{}))"
        result = _inject_resource_filter(expr, "resource_id", "lkc-123")
        assert result == 'sum(rate(metric_total{resource_id="lkc-123"}))'

    def test_no_filter_strips_braces(self) -> None:
        expr = "sum(rate(metric_total{}))"
        result = _inject_resource_filter(expr, "resource_id", None)
        assert result == "sum(rate(metric_total))"

    def test_multiple_placeholders_replaces_only_first(self) -> None:
        expr = "sum(rate(metric_total{})) / sum(rate(other{}))"
        result = _inject_resource_filter(expr, "rid", "lkc-1")
        assert result == 'sum(rate(metric_total{rid="lkc-1"})) / sum(rate(other{}))'

    def test_multiple_placeholders_no_filter_strips_first_only(self) -> None:
        expr = "sum(rate(metric_total{})) / sum(rate(other{}))"
        result = _inject_resource_filter(expr, "rid", None)
        assert result == "sum(rate(metric_total)) / sum(rate(other{}))"

    def test_filter_without_placeholder_warns(self, caplog: pytest.LogCaptureFixture) -> None:
        expr = "sum(rate(metric_total))"
        result = _inject_resource_filter(expr, "resource_id", "lkc-123")
        assert result == expr
        assert "no {} placeholder" in caplog.text


# ===========================================================================
# Response parsing tests
# ===========================================================================


class TestParseResponse:
    def _source(self) -> PrometheusMetricsSource:
        return PrometheusMetricsSource(_make_config())

    def test_range_response(self) -> None:
        src = self._source()
        rows = src._parse_response(_RANGE_RESPONSE, _QUERY)
        assert len(rows) == 3
        assert all(isinstance(r, MetricRow) for r in rows)
        assert rows[0].metric_key == "bytes_in"
        assert rows[0].labels == {"topic": "my-topic", "cluster_id": "lkc-123"}
        assert rows[0].value == 100.5

    def test_instant_response(self) -> None:
        src = self._source()
        rows = src._parse_response(_INSTANT_RESPONSE, _QUERY)
        assert len(rows) == 1
        assert rows[0].value == 42.0

    def test_empty_result(self) -> None:
        src = self._source()
        rows = src._parse_response(_EMPTY_RESPONSE, _QUERY)
        assert rows == []

    def test_error_response(self) -> None:
        src = self._source()
        with pytest.raises(MetricsQueryError, match="invalid query"):
            src._parse_response(_ERROR_RESPONSE, _QUERY)

    def test_malformed_values_skipped(self, caplog: pytest.LogCaptureFixture) -> None:
        src = self._source()
        rows = src._parse_response(_MALFORMED_RESPONSE, _QUERY)
        # First value OK, second bad val, third bad timestamp
        assert len(rows) == 1
        assert rows[0].value == 100.0
        assert "malformed" in caplog.text.lower()


# ===========================================================================
# Auth tests
# ===========================================================================


class TestAuth:
    def test_no_auth(self) -> None:
        src = PrometheusMetricsSource(_make_config())
        assert src._auth is None

    def test_basic_auth(self) -> None:
        from requests.auth import HTTPBasicAuth

        cfg = _make_config(auth=AuthConfig(type="basic", username="u", password="p"))
        src = PrometheusMetricsSource(cfg)
        assert isinstance(src._auth, HTTPBasicAuth)

    def test_digest_auth(self) -> None:
        from requests.auth import HTTPDigestAuth

        cfg = _make_config(auth=AuthConfig(type="digest", username="u", password="p"))
        src = PrometheusMetricsSource(cfg)
        assert isinstance(src._auth, HTTPDigestAuth)

    def test_bearer_auth(self) -> None:
        cfg = _make_config(auth=AuthConfig(type="bearer", token="mytoken"))
        src = PrometheusMetricsSource(cfg)
        assert src._auth is None  # bearer uses headers
        assert src._extra_headers["Authorization"] == "Bearer mytoken"


# ===========================================================================
# Retry tests
# ===========================================================================


class TestRetry:
    @patch("core.metrics.prometheus.requests.post")
    def test_transient_then_success(self, mock_post: MagicMock) -> None:
        mock_post.side_effect = [
            _mock_response("", 503),
            _mock_response(_RANGE_RESPONSE, 200),
        ]
        src = PrometheusMetricsSource(_make_config(max_retries=3, base_delay=0.0))
        result = src.query([_QUERY], _START, _END, _STEP)
        assert "bytes_in" in result
        assert mock_post.call_count == 2

    @patch("core.metrics.prometheus.requests.post")
    def test_all_retries_exhausted(self, mock_post: MagicMock) -> None:
        mock_post.return_value = _mock_response("", 503)
        src = PrometheusMetricsSource(_make_config(max_retries=2, base_delay=0.0))
        with pytest.raises(MetricsQueryError, match="Exhausted"):
            src.query([_QUERY], _START, _END, _STEP)

    @patch("core.metrics.prometheus.requests.post")
    def test_non_transient_immediate_fail(self, mock_post: MagicMock) -> None:
        mock_post.return_value = _mock_response("Forbidden", 403)
        src = PrometheusMetricsSource(_make_config(max_retries=3, base_delay=0.0))
        with pytest.raises(MetricsQueryError, match="403"):
            src.query([_QUERY], _START, _END, _STEP)
        assert mock_post.call_count == 1

    @patch("core.metrics.prometheus.requests.post")
    def test_exhausted_retries_chains_last_exception(self, mock_post: MagicMock) -> None:
        import requests as req

        mock_post.side_effect = req.ConnectionError("refused")
        src = PrometheusMetricsSource(_make_config(max_retries=2, base_delay=0.0))
        with pytest.raises(MetricsQueryError, match="Exhausted") as exc_info:
            src.query([_QUERY], _START, _END, _STEP)
        assert isinstance(exc_info.value.__cause__, req.ConnectionError)

    @patch("core.metrics.prometheus.requests.post")
    def test_exhausted_retries_transient_status_no_chain(self, mock_post: MagicMock) -> None:
        """Transient HTTP status (no RequestException) → no chained cause."""
        mock_post.return_value = _mock_response("", 503)
        src = PrometheusMetricsSource(_make_config(max_retries=2, base_delay=0.0))
        with pytest.raises(MetricsQueryError, match="Exhausted") as exc_info:
            src.query([_QUERY], _START, _END, _STEP)
        assert exc_info.value.__cause__ is None

    @patch("core.metrics.prometheus.requests.post")
    def test_connection_error_retries(self, mock_post: MagicMock) -> None:
        import requests as req

        mock_post.side_effect = [
            req.ConnectionError("refused"),
            _mock_response(_RANGE_RESPONSE, 200),
        ]
        src = PrometheusMetricsSource(_make_config(max_retries=3, base_delay=0.0))
        result = src.query([_QUERY], _START, _END, _STEP)
        assert "bytes_in" in result


# ===========================================================================
# Parallel execution tests
# ===========================================================================


class TestParallelExecution:
    @patch("core.metrics.prometheus.requests.post")
    def test_multiple_queries_all_succeed(self, mock_post: MagicMock) -> None:
        mock_post.return_value = _mock_response(_RANGE_RESPONSE, 200)
        q2 = MetricQuery(
            key="bytes_out",
            query_expression="kafka_bytesout{}",
            label_keys=("topic",),
            resource_label="kafka_id",
        )
        src = PrometheusMetricsSource(_make_config())
        result = src.query([_QUERY, q2], _START, _END, _STEP)
        assert "bytes_in" in result
        assert "bytes_out" in result

    @patch("core.metrics.prometheus.requests.post")
    def test_multiple_failures_logs_additional_errors(
        self, mock_post: MagicMock, caplog: pytest.LogCaptureFixture
    ) -> None:
        """When 2+ queries fail, first is raised and extras are logged."""
        mock_post.side_effect = [
            _mock_response(_RANGE_RESPONSE, 200),
            _mock_response("Forbidden", 403),
            _mock_response("Forbidden", 403),
        ]
        q2 = MetricQuery(
            key="bytes_out",
            query_expression="kafka_bytesout{}",
            label_keys=("topic",),
            resource_label="kafka_id",
        )
        q3 = MetricQuery(
            key="requests",
            query_expression="http_requests_total{}",
            label_keys=("method",),
            resource_label="instance",
        )
        src = PrometheusMetricsSource(_make_config())
        with caplog.at_level(logging.ERROR), pytest.raises(MetricsQueryError):
            src.query([_QUERY, q2, q3], _START, _END, _STEP)
        assert "Additional query error" in caplog.text

    @patch("core.metrics.prometheus.requests.post")
    def test_one_fails_others_succeed(self, mock_post: MagicMock) -> None:
        mock_post.side_effect = [
            _mock_response(_RANGE_RESPONSE, 200),
            _mock_response("Forbidden", 403),
        ]
        q2 = MetricQuery(
            key="bytes_out",
            query_expression="kafka_bytesout{}",
            label_keys=("topic",),
            resource_label="kafka_id",
        )
        src = PrometheusMetricsSource(_make_config())
        with pytest.raises(MetricsQueryError):
            src.query([_QUERY, q2], _START, _END, _STEP)


# ===========================================================================
# Cache tests
# ===========================================================================


class TestCache:
    @patch("core.metrics.prometheus.requests.post")
    def test_same_params_one_call(self, mock_post: MagicMock) -> None:
        mock_post.return_value = _mock_response(_RANGE_RESPONSE, 200)
        src = PrometheusMetricsSource(_make_config(cache_ttl_seconds=300.0))
        src.query([_QUERY], _START, _END, _STEP)
        src.query([_QUERY], _START, _END, _STEP)
        assert mock_post.call_count == 1

    @patch("core.metrics.prometheus.requests.post")
    def test_different_params_separate_calls(self, mock_post: MagicMock) -> None:
        mock_post.return_value = _mock_response(_RANGE_RESPONSE, 200)
        src = PrometheusMetricsSource(_make_config())
        src.query([_QUERY], _START, _END, _STEP)
        other_end = datetime(2026, 1, 3, tzinfo=UTC)
        src.query([_QUERY], _START, other_end, _STEP)
        assert mock_post.call_count == 2

    @patch("core.metrics.prometheus.requests.post")
    def test_ttl_expiry(self, mock_post: MagicMock) -> None:
        mock_post.return_value = _mock_response(_RANGE_RESPONSE, 200)
        src = PrometheusMetricsSource(_make_config(cache_ttl_seconds=0.0))

        src.query([_QUERY], _START, _END, _STEP)
        # TTL is 0 so the next call should re-fetch
        src.query([_QUERY], _START, _END, _STEP)
        assert mock_post.call_count == 2

    @patch("core.metrics.prometheus.requests.post")
    def test_cache_full_evicts_expired(self, mock_post: MagicMock) -> None:
        """When cache is full and entries are expired, eviction makes room."""
        mock_post.return_value = _mock_response(_RANGE_RESPONSE, 200)
        src = PrometheusMetricsSource(_make_config(cache_maxsize=1, cache_ttl_seconds=0.0))

        # First query fills cache; TTL=0 means it expires immediately
        src.query([_QUERY], _START, _END, _STEP)
        # Second query with different params triggers eviction of expired entry
        other_end = datetime(2026, 1, 3, tzinfo=UTC)
        src.query([_QUERY], _START, other_end, _STEP)
        # Both should have made HTTP calls (no cache hit)
        assert mock_post.call_count == 2

    @patch("core.metrics.prometheus.requests.post")
    def test_cache_full_non_expired_skips_caching(self, mock_post: MagicMock) -> None:
        """When cache is full with non-expired entries, new results are not cached."""
        mock_post.return_value = _mock_response(_RANGE_RESPONSE, 200)
        src = PrometheusMetricsSource(_make_config(cache_maxsize=1, cache_ttl_seconds=300.0))

        # Fill cache with 1 entry (long TTL, won't expire)
        src.query([_QUERY], _START, _END, _STEP)
        # Different params — cache full, nothing expired, result NOT cached
        end2 = datetime(2026, 1, 3, tzinfo=UTC)
        src.query([_QUERY], _START, end2, _STEP)
        # Same params as step 2 — should make another HTTP call since step 2 wasn't cached
        src.query([_QUERY], _START, end2, _STEP)
        assert mock_post.call_count == 3


# ===========================================================================
# Step guard tests
# ===========================================================================


class TestStepGuard:
    @patch("core.metrics.prometheus.requests.post")
    def test_zero_step_defaults_to_config(self, mock_post: MagicMock, caplog: pytest.LogCaptureFixture) -> None:
        mock_post.return_value = _mock_response(_RANGE_RESPONSE, 200)
        cfg = _make_config(step_seconds=900)
        src = PrometheusMetricsSource(cfg)
        with caplog.at_level(logging.WARNING):
            src.query([_QUERY], _START, _END, step=timedelta(seconds=0))
        assert "non-positive" in caplog.text
        # Verify the default step was used in the POST data
        call_data = mock_post.call_args.kwargs["data"]
        assert call_data["step"] == "900"

    @patch("core.metrics.prometheus.requests.post")
    def test_negative_step_defaults_to_config(self, mock_post: MagicMock, caplog: pytest.LogCaptureFixture) -> None:
        mock_post.return_value = _mock_response(_RANGE_RESPONSE, 200)
        cfg = _make_config(step_seconds=600)
        src = PrometheusMetricsSource(cfg)
        with caplog.at_level(logging.WARNING):
            src.query([_QUERY], _START, _END, step=timedelta(seconds=-10))
        assert "non-positive" in caplog.text


# ===========================================================================
# Integration-style tests
# ===========================================================================


class TestQueryIntegration:
    @patch("core.metrics.prometheus.requests.post")
    def test_query_multiple_metric_queries(self, mock_post: MagicMock) -> None:
        mock_post.return_value = _mock_response(_RANGE_RESPONSE, 200)
        q2 = MetricQuery(
            key="requests",
            query_expression="http_requests_total{}",
            label_keys=("method",),
            resource_label="instance",
        )
        src = PrometheusMetricsSource(_make_config())
        result = src.query([_QUERY, q2], _START, _END, _STEP)
        assert isinstance(result, dict)
        assert len(result) == 2

    @patch("core.metrics.prometheus.requests.post")
    def test_query_empty_list(self, mock_post: MagicMock) -> None:
        src = PrometheusMetricsSource(_make_config())
        result = src.query([], _START, _END, _STEP)
        assert result == {}
        mock_post.assert_not_called()
