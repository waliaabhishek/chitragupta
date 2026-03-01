"""GAP-18: Tests for instant query mode in PrometheusMetricsSource.

All tests in this module are expected to FAIL until GAP-18 is implemented:
- MetricQuery.query_mode field does not yet exist
- PrometheusMetricsSource._execute_instant does not yet exist
- PrometheusMetricsSource._url_instant does not yet exist
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock

import httpx
import pytest

from core.metrics.prometheus import PrometheusConfig, PrometheusMetricsSource
from core.metrics.protocol import MetricsQueryError
from core.models.metrics import MetricQuery, MetricRow

# ---------------------------------------------------------------------------
# Fixtures & constants
# ---------------------------------------------------------------------------

_START = datetime(2026, 1, 1, tzinfo=UTC)
_END = datetime(2026, 1, 2, tzinfo=UTC)
_STEP = timedelta(hours=1)

_INSTANT_RESPONSE = json.dumps(
    {
        "status": "success",
        "data": {
            "resultType": "vector",
            "result": [
                {
                    "metric": {"kafka_id": "lkc-test", "principal_id": "sa-aaa"},
                    "value": [1735776000.0, "42.0"],
                }
            ],
        },
    }
)

_RANGE_RESPONSE = json.dumps(
    {
        "status": "success",
        "data": {
            "resultType": "matrix",
            "result": [
                {
                    "metric": {"kafka_id": "lkc-test", "principal_id": "sa-aaa"},
                    "values": [
                        [1735689600.0, "100.0"],
                        [1735693200.0, "100.0"],
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


def _make_source_with_mock(mock_post: MagicMock, **config_overrides: object) -> PrometheusMetricsSource:
    client = MagicMock(spec=httpx.Client)
    client.post = mock_post
    return PrometheusMetricsSource(_make_config(**config_overrides), client=client)


# ---------------------------------------------------------------------------
# Test 1: instant mode routes to _execute_instant → POST /api/v1/query
# ---------------------------------------------------------------------------


class TestInstantModeRouting:
    def test_instant_query_mode_posts_to_query_endpoint(self) -> None:
        """query() with query_mode='instant' must POST to /api/v1/query with time= param."""
        mock_post = MagicMock(return_value=_mock_response(_INSTANT_RESPONSE))
        src = _make_source_with_mock(mock_post)

        mq = MetricQuery(
            key="bytes_in",
            query_expression="confluent_kafka_server_request_bytes{}",
            label_keys=("kafka_id", "principal_id"),
            resource_label="kafka_id",
            query_mode="instant",
        )

        result = src.query([mq], _START, _END, _STEP)

        assert "bytes_in" in result
        assert mock_post.call_count == 1

        call_kwargs = mock_post.call_args
        url_called = (
            call_kwargs.args[0] if call_kwargs.args else call_kwargs.kwargs.get("url") or mock_post.call_args[0][0]
        )
        assert url_called.endswith("/api/v1/query"), f"Expected /api/v1/query but got {url_called}"

        data_sent = call_kwargs.kwargs.get("data") or call_kwargs.args[1]
        assert "time" in data_sent, "Instant query must include 'time' param"
        assert "start" not in data_sent, "Instant query must NOT include 'start' param"
        assert "end" not in data_sent, "Instant query must NOT include 'end' param"
        assert "step" not in data_sent, "Instant query must NOT include 'step' param"

    def test_instant_query_time_equals_end_timestamp(self) -> None:
        """The 'time' param for instant queries must equal the billing end timestamp."""
        mock_post = MagicMock(return_value=_mock_response(_INSTANT_RESPONSE))
        src = _make_source_with_mock(mock_post)

        mq = MetricQuery(
            key="bytes_in",
            query_expression="confluent_kafka_server_request_bytes{}",
            label_keys=("kafka_id", "principal_id"),
            resource_label="kafka_id",
            query_mode="instant",
        )

        src.query([mq], _START, _END, _STEP)

        data_sent = mock_post.call_args.kwargs.get("data") or mock_post.call_args[0][1]
        expected_time = "2026-01-02T00:00:00+00:00"
        assert data_sent["time"] == expected_time, f"Expected time={expected_time!r}, got {data_sent['time']!r}"


# ---------------------------------------------------------------------------
# Test 2: range mode routes to _execute_query → POST /api/v1/query_range
# ---------------------------------------------------------------------------


class TestRangeModeRouting:
    def test_range_query_mode_posts_to_query_range_endpoint(self) -> None:
        """query() with query_mode='range' (default) must POST to /api/v1/query_range."""
        mock_post = MagicMock(return_value=_mock_response(_RANGE_RESPONSE))
        src = _make_source_with_mock(mock_post)

        mq = MetricQuery(
            key="bytes_in",
            query_expression="confluent_kafka_server_request_bytes{}",
            label_keys=("kafka_id", "principal_id"),
            resource_label="kafka_id",
            query_mode="range",
        )

        result = src.query([mq], _START, _END, _STEP)

        assert "bytes_in" in result
        assert mock_post.call_count == 1

        call_kwargs = mock_post.call_args
        url_called = (
            call_kwargs.args[0] if call_kwargs.args else call_kwargs.kwargs.get("url") or mock_post.call_args[0][0]
        )
        assert url_called.endswith("/api/v1/query_range"), f"Expected /api/v1/query_range but got {url_called}"

        data_sent = call_kwargs.kwargs.get("data") or call_kwargs.args[1]
        assert "start" in data_sent, "Range query must include 'start' param"
        assert "end" in data_sent, "Range query must include 'end' param"
        assert "step" in data_sent, "Range query must include 'step' param"
        assert "time" not in data_sent, "Range query must NOT include 'time' param"

    def test_default_query_mode_is_range(self) -> None:
        """MetricQuery without explicit query_mode defaults to 'range'."""
        mq = MetricQuery(
            key="bytes_in",
            query_expression="some_metric{}",
            label_keys=("kafka_id",),
            resource_label="kafka_id",
        )
        assert mq.query_mode == "range"

    def test_range_query_includes_correct_time_params(self) -> None:
        """Range query must pass start, end, step derived from call arguments."""
        mock_post = MagicMock(return_value=_mock_response(_RANGE_RESPONSE))
        src = _make_source_with_mock(mock_post)

        mq = MetricQuery(
            key="bytes_in",
            query_expression="confluent_kafka_server_request_bytes{}",
            label_keys=("kafka_id", "principal_id"),
            resource_label="kafka_id",
            query_mode="range",
        )

        src.query([mq], _START, _END, timedelta(hours=1))

        data_sent = mock_post.call_args.kwargs.get("data") or mock_post.call_args[0][1]
        assert data_sent["start"] == "2026-01-01T00:00:00+00:00"
        assert data_sent["end"] == "2026-01-02T00:00:00+00:00"
        assert data_sent["step"] == "3600"


# ---------------------------------------------------------------------------
# Test 3: _execute_instant caches on second call
# ---------------------------------------------------------------------------


class TestInstantQueryCaching:
    def test_same_expression_and_timestamp_cached(self) -> None:
        """Second call to _execute_instant with same args returns cached result without HTTP."""
        mock_post = MagicMock(return_value=_mock_response(_INSTANT_RESPONSE))
        src = _make_source_with_mock(mock_post, cache_ttl_seconds=300.0)

        mq = MetricQuery(
            key="bytes_in",
            query_expression="confluent_kafka_server_request_bytes{}",
            label_keys=("kafka_id", "principal_id"),
            resource_label="kafka_id",
            query_mode="instant",
        )

        src.query([mq], _START, _END, _STEP)
        src.query([mq], _START, _END, _STEP)

        assert mock_post.call_count == 1, "Second instant query with same expression+timestamp must use cache"

    def test_different_timestamp_not_cached(self) -> None:
        """Different billing end timestamps produce separate HTTP requests."""
        mock_post = MagicMock(return_value=_mock_response(_INSTANT_RESPONSE))
        src = _make_source_with_mock(mock_post, cache_ttl_seconds=300.0)

        mq = MetricQuery(
            key="bytes_in",
            query_expression="confluent_kafka_server_request_bytes{}",
            label_keys=("kafka_id", "principal_id"),
            resource_label="kafka_id",
            query_mode="instant",
        )

        end1 = datetime(2026, 1, 2, tzinfo=UTC)
        end2 = datetime(2026, 1, 3, tzinfo=UTC)

        src.query([mq], _START, end1, _STEP)
        src.query([mq], _START, end2, _STEP)

        assert mock_post.call_count == 2, "Different timestamps must produce separate HTTP requests"

    def test_instant_cache_key_distinct_from_range_cache_key(self) -> None:
        """Instant and range queries for same expression do not share cache entries."""
        instant_response_count = 0
        range_response_count = 0

        def mock_post_side_effect(url: str, **kwargs: object) -> MagicMock:
            nonlocal instant_response_count, range_response_count
            if "/api/v1/query_range" in url:
                range_response_count += 1
                return _mock_response(_RANGE_RESPONSE)
            else:
                instant_response_count += 1
                return _mock_response(_INSTANT_RESPONSE)

        client = MagicMock(spec=httpx.Client)
        client.post = MagicMock(side_effect=mock_post_side_effect)
        src = PrometheusMetricsSource(_make_config(cache_ttl_seconds=300.0), client=client)

        instant_mq = MetricQuery(
            key="bytes_in",
            query_expression="confluent_kafka_server_request_bytes{}",
            label_keys=("kafka_id", "principal_id"),
            resource_label="kafka_id",
            query_mode="instant",
        )
        range_mq = MetricQuery(
            key="bytes_in",
            query_expression="confluent_kafka_server_request_bytes{}",
            label_keys=("kafka_id", "principal_id"),
            resource_label="kafka_id",
            query_mode="range",
        )

        src.query([instant_mq], _START, _END, _STEP)
        src.query([range_mq], _START, _END, _STEP)

        assert instant_response_count == 1, "Instant query must make 1 HTTP call"
        assert range_response_count == 1, "Range query must make 1 HTTP call (not reuse instant cache)"


# ---------------------------------------------------------------------------
# Test 4: Replay test — bursty billing fixture
# ---------------------------------------------------------------------------


_FIXTURES_DIR = Path(__file__).parent.parent.parent.parent / "fixtures"


class TestBurstyBillingReplay:
    """Replay fixture: sa-aaa active for 1h out of 24h billing window.

    Instant query (at billing end): sa-aaa not present → ratio = 0.
    Range query (24h, 1h step): sa-aaa contributes 1 burst hour → ratio > 0.
    """

    def _load_fixture(self) -> dict:  # type: ignore[type-arg]
        fixture_path = _FIXTURES_DIR / "ccloud_bursty_billing.json"
        with open(fixture_path) as f:
            return json.load(f)

    def _compute_ratio(self, rows: list[MetricRow], target_principal: str) -> float:
        """Compute allocation ratio for target_principal from MetricRow list."""
        totals: dict[str, float] = {}
        for row in rows:
            principal = row.labels.get("principal_id", "")
            totals[principal] = totals.get(principal, 0.0) + row.value
        grand_total = sum(totals.values())
        if grand_total == 0.0:
            return 0.0
        return totals.get(target_principal, 0.0) / grand_total

    def test_instant_vs_range_produce_different_ratios(self) -> None:
        """Instant and range modes produce different allocation ratios for bursty principal."""
        fixture = self._load_fixture()

        instant_text = json.dumps(fixture["instant_response"])
        range_text = json.dumps(fixture["range_response"])

        def post_side_effect(url: str, **kwargs: object) -> MagicMock:
            if "/api/v1/query_range" in url:
                return _mock_response(range_text)
            return _mock_response(instant_text)

        client = MagicMock(spec=httpx.Client)
        client.post = MagicMock(side_effect=post_side_effect)
        src = PrometheusMetricsSource(_make_config(), client=client)

        instant_mq = MetricQuery(
            key="bytes_in",
            query_expression="confluent_kafka_server_request_bytes{}",
            label_keys=("kafka_id", "principal_id"),
            resource_label="kafka_id",
            query_mode="instant",
        )
        range_mq = MetricQuery(
            key="bytes_in",
            query_expression="confluent_kafka_server_request_bytes{}",
            label_keys=("kafka_id", "principal_id"),
            resource_label="kafka_id",
            query_mode="range",
        )

        billing_start = datetime.fromisoformat(fixture["billing_start"])
        billing_end = datetime.fromisoformat(fixture["billing_end"])

        instant_result = src.query([instant_mq], billing_start, billing_end, _STEP)
        range_result = src.query([range_mq], billing_start, billing_end, _STEP)

        instant_ratio_aaa = self._compute_ratio(instant_result["bytes_in"], "sa-aaa")
        range_ratio_aaa = self._compute_ratio(range_result["bytes_in"], "sa-aaa")

        assert instant_ratio_aaa != range_ratio_aaa, (
            f"Bursty principal sa-aaa must have different ratios: "
            f"instant={instant_ratio_aaa:.4f}, range={range_ratio_aaa:.4f}"
        )

    def test_instant_query_misses_bursty_principal(self) -> None:
        """Instant query at billing end timestamp returns 0 ratio for principal that stopped early."""
        fixture = self._load_fixture()
        instant_text = json.dumps(fixture["instant_response"])

        mock_post = MagicMock(return_value=_mock_response(instant_text))
        src = _make_source_with_mock(mock_post)

        mq = MetricQuery(
            key="bytes_in",
            query_expression="confluent_kafka_server_request_bytes{}",
            label_keys=("kafka_id", "principal_id"),
            resource_label="kafka_id",
            query_mode="instant",
        )

        billing_start = datetime.fromisoformat(fixture["billing_start"])
        billing_end = datetime.fromisoformat(fixture["billing_end"])

        result = src.query([mq], billing_start, billing_end, _STEP)
        ratio_aaa = self._compute_ratio(result["bytes_in"], "sa-aaa")

        # sa-aaa is absent from instant_response (not active at billing_end)
        assert ratio_aaa == 0.0, f"Instant query misses bursty sa-aaa: expected ratio=0.0, got {ratio_aaa:.4f}"

    def test_range_query_captures_bursty_principal(self) -> None:
        """Range query captures the burst hour and assigns positive ratio to bursty principal."""
        fixture = self._load_fixture()
        range_text = json.dumps(fixture["range_response"])

        mock_post = MagicMock(return_value=_mock_response(range_text))
        src = _make_source_with_mock(mock_post)

        mq = MetricQuery(
            key="bytes_in",
            query_expression="confluent_kafka_server_request_bytes{}",
            label_keys=("kafka_id", "principal_id"),
            resource_label="kafka_id",
            query_mode="range",
        )

        billing_start = datetime.fromisoformat(fixture["billing_start"])
        billing_end = datetime.fromisoformat(fixture["billing_end"])

        result = src.query([mq], billing_start, billing_end, _STEP)
        ratio_aaa = self._compute_ratio(result["bytes_in"], "sa-aaa")

        # sa-aaa bursts for 1h: 200 / (200 + 24*100) = 200/2600 ≈ 0.077
        assert ratio_aaa > 0.0, f"Range query must capture bursty sa-aaa: expected ratio>0, got {ratio_aaa:.4f}"
        assert abs(ratio_aaa - (200.0 / 2600.0)) < 1e-6, f"Expected ratio≈{200.0 / 2600.0:.6f}, got {ratio_aaa:.6f}"


# ---------------------------------------------------------------------------
# Test 5: Error paths for instant mode
# ---------------------------------------------------------------------------


class TestInstantModeErrors:
    def test_instant_http_error_raises(self) -> None:
        """HTTP 4xx from instant endpoint raises MetricsQueryError."""
        mock_post = MagicMock(return_value=_mock_response("Forbidden", status_code=403))
        src = _make_source_with_mock(mock_post)

        mq = MetricQuery(
            key="bytes_in",
            query_expression="confluent_kafka_server_request_bytes{}",
            label_keys=("kafka_id", "principal_id"),
            resource_label="kafka_id",
            query_mode="instant",
        )

        with pytest.raises(MetricsQueryError, match="403"):
            src.query([mq], _START, _END, _STEP)

    def test_instant_prometheus_error_raises(self) -> None:
        """Prometheus error response from instant endpoint raises MetricsQueryError."""
        error_response = json.dumps(
            {
                "status": "error",
                "errorType": "bad_data",
                "error": "invalid query expression",
            }
        )
        mock_post = MagicMock(return_value=_mock_response(error_response))
        src = _make_source_with_mock(mock_post)

        mq = MetricQuery(
            key="bytes_in",
            query_expression="confluent_kafka_server_request_bytes{}",
            label_keys=("kafka_id", "principal_id"),
            resource_label="kafka_id",
            query_mode="instant",
        )

        with pytest.raises(MetricsQueryError, match="invalid query"):
            src.query([mq], _START, _END, _STEP)
