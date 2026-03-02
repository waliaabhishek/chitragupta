from __future__ import annotations

import base64
import time
from unittest.mock import patch

import httpx
import pytest
import respx
from pydantic import SecretStr

from plugins.confluent_cloud.connections import CCloudConnection
from plugins.confluent_cloud.exceptions import CCloudApiError, CCloudConnectionError


def _resp(json_data: object = None, status: int = 200, text: str = "", headers: dict | None = None) -> httpx.Response:
    """Build a mock httpx.Response."""
    import json as _json

    content = _json.dumps(json_data).encode() if json_data is not None else text.encode()
    return httpx.Response(status, content=content, headers=headers or {})


def test_ccloud_connection_construction():
    conn = CCloudConnection(
        api_key="key123",
        api_secret=SecretStr("secret456"),
    )
    assert conn.api_key == "key123"
    assert conn.api_secret.get_secret_value() == "secret456"
    assert conn.base_url == "https://api.confluent.cloud"
    assert conn.timeout_seconds == 30
    assert conn.max_retries == 5


def test_ccloud_connection_custom_base_url():
    conn = CCloudConnection(
        api_key="key",
        api_secret=SecretStr("secret"),
        base_url="https://custom.api.com",
    )
    assert conn.base_url == "https://custom.api.com"


@respx.mock
def test_get_single_page():
    respx.get("https://api.confluent.cloud/test/endpoint").mock(
        return_value=_resp({"data": [{"id": "1"}, {"id": "2"}], "metadata": {}})
    )

    conn = CCloudConnection(api_key="key", api_secret=SecretStr("secret"))
    items = list(conn.get("/test/endpoint"))

    assert items == [{"id": "1"}, {"id": "2"}]


@respx.mock
def test_get_with_pagination():
    route = respx.get("https://api.confluent.cloud/test/endpoint")
    route.side_effect = [
        _resp(
            {
                "data": [{"id": "1"}],
                "metadata": {"next": "https://api.confluent.cloud/test/endpoint?page_token=abc"},
            }
        ),
        _resp({"data": [{"id": "2"}], "metadata": {}}),
    ]

    conn = CCloudConnection(api_key="key", api_secret=SecretStr("secret"))
    items = list(conn.get("/test/endpoint"))

    assert items == [{"id": "1"}, {"id": "2"}]


@respx.mock
def test_get_empty_response():
    respx.get("https://api.confluent.cloud/test/endpoint").mock(return_value=_resp({"data": [], "metadata": {}}))

    conn = CCloudConnection(api_key="key", api_secret=SecretStr("secret"))
    items = list(conn.get("/test/endpoint"))

    assert items == []


@respx.mock
def test_get_404_returns_empty():
    respx.get("https://api.confluent.cloud/test/endpoint").mock(return_value=httpx.Response(404, text="Not found"))

    conn = CCloudConnection(api_key="key", api_secret=SecretStr("secret"))
    items = list(conn.get("/test/endpoint"))

    assert items == []


@respx.mock
def test_get_500_raises():
    respx.get("https://api.confluent.cloud/test/endpoint").mock(
        return_value=httpx.Response(500, text="Internal Server Error")
    )

    conn = CCloudConnection(api_key="key", api_secret=SecretStr("secret"))

    with pytest.raises(CCloudApiError) as exc_info:
        list(conn.get("/test/endpoint"))

    assert exc_info.value.status_code == 500


@respx.mock
def test_get_429_with_retry_after_header():
    route = respx.get("https://api.confluent.cloud/test")
    route.side_effect = [
        httpx.Response(429, text="", headers={"Retry-After": "0.05"}),
        _resp({"data": [{"id": "1"}], "metadata": {}}),
    ]

    conn = CCloudConnection(api_key="key", api_secret=SecretStr("secret"))
    start = time.monotonic()
    items = list(conn.get("/test"))
    elapsed = time.monotonic() - start

    assert items == [{"id": "1"}]
    assert elapsed >= 0.05  # Waited for Retry-After


@respx.mock
def test_get_429_with_exponential_backoff():
    route = respx.get("https://api.confluent.cloud/test")
    route.side_effect = [
        httpx.Response(429, text="Rate limited"),
        _resp({"data": [{"id": "1"}], "metadata": {}}),
    ]

    conn = CCloudConnection(
        api_key="key",
        api_secret=SecretStr("secret"),
        base_backoff_seconds=0.01,  # Fast for tests
    )
    items = list(conn.get("/test"))

    assert items == [{"id": "1"}]


@respx.mock
def test_get_max_retries_exhausted():
    # max_retries=5 means 6 total attempts
    respx.get("https://api.confluent.cloud/test").mock(side_effect=[httpx.Response(429, text="Rate limited")] * 6)

    conn = CCloudConnection(
        api_key="key",
        api_secret=SecretStr("secret"),
        max_retries=5,
        base_backoff_seconds=0.001,  # Fast for tests
    )

    with pytest.raises(CCloudApiError) as exc_info:
        list(conn.get("/test"))

    assert exc_info.value.status_code == 429


@respx.mock
def test_get_timeout_then_success():
    route = respx.get("https://api.confluent.cloud/test")
    route.side_effect = [
        httpx.TimeoutException("Connection timed out"),
        _resp({"data": [{"id": "1"}], "metadata": {}}),
    ]

    conn = CCloudConnection(
        api_key="key",
        api_secret=SecretStr("secret"),
        base_backoff_seconds=0.001,
    )
    items = list(conn.get("/test"))

    assert items == [{"id": "1"}]


@respx.mock
def test_post_success():
    respx.post("https://api.confluent.cloud/test/create").mock(
        return_value=_resp({"id": "new-123", "status": "created"})
    )

    conn = CCloudConnection(api_key="key", api_secret=SecretStr("secret"))
    result = conn.post("/test/create", json={"name": "test-resource"})

    assert result == {"id": "new-123", "status": "created"}


@respx.mock
def test_post_error():
    respx.post("https://api.confluent.cloud/test/create").mock(return_value=httpx.Response(400, text="Bad Request"))

    conn = CCloudConnection(api_key="key", api_secret=SecretStr("secret"))

    with pytest.raises(CCloudApiError) as exc_info:
        conn.post("/test/create", json={})

    assert exc_info.value.status_code == 400


@respx.mock
def test_get_429_with_ratelimit_reset_header():
    """Test Confluent-specific rateLimit-reset header (relative seconds, not Unix timestamp)."""
    route = respx.get("https://api.confluent.cloud/test")
    route.side_effect = [
        httpx.Response(429, text="", headers={"rateLimit-reset": "0.05"}),
        _resp({"data": [{"id": "1"}], "metadata": {}}),
    ]

    conn = CCloudConnection(api_key="key", api_secret=SecretStr("secret"))
    start = time.monotonic()
    items = list(conn.get("/test"))
    elapsed = time.monotonic() - start

    assert items == [{"id": "1"}]
    assert elapsed >= 0.05  # Waited for rateLimit-reset seconds


@respx.mock
def test_get_429_with_pascal_case_ratelimit_reset_header():
    """Test RateLimit-Reset header variant (pascal-case)."""
    route = respx.get("https://api.confluent.cloud/test")
    route.side_effect = [
        httpx.Response(429, text="", headers={"RateLimit-Reset": "0.05"}),
        _resp({"data": [{"id": "1"}], "metadata": {}}),
    ]

    conn = CCloudConnection(api_key="key", api_secret=SecretStr("secret"))
    start = time.monotonic()
    items = list(conn.get("/test"))
    elapsed = time.monotonic() - start

    assert items == [{"id": "1"}]
    assert elapsed >= 0.05  # Waited for RateLimit-Reset seconds


@respx.mock
def test_connection_error_raises_ccloud_connection_error():
    respx.get("https://api.confluent.cloud/test").mock(side_effect=httpx.ConnectError("Connection refused"))

    conn = CCloudConnection(api_key="key", api_secret=SecretStr("secret"))

    with pytest.raises(CCloudConnectionError):
        list(conn.get("/test"))


@respx.mock
def test_request_has_basic_auth_header():
    respx.get("https://api.confluent.cloud/test").mock(return_value=_resp({"data": [], "metadata": {}}))

    conn = CCloudConnection(api_key="mykey", api_secret=SecretStr("mysecret"))
    list(conn.get("/test"))

    sent_request = respx.calls[0].request
    expected = base64.b64encode(b"mykey:mysecret").decode()
    assert sent_request.headers["Authorization"] == f"Basic {expected}"


@respx.mock
def test_get_data_null_returns_empty():
    respx.get("https://api.confluent.cloud/test").mock(return_value=_resp({"data": None, "metadata": {}}))

    conn = CCloudConnection(api_key="key", api_secret=SecretStr("secret"))
    items = list(conn.get("/test"))

    assert items == []


def test_connection_uses_client_for_pooling():
    """Verify that CCloudConnection uses httpx.Client for connection pooling."""
    conn = CCloudConnection(api_key="key", api_secret=SecretStr("secret"))

    assert hasattr(conn, "_client")
    assert conn._client is not None
    assert isinstance(conn._client, httpx.Client)
    assert isinstance(conn._client.auth, httpx.BasicAuth)


def test_connection_close():
    """Verify close() properly closes the underlying client."""
    conn = CCloudConnection(api_key="key", api_secret=SecretStr("secret"))

    with patch.object(conn._client, "close") as mock_close:
        conn.close()
        mock_close.assert_called_once()


# =============================================================================
# get_raw() tests
# =============================================================================


class TestGetRaw:
    """Tests for CCloudConnection.get_raw() method."""

    @respx.mock
    def test_get_raw_returns_full_response(self):
        """get_raw() returns the full JSON response without pagination."""
        respx.get("https://api.confluent.cloud/test/raw").mock(
            return_value=_resp({"my-connector": {"info": {"config": {"name": "my-connector"}}}})
        )

        conn = CCloudConnection(api_key="key", api_secret=SecretStr("secret"))
        result = conn.get_raw("/test/raw")

        assert "my-connector" in result
        assert result["my-connector"]["info"]["config"]["name"] == "my-connector"

    @respx.mock
    def test_get_raw_retries_on_429(self):
        """get_raw() retries on rate limit just like get()."""
        route = respx.get("https://api.confluent.cloud/test/raw")
        route.side_effect = [
            httpx.Response(429, text=""),
            _resp({"data": "ok"}),
        ]

        conn = CCloudConnection(
            api_key="key",
            api_secret=SecretStr("secret"),
            base_backoff_seconds=0.01,
        )
        result = conn.get_raw("/test/raw")
        assert result == {"data": "ok"}

    @respx.mock
    def test_get_raw_returns_empty_dict_on_404(self):
        """get_raw() returns {} on 404 (not the standard envelope)."""
        respx.get("https://api.confluent.cloud/test/missing").mock(return_value=httpx.Response(404, text="Not found"))

        conn = CCloudConnection(api_key="key", api_secret=SecretStr("secret"))
        result = conn.get_raw("/test/missing")

        assert result == {}

    @respx.mock
    def test_get_raw_with_params(self):
        """get_raw() passes query parameters correctly."""
        respx.get("https://api.confluent.cloud/test/endpoint").mock(return_value=_resp({"result": "ok"}))

        conn = CCloudConnection(api_key="key", api_secret=SecretStr("secret"))
        result = conn.get_raw("/test/endpoint", params={"expand": "info,status"})

        assert result == {"result": "ok"}
        assert "expand=info" in str(respx.calls[0].request.url)


# =============================================================================
# Proactive throttling tests
# =============================================================================


class TestProactiveThrottling:
    """Tests for request_interval_seconds proactive throttling."""

    @respx.mock
    def test_throttling_spaces_requests(self):
        """request_interval_seconds introduces delay between requests."""
        respx.get("https://api.confluent.cloud/test").mock(return_value=_resp({"data": [], "metadata": {}}))

        # 100ms interval between requests
        conn = CCloudConnection(
            api_key="key",
            api_secret=SecretStr("secret"),
            request_interval_seconds=0.1,
        )

        start = time.time()
        list(conn.get("/test"))  # 1st request
        list(conn.get("/test"))  # 2nd request (should wait ~100ms)
        list(conn.get("/test"))  # 3rd request (should wait ~100ms)
        elapsed = time.time() - start

        # Should take at least 200ms (2 intervals) but allow some tolerance
        assert elapsed >= 0.18, f"Expected >= 180ms, got {elapsed * 1000:.0f}ms"

    @respx.mock
    def test_throttling_disabled_when_zero(self):
        """request_interval_seconds=0 disables throttling."""
        respx.get("https://api.confluent.cloud/test").mock(return_value=_resp({"data": [], "metadata": {}}))

        conn = CCloudConnection(
            api_key="key",
            api_secret=SecretStr("secret"),
            request_interval_seconds=0.0,  # Disabled
        )

        start = time.time()
        list(conn.get("/test"))
        list(conn.get("/test"))
        list(conn.get("/test"))
        elapsed = time.time() - start

        # Should be fast (< 100ms total)
        assert elapsed < 0.1, f"Expected < 100ms, got {elapsed * 1000:.0f}ms"

    def test_default_throttling_interval(self):
        """Default request_interval_seconds is 0.1 (100ms)."""
        conn = CCloudConnection(api_key="key", api_secret=SecretStr("secret"))
        assert conn.request_interval_seconds == 0.1


# =============================================================================
# _get_rate_limit_wait() unit tests — GAP-25
# =============================================================================


class TestGetRateLimitWait:
    """Unit tests for _get_rate_limit_wait: X-RateLimit-Reset header support."""

    def _make_conn(self) -> CCloudConnection:
        return CCloudConnection(
            api_key="key",
            api_secret=SecretStr("secret"),
            base_backoff_seconds=0.001,
        )

    def _make_429(self, headers: dict) -> httpx.Response:
        return httpx.Response(429, content=b"", headers=headers)

    def test_get_rate_limit_wait_x_ratelimit_reset_future_timestamp(self) -> None:
        """X-RateLimit-Reset future Unix timestamp → wait = reset_time - now (plus jitter)."""
        conn = self._make_conn()
        now = 1000.0
        reset_time = 1060.0  # 60 s in the future
        response = self._make_429({"X-RateLimit-Reset": str(reset_time)})

        with patch("plugins.confluent_cloud.connections.time") as mock_time:
            mock_time.time.return_value = now
            result = conn._get_rate_limit_wait(response, attempt=1)

        # base_wait = 60.0, floor leaves it at 60.0, jitter factor ∈ [1.1, 1.2]
        assert 60.0 * 1.1 <= result <= 60.0 * 1.2

    def test_get_rate_limit_wait_x_ratelimit_reset_past_timestamp(self) -> None:
        """X-RateLimit-Reset past Unix timestamp → wait floored at 1.0 (plus jitter).

        Uses attempt=10 so exponential backoff would produce ~1.024 + jitter ≈ [1.13, 2.43],
        which is outside [1.1, 1.2]. The floor-to-1.0 path (from a past timestamp) produces
        exactly [1.1, 1.2], making this test a reliable discriminator.
        """
        conn = self._make_conn()
        now = 1000.0
        reset_time = 990.0  # 10 s in the past → negative wait
        response = self._make_429({"X-RateLimit-Reset": str(reset_time)})

        with patch("plugins.confluent_cloud.connections.time") as mock_time:
            mock_time.time.return_value = now
            result = conn._get_rate_limit_wait(response, attempt=10)

        # base_wait = -10.0 → floored to 1.0, jitter factor ∈ [1.1, 1.2]
        assert 1.0 * 1.1 <= result <= 1.0 * 1.2

    def test_get_rate_limit_wait_retry_after_takes_precedence_over_x_ratelimit_reset(self) -> None:
        """When both Retry-After and X-RateLimit-Reset present, Retry-After wins."""
        conn = self._make_conn()
        # X-RateLimit-Reset is a far-future timestamp; if it were used the wait would be huge
        response = self._make_429({"Retry-After": "30", "X-RateLimit-Reset": "9999999999"})

        result = conn._get_rate_limit_wait(response, attempt=1)

        # base_wait = 30.0, jitter factor ∈ [1.1, 1.2]
        assert 30.0 * 1.1 <= result <= 30.0 * 1.2

    def test_get_rate_limit_wait_no_headers_falls_to_exponential_backoff(self) -> None:
        """No rate-limit headers → _calculate_backoff() is called."""
        conn = self._make_conn()
        response = self._make_429({})

        with patch.object(conn, "_calculate_backoff", return_value=5.0) as mock_backoff:
            result = conn._get_rate_limit_wait(response, attempt=2)

        mock_backoff.assert_called_once_with(2)
        # base_wait = 5.0, jitter factor ∈ [1.1, 1.2]
        assert 5.0 * 1.1 <= result <= 5.0 * 1.2
