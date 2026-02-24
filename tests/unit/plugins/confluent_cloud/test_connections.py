from __future__ import annotations

import base64
import time
from unittest.mock import patch

import pytest
import requests
import responses
from pydantic import SecretStr

from plugins.confluent_cloud.connections import CCloudConnection
from plugins.confluent_cloud.exceptions import CCloudApiError, CCloudConnectionError


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


@responses.activate
def test_get_single_page():
    responses.add(
        responses.GET,
        "https://api.confluent.cloud/test/endpoint",
        json={"data": [{"id": "1"}, {"id": "2"}], "metadata": {}},
        status=200,
    )

    conn = CCloudConnection(api_key="key", api_secret=SecretStr("secret"))
    items = list(conn.get("/test/endpoint"))

    assert items == [{"id": "1"}, {"id": "2"}]


@responses.activate
def test_get_with_pagination():
    responses.add(
        responses.GET,
        "https://api.confluent.cloud/test/endpoint",
        json={
            "data": [{"id": "1"}],
            "metadata": {"next": "https://api.confluent.cloud/test/endpoint?page_token=abc"},
        },
        status=200,
    )
    responses.add(
        responses.GET,
        "https://api.confluent.cloud/test/endpoint",
        json={"data": [{"id": "2"}], "metadata": {}},
        status=200,
    )

    conn = CCloudConnection(api_key="key", api_secret=SecretStr("secret"))
    items = list(conn.get("/test/endpoint"))

    assert items == [{"id": "1"}, {"id": "2"}]


@responses.activate
def test_get_empty_response():
    responses.add(
        responses.GET,
        "https://api.confluent.cloud/test/endpoint",
        json={"data": [], "metadata": {}},
        status=200,
    )

    conn = CCloudConnection(api_key="key", api_secret=SecretStr("secret"))
    items = list(conn.get("/test/endpoint"))

    assert items == []


@responses.activate
def test_get_404_returns_empty():
    responses.add(
        responses.GET,
        "https://api.confluent.cloud/test/endpoint",
        status=404,
        body="Not found",
    )

    conn = CCloudConnection(api_key="key", api_secret=SecretStr("secret"))
    items = list(conn.get("/test/endpoint"))

    assert items == []


@responses.activate
def test_get_500_raises():
    responses.add(
        responses.GET,
        "https://api.confluent.cloud/test/endpoint",
        status=500,
        body="Internal Server Error",
    )

    conn = CCloudConnection(api_key="key", api_secret=SecretStr("secret"))

    with pytest.raises(CCloudApiError) as exc_info:
        list(conn.get("/test/endpoint"))

    assert exc_info.value.status_code == 500


@responses.activate
def test_get_429_with_retry_after_header():
    responses.add(
        responses.GET,
        "https://api.confluent.cloud/test",
        status=429,
        headers={"Retry-After": "0.05"},
    )
    responses.add(
        responses.GET,
        "https://api.confluent.cloud/test",
        json={"data": [{"id": "1"}], "metadata": {}},
        status=200,
    )

    conn = CCloudConnection(api_key="key", api_secret=SecretStr("secret"))
    start = time.monotonic()
    items = list(conn.get("/test"))
    elapsed = time.monotonic() - start

    assert items == [{"id": "1"}]
    assert elapsed >= 0.05  # Waited for Retry-After


@responses.activate
def test_get_429_with_exponential_backoff():
    # 429 without headers, then success
    responses.add(
        responses.GET,
        "https://api.confluent.cloud/test",
        status=429,
        body="Rate limited",
    )
    responses.add(
        responses.GET,
        "https://api.confluent.cloud/test",
        json={"data": [{"id": "1"}], "metadata": {}},
        status=200,
    )

    conn = CCloudConnection(
        api_key="key",
        api_secret=SecretStr("secret"),
        base_backoff_seconds=0.01,  # Fast for tests
    )
    items = list(conn.get("/test"))

    assert items == [{"id": "1"}]


@responses.activate
def test_get_max_retries_exhausted():
    # 6 responses: max_retries=5 means 6 total attempts
    for _ in range(6):
        responses.add(
            responses.GET,
            "https://api.confluent.cloud/test",
            status=429,
            body="Rate limited",
        )

    conn = CCloudConnection(
        api_key="key",
        api_secret=SecretStr("secret"),
        max_retries=5,
        base_backoff_seconds=0.001,  # Fast for tests
    )

    with pytest.raises(CCloudApiError) as exc_info:
        list(conn.get("/test"))

    assert exc_info.value.status_code == 429


@responses.activate
def test_get_timeout_then_success():
    # First call times out, second succeeds
    responses.add(
        responses.GET,
        "https://api.confluent.cloud/test",
        body=requests.exceptions.Timeout("Connection timed out"),
    )
    responses.add(
        responses.GET,
        "https://api.confluent.cloud/test",
        json={"data": [{"id": "1"}], "metadata": {}},
        status=200,
    )

    conn = CCloudConnection(
        api_key="key",
        api_secret=SecretStr("secret"),
        base_backoff_seconds=0.001,
    )
    items = list(conn.get("/test"))

    assert items == [{"id": "1"}]


@responses.activate
def test_post_success():
    responses.add(
        responses.POST,
        "https://api.confluent.cloud/test/create",
        json={"id": "new-123", "status": "created"},
        status=200,
    )

    conn = CCloudConnection(api_key="key", api_secret=SecretStr("secret"))
    result = conn.post("/test/create", json={"name": "test-resource"})

    assert result == {"id": "new-123", "status": "created"}


@responses.activate
def test_post_error():
    responses.add(
        responses.POST,
        "https://api.confluent.cloud/test/create",
        status=400,
        body="Bad Request",
    )

    conn = CCloudConnection(api_key="key", api_secret=SecretStr("secret"))

    with pytest.raises(CCloudApiError) as exc_info:
        conn.post("/test/create", json={})

    assert exc_info.value.status_code == 400


@responses.activate
def test_get_429_with_ratelimit_reset_header():
    """Test Confluent-specific rateLimit-reset header (relative seconds, not Unix timestamp).

    Per Confluent Cloud API docs: https://api.telemetry.confluent.cloud/docs
    The rateLimit-reset header contains relative seconds until window resets.
    """
    responses.add(
        responses.GET,
        "https://api.confluent.cloud/test",
        status=429,
        headers={"rateLimit-reset": "0.05"},  # Relative seconds, per Confluent docs
    )
    responses.add(
        responses.GET,
        "https://api.confluent.cloud/test",
        json={"data": [{"id": "1"}], "metadata": {}},
        status=200,
    )

    conn = CCloudConnection(api_key="key", api_secret=SecretStr("secret"))
    start = time.monotonic()
    items = list(conn.get("/test"))
    elapsed = time.monotonic() - start

    assert items == [{"id": "1"}]
    assert elapsed >= 0.05  # Waited for rateLimit-reset seconds


@responses.activate
def test_get_429_with_pascal_case_ratelimit_reset_header():
    """Test RateLimit-Reset header variant (pascal-case).

    Some Confluent APIs may use this casing. Defensive handling ensures we
    respect rate limit signals regardless of header casing.
    """
    responses.add(
        responses.GET,
        "https://api.confluent.cloud/test",
        status=429,
        headers={"RateLimit-Reset": "0.05"},
    )
    responses.add(
        responses.GET,
        "https://api.confluent.cloud/test",
        json={"data": [{"id": "1"}], "metadata": {}},
        status=200,
    )

    conn = CCloudConnection(api_key="key", api_secret=SecretStr("secret"))
    start = time.monotonic()
    items = list(conn.get("/test"))
    elapsed = time.monotonic() - start

    assert items == [{"id": "1"}]
    assert elapsed >= 0.05  # Waited for RateLimit-Reset seconds


@responses.activate
def test_connection_error_raises_ccloud_connection_error():
    responses.add(
        responses.GET,
        "https://api.confluent.cloud/test",
        body=requests.exceptions.ConnectionError("Connection refused"),
    )

    conn = CCloudConnection(api_key="key", api_secret=SecretStr("secret"))

    with pytest.raises(CCloudConnectionError):
        list(conn.get("/test"))


@responses.activate
def test_request_has_basic_auth_header():
    responses.add(
        responses.GET,
        "https://api.confluent.cloud/test",
        json={"data": [], "metadata": {}},
        status=200,
    )

    conn = CCloudConnection(api_key="mykey", api_secret=SecretStr("mysecret"))
    list(conn.get("/test"))

    sent_request = responses.calls[0].request
    expected = base64.b64encode(b"mykey:mysecret").decode()
    assert sent_request.headers["Authorization"] == f"Basic {expected}"


@responses.activate
def test_get_data_null_returns_empty():
    responses.add(
        responses.GET,
        "https://api.confluent.cloud/test",
        json={"data": None, "metadata": {}},
        status=200,
    )

    conn = CCloudConnection(api_key="key", api_secret=SecretStr("secret"))
    items = list(conn.get("/test"))

    assert items == []


def test_connection_uses_session_for_pooling():
    """Verify that CCloudConnection uses requests.Session for connection pooling."""
    conn = CCloudConnection(api_key="key", api_secret=SecretStr("secret"))

    # Verify session is created
    assert hasattr(conn, "_session")
    assert conn._session is not None

    # Verify auth is set on session
    assert conn._session.auth is not None


def test_connection_close():
    """Verify close() properly closes the underlying session."""
    conn = CCloudConnection(api_key="key", api_secret=SecretStr("secret"))

    with patch.object(conn._session, "close") as mock_close:
        conn.close()
        mock_close.assert_called_once()


# =============================================================================
# get_raw() tests (Chunk 2.2)
# =============================================================================


class TestGetRaw:
    """Tests for CCloudConnection.get_raw() method."""

    @responses.activate
    def test_get_raw_returns_full_response(self):
        """get_raw() returns the full JSON response without pagination."""
        responses.add(
            responses.GET,
            "https://api.confluent.cloud/test/raw",
            json={"my-connector": {"info": {"config": {"name": "my-connector"}}}},
            status=200,
        )

        conn = CCloudConnection(api_key="key", api_secret=SecretStr("secret"))
        result = conn.get_raw("/test/raw")

        assert "my-connector" in result
        assert result["my-connector"]["info"]["config"]["name"] == "my-connector"

    @responses.activate
    def test_get_raw_retries_on_429(self):
        """get_raw() retries on rate limit just like get()."""
        responses.add(responses.GET, "https://api.confluent.cloud/test/raw", status=429)
        responses.add(
            responses.GET,
            "https://api.confluent.cloud/test/raw",
            json={"data": "ok"},
            status=200,
        )

        conn = CCloudConnection(
            api_key="key",
            api_secret=SecretStr("secret"),
            base_backoff_seconds=0.01,
        )
        result = conn.get_raw("/test/raw")
        assert result == {"data": "ok"}

    @responses.activate
    def test_get_raw_returns_empty_dict_on_404(self):
        """get_raw() returns {} on 404 (not the standard envelope)."""
        responses.add(
            responses.GET,
            "https://api.confluent.cloud/test/missing",
            status=404,
            body="Not found",
        )

        conn = CCloudConnection(api_key="key", api_secret=SecretStr("secret"))
        result = conn.get_raw("/test/missing")

        # Should return {} for non-standard endpoints, not the envelope
        assert result == {}

    @responses.activate
    def test_get_raw_with_params(self):
        """get_raw() passes query parameters correctly."""
        responses.add(
            responses.GET,
            "https://api.confluent.cloud/test/endpoint",
            json={"result": "ok"},
            status=200,
        )

        conn = CCloudConnection(api_key="key", api_secret=SecretStr("secret"))
        result = conn.get_raw("/test/endpoint", params={"expand": "info,status"})

        assert result == {"result": "ok"}
        assert "expand=info" in responses.calls[0].request.url


# =============================================================================
# Proactive throttling tests (Chunk 2.2)
# =============================================================================


class TestProactiveThrottling:
    """Tests for request_interval_seconds proactive throttling."""

    @responses.activate
    def test_throttling_spaces_requests(self):
        """request_interval_seconds introduces delay between requests."""
        for _ in range(3):
            responses.add(
                responses.GET,
                "https://api.confluent.cloud/test",
                json={"data": [], "metadata": {}},
                status=200,
            )

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

    @responses.activate
    def test_throttling_disabled_when_zero(self):
        """request_interval_seconds=0 disables throttling."""
        for _ in range(3):
            responses.add(
                responses.GET,
                "https://api.confluent.cloud/test",
                json={"data": [], "metadata": {}},
                status=200,
            )

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
