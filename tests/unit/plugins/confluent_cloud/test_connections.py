from __future__ import annotations

import time

import pytest
import requests
import responses
from pydantic import SecretStr


def test_ccloud_connection_construction():
    from plugins.confluent_cloud.connections import CCloudConnection

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
    from plugins.confluent_cloud.connections import CCloudConnection

    conn = CCloudConnection(
        api_key="key",
        api_secret=SecretStr("secret"),
        base_url="https://custom.api.com",
    )
    assert conn.base_url == "https://custom.api.com"


@responses.activate
def test_get_single_page():
    from plugins.confluent_cloud.connections import CCloudConnection

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
    from plugins.confluent_cloud.connections import CCloudConnection

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
    from plugins.confluent_cloud.connections import CCloudConnection

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
    from plugins.confluent_cloud.connections import CCloudConnection

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
    from plugins.confluent_cloud.connections import CCloudConnection
    from plugins.confluent_cloud.exceptions import CCloudApiError

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
    from plugins.confluent_cloud.connections import CCloudConnection

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
    from plugins.confluent_cloud.connections import CCloudConnection

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
    from plugins.confluent_cloud.connections import CCloudConnection
    from plugins.confluent_cloud.exceptions import CCloudApiError

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
    from plugins.confluent_cloud.connections import CCloudConnection

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
