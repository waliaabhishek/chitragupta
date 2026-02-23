from __future__ import annotations

import pytest
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
