from __future__ import annotations

import pytest
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
