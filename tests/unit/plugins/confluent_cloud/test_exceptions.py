from __future__ import annotations


def test_ccloud_api_error_has_status_code():
    from plugins.confluent_cloud.exceptions import CCloudApiError

    err = CCloudApiError(status_code=429, message="Rate limited")
    assert err.status_code == 429
    assert "Rate limited" in str(err)


def test_ccloud_connection_error():
    from plugins.confluent_cloud.exceptions import CCloudConnectionError

    err = CCloudConnectionError("DNS resolution failed")
    assert "DNS resolution failed" in str(err)
