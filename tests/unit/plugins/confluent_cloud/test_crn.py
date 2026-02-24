from __future__ import annotations

from plugins.confluent_cloud.crn import parse_ccloud_crn


def test_full_crn():
    result = parse_ccloud_crn(
        "crn://confluent.cloud/organization=abc/environment=env-xyz/kafka=lkc-123"
    )
    assert result == {"organization": "abc", "environment": "env-xyz", "kafka": "lkc-123"}


def test_org_only_crn():
    result = parse_ccloud_crn("crn://confluent.cloud/organization=abc")
    assert result == {"organization": "abc"}


def test_empty_string():
    result = parse_ccloud_crn("")
    assert result == {}


def test_invalid_format():
    result = parse_ccloud_crn("not-a-crn")
    assert result == {}


def test_crn_without_prefix():
    result = parse_ccloud_crn("/organization=abc/kafka=lkc-123")
    assert result == {"organization": "abc", "kafka": "lkc-123"}


def test_crn_with_flink_compute_pool():
    result = parse_ccloud_crn(
        "crn://confluent.cloud/organization=org-123/environment=env-abc/flink-compute-pool=lfcp-xyz"
    )
    assert result == {
        "organization": "org-123",
        "environment": "env-abc",
        "flink-compute-pool": "lfcp-xyz",
    }
