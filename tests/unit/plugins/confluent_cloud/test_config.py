from __future__ import annotations

import pytest
from pydantic import ValidationError


def test_minimal_config():
    from plugins.confluent_cloud.config import CCloudPluginConfig

    config = CCloudPluginConfig.from_plugin_settings(
        {
            "ccloud_api": {"key": "mykey", "secret": "mysecret"},
        }
    )
    assert config.ccloud_api.key == "mykey"
    assert config.ccloud_api.secret.get_secret_value() == "mysecret"
    assert config.billing_api.days_per_query == 15


def test_full_config():
    from plugins.confluent_cloud.config import CCloudPluginConfig

    config = CCloudPluginConfig.from_plugin_settings(
        {
            "ccloud_api": {"key": "k", "secret": "s"},
            "billing_api": {"days_per_query": 7},
            "metrics": {
                "type": "prometheus",
                "url": "http://prom:9090",
                "auth_type": "basic",
                "username": "user",
                "password": "pass",
            },
            "flink": [
                {"region_id": "us-east-1", "key": "fk1", "secret": "fs1"},
                {"region_id": "us-west-2", "key": "fk2", "secret": "fs2"},
            ],
            "allocator_params": {"kafka_cku_shared_ratio": 0.3},
        }
    )
    assert config.billing_api.days_per_query == 7
    assert config.metrics is not None
    assert config.metrics.url == "http://prom:9090"
    assert len(config.flink) == 2
    assert config.allocator_params["kafka_cku_shared_ratio"] == 0.3


def test_missing_ccloud_api_raises():
    from plugins.confluent_cloud.config import CCloudPluginConfig

    with pytest.raises(ValidationError):
        CCloudPluginConfig.from_plugin_settings({})


def test_invalid_days_per_query():
    from plugins.confluent_cloud.config import CCloudPluginConfig

    with pytest.raises(ValidationError):
        CCloudPluginConfig.from_plugin_settings(
            {
                "ccloud_api": {"key": "k", "secret": "s"},
                "billing_api": {"days_per_query": 0},
            }
        )

    with pytest.raises(ValidationError):
        CCloudPluginConfig.from_plugin_settings(
            {
                "ccloud_api": {"key": "k", "secret": "s"},
                "billing_api": {"days_per_query": 31},
            }
        )


def test_metrics_basic_auth_requires_credentials():
    from plugins.confluent_cloud.config import CCloudPluginConfig

    with pytest.raises(ValidationError):
        CCloudPluginConfig.from_plugin_settings(
            {
                "ccloud_api": {"key": "k", "secret": "s"},
                "metrics": {
                    "type": "prometheus",
                    "url": "http://prom:9090",
                    "auth_type": "basic",
                    # Missing username/password
                },
            }
        )


def test_secret_not_in_repr():
    from plugins.confluent_cloud.config import CCloudPluginConfig

    config = CCloudPluginConfig.from_plugin_settings(
        {
            "ccloud_api": {"key": "mykey", "secret": "supersecret"},
        }
    )
    repr_str = repr(config)
    assert "supersecret" not in repr_str


def test_metrics_auth_none_with_credentials_raises():
    from plugins.confluent_cloud.config import CCloudPluginConfig

    with pytest.raises(ValidationError):
        CCloudPluginConfig.from_plugin_settings(
            {
                "ccloud_api": {"key": "k", "secret": "s"},
                "metrics": {
                    "type": "prometheus",
                    "url": "http://prom:9090",
                    "auth_type": "none",
                    "username": "user",  # Contradictory!
                },
            }
        )


def test_allocator_params_ratio_must_be_numeric():
    from plugins.confluent_cloud.config import CCloudPluginConfig

    with pytest.raises(ValidationError):
        CCloudPluginConfig.from_plugin_settings(
            {
                "ccloud_api": {"key": "k", "secret": "s"},
                "allocator_params": {"kafka_cku_shared_ratio": "0.3"},  # String, not float
            }
        )


def test_allocator_params_non_ratio_can_be_string():
    from plugins.confluent_cloud.config import CCloudPluginConfig

    config = CCloudPluginConfig.from_plugin_settings(
        {
            "ccloud_api": {"key": "k", "secret": "s"},
            "allocator_params": {"default_strategy": "usage_ratio"},  # String OK for non-ratio
        }
    )
    assert config.allocator_params["default_strategy"] == "usage_ratio"


def test_metrics_bearer_auth_requires_token():
    from plugins.confluent_cloud.config import CCloudPluginConfig

    with pytest.raises(ValidationError):
        CCloudPluginConfig.from_plugin_settings(
            {
                "ccloud_api": {"key": "k", "secret": "s"},
                "metrics": {
                    "type": "prometheus",
                    "url": "http://prom:9090",
                    "auth_type": "bearer",
                    # Missing bearer_token
                },
            }
        )


def test_cku_ratios_invalid_both_set_wrong_sum() -> None:
    from plugins.confluent_cloud.config import CCloudPluginConfig

    with pytest.raises(ValidationError, match="must sum to 1.0"):
        CCloudPluginConfig.from_plugin_settings(
            {
                "ccloud_api": {"key": "k", "secret": "s"},
                "allocator_params": {
                    "kafka_cku_usage_ratio": 0.60,
                    "kafka_cku_shared_ratio": 0.20,
                },
            }
        )


def test_cku_ratios_invalid_one_set_wrong_sum() -> None:
    from plugins.confluent_cloud.config import CCloudPluginConfig

    # usage=0.60, shared defaults to 0.30 → total 0.90
    with pytest.raises(ValidationError, match="must sum to 1.0"):
        CCloudPluginConfig.from_plugin_settings(
            {
                "ccloud_api": {"key": "k", "secret": "s"},
                "allocator_params": {
                    "kafka_cku_usage_ratio": 0.60,
                },
            }
        )


def test_cku_ratios_valid() -> None:
    from plugins.confluent_cloud.config import CCloudPluginConfig

    config = CCloudPluginConfig.from_plugin_settings(
        {
            "ccloud_api": {"key": "k", "secret": "s"},
            "allocator_params": {
                "kafka_cku_usage_ratio": 0.80,
                "kafka_cku_shared_ratio": 0.20,
            },
        }
    )
    assert config.allocator_params["kafka_cku_usage_ratio"] == 0.80
    assert config.allocator_params["kafka_cku_shared_ratio"] == 0.20


def test_cku_ratios_none_set_passes() -> None:
    from plugins.confluent_cloud.config import CCloudPluginConfig

    config = CCloudPluginConfig.from_plugin_settings(
        {
            "ccloud_api": {"key": "k", "secret": "s"},
            "allocator_params": {},
        }
    )
    assert "kafka_cku_usage_ratio" not in config.allocator_params
    assert "kafka_cku_shared_ratio" not in config.allocator_params


def test_cku_ratios_boundary_within_tolerance() -> None:
    from plugins.confluent_cloud.config import CCloudPluginConfig

    # sum = 1.00001, within 0.0001 tolerance → should pass
    config = CCloudPluginConfig.from_plugin_settings(
        {
            "ccloud_api": {"key": "k", "secret": "s"},
            "allocator_params": {
                "kafka_cku_usage_ratio": 0.70001,
                "kafka_cku_shared_ratio": 0.30,
            },
        }
    )
    assert config.allocator_params["kafka_cku_usage_ratio"] == 0.70001


def test_cku_ratios_boundary_outside_tolerance() -> None:
    from plugins.confluent_cloud.config import CCloudPluginConfig

    # sum = 1.001, outside 0.0001 tolerance → should fail
    with pytest.raises(ValidationError, match="must sum to 1.0"):
        CCloudPluginConfig.from_plugin_settings(
            {
                "ccloud_api": {"key": "k", "secret": "s"},
                "allocator_params": {
                    "kafka_cku_usage_ratio": 0.701,
                    "kafka_cku_shared_ratio": 0.30,
                },
            }
        )
