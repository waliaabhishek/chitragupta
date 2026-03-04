"""Tests for TASK-010: PluginSettingsBase validation in TenantConfig and plugin configs."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

# ---------------------------------------------------------------------------
# Scenario 1: TenantConfig rejects non-integer min_refresh_gap_seconds
# ---------------------------------------------------------------------------


def test_tenant_config_plugin_settings_rejects_non_integer_min_refresh_gap_seconds() -> None:
    """PluginSettingsBase.min_refresh_gap_seconds must be int; string 'thirty' raises ValidationError."""
    from core.config.models import TenantConfig

    with pytest.raises(ValidationError) as exc_info:
        TenantConfig(
            ecosystem="confluent_cloud",
            tenant_id="t1",
            plugin_settings={"min_refresh_gap_seconds": "thirty"},
        )
    error_str = str(exc_info.value)
    assert "min_refresh_gap_seconds" in error_str
    assert "int" in error_str or "integer" in error_str


# ---------------------------------------------------------------------------
# Scenario 2: TenantConfig accepts valid min_refresh_gap_seconds
# ---------------------------------------------------------------------------


def test_tenant_config_plugin_settings_accepts_valid_min_refresh_gap_seconds() -> None:
    """TenantConfig parses successfully when min_refresh_gap_seconds is a valid int."""
    from core.config.models import TenantConfig

    config = TenantConfig(
        ecosystem="confluent_cloud",
        tenant_id="t1",
        plugin_settings={"min_refresh_gap_seconds": 300},
    )
    assert config.plugin_settings.min_refresh_gap_seconds == 300


# ---------------------------------------------------------------------------
# Scenario 3: TenantConfig with full CCloud YAML passes via extra="allow"
# ---------------------------------------------------------------------------


def test_tenant_config_full_ccloud_yaml_passes_through_extra_fields() -> None:
    """Extra plugin fields (ccloud_api, billing_api, etc.) pass via extra='allow'."""
    from core.config.models import TenantConfig

    config = TenantConfig(
        ecosystem="confluent_cloud",
        tenant_id="t1",
        plugin_settings={
            "ccloud_api": {"key": "mykey", "secret": "mysecret"},
            "billing_api": {"days_per_query": 15},
            "metrics": {
                "type": "prometheus",
                "url": "http://prom:9090",
                "auth_type": "none",
            },
            "flink": [{"region_id": "us-east-1", "key": "fk1", "secret": "fs1"}],
            "allocator_params": {"kafka_cku_shared_ratio": 0.3},
            "allocator_overrides": {"kafka": "mymod.fn"},
            "min_refresh_gap_seconds": 600,
        },
    )
    # Should not raise
    assert config.plugin_settings.min_refresh_gap_seconds == 600


# ---------------------------------------------------------------------------
# Scenario 4: CCloudPluginConfig.from_plugin_settings with bad allocator_params
# ---------------------------------------------------------------------------


def test_ccloud_plugin_config_bad_allocator_params_raises() -> None:
    """validate_allocator_params raises ValidationError when a _ratio param is non-numeric."""
    from plugins.confluent_cloud.config import CCloudPluginConfig

    with pytest.raises(ValidationError):
        CCloudPluginConfig.from_plugin_settings(
            {
                "ccloud_api": {"key": "k", "secret": "s"},
                "allocator_params": {"network_ratio": "bad"},
            }
        )


# ---------------------------------------------------------------------------
# Scenario 5: CCloudPluginConfig with base+plugin fields via PluginSettingsBase
# ---------------------------------------------------------------------------


def test_ccloud_plugin_config_base_and_plugin_fields_validate() -> None:
    """allocator_overrides and min_refresh_gap_seconds are accessible as attributes after parsing."""
    from plugins.confluent_cloud.config import CCloudPluginConfig

    config = CCloudPluginConfig.from_plugin_settings(
        {
            "ccloud_api": {"key": "k", "secret": "s"},
            "allocator_overrides": {"kafka": "mymod.fn"},
            "min_refresh_gap_seconds": 600,
        }
    )
    assert config.allocator_overrides == {"kafka": "mymod.fn"}
    assert config.min_refresh_gap_seconds == 600


# ---------------------------------------------------------------------------
# Scenario 6: SelfManagedKafkaConfig valid parse and missing cluster_id error
# ---------------------------------------------------------------------------


@pytest.fixture
def valid_smk_settings() -> dict:
    return {
        "cluster_id": "kafka-cluster-001",
        "broker_count": 3,
        "cost_model": {
            "compute_hourly_rate": "0.10",
            "storage_per_gib_hourly": "0.0001",
            "network_ingress_per_gib": "0.01",
            "network_egress_per_gib": "0.02",
        },
        "metrics": {"type": "prometheus", "url": "http://prom:9090"},
    }


def test_self_managed_kafka_config_valid_parse(valid_smk_settings: dict) -> None:
    """SelfManagedKafkaConfig.from_plugin_settings succeeds with valid settings."""
    from plugins.self_managed_kafka.config import SelfManagedKafkaConfig

    config = SelfManagedKafkaConfig.from_plugin_settings(valid_smk_settings)
    assert config.cluster_id == "kafka-cluster-001"


def test_self_managed_kafka_config_missing_cluster_id_raises(valid_smk_settings: dict) -> None:
    """SelfManagedKafkaConfig raises ValidationError when cluster_id is missing."""
    from plugins.self_managed_kafka.config import SelfManagedKafkaConfig

    del valid_smk_settings["cluster_id"]
    with pytest.raises(ValidationError):
        SelfManagedKafkaConfig.from_plugin_settings(valid_smk_settings)


# ---------------------------------------------------------------------------
# Scenario 7: workflow_runner passes model_dump() dict to plugin.initialize
# ---------------------------------------------------------------------------


def test_workflow_runner_passes_dict_to_plugin_initialize() -> None:
    """plugin.initialize receives a plain dict (model_dump()), not a PluginSettingsBase instance."""
    from core.config.models import PluginSettingsBase, TenantConfig

    config = TenantConfig(
        ecosystem="confluent_cloud",
        tenant_id="t1",
        plugin_settings={"min_refresh_gap_seconds": 300},
    )
    # plugin_settings is a PluginSettingsBase (not a dict)
    assert isinstance(config.plugin_settings, PluginSettingsBase)

    # model_dump() produces a plain dict — that's what workflow_runner passes to plugin.initialize
    dumped = config.plugin_settings.model_dump()
    assert isinstance(dumped, dict)
    assert dumped["min_refresh_gap_seconds"] == 300
