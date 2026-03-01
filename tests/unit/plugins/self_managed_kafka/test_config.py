"""Tests for self-managed Kafka plugin configuration models."""

from __future__ import annotations

from decimal import Decimal

import pytest
from pydantic import ValidationError


@pytest.fixture
def base_cost_model() -> dict:
    return {
        "compute_hourly_rate": "0.10",
        "storage_per_gib_hourly": "0.0001",
        "network_ingress_per_gib": "0.01",
        "network_egress_per_gib": "0.02",
    }


@pytest.fixture
def base_metrics() -> dict:
    return {"type": "prometheus", "url": "http://prom:9090"}


@pytest.fixture
def base_settings(base_cost_model, base_metrics) -> dict:
    return {
        "cluster_id": "kafka-cluster-001",
        "broker_count": 3,
        "cost_model": base_cost_model,
        "metrics": base_metrics,
    }


class TestCostModelConfig:
    def test_valid_cost_model(self, base_cost_model):
        from plugins.self_managed_kafka.config import CostModelConfig

        model = CostModelConfig.model_validate(base_cost_model)
        assert model.compute_hourly_rate == Decimal("0.10")
        assert model.storage_per_gib_hourly == Decimal("0.0001")
        assert model.network_ingress_per_gib == Decimal("0.01")
        assert model.network_egress_per_gib == Decimal("0.02")

    def test_decimal_precision_preserved(self):
        from plugins.self_managed_kafka.config import CostModelConfig

        model = CostModelConfig.model_validate(
            {
                "compute_hourly_rate": "0.123456789",
                "storage_per_gib_hourly": "0.000012345",
                "network_ingress_per_gib": "0.001",
                "network_egress_per_gib": "0.002",
            }
        )
        assert model.compute_hourly_rate == Decimal("0.123456789")
        assert model.storage_per_gib_hourly == Decimal("0.000012345")

    def test_region_overrides_empty_by_default(self, base_cost_model):
        from plugins.self_managed_kafka.config import CostModelConfig

        model = CostModelConfig.model_validate(base_cost_model)
        assert model.region_overrides == {}

    def test_region_overrides_parsed(self, base_cost_model):
        from plugins.self_managed_kafka.config import CostModelConfig

        base_cost_model["region_overrides"] = {
            "us-west-2": {"compute_hourly_rate": "0.08"},
        }
        model = CostModelConfig.model_validate(base_cost_model)
        assert model.region_overrides["us-west-2"].compute_hourly_rate == Decimal("0.08")
        assert model.region_overrides["us-west-2"].storage_per_gib_hourly is None

    def test_missing_required_fields_raises(self):
        from plugins.self_managed_kafka.config import CostModelConfig

        with pytest.raises(ValidationError):
            CostModelConfig.model_validate({"compute_hourly_rate": "0.10"})


class TestMetricsConfig:
    def test_no_auth(self, base_metrics):
        from plugins.self_managed_kafka.config import MetricsConfig

        config = MetricsConfig.model_validate(base_metrics)
        assert config.auth_type == "none"
        assert config.username is None

    def test_basic_auth(self):
        from plugins.self_managed_kafka.config import MetricsConfig

        config = MetricsConfig.model_validate(
            {
                "url": "http://prom:9090",
                "auth_type": "basic",
                "username": "user",
                "password": "pass",
            }
        )
        assert config.username == "user"
        assert config.password is not None
        assert config.password.get_secret_value() == "pass"

    def test_basic_auth_missing_password_raises(self):
        from plugins.self_managed_kafka.config import MetricsConfig

        with pytest.raises(ValidationError, match="password required"):
            MetricsConfig.model_validate({"url": "http://prom:9090", "auth_type": "basic", "username": "user"})

    def test_bearer_auth(self):
        from plugins.self_managed_kafka.config import MetricsConfig

        config = MetricsConfig.model_validate(
            {"url": "http://prom:9090", "auth_type": "bearer", "bearer_token": "tok123"}
        )
        assert config.bearer_token is not None
        assert config.bearer_token.get_secret_value() == "tok123"

    def test_bearer_auth_missing_token_raises(self):
        from plugins.self_managed_kafka.config import MetricsConfig

        with pytest.raises(ValidationError, match="bearer_token required"):
            MetricsConfig.model_validate({"url": "http://prom:9090", "auth_type": "bearer"})

    def test_none_auth_with_credentials_raises(self):
        from plugins.self_managed_kafka.config import MetricsConfig

        with pytest.raises(ValidationError, match="credentials provided"):
            MetricsConfig.model_validate({"url": "http://prom:9090", "auth_type": "none", "username": "oops"})


class TestIdentitySourceConfig:
    def test_default_prometheus_source(self):
        from plugins.self_managed_kafka.config import IdentitySourceConfig

        config = IdentitySourceConfig.model_validate({})
        assert config.source == "prometheus"
        assert config.default_team == "UNASSIGNED"
        assert config.principal_to_team == {}
        assert config.static_identities == []

    def test_static_source(self):
        from plugins.self_managed_kafka.config import IdentitySourceConfig

        config = IdentitySourceConfig.model_validate(
            {
                "source": "static",
                "static_identities": [
                    {"identity_id": "team-data", "identity_type": "team", "display_name": "Data Team"},
                ],
            }
        )
        assert config.source == "static"
        assert len(config.static_identities) == 1
        assert config.static_identities[0].identity_id == "team-data"

    def test_both_source(self):
        from plugins.self_managed_kafka.config import IdentitySourceConfig

        config = IdentitySourceConfig.model_validate({"source": "both"})
        assert config.source == "both"

    def test_principal_to_team_mapping(self):
        from plugins.self_managed_kafka.config import IdentitySourceConfig

        config = IdentitySourceConfig.model_validate(
            {"principal_to_team": {"User:alice": "team-data", "User:bob": "team-analytics"}}
        )
        assert config.principal_to_team["User:alice"] == "team-data"

    def test_static_identity_config(self):
        from plugins.self_managed_kafka.config import StaticIdentityConfig

        cfg = StaticIdentityConfig.model_validate(
            {"identity_id": "User:alice", "identity_type": "principal", "display_name": "Alice", "team": "data"}
        )
        assert cfg.identity_id == "User:alice"
        assert cfg.team == "data"


class TestResourceSourceConfig:
    def test_default_prometheus_source(self):
        from plugins.self_managed_kafka.config import ResourceSourceConfig

        config = ResourceSourceConfig.model_validate({})
        assert config.source == "prometheus"
        assert config.bootstrap_servers is None

    def test_admin_api_source_requires_bootstrap_servers(self):
        from plugins.self_managed_kafka.config import ResourceSourceConfig

        with pytest.raises(ValidationError, match="bootstrap_servers required"):
            ResourceSourceConfig.model_validate({"source": "admin_api"})

    def test_admin_api_source_valid(self):
        from plugins.self_managed_kafka.config import ResourceSourceConfig

        config = ResourceSourceConfig.model_validate(
            {
                "source": "admin_api",
                "bootstrap_servers": "kafka:9092",
                "security_protocol": "SASL_SSL",
                "sasl_mechanism": "SCRAM-SHA-256",
                "sasl_username": "admin",
                "sasl_password": "secret",
            }
        )
        assert config.source == "admin_api"
        assert config.bootstrap_servers == "kafka:9092"
        assert config.sasl_mechanism == "SCRAM-SHA-256"

    def test_default_security_protocol(self):
        from plugins.self_managed_kafka.config import ResourceSourceConfig

        config = ResourceSourceConfig.model_validate({})
        assert config.security_protocol == "PLAINTEXT"


class TestSelfManagedKafkaConfig:
    def test_valid_full_config(self, base_settings):
        from plugins.self_managed_kafka.config import SelfManagedKafkaConfig

        config = SelfManagedKafkaConfig.from_plugin_settings(base_settings)
        assert config.cluster_id == "kafka-cluster-001"
        assert config.broker_count == 3
        assert config.region is None

    def test_broker_count_must_be_positive(self, base_settings):
        from plugins.self_managed_kafka.config import SelfManagedKafkaConfig

        base_settings["broker_count"] = 0
        with pytest.raises(ValidationError):
            SelfManagedKafkaConfig.from_plugin_settings(base_settings)

    def test_missing_metrics_raises(self, base_settings):
        from plugins.self_managed_kafka.config import SelfManagedKafkaConfig

        del base_settings["metrics"]
        with pytest.raises(ValidationError):
            SelfManagedKafkaConfig.from_plugin_settings(base_settings)

    def test_missing_cost_model_raises(self, base_settings):
        from plugins.self_managed_kafka.config import SelfManagedKafkaConfig

        del base_settings["cost_model"]
        with pytest.raises(ValidationError):
            SelfManagedKafkaConfig.from_plugin_settings(base_settings)

    def test_get_effective_cost_model_no_region(self, base_settings):
        from plugins.self_managed_kafka.config import SelfManagedKafkaConfig

        config = SelfManagedKafkaConfig.from_plugin_settings(base_settings)
        effective = config.get_effective_cost_model()
        assert effective.compute_hourly_rate == Decimal("0.10")

    def test_get_effective_cost_model_with_region_override(self, base_settings):
        from plugins.self_managed_kafka.config import SelfManagedKafkaConfig

        base_settings["region"] = "us-west-2"
        base_settings["cost_model"]["region_overrides"] = {"us-west-2": {"compute_hourly_rate": "0.08"}}
        config = SelfManagedKafkaConfig.from_plugin_settings(base_settings)
        effective = config.get_effective_cost_model()
        assert effective.compute_hourly_rate == Decimal("0.08")
        # Other rates use base values
        assert effective.storage_per_gib_hourly == Decimal("0.0001")

    def test_get_effective_cost_model_region_not_in_overrides(self, base_settings):
        from plugins.self_managed_kafka.config import SelfManagedKafkaConfig

        base_settings["region"] = "eu-west-1"
        base_settings["cost_model"]["region_overrides"] = {"us-west-2": {"compute_hourly_rate": "0.08"}}
        config = SelfManagedKafkaConfig.from_plugin_settings(base_settings)
        effective = config.get_effective_cost_model()
        assert effective.compute_hourly_rate == Decimal("0.10")

    def test_default_identity_source(self, base_settings):
        from plugins.self_managed_kafka.config import SelfManagedKafkaConfig

        config = SelfManagedKafkaConfig.from_plugin_settings(base_settings)
        assert config.identity_source.source == "prometheus"

    def test_default_resource_source(self, base_settings):
        from plugins.self_managed_kafka.config import SelfManagedKafkaConfig

        config = SelfManagedKafkaConfig.from_plugin_settings(base_settings)
        assert config.resource_source.source == "prometheus"


class TestCostModelConfigGiBFields:
    """Issue 2: config fields renamed from _per_gb to _per_gib."""

    def test_cost_model_config_has_storage_per_gib_hourly(self):
        from plugins.self_managed_kafka.config import CostModelConfig

        model = CostModelConfig.model_validate(
            {
                "compute_hourly_rate": "0.10",
                "storage_per_gib_hourly": "0.0001",
                "network_ingress_per_gib": "0.01",
                "network_egress_per_gib": "0.02",
            }
        )
        assert model.storage_per_gib_hourly == Decimal("0.0001")

    def test_cost_model_config_has_network_ingress_per_gib(self):
        from plugins.self_managed_kafka.config import CostModelConfig

        model = CostModelConfig.model_validate(
            {
                "compute_hourly_rate": "0.10",
                "storage_per_gib_hourly": "0.0001",
                "network_ingress_per_gib": "0.01",
                "network_egress_per_gib": "0.02",
            }
        )
        assert model.network_ingress_per_gib == Decimal("0.01")

    def test_cost_model_config_has_network_egress_per_gib(self):
        from plugins.self_managed_kafka.config import CostModelConfig

        model = CostModelConfig.model_validate(
            {
                "compute_hourly_rate": "0.10",
                "storage_per_gib_hourly": "0.0001",
                "network_ingress_per_gib": "0.01",
                "network_egress_per_gib": "0.02",
            }
        )
        assert model.network_egress_per_gib == Decimal("0.02")


class TestBytesPerGiBConstant:
    """Issue 2: _BYTES_PER_GB renamed to _BYTES_PER_GIB in cost_input."""

    def test_bytes_per_gib_constant_exists_and_correct(self):
        from plugins.self_managed_kafka.cost_input import _BYTES_PER_GIB

        assert Decimal("1073741824") == _BYTES_PER_GIB
