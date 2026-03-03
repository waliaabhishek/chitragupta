from __future__ import annotations

import pytest
from pydantic import ValidationError

# ===========================================================================
# MetricsConnectionConfig validation tests
# ===========================================================================


class TestMetricsConnectionConfigValidation:
    def test_basic_auth_missing_username_raises(self) -> None:
        from core.metrics.config import MetricsConnectionConfig

        with pytest.raises((ValueError, ValidationError)):
            MetricsConnectionConfig(url="http://prom:9090", auth_type="basic", password="secret")

    def test_basic_auth_missing_password_raises(self) -> None:
        from core.metrics.config import MetricsConnectionConfig

        with pytest.raises((ValueError, ValidationError)):
            MetricsConnectionConfig(url="http://prom:9090", auth_type="basic", username="user")

    def test_bearer_auth_missing_token_raises(self) -> None:
        from core.metrics.config import MetricsConnectionConfig

        with pytest.raises((ValueError, ValidationError)):
            MetricsConnectionConfig(url="http://prom:9090", auth_type="bearer")

    def test_none_auth_with_password_raises(self) -> None:
        from core.metrics.config import MetricsConnectionConfig

        with pytest.raises((ValueError, ValidationError)):
            MetricsConnectionConfig(url="http://prom:9090", auth_type="none", password="oops")

    def test_none_auth_no_credentials_valid(self) -> None:
        from core.metrics.config import MetricsConnectionConfig

        cfg = MetricsConnectionConfig(url="http://prom:9090", auth_type="none")
        assert cfg.url == "http://prom:9090"
        assert cfg.auth_type == "none"
        assert cfg.username is None
        assert cfg.password is None
        assert cfg.bearer_token is None

    def test_basic_auth_with_credentials_valid(self) -> None:
        from core.metrics.config import MetricsConnectionConfig

        cfg = MetricsConnectionConfig(
            url="http://prom:9090",
            auth_type="basic",
            username="user",
            password="pass",
        )
        assert cfg.auth_type == "basic"
        assert cfg.username == "user"

    def test_bearer_auth_with_token_valid(self) -> None:
        from core.metrics.config import MetricsConnectionConfig

        cfg = MetricsConnectionConfig(
            url="http://prom:9090",
            auth_type="bearer",
            bearer_token="mytoken",
        )
        assert cfg.auth_type == "bearer"

    def test_default_auth_type_is_none(self) -> None:
        from core.metrics.config import MetricsConnectionConfig

        cfg = MetricsConnectionConfig(url="http://prom:9090")
        assert cfg.auth_type == "none"

    def test_none_auth_with_username_raises(self) -> None:
        from core.metrics.config import MetricsConnectionConfig

        with pytest.raises((ValueError, ValidationError)):
            MetricsConnectionConfig(url="http://prom:9090", auth_type="none", username="user")

    def test_none_auth_with_bearer_token_raises(self) -> None:
        from core.metrics.config import MetricsConnectionConfig

        with pytest.raises((ValueError, ValidationError)):
            MetricsConnectionConfig(url="http://prom:9090", auth_type="none", bearer_token="tok")


# ===========================================================================
# create_metrics_source() factory tests
# ===========================================================================


class TestCreateMetricsSource:
    def test_none_auth_returns_prometheus_source_with_no_auth(self) -> None:
        from core.metrics.config import MetricsConnectionConfig, create_metrics_source
        from core.metrics.prometheus import PrometheusMetricsSource

        cfg = MetricsConnectionConfig(url="http://prom:9090", auth_type="none")
        source = create_metrics_source(cfg)

        assert isinstance(source, PrometheusMetricsSource)
        assert source._config.auth is None

    def test_basic_auth_builds_auth_config_with_unwrapped_secret(self) -> None:
        from core.metrics.config import MetricsConnectionConfig, create_metrics_source
        from core.metrics.prometheus import PrometheusMetricsSource

        cfg = MetricsConnectionConfig(
            url="http://prom:9090",
            auth_type="basic",
            username="myuser",
            password="mysecret",
        )
        source = create_metrics_source(cfg)

        assert isinstance(source, PrometheusMetricsSource)
        assert source._config.auth is not None
        assert source._config.auth.type == "basic"
        assert source._config.auth.username == "myuser"
        assert source._config.auth.password == "mysecret"  # unwrapped from SecretStr

    def test_bearer_auth_builds_auth_config_with_unwrapped_token(self) -> None:
        from core.metrics.config import MetricsConnectionConfig, create_metrics_source
        from core.metrics.prometheus import PrometheusMetricsSource

        cfg = MetricsConnectionConfig(
            url="http://prom:9090",
            auth_type="bearer",
            bearer_token="mytoken",
        )
        source = create_metrics_source(cfg)

        assert isinstance(source, PrometheusMetricsSource)
        assert source._config.auth is not None
        assert source._config.auth.type == "bearer"
        assert source._config.auth.token == "mytoken"  # unwrapped from SecretStr

    def test_url_propagated_to_prometheus_config(self) -> None:
        from core.metrics.config import MetricsConnectionConfig, create_metrics_source

        cfg = MetricsConnectionConfig(url="http://custom-host:8080", auth_type="none")
        source = create_metrics_source(cfg)

        assert source._config.url == "http://custom-host:8080"


# ===========================================================================
# Integration smoke tests
# ===========================================================================


class TestIntegrationSmoke:
    def test_ccloud_plugin_config_parses_metrics_field(self) -> None:
        from core.metrics.config import MetricsConnectionConfig
        from plugins.confluent_cloud.config import CCloudPluginConfig

        config = CCloudPluginConfig.from_plugin_settings(
            {
                "ccloud_api": {"key": "k", "secret": "s"},
                "metrics": {"url": "http://prom:9090", "auth_type": "none"},
            }
        )

        assert config.metrics is not None
        assert isinstance(config.metrics, MetricsConnectionConfig)
        assert config.metrics.url == "http://prom:9090"
        assert config.metrics.auth_type == "none"

    def test_self_managed_kafka_config_parses_metrics_with_basic_auth(self) -> None:
        from core.metrics.config import MetricsConnectionConfig
        from plugins.self_managed_kafka.config import SelfManagedKafkaConfig

        config = SelfManagedKafkaConfig.from_plugin_settings(
            {
                "cluster_id": "kafka-001",
                "broker_count": 3,
                "cost_model": {
                    "compute_hourly_rate": "0.10",
                    "storage_per_gib_hourly": "0.0001",
                    "network_ingress_per_gib": "0.01",
                    "network_egress_per_gib": "0.02",
                },
                "metrics": {
                    "url": "http://prom:9090",
                    "auth_type": "basic",
                    "username": "u",
                    "password": "p",
                },
            }
        )

        assert isinstance(config.metrics, MetricsConnectionConfig)
        assert config.metrics.auth_type == "basic"
        assert config.metrics.username == "u"
