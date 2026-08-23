"""Tests for self-managed Kafka plugin configuration models."""

from __future__ import annotations

import re
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
        "metrics_identifier": "kraft-a-001",
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
        from core.metrics.config import MetricsConnectionConfig

        config = MetricsConnectionConfig.model_validate(base_metrics)
        assert config.auth_type == "none"
        assert config.username is None

    def test_basic_auth(self):
        from core.metrics.config import MetricsConnectionConfig

        config = MetricsConnectionConfig.model_validate(
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
        from core.metrics.config import MetricsConnectionConfig

        with pytest.raises(ValidationError, match="password required"):
            MetricsConnectionConfig.model_validate(
                {"url": "http://prom:9090", "auth_type": "basic", "username": "user"}
            )

    def test_bearer_auth(self):
        from core.metrics.config import MetricsConnectionConfig

        config = MetricsConnectionConfig.model_validate(
            {"url": "http://prom:9090", "auth_type": "bearer", "bearer_token": "tok123"}
        )
        assert config.bearer_token is not None
        assert config.bearer_token.get_secret_value() == "tok123"

    def test_bearer_auth_missing_token_raises(self):
        from core.metrics.config import MetricsConnectionConfig

        with pytest.raises(ValidationError, match="bearer_token required"):
            MetricsConnectionConfig.model_validate({"url": "http://prom:9090", "auth_type": "bearer"})

    def test_none_auth_with_credentials_raises(self):
        from core.metrics.config import MetricsConnectionConfig

        with pytest.raises(ValidationError, match="credentials provided"):
            MetricsConnectionConfig.model_validate({"url": "http://prom:9090", "auth_type": "none", "username": "oops"})


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
        assert config.metrics_identifier == "kraft-a-001"
        assert config.metrics_identifier_label == "kafka_cluster_id"
        assert config.broker_count == 3
        assert config.region is None

    def test_metrics_identifier_is_required(self, base_settings):
        from plugins.self_managed_kafka.config import SelfManagedKafkaConfig

        del base_settings["metrics_identifier"]

        with pytest.raises(ValidationError) as error:
            SelfManagedKafkaConfig.from_plugin_settings(base_settings)

        assert error.value.errors()[0]["loc"] == ("metrics_identifier",)

    @pytest.mark.parametrize("field", ["metrics_identifier", "metrics_identifier_label"])
    def test_metrics_selector_values_must_not_be_blank(self, base_settings, field: str) -> None:
        from plugins.self_managed_kafka.config import SelfManagedKafkaConfig

        base_settings[field] = ""

        with pytest.raises(ValidationError) as error:
            SelfManagedKafkaConfig.from_plugin_settings(base_settings)

        assert error.value.errors()[0]["loc"] == (field,)

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

    def test_discovery_window_hours_accepts_valid_value(self, base_settings):
        """discovery_window_hours=24 is accepted."""
        from plugins.self_managed_kafka.config import SelfManagedKafkaConfig

        base_settings["discovery_window_hours"] = 24
        config = SelfManagedKafkaConfig.from_plugin_settings(base_settings)
        assert config.discovery_window_hours == 24

    def test_discovery_window_hours_rejects_zero(self, base_settings):
        """discovery_window_hours=0 raises ValidationError (must be gt=0)."""
        from plugins.self_managed_kafka.config import SelfManagedKafkaConfig

        base_settings["discovery_window_hours"] = 0
        with pytest.raises(ValidationError):
            SelfManagedKafkaConfig.from_plugin_settings(base_settings)

    def test_discovery_window_hours_defaults_to_one(self, base_settings):
        """discovery_window_hours defaults to 1 when not specified."""
        from plugins.self_managed_kafka.config import SelfManagedKafkaConfig

        config = SelfManagedKafkaConfig.from_plugin_settings(base_settings)
        assert config.discovery_window_hours == 1

    @pytest.mark.parametrize(
        ("configured_days", "expected_days"),
        [
            (None, 5),
            (1, 1),
            (17, 17),
            (30, 30),
        ],
    )
    def test_historical_acquisition_chunk_days_accepts_the_supported_range(
        self,
        base_settings: dict[str, object],
        configured_days: int | None,
        expected_days: int,
    ) -> None:
        from plugins.self_managed_kafka.config import SelfManagedKafkaConfig

        if configured_days is not None:
            base_settings["historical_acquisition_chunk_days"] = configured_days

        config = SelfManagedKafkaConfig.from_plugin_settings(base_settings)

        assert config.historical_acquisition_chunk_days == expected_days

    @pytest.mark.parametrize("configured_days", [0, 31, -1, 1.5, "seven"])
    def test_historical_acquisition_chunk_days_rejects_values_outside_the_supported_integer_range(
        self,
        base_settings: dict[str, object],
        configured_days: object,
    ) -> None:
        from plugins.self_managed_kafka.config import SelfManagedKafkaConfig

        base_settings["historical_acquisition_chunk_days"] = configured_days

        with pytest.raises(ValidationError) as raised:
            SelfManagedKafkaConfig.from_plugin_settings(base_settings)

        assert raised.value.errors()[0]["loc"] == ("historical_acquisition_chunk_days",)

    def test_telemetry_aliases_default_to_empty_mappings_and_preserve_the_canonical_selector(
        self, base_settings: dict[str, object]
    ) -> None:
        from plugins.self_managed_kafka.config import SelfManagedKafkaConfig

        config = SelfManagedKafkaConfig.from_plugin_settings(base_settings)

        assert config.metric_name_overrides == {}
        assert config.label_name_overrides == {}
        assert config.metrics_identifier == "kraft-a-001"
        assert config.metrics_identifier_label == "kafka_cluster_id"

    def test_telemetry_aliases_accept_partial_metric_and_label_mappings(self, base_settings: dict[str, object]) -> None:
        from plugins.self_managed_kafka.config import SelfManagedKafkaConfig

        base_settings["metric_name_overrides"] = {"kafka_log_log_size": "company_kafka_partition_size"}
        base_settings["label_name_overrides"] = {
            "kafka_log_log_size": {
                "broker": "node",
                "topic": "topic_name",
            }
        }

        config = SelfManagedKafkaConfig.from_plugin_settings(base_settings)

        assert config.metric_name_overrides == {"kafka_log_log_size": "company_kafka_partition_size"}
        assert config.label_name_overrides == {"kafka_log_log_size": {"broker": "node", "topic": "topic_name"}}

    @pytest.mark.parametrize(
        ("field", "value", "expected_detail"),
        [
            (
                "metric_name_overrides",
                [],
                "metric_name_overrides must be a mapping of canonical metric names to one physical metric name",
            ),
            (
                "metric_name_overrides",
                {"kafka_log_log_size": ["one", "two"]},
                "metric_name_overrides[kafka_log_log_size] must resolve to exactly one physical metric name",
            ),
            (
                "metric_name_overrides",
                {"unknown_metric": "physical_metric"},
                "metric_name_overrides contains unknown canonical metric family unknown_metric",
            ),
            (
                "metric_name_overrides",
                {"kafka_log_log_size": "not-a-metric"},
                "metric_name_overrides[kafka_log_log_size] is not a valid Prometheus metric identifier",
            ),
            (
                "metric_name_overrides",
                {"kafka_log_log_size": ""},
                "metric_name_overrides[kafka_log_log_size] is not a valid Prometheus metric identifier",
            ),
            (
                "metric_name_overrides",
                {"up": "kafka_log_log_size"},
                (
                    "physical metric kafka_log_log_size is assigned to multiple canonical metric families: "
                    "kafka_log_log_size, up"
                ),
            ),
            (
                "label_name_overrides",
                [],
                "label_name_overrides must be a mapping of canonical metric families to label mappings",
            ),
            (
                "label_name_overrides",
                {"kafka_log_log_size": []},
                "label_name_overrides[kafka_log_log_size] must be a mapping of canonical labels to physical labels",
            ),
            (
                "label_name_overrides",
                {"unknown_metric": {}},
                "label_name_overrides contains unknown canonical metric family unknown_metric",
            ),
            (
                "label_name_overrides",
                {"up": {"broker": "node"}},
                "label_name_overrides[up] contains unknown canonical label broker",
            ),
            (
                "label_name_overrides",
                {"kafka_log_log_size": {"broker": ["node"]}},
                "label_name_overrides[kafka_log_log_size][broker] must resolve to exactly one physical label name",
            ),
            (
                "label_name_overrides",
                {"kafka_log_log_size": {"broker": "not-a-label"}},
                "label_name_overrides[kafka_log_log_size][broker] is not a valid Prometheus label identifier",
            ),
            (
                "label_name_overrides",
                {"kafka_log_log_size": {"broker": ""}},
                "label_name_overrides[kafka_log_log_size][broker] is not a valid Prometheus label identifier",
            ),
            (
                "label_name_overrides",
                {"kafka_log_log_size": {"broker": "topic"}},
                "physical label topic is assigned to multiple canonical labels in kafka_log_log_size: broker, topic",
            ),
        ],
    )
    def test_telemetry_alias_validation_rejects_invalid_mapping_shapes_and_resolved_names(
        self,
        base_settings: dict[str, object],
        field: str,
        value: object,
        expected_detail: str,
    ) -> None:
        from plugins.self_managed_kafka.config import SelfManagedKafkaConfig

        base_settings[field] = value

        with pytest.raises(ValidationError, match=re.escape(expected_detail)):
            SelfManagedKafkaConfig.from_plugin_settings(base_settings)

    @pytest.mark.parametrize(
        ("selector_label", "label_name_overrides", "expected_detail"),
        [
            (
                "not-a-label",
                {},
                "metrics_identifier_label is not a valid Prometheus label identifier",
            ),
            (
                "broker",
                {},
                (
                    "physical label broker in kafka_server_brokertopicmetrics_alltopics_bytesin_total "
                    "conflicts with metrics_identifier_label"
                ),
            ),
            (
                "deployment",
                {"kafka_log_log_size": {"broker": "deployment"}},
                "physical label deployment in kafka_log_log_size conflicts with metrics_identifier_label",
            ),
        ],
    )
    def test_telemetry_alias_validation_keeps_the_global_selector_separate_from_family_labels(
        self,
        base_settings: dict[str, object],
        selector_label: str,
        label_name_overrides: dict[str, dict[str, str]],
        expected_detail: str,
    ) -> None:
        from plugins.self_managed_kafka.config import SelfManagedKafkaConfig

        base_settings["metrics_identifier_label"] = selector_label
        base_settings["label_name_overrides"] = label_name_overrides

        with pytest.raises(ValidationError, match=re.escape(expected_detail)):
            SelfManagedKafkaConfig.from_plugin_settings(base_settings)


class TestPrincipalAttributionConfig:
    def test_omitted_or_disabled_principal_attribution_keeps_the_baseline_static_policy(
        self, base_settings: dict[str, object]
    ) -> None:
        from plugins.self_managed_kafka.config import SelfManagedKafkaConfig

        omitted = SelfManagedKafkaConfig.from_plugin_settings(base_settings)
        base_settings["principal_attribution"] = {"enabled": False}
        disabled = SelfManagedKafkaConfig.from_plugin_settings(base_settings)

        assert omitted.principal_attribution.enabled is False
        assert disabled.principal_attribution.enabled is False
        assert omitted.principal_attribution.compute_policy == "unattributed"
        assert disabled.principal_attribution.storage_policy == "unattributed"

    @pytest.mark.parametrize(
        "principal_attribution",
        [
            {"enabled": True, "scrape_interval_seconds": 5, "max_gap_seconds": 10},
            {"enabled": True, "scrape_interval_seconds": 17, "max_gap_seconds": 23},
            {"enabled": True, "scrape_interval_seconds": 30, "max_gap_seconds": 60},
            {"enabled": True, "scrape_interval_seconds": 60, "max_gap_seconds": 120},
        ],
    )
    def test_enabled_principal_attribution_accepts_independent_positive_cadence_and_gap_values(
        self, base_settings: dict[str, object], principal_attribution: dict[str, object]
    ) -> None:
        from plugins.self_managed_kafka.config import SelfManagedKafkaConfig

        base_settings["identity_source"] = {"source": "both"}
        base_settings["principal_attribution"] = principal_attribution

        config = SelfManagedKafkaConfig.from_plugin_settings(base_settings)

        assert config.principal_attribution.enabled is True
        assert config.principal_attribution.scrape_interval_seconds == principal_attribution["scrape_interval_seconds"]
        assert config.principal_attribution.max_gap_seconds == principal_attribution["max_gap_seconds"]

    @pytest.mark.parametrize(
        ("identity_source", "principal_attribution", "expected_location"),
        [
            ("static", {"enabled": True, "scrape_interval_seconds": 5, "max_gap_seconds": 10}, "identity_source"),
            ("prometheus", {"enabled": True, "max_gap_seconds": 10}, "principal_attribution"),
            ("both", {"enabled": True, "scrape_interval_seconds": 5}, "principal_attribution"),
            ("both", {"enabled": True, "scrape_interval_seconds": 0, "max_gap_seconds": 10}, "principal_attribution"),
            ("both", {"enabled": True, "scrape_interval_seconds": 5, "max_gap_seconds": 0}, "principal_attribution"),
            (
                "both",
                {
                    "enabled": True,
                    "scrape_interval_seconds": 5,
                    "max_gap_seconds": 10,
                    "compute_policy": "automatic",
                },
                "principal_attribution",
            ),
        ],
    )
    def test_enabled_principal_attribution_rejects_invalid_identity_source_or_cadence_inputs(
        self,
        base_settings: dict[str, object],
        identity_source: str,
        principal_attribution: dict[str, object],
        expected_location: str,
    ) -> None:
        from plugins.self_managed_kafka.config import SelfManagedKafkaConfig

        base_settings["identity_source"] = {"source": identity_source}
        base_settings["principal_attribution"] = principal_attribution

        with pytest.raises(ValidationError) as error:
            SelfManagedKafkaConfig.from_plugin_settings(base_settings)

        assert any(issue["loc"][0] == expected_location for issue in error.value.errors())


class TestSelfManagedTopicAttributionConfig:
    def test_defaults_to_disabled_without_implicit_topic_exclusions(self, base_settings: dict[str, object]) -> None:
        from plugins.self_managed_kafka.config import SelfManagedKafkaConfig

        config = SelfManagedKafkaConfig.from_plugin_settings(base_settings)

        assert config.topic_attribution.enabled is False
        assert config.topic_attribution.compute_policy == "disabled"
        assert config.topic_attribution.exclude_topic_patterns == []
        assert config.topic_attribution.retention_days == 90
        assert config.topic_attribution.emitters == []

    def test_accepts_the_versioned_shared_compute_policy(self, base_settings: dict[str, object]) -> None:
        from plugins.self_managed_kafka.config import SelfManagedKafkaConfig

        base_settings["topic_attribution"] = {
            "enabled": True,
            "compute_policy": "shared_even_v1",
            "exclude_topic_patterns": ["__consumer_offsets", "_confluent-*"],
            "retention_days": 14,
        }

        config = SelfManagedKafkaConfig.from_plugin_settings(base_settings)

        assert config.topic_attribution.enabled is True
        assert config.topic_attribution.compute_policy == "shared_even_v1"
        assert config.topic_attribution.exclude_topic_patterns == ["__consumer_offsets", "_confluent-*"]
        assert config.topic_attribution.retention_days == 14

    def test_rejects_unknown_compute_policy_and_invalid_exclusion_shape(self, base_settings: dict[str, object]) -> None:
        from plugins.self_managed_kafka.config import SelfManagedKafkaConfig

        base_settings["topic_attribution"] = {
            "enabled": True,
            "compute_policy": "usage_weighted_v1",
            "exclude_topic_patterns": "__consumer_offsets",
        }

        with pytest.raises(ValidationError) as error:
            SelfManagedKafkaConfig.from_plugin_settings(base_settings)

        locations = {issue["loc"] for issue in error.value.errors()}
        assert ("topic_attribution", "compute_policy") in locations
        assert ("topic_attribution", "exclude_topic_patterns") in locations


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
