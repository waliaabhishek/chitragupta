"""Tests for GenericMetricsOnlyPlugin config validation."""

from __future__ import annotations

from decimal import Decimal

import pytest
from pydantic import ValidationError


@pytest.fixture
def base_metrics() -> dict:
    return {"url": "http://prom:9090"}


@pytest.fixture
def base_identity_source() -> dict:
    return {
        "source": "prometheus",
        "label": "datname",
        "discovery_query": "group by (datname) (pg_stat_database_blks_hit)",
    }


@pytest.fixture
def base_cost_types() -> list:
    return [
        {
            "name": "PG_COMPUTE",
            "product_category": "postgres",
            "rate": "2.50",
            "cost_quantity": {"type": "fixed", "count": 2},
            "allocation_strategy": "even_split",
        }
    ]


@pytest.fixture
def base_settings(base_metrics, base_identity_source, base_cost_types) -> dict:
    return {
        "ecosystem_name": "self_managed_postgres",
        "cluster_id": "pg-prod-1",
        "metrics": base_metrics,
        "identity_source": base_identity_source,
        "cost_types": base_cost_types,
    }


class TestCostTypeConfigUsageRatioValidation:
    def test_usage_ratio_without_allocation_query_raises(self, base_settings: dict) -> None:
        """Test case 1: usage_ratio with no allocation_query raises ValidationError."""
        from plugins.generic_metrics_only.config import GenericMetricsOnlyConfig

        base_settings["cost_types"] = [
            {
                "name": "PG_NETWORK",
                "product_category": "postgres",
                "rate": "0.05",
                "cost_quantity": {
                    "type": "network_gib",
                    "query": "sum(pg_stat_database_blks_read)",
                },
                "allocation_strategy": "usage_ratio",
                # Missing allocation_query
                "allocation_label": "datname",
            }
        ]
        with pytest.raises(ValidationError, match="allocation_query required"):
            GenericMetricsOnlyConfig.model_validate(base_settings)

    def test_usage_ratio_without_allocation_label_raises(self, base_settings: dict) -> None:
        from plugins.generic_metrics_only.config import GenericMetricsOnlyConfig

        base_settings["cost_types"] = [
            {
                "name": "PG_NETWORK",
                "product_category": "postgres",
                "rate": "0.05",
                "cost_quantity": {
                    "type": "network_gib",
                    "query": "sum(pg_stat_database_blks_read)",
                },
                "allocation_strategy": "usage_ratio",
                "allocation_query": "sum by (datname) (pg_stat_database_blks_read)",
                # Missing allocation_label
            }
        ]
        with pytest.raises(ValidationError, match="allocation_label required"):
            GenericMetricsOnlyConfig.model_validate(base_settings)

    def test_even_split_without_allocation_query_is_valid(self, base_settings: dict) -> None:
        from plugins.generic_metrics_only.config import GenericMetricsOnlyConfig

        config = GenericMetricsOnlyConfig.model_validate(base_settings)
        assert config.cost_types[0].allocation_strategy == "even_split"


class TestGenericIdentitySourceConfigValidation:
    def test_prometheus_source_without_discovery_query_raises(self) -> None:
        """Test case 2: prometheus source without discovery_query raises ValidationError."""
        from plugins.generic_metrics_only.config import GenericIdentitySourceConfig

        with pytest.raises(ValidationError, match="discovery_query required"):
            GenericIdentitySourceConfig.model_validate({"source": "prometheus"})

    def test_both_source_without_discovery_query_raises(self) -> None:
        from plugins.generic_metrics_only.config import GenericIdentitySourceConfig

        with pytest.raises(ValidationError, match="discovery_query required"):
            GenericIdentitySourceConfig.model_validate({"source": "both"})

    def test_static_source_without_discovery_query_is_valid(self) -> None:
        from plugins.generic_metrics_only.config import GenericIdentitySourceConfig

        config = GenericIdentitySourceConfig.model_validate({"source": "static"})
        assert config.source == "static"
        assert config.discovery_query is None

    def test_prometheus_source_with_discovery_query_is_valid(self) -> None:
        from plugins.generic_metrics_only.config import GenericIdentitySourceConfig

        config = GenericIdentitySourceConfig.model_validate(
            {
                "source": "prometheus",
                "discovery_query": "group by (datname) (pg_stat_database_blks_hit)",
            }
        )
        assert config.discovery_query == "group by (datname) (pg_stat_database_blks_hit)"


class TestCostQuantityDiscriminator:
    def test_cost_quantity_fixed_parses_correctly(self) -> None:
        """Test case 3: CostQuantityFixed parses correctly via discriminator."""
        from plugins.generic_metrics_only.config import CostTypeConfig

        ct = CostTypeConfig.model_validate(
            {
                "name": "PG_COMPUTE",
                "product_category": "postgres",
                "rate": "2.50",
                "cost_quantity": {"type": "fixed", "count": 3},
                "allocation_strategy": "even_split",
            }
        )
        assert ct.cost_quantity.type == "fixed"
        assert ct.cost_quantity.count == 3  # type: ignore[union-attr]

    def test_cost_quantity_storage_gib_parses_correctly(self) -> None:
        """Test case 3: CostQuantityStorageGib parses correctly via discriminator."""
        from plugins.generic_metrics_only.config import CostTypeConfig

        ct = CostTypeConfig.model_validate(
            {
                "name": "PG_STORAGE",
                "product_category": "postgres",
                "rate": "0.0001",
                "cost_quantity": {"type": "storage_gib", "query": "sum(pg_database_size_bytes)"},
                "allocation_strategy": "even_split",
            }
        )
        assert ct.cost_quantity.type == "storage_gib"
        assert ct.cost_quantity.query == "sum(pg_database_size_bytes)"  # type: ignore[union-attr]

    def test_cost_quantity_network_gib_parses_correctly(self) -> None:
        """Test case 3: CostQuantityNetworkGib parses correctly via discriminator."""
        from plugins.generic_metrics_only.config import CostTypeConfig

        ct = CostTypeConfig.model_validate(
            {
                "name": "PG_NETWORK",
                "product_category": "postgres",
                "rate": "0.05",
                "cost_quantity": {
                    "type": "network_gib",
                    "query": "sum(pg_stat_database_blks_read)",
                },
                "allocation_strategy": "usage_ratio",
                "allocation_query": "sum by (datname) (pg_stat_database_blks_read)",
                "allocation_label": "datname",
            }
        )
        assert ct.cost_quantity.type == "network_gib"
        assert ct.cost_quantity.query == "sum(pg_stat_database_blks_read)"  # type: ignore[union-attr]

    def test_rate_is_decimal(self, base_settings: dict) -> None:
        from plugins.generic_metrics_only.config import GenericMetricsOnlyConfig

        config = GenericMetricsOnlyConfig.model_validate(base_settings)
        assert config.cost_types[0].rate == Decimal("2.50")

    def test_cost_types_min_length_enforced(self, base_settings: dict) -> None:
        from plugins.generic_metrics_only.config import GenericMetricsOnlyConfig

        base_settings["cost_types"] = []
        with pytest.raises(ValidationError):
            GenericMetricsOnlyConfig.model_validate(base_settings)
