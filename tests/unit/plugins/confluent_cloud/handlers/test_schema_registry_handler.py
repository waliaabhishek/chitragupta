"""Tests for SchemaRegistryHandler."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

import pytest

from core.models import CoreIdentity, CoreResource


class TestSchemaRegistryHandlerProperties:
    """Tests for SchemaRegistryHandler properties."""

    def test_service_type(self) -> None:
        """service_type returns 'schema_registry'."""
        from plugins.confluent_cloud.handlers.schema_registry import (
            SchemaRegistryHandler,
        )

        handler = SchemaRegistryHandler(connection=None, config=None, ecosystem="confluent_cloud")
        assert handler.service_type == "schema_registry"

    def test_handles_product_types(self) -> None:
        """handles_product_types returns SR product types."""
        from plugins.confluent_cloud.handlers.schema_registry import (
            SchemaRegistryHandler,
        )

        handler = SchemaRegistryHandler(connection=None, config=None, ecosystem="confluent_cloud")
        expected = ("SCHEMA_REGISTRY", "GOVERNANCE_BASE", "NUM_RULES")
        assert handler.handles_product_types == expected


class TestSchemaRegistryHandlerGetAllocator:
    """Tests for get_allocator method."""

    def test_schema_registry_allocator(self) -> None:
        """SCHEMA_REGISTRY returns schema_registry_allocator."""
        from plugins.confluent_cloud.allocators.sr_allocators import (
            schema_registry_allocator,
        )
        from plugins.confluent_cloud.handlers.schema_registry import (
            SchemaRegistryHandler,
        )

        handler = SchemaRegistryHandler(connection=None, config=None, ecosystem="confluent_cloud")
        assert handler.get_allocator("SCHEMA_REGISTRY") is schema_registry_allocator

    def test_governance_base_allocator(self) -> None:
        """GOVERNANCE_BASE returns schema_registry_allocator."""
        from plugins.confluent_cloud.allocators.sr_allocators import (
            schema_registry_allocator,
        )
        from plugins.confluent_cloud.handlers.schema_registry import (
            SchemaRegistryHandler,
        )

        handler = SchemaRegistryHandler(connection=None, config=None, ecosystem="confluent_cloud")
        assert handler.get_allocator("GOVERNANCE_BASE") is schema_registry_allocator

    def test_num_rules_allocator(self) -> None:
        """NUM_RULES returns schema_registry_allocator."""
        from plugins.confluent_cloud.allocators.sr_allocators import (
            schema_registry_allocator,
        )
        from plugins.confluent_cloud.handlers.schema_registry import (
            SchemaRegistryHandler,
        )

        handler = SchemaRegistryHandler(connection=None, config=None, ecosystem="confluent_cloud")
        assert handler.get_allocator("NUM_RULES") is schema_registry_allocator

    def test_unknown_product_type_raises(self) -> None:
        """Unknown product type raises ValueError."""
        from plugins.confluent_cloud.handlers.schema_registry import (
            SchemaRegistryHandler,
        )

        handler = SchemaRegistryHandler(connection=None, config=None, ecosystem="confluent_cloud")
        with pytest.raises(ValueError, match="Unknown product type"):
            handler.get_allocator("UNKNOWN_TYPE")


class TestSchemaRegistryHandlerGatherResources:
    """Tests for gather_resources method."""

    def test_returns_empty_when_connection_is_none(self, mock_uow: MagicMock) -> None:
        """gather_resources yields nothing when connection is None."""
        from plugins.confluent_cloud.handlers.schema_registry import (
            SchemaRegistryHandler,
        )

        handler = SchemaRegistryHandler(connection=None, config=None, ecosystem="confluent_cloud")
        result = list(handler.gather_resources("org-123", mock_uow))
        assert result == []

    def test_calls_gather_environments_and_registries(self, mock_uow: MagicMock) -> None:
        """gather_resources calls gather_schema_registries with env_ids from shared_ctx."""
        from unittest.mock import patch

        from core.models import ResourceStatus
        from plugins.confluent_cloud.handlers.schema_registry import (
            SchemaRegistryHandler,
        )
        from plugins.confluent_cloud.shared_context import CCloudSharedContext

        mock_conn = MagicMock()
        env_resource = CoreResource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="env-abc",
            resource_type="environment",
            status=ResourceStatus.ACTIVE,
            metadata={},
        )
        ctx = CCloudSharedContext(
            environment_resources=(env_resource,),
            kafka_cluster_resources=(),
        )
        sr_resource = MagicMock()

        with patch(
            "plugins.confluent_cloud.gathering.gather_schema_registries",
            return_value=[sr_resource],
        ) as mock_srs:
            handler = SchemaRegistryHandler(connection=mock_conn, config=None, ecosystem="confluent_cloud")
            result = list(handler.gather_resources("org-123", mock_uow, ctx))

        mock_srs.assert_called_once_with(mock_conn, "confluent_cloud", "org-123", ["env-abc"])
        # SR handler does NOT yield environments (Kafka handler does that)
        assert result == [sr_resource]


class TestSchemaRegistryHandlerGetMetrics:
    """Tests for get_metrics_for_product_type method."""

    def test_schema_registry_no_metrics(self) -> None:
        """SCHEMA_REGISTRY returns empty list (no metrics needed)."""
        from plugins.confluent_cloud.handlers.schema_registry import (
            SchemaRegistryHandler,
        )

        handler = SchemaRegistryHandler(connection=None, config=None, ecosystem="confluent_cloud")
        metrics = handler.get_metrics_for_product_type("SCHEMA_REGISTRY")
        assert metrics == []

    def test_governance_base_no_metrics(self) -> None:
        """GOVERNANCE_BASE returns empty list."""
        from plugins.confluent_cloud.handlers.schema_registry import (
            SchemaRegistryHandler,
        )

        handler = SchemaRegistryHandler(connection=None, config=None, ecosystem="confluent_cloud")
        metrics = handler.get_metrics_for_product_type("GOVERNANCE_BASE")
        assert metrics == []


class TestSchemaRegistryHandlerGatherIdentities:
    """Tests for gather_identities method."""

    def test_gather_identities_returns_empty(self, mock_uow: MagicMock) -> None:
        """gather_identities returns empty (Kafka gathers org identities)."""
        from plugins.confluent_cloud.handlers.schema_registry import (
            SchemaRegistryHandler,
        )

        handler = SchemaRegistryHandler(connection=None, config=None, ecosystem="confluent_cloud")
        result = list(handler.gather_identities("org-123", mock_uow))
        assert result == []


class TestSchemaRegistryHandlerResolveIdentities:
    """Tests for resolve_identities method."""

    def test_resolves_api_key_owners(self, mock_uow: MagicMock) -> None:
        """API key owners are resolved to resource_active."""
        from plugins.confluent_cloud.handlers.schema_registry import (
            SchemaRegistryHandler,
        )

        api_key = CoreIdentity(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            identity_id="api-key-1",
            identity_type="api_key",
            metadata={"resource_id": "lsrc-xyz", "owner_id": "sa-owner"},
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        sa_owner = CoreIdentity(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            identity_id="sa-owner",
            identity_type="service_account",
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        mock_uow.identities.find_by_period.return_value = ([api_key, sa_owner], 2)

        handler = SchemaRegistryHandler(connection=None, config=None, ecosystem="confluent_cloud")
        result = handler.resolve_identities(
            tenant_id="org-123",
            resource_id="lsrc-xyz",
            billing_timestamp=datetime(2026, 2, 1, tzinfo=UTC),
            billing_duration=timedelta(hours=24),
            metrics_data=None,
            uow=mock_uow,
        )

        assert len(result.resource_active) == 1
        assert "sa-owner" in result.resource_active.ids()

    def test_tenant_period_is_empty(self, mock_uow: MagicMock) -> None:
        """tenant_period is returned empty (orchestrator fills it)."""
        from plugins.confluent_cloud.handlers.schema_registry import (
            SchemaRegistryHandler,
        )

        mock_uow.identities.find_by_period.return_value = ([], 0)

        handler = SchemaRegistryHandler(connection=None, config=None, ecosystem="confluent_cloud")
        result = handler.resolve_identities(
            tenant_id="org-123",
            resource_id="lsrc-xyz",
            billing_timestamp=datetime(2026, 2, 1, tzinfo=UTC),
            billing_duration=timedelta(hours=24),
            metrics_data=None,
            uow=mock_uow,
        )

        assert len(result.tenant_period) == 0
