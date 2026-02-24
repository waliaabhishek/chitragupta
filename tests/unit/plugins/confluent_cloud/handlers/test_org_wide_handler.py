"""Tests for OrgWideCostHandler."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

import pytest

from plugins.confluent_cloud.handlers.org_wide import OrgWideCostHandler


@pytest.fixture
def handler() -> OrgWideCostHandler:
    return OrgWideCostHandler(connection=None, config=None, ecosystem="confluent_cloud")


class TestOrgWideCostHandlerProperties:
    """Test handler properties."""

    def test_service_type(self, handler: OrgWideCostHandler) -> None:
        assert handler.service_type == "org_wide"

    def test_handles_product_types(self, handler: OrgWideCostHandler) -> None:
        assert handler.handles_product_types == ("AUDIT_LOG_READ", "SUPPORT")


class TestOrgWideCostHandlerGatherResources:
    """Test gather_resources returns empty."""

    def test_returns_empty(self, handler: OrgWideCostHandler) -> None:
        uow = MagicMock()
        resources = list(handler.gather_resources("org-123", uow))
        assert resources == []


class TestOrgWideCostHandlerGatherIdentities:
    """Test gather_identities returns empty."""

    def test_returns_empty(self, handler: OrgWideCostHandler) -> None:
        uow = MagicMock()
        identities = list(handler.gather_identities("org-123", uow))
        assert identities == []


class TestOrgWideCostHandlerResolveIdentities:
    """Test resolve_identities returns empty IdentityResolution."""

    def test_returns_empty_resolution(self, handler: OrgWideCostHandler) -> None:
        uow = MagicMock()
        result = handler.resolve_identities(
            tenant_id="org-123",
            resource_id="org-123",
            billing_timestamp=datetime(2026, 2, 1, tzinfo=UTC),
            billing_duration=timedelta(hours=1),
            metrics_data=None,
            uow=uow,
        )
        assert len(result.resource_active) == 0
        assert len(result.metrics_derived) == 0
        assert len(result.tenant_period) == 0


class TestOrgWideCostHandlerGetMetrics:
    """Test get_metrics_for_product_type returns empty."""

    def test_returns_empty_list(self, handler: OrgWideCostHandler) -> None:
        assert handler.get_metrics_for_product_type("AUDIT_LOG_READ") == []
        assert handler.get_metrics_for_product_type("SUPPORT") == []


class TestOrgWideCostHandlerGetAllocator:
    """Test get_allocator returns org_wide_allocator."""

    def test_audit_log_read(self, handler: OrgWideCostHandler) -> None:
        from plugins.confluent_cloud.allocators.org_wide_allocators import org_wide_allocator

        assert handler.get_allocator("AUDIT_LOG_READ") is org_wide_allocator

    def test_support(self, handler: OrgWideCostHandler) -> None:
        from plugins.confluent_cloud.allocators.org_wide_allocators import org_wide_allocator

        assert handler.get_allocator("SUPPORT") is org_wide_allocator

    def test_unknown_raises(self, handler: OrgWideCostHandler) -> None:
        with pytest.raises(ValueError, match="Unknown product type"):
            handler.get_allocator("KAFKA_BASE")
