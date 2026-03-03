"""Tests for DefaultHandler."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

import pytest

from plugins.confluent_cloud.handlers.default import DefaultHandler


@pytest.fixture
def handler() -> DefaultHandler:
    return DefaultHandler(ecosystem="confluent_cloud")


class TestDefaultHandlerConstructor:
    """Test constructor dependency cleanup — only ecosystem param."""

    def test_accepts_only_ecosystem(self) -> None:
        handler = DefaultHandler(ecosystem="confluent_cloud")
        assert handler.service_type == "default"

    def test_rejects_connection_kwarg(self) -> None:
        with pytest.raises(TypeError):
            DefaultHandler(ecosystem="confluent_cloud", connection=MagicMock())  # type: ignore[call-arg]

    def test_rejects_config_kwarg(self) -> None:
        with pytest.raises(TypeError):
            DefaultHandler(ecosystem="confluent_cloud", config=MagicMock())  # type: ignore[call-arg]


class TestDefaultHandlerProperties:
    """Test handler properties."""

    def test_service_type(self, handler: DefaultHandler) -> None:
        assert handler.service_type == "default"

    def test_handles_product_types(self, handler: DefaultHandler) -> None:
        expected = (
            "TABLEFLOW_DATA_PROCESSED",
            "TABLEFLOW_NUM_TOPICS",
            "TABLEFLOW_STORAGE",
            "CLUSTER_LINKING_PER_LINK",
            "CLUSTER_LINKING_READ",
            "CLUSTER_LINKING_WRITE",
        )
        assert handler.handles_product_types == expected

    def test_no_connection_required(self) -> None:
        """DefaultHandler works with only ecosystem."""
        handler = DefaultHandler(ecosystem="confluent_cloud")
        assert handler.service_type == "default"


class TestDefaultHandlerGatherResources:
    """Test gather_resources returns empty."""

    def test_returns_empty(self, handler: DefaultHandler) -> None:
        uow = MagicMock()
        resources = list(handler.gather_resources("org-123", uow))
        assert resources == []


class TestDefaultHandlerGatherIdentities:
    """Test gather_identities returns empty."""

    def test_returns_empty(self, handler: DefaultHandler) -> None:
        uow = MagicMock()
        identities = list(handler.gather_identities("org-123", uow))
        assert identities == []


class TestDefaultHandlerResolveIdentities:
    """Test resolve_identities returns empty IdentityResolution."""

    def test_returns_empty_resolution(self, handler: DefaultHandler) -> None:
        uow = MagicMock()
        result = handler.resolve_identities(
            tenant_id="org-123",
            resource_id="tableflow-1",
            billing_timestamp=datetime(2026, 2, 1, tzinfo=UTC),
            billing_duration=timedelta(hours=1),
            metrics_data=None,
            uow=uow,
        )
        assert len(result.resource_active) == 0
        assert len(result.metrics_derived) == 0
        assert len(result.tenant_period) == 0


class TestDefaultHandlerGetMetrics:
    """Test get_metrics_for_product_type returns empty."""

    def test_returns_empty_list(self, handler: DefaultHandler) -> None:
        for pt in handler.handles_product_types:
            assert handler.get_metrics_for_product_type(pt) == []


class TestDefaultHandlerGetAllocator:
    """Test get_allocator routes correctly."""

    def test_tableflow_uses_default_allocator(self, handler: DefaultHandler) -> None:
        from plugins.confluent_cloud.allocators.default_allocators import default_allocator

        assert handler.get_allocator("TABLEFLOW_DATA_PROCESSED") is default_allocator
        assert handler.get_allocator("TABLEFLOW_NUM_TOPICS") is default_allocator
        assert handler.get_allocator("TABLEFLOW_STORAGE") is default_allocator

    def test_cluster_linking_uses_cluster_linking_allocator(self, handler: DefaultHandler) -> None:
        from plugins.confluent_cloud.allocators.default_allocators import cluster_linking_allocator

        assert handler.get_allocator("CLUSTER_LINKING_PER_LINK") is cluster_linking_allocator
        assert handler.get_allocator("CLUSTER_LINKING_READ") is cluster_linking_allocator
        assert handler.get_allocator("CLUSTER_LINKING_WRITE") is cluster_linking_allocator

    def test_unknown_raises(self, handler: DefaultHandler) -> None:
        with pytest.raises(ValueError, match="Unknown product type"):
            handler.get_allocator("KAFKA_BASE")
