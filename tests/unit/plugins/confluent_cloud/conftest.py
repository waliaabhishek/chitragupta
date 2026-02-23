from __future__ import annotations

import pytest

from core.models import Resource, ResourceStatus


@pytest.fixture
def make_resource():
    """Factory for creating test Resource objects."""

    def _make(
        resource_id: str = "res-001",
        resource_type: str = "generic",
        metadata: dict | None = None,
        **kwargs,
    ) -> Resource:
        defaults = {
            "ecosystem": "confluent_cloud",
            "tenant_id": "org-123",
            "resource_id": resource_id,
            "resource_type": resource_type,
            "status": ResourceStatus.ACTIVE,
            "metadata": metadata or {},
        }
        defaults.update(kwargs)
        return Resource(**defaults)

    return _make
