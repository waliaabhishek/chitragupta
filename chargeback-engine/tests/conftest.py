from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest

from core.models.billing import BillingLineItem
from core.models.identity import Identity
from core.models.resource import Resource, ResourceStatus

_NOW = datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC)


def make_resource(**overrides: Any) -> Resource:
    """Factory for Resource with sensible defaults."""
    defaults: dict[str, Any] = {
        "ecosystem": "confluent",
        "tenant_id": "t-001",
        "resource_id": "lkc-abc123",
        "resource_type": "kafka_cluster",
        "display_name": "test-cluster",
        "status": ResourceStatus.ACTIVE,
        "created_at": _NOW,
        "metadata": {},
    }
    defaults.update(overrides)
    return Resource(**defaults)


def make_identity(**overrides: Any) -> Identity:
    """Factory for Identity with sensible defaults."""
    defaults: dict[str, Any] = {
        "ecosystem": "confluent",
        "tenant_id": "t-001",
        "identity_id": "u-user1",
        "identity_type": "user",
        "display_name": "Test User",
        "created_at": _NOW,
        "metadata": {},
    }
    defaults.update(overrides)
    return Identity(**defaults)


def make_billing_line(**overrides: Any) -> BillingLineItem:
    """Factory for BillingLineItem with sensible defaults."""
    defaults: dict[str, Any] = {
        "ecosystem": "confluent",
        "tenant_id": "t-001",
        "timestamp": _NOW,
        "resource_id": "lkc-abc123",
        "product_category": "kafka",
        "product_type": "kafka_num_ckus",
        "quantity": Decimal("100"),
        "unit_price": Decimal("0.01"),
        "total_cost": Decimal("1.00"),
        "currency": "USD",
        "granularity": "daily",
        "metadata": {},
    }
    defaults.update(overrides)
    return BillingLineItem(**defaults)


@pytest.fixture
def resource_factory():
    """Pytest fixture wrapping make_resource."""
    return make_resource


@pytest.fixture
def identity_factory():
    """Pytest fixture wrapping make_identity."""
    return make_identity


@pytest.fixture
def billing_line_factory():
    """Pytest fixture wrapping make_billing_line."""
    return make_billing_line
