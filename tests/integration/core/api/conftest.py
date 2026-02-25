from __future__ import annotations

import tempfile
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from fastapi.testclient import TestClient

from core.api.app import create_app
from core.config.models import ApiConfig, AppSettings, LoggingConfig, StorageConfig, TenantConfig
from core.models.billing import BillingLineItem
from core.models.chargeback import ChargebackRow, CostType
from core.models.identity import Identity
from core.models.pipeline import PipelineState
from core.models.resource import Resource, ResourceStatus
from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend

if TYPE_CHECKING:
    from collections.abc import Iterator


@pytest.fixture
def temp_db_path() -> Iterator[str]:
    """Create a temp database file path."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        path = f.name
    yield f"sqlite:///{path}"
    Path(path).unlink(missing_ok=True)


@pytest.fixture
def in_memory_backend(temp_db_path: str) -> Iterator[SQLModelBackend]:
    """Create a fresh SQLite backend for testing."""
    backend = SQLModelBackend(temp_db_path, use_migrations=False)
    backend.create_tables()
    yield backend
    backend.dispose()


@pytest.fixture
def tenant_config(temp_db_path: str) -> TenantConfig:
    return TenantConfig(
        tenant_id="test-tenant",
        ecosystem="test-eco",
        storage=StorageConfig(connection_string=temp_db_path),
    )


@pytest.fixture
def settings_with_tenant(tenant_config: TenantConfig) -> AppSettings:
    return AppSettings(
        api=ApiConfig(host="127.0.0.1", port=8080),
        logging=LoggingConfig(),
        tenants={"test-tenant": tenant_config},
    )


@pytest.fixture
def app_with_backend(settings_with_tenant: AppSettings, in_memory_backend: SQLModelBackend) -> Iterator[TestClient]:
    """Create a test app with a pre-initialized backend."""
    app = create_app(settings_with_tenant)
    with TestClient(app) as client:
        # After lifespan runs, inject our test backend
        app.state.backends["test-tenant"] = in_memory_backend
        yield client


@pytest.fixture
def sample_resource() -> Resource:
    return Resource(
        ecosystem="test-eco",
        tenant_id="test-tenant",
        resource_id="resource-1",
        resource_type="kafka_cluster",
        display_name="Test Cluster",
        parent_id=None,
        owner_id="user-1",
        status=ResourceStatus.ACTIVE,
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
        deleted_at=None,
        last_seen_at=datetime(2026, 2, 24, tzinfo=UTC),
        metadata={"region": "us-west-2"},
    )


@pytest.fixture
def sample_identity() -> Identity:
    return Identity(
        ecosystem="test-eco",
        tenant_id="test-tenant",
        identity_id="user-1",
        identity_type="service_account",
        display_name="Test User",
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
        deleted_at=None,
        last_seen_at=datetime(2026, 2, 24, tzinfo=UTC),
        metadata={},
    )


@pytest.fixture
def sample_billing() -> BillingLineItem:
    return BillingLineItem(
        ecosystem="test-eco",
        tenant_id="test-tenant",
        timestamp=datetime(2026, 2, 15, tzinfo=UTC),
        resource_id="resource-1",
        product_category="compute",
        product_type="kafka",
        quantity=Decimal("100"),
        unit_price=Decimal("0.10"),
        total_cost=Decimal("10.00"),
        currency="USD",
        granularity="daily",
        metadata={},
    )


@pytest.fixture
def sample_chargeback() -> ChargebackRow:
    return ChargebackRow(
        ecosystem="test-eco",
        tenant_id="test-tenant",
        timestamp=datetime(2026, 2, 15, tzinfo=UTC),
        resource_id="resource-1",
        product_category="compute",
        product_type="kafka",
        identity_id="user-1",
        cost_type=CostType.USAGE,
        amount=Decimal("10.00"),
        allocation_method="direct",
        allocation_detail=None,
        tags=[],
        metadata={},
    )


@pytest.fixture
def sample_pipeline_state() -> PipelineState:
    from datetime import date

    return PipelineState(
        ecosystem="test-eco",
        tenant_id="test-tenant",
        tracking_date=date(2026, 2, 15),
        billing_gathered=True,
        resources_gathered=True,
        chargeback_calculated=True,
    )
