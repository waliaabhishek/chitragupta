from __future__ import annotations

from datetime import UTC, datetime

from fastapi.testclient import TestClient  # noqa: TC002
from sqlmodel import Session  # noqa: TC002

from core.api.app import create_app
from core.api.routes.chargebacks import router as chargebacks_router
from core.models.chargeback import AllocationDetail, CostType
from core.storage.backends.sqlmodel.tables import (
    ChargebackDimensionTable,
    ChargebackFactTable,
)
from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend  # noqa: TC001


def _insert_row(
    session: Session,
    *,
    ecosystem: str = "test-eco",
    tenant_id: str = "test-tenant",
    resource_id: str | None = "r1",
    product_type: str = "kafka",
    identity_id: str = "sa-1",
    cost_type: str = CostType.USAGE.value,
    allocation_detail: str | None,
    timestamp: datetime = datetime(2026, 3, 10, tzinfo=UTC),
    amount: str = "10.00",
) -> None:
    from sqlmodel import col, select

    existing_dim = session.exec(
        select(ChargebackDimensionTable).where(
            col(ChargebackDimensionTable.ecosystem) == ecosystem,
            col(ChargebackDimensionTable.tenant_id) == tenant_id,
            col(ChargebackDimensionTable.resource_id) == resource_id,
            col(ChargebackDimensionTable.product_type) == product_type,
            col(ChargebackDimensionTable.identity_id) == identity_id,
            col(ChargebackDimensionTable.cost_type) == cost_type,
            col(ChargebackDimensionTable.allocation_detail) == allocation_detail,
        )
    ).first()

    if existing_dim is None:
        dim = ChargebackDimensionTable(
            ecosystem=ecosystem,
            tenant_id=tenant_id,
            resource_id=resource_id,
            product_category="compute",
            product_type=product_type,
            identity_id=identity_id,
            cost_type=cost_type,
            allocation_method="direct",
            allocation_detail=allocation_detail,
        )
        session.add(dim)
        session.flush()
        dim_id = dim.dimension_id
    else:
        dim_id = existing_dim.dimension_id

    fact = ChargebackFactTable(
        dimension_id=dim_id,
        timestamp=timestamp,
        amount=amount,
        tags_json="[]",
    )
    session.add(fact)
    session.flush()


class TestAllocationIssuesRoute:
    def test_returns_paginated_response_structure(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        """GET /chargebacks/allocation-issues returns PaginatedResponse fields."""
        with in_memory_backend.create_unit_of_work() as uow:
            _insert_row(
                uow._session,  # type: ignore[attr-defined]
                allocation_detail=AllocationDetail.NO_IDENTITIES_LOCATED.value,
                amount="25.00",
            )
            uow.commit()

        response = app_with_backend.get("/api/v1/tenants/test-tenant/chargebacks/allocation-issues")
        assert response.status_code == 200
        data = response.json()
        assert "items" in data
        assert "total" in data
        assert "page" in data
        assert "page_size" in data
        assert "pages" in data

    def test_returns_correct_allocation_issue_fields(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        """Response items contain all AllocationIssueResponse fields."""
        with in_memory_backend.create_unit_of_work() as uow:
            _insert_row(
                uow._session,  # type: ignore[attr-defined]
                resource_id="cluster-1",
                product_type="kafka",
                identity_id="sa-test",
                cost_type=CostType.USAGE.value,
                allocation_detail=AllocationDetail.NO_IDENTITIES_LOCATED.value,
                amount="42.00",
            )
            uow.commit()

        response = app_with_backend.get("/api/v1/tenants/test-tenant/chargebacks/allocation-issues")
        assert response.status_code == 200
        items = response.json()["items"]
        assert len(items) == 1
        item = items[0]
        assert item["ecosystem"] == "test-eco"
        assert item["resource_id"] == "cluster-1"
        assert item["product_type"] == "kafka"
        assert item["identity_id"] == "sa-test"
        assert item["allocation_detail"] == AllocationDetail.NO_IDENTITIES_LOCATED.value
        assert item["row_count"] == 1
        assert float(item["usage_cost"]) == 42.0
        assert float(item["shared_cost"]) == 0.0
        assert float(item["total_cost"]) == 42.0

    def test_excludes_success_codes(self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend) -> None:
        """Success-code rows must not appear in the response."""
        with in_memory_backend.create_unit_of_work() as uow:
            _insert_row(
                uow._session,  # type: ignore[attr-defined]
                identity_id="sa-ok",
                allocation_detail=AllocationDetail.USAGE_RATIO_ALLOCATION.value,
                amount="100.00",
            )
            _insert_row(
                uow._session,  # type: ignore[attr-defined]
                identity_id="sa-fail",
                allocation_detail=AllocationDetail.NO_IDENTITIES_LOCATED.value,
                timestamp=datetime(2026, 3, 9, tzinfo=UTC),
                amount="50.00",
            )
            uow.commit()

        response = app_with_backend.get("/api/v1/tenants/test-tenant/chargebacks/allocation-issues")
        data = response.json()
        assert data["total"] == 1
        assert data["items"][0]["identity_id"] == "sa-fail"

    def test_empty_when_no_data(self, app_with_backend: TestClient) -> None:
        """Returns empty list when no chargeback data exists."""
        response = app_with_backend.get("/api/v1/tenants/test-tenant/chargebacks/allocation-issues")
        assert response.status_code == 200
        data = response.json()
        assert data["items"] == []
        assert data["total"] == 0

    def test_unknown_tenant_returns_404(self, app_with_backend: TestClient) -> None:
        """Returns 404 for unknown tenant."""
        response = app_with_backend.get("/api/v1/tenants/no-such-tenant/chargebacks/allocation-issues")
        assert response.status_code == 404


class TestAllocationIssuesRouteOrder:
    def test_allocation_issues_resolves_before_dimension_id(self) -> None:
        """The static route /allocation-issues must not be captured by /{dimension_id}."""
        routes = {r.path: r.endpoint for r in chargebacks_router.routes}  # type: ignore[attr-defined]
        static_path = "/tenants/{tenant_name}/chargebacks/allocation-issues"
        dynamic_path = "/tenants/{tenant_name}/chargebacks/{dimension_id}"

        assert static_path in routes, "allocation-issues route must be registered"
        assert dynamic_path in routes, "dimension_id route must be registered"

        from core.api.routes.chargebacks import get_chargeback_dimension, list_allocation_issues  # noqa: F401 — red

        assert routes[static_path] is list_allocation_issues
        assert routes[dynamic_path] is get_chargeback_dimension

    def test_allocation_issues_route_registered_in_app(self) -> None:
        """The full app has /chargebacks/allocation-issues registered."""
        from core.config.models import ApiConfig, AppSettings, LoggingConfig, StorageConfig, TenantConfig

        settings = AppSettings(
            api=ApiConfig(host="127.0.0.1", port=8080),
            logging=LoggingConfig(),
            tenants={
                "test": TenantConfig(
                    tenant_id="t-123",
                    ecosystem="test-eco",
                    storage=StorageConfig(connection_string="sqlite:///:memory:"),
                )
            },
        )
        app = create_app(settings)
        route_paths = [r.path for r in app.routes]
        assert "/api/v1/tenants/{tenant_name}/chargebacks/allocation-issues" in route_paths
