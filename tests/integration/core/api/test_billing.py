from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from fastapi.testclient import TestClient  # noqa: TC002

from core.models.billing import CoreBillingLineItem
from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend  # noqa: TC001


class TestListBilling:
    def test_list_billing_empty(self, app_with_backend: TestClient) -> None:
        response = app_with_backend.get("/api/v1/tenants/test-tenant/billing")
        assert response.status_code == 200
        data = response.json()
        assert data["items"] == []
        assert data["total"] == 0

    def test_list_billing_with_data(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend, sample_billing: CoreBillingLineItem
    ) -> None:
        with in_memory_backend.create_unit_of_work() as uow:
            uow.billing.upsert(sample_billing)
            uow.commit()

        response = app_with_backend.get("/api/v1/tenants/test-tenant/billing")
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 1
        assert len(data["items"]) == 1
        assert data["items"][0]["resource_id"] == "resource-1"
        assert data["items"][0]["total_cost"] == "10.00"

    def test_list_billing_filter_by_date(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        base = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
        # Insert at 6, 3, and 1 days ago; filter [5 days ago, 2 days ago] captures only 3-days-ago
        days_ago_list = [6, 3, 1]
        with in_memory_backend.create_unit_of_work() as uow:
            for days_ago in days_ago_list:
                uow.billing.upsert(
                    CoreBillingLineItem(
                        ecosystem="test-eco",
                        tenant_id="test-tenant",
                        timestamp=base - timedelta(days=days_ago),
                        resource_id=f"r-{days_ago}",
                        product_category="compute",
                        product_type="kafka",
                        quantity=Decimal("1"),
                        unit_price=Decimal("1"),
                        total_cost=Decimal("1"),
                        currency="USD",
                        granularity="daily",
                        metadata={},
                    )
                )
            uow.commit()

        start_date = (base - timedelta(days=5)).date()
        end_date = (base - timedelta(days=2)).date()
        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/billing",
            params={"start_date": str(start_date), "end_date": str(end_date)},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 1
        assert data["items"][0]["resource_id"] == "r-3"

    def test_list_billing_filter_by_product_type(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        base_ts = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
        with in_memory_backend.create_unit_of_work() as uow:
            for ptype in ["kafka", "connect", "ksql"]:
                uow.billing.upsert(
                    CoreBillingLineItem(
                        ecosystem="test-eco",
                        tenant_id="test-tenant",
                        timestamp=base_ts,
                        resource_id=f"r-{ptype}",
                        product_category="compute",
                        product_type=ptype,
                        quantity=Decimal("1"),
                        unit_price=Decimal("1"),
                        total_cost=Decimal("1"),
                        currency="USD",
                        granularity="daily",
                        metadata={},
                    )
                )
            uow.commit()

        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/billing",
            params={"product_type": "kafka"},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 1
        assert data["items"][0]["product_type"] == "kafka"

    def test_list_billing_pagination(self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend) -> None:
        base_ts = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        with in_memory_backend.create_unit_of_work() as uow:
            for i in range(25):
                uow.billing.upsert(
                    CoreBillingLineItem(
                        ecosystem="test-eco",
                        tenant_id="test-tenant",
                        timestamp=base_ts - timedelta(hours=i + 1),
                        resource_id=f"r-{i}",
                        product_category="compute",
                        product_type="kafka",
                        quantity=Decimal("1"),
                        unit_price=Decimal("1"),
                        total_cost=Decimal("1"),
                        currency="USD",
                        granularity="hourly",
                        metadata={},
                    )
                )
            uow.commit()

        # Page 1
        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/billing",
            params={"page": 1, "page_size": 10},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 25
        assert len(data["items"]) == 10
        assert data["page"] == 1
        assert data["pages"] == 3

        # Page 3
        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/billing",
            params={"page": 3, "page_size": 10},
        )
        assert response.status_code == 200
        data = response.json()
        assert len(data["items"]) == 5

    def test_list_billing_invalid_date_range(self, app_with_backend: TestClient) -> None:
        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/billing",
            params={"start_date": "2026-02-20", "end_date": "2026-02-10"},
        )
        assert response.status_code == 400
        assert "start_date" in response.json()["detail"]
