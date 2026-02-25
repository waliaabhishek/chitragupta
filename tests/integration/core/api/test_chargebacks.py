from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from fastapi.testclient import TestClient  # noqa: TC002

from core.models.chargeback import ChargebackRow, CostType
from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend  # noqa: TC001


class TestListChargebacks:
    def test_list_chargebacks_empty(self, app_with_backend: TestClient) -> None:
        response = app_with_backend.get("/api/v1/tenants/test-tenant/chargebacks")
        assert response.status_code == 200
        data = response.json()
        assert data["items"] == []
        assert data["total"] == 0

    def test_list_chargebacks_with_data(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend, sample_chargeback: ChargebackRow
    ) -> None:
        with in_memory_backend.create_unit_of_work() as uow:
            uow.chargebacks.upsert(sample_chargeback)
            uow.commit()

        response = app_with_backend.get("/api/v1/tenants/test-tenant/chargebacks")
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 1
        assert len(data["items"]) == 1
        assert data["items"][0]["identity_id"] == "user-1"
        assert data["items"][0]["amount"] == "10.00"

    def test_list_chargebacks_filter_by_identity(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        with in_memory_backend.create_unit_of_work() as uow:
            for uid in ["user-1", "user-2", "user-3"]:
                uow.chargebacks.upsert(
                    ChargebackRow(
                        ecosystem="test-eco",
                        tenant_id="test-tenant",
                        timestamp=datetime(2026, 2, 15, tzinfo=UTC),
                        resource_id=f"r-{uid}",
                        product_category="compute",
                        product_type="kafka",
                        identity_id=uid,
                        cost_type=CostType.USAGE,
                        amount=Decimal("10.00"),
                        allocation_method="direct",
                        allocation_detail=None,
                        tags=[],
                        metadata={},
                    )
                )
            uow.commit()

        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/chargebacks",
            params={"identity_id": "user-2"},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 1
        assert data["items"][0]["identity_id"] == "user-2"

    def test_list_chargebacks_filter_by_cost_type(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        with in_memory_backend.create_unit_of_work() as uow:
            for ct in [CostType.USAGE, CostType.SHARED]:
                uow.chargebacks.upsert(
                    ChargebackRow(
                        ecosystem="test-eco",
                        tenant_id="test-tenant",
                        timestamp=datetime(2026, 2, 15, tzinfo=UTC),
                        resource_id=f"r-{ct.value}",
                        product_category="compute",
                        product_type="kafka",
                        identity_id="user-1",
                        cost_type=ct,
                        amount=Decimal("10.00"),
                        allocation_method="direct",
                        allocation_detail=None,
                        tags=[],
                        metadata={},
                    )
                )
            uow.commit()

        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/chargebacks",
            params={"cost_type": "shared"},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 1
        assert data["items"][0]["cost_type"] == "shared"

    def test_list_chargebacks_pagination(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        with in_memory_backend.create_unit_of_work() as uow:
            for i in range(15):
                uow.chargebacks.upsert(
                    ChargebackRow(
                        ecosystem="test-eco",
                        tenant_id="test-tenant",
                        timestamp=datetime(2026, 2, 15, i, tzinfo=UTC),
                        resource_id=f"r-{i}",
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
                )
            uow.commit()

        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/chargebacks",
            params={"page": 1, "page_size": 10},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 15
        assert len(data["items"]) == 10
        assert data["pages"] == 2

        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/chargebacks",
            params={"page": 2, "page_size": 10},
        )
        assert response.status_code == 200
        data = response.json()
        assert len(data["items"]) == 5

    def test_list_chargebacks_filter_by_resource_id(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        with in_memory_backend.create_unit_of_work() as uow:
            for rid in ["r-1", "r-2", "r-3"]:
                uow.chargebacks.upsert(
                    ChargebackRow(
                        ecosystem="test-eco",
                        tenant_id="test-tenant",
                        timestamp=datetime(2026, 2, 15, tzinfo=UTC),
                        resource_id=rid,
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
                )
            uow.commit()

        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/chargebacks",
            params={"resource_id": "r-2"},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 1
        assert data["items"][0]["resource_id"] == "r-2"

    def test_list_chargebacks_invalid_date_range(self, app_with_backend: TestClient) -> None:
        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/chargebacks",
            params={"start_date": "2026-02-20", "end_date": "2026-02-10"},
        )
        assert response.status_code == 400
        assert "start_date" in response.json()["detail"]
