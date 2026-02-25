from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from fastapi.testclient import TestClient  # noqa: TC002

from core.models.chargeback import ChargebackRow, CostType
from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend  # noqa: TC001


def _seed_chargebacks(backend: SQLModelBackend) -> None:
    """Insert multiple chargeback rows for aggregation tests."""
    with backend.create_unit_of_work() as uow:
        for i, uid in enumerate(["user-1", "user-1", "user-2"]):
            uow.chargebacks.upsert(
                ChargebackRow(
                    ecosystem="test-eco",
                    tenant_id="test-tenant",
                    timestamp=datetime(2026, 2, 15, i, tzinfo=UTC),
                    resource_id=f"r-{i}",
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


class TestAggregateChargebacks:
    def test_aggregate_empty(self, app_with_backend: TestClient) -> None:
        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/chargebacks/aggregate",
            params={"group_by": "identity_id"},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["buckets"] == []
        assert data["total_amount"] == "0"
        assert data["total_rows"] == 0

    def test_aggregate_by_identity(self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend) -> None:
        _seed_chargebacks(in_memory_backend)
        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/chargebacks/aggregate",
            params={
                "group_by": "identity_id",
                "time_bucket": "day",
                "start_date": "2026-02-01",
                "end_date": "2026-02-28",
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert data["total_rows"] >= 1
        buckets = data["buckets"]
        # Check dimensions dict structure
        for b in buckets:
            assert "identity_id" in b["dimensions"]
        keys = {b["dimensions"]["identity_id"] for b in buckets}
        assert "user-1" in keys
        assert "user-2" in keys

    def test_aggregate_by_multiple_dimensions(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        _seed_chargebacks(in_memory_backend)
        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/chargebacks/aggregate",
            params={
                "group_by": ["identity_id", "product_type"],
                "time_bucket": "day",
                "start_date": "2026-02-01",
                "end_date": "2026-02-28",
            },
        )
        assert response.status_code == 200
        data = response.json()
        for b in data["buckets"]:
            assert "identity_id" in b["dimensions"]
            assert "product_type" in b["dimensions"]

    def test_aggregate_invalid_group_by(self, app_with_backend: TestClient) -> None:
        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/chargebacks/aggregate",
            params={"group_by": "invalid_column"},
        )
        assert response.status_code == 400
        assert "group_by" in response.json()["detail"]

    def test_aggregate_invalid_time_bucket(self, app_with_backend: TestClient) -> None:
        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/chargebacks/aggregate",
            params={"group_by": "identity_id", "time_bucket": "year"},
        )
        assert response.status_code == 400
        assert "time_bucket" in response.json()["detail"]

    def test_aggregate_invalid_date_range(self, app_with_backend: TestClient) -> None:
        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/chargebacks/aggregate",
            params={
                "group_by": "identity_id",
                "start_date": "2026-02-28",
                "end_date": "2026-02-01",
            },
        )
        assert response.status_code == 400

    def test_aggregate_nonexistent_tenant(self, app_with_backend: TestClient) -> None:
        response = app_with_backend.get(
            "/api/v1/tenants/no-such-tenant/chargebacks/aggregate",
            params={"group_by": "identity_id"},
        )
        assert response.status_code == 404

    def test_aggregate_by_product_type(self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend) -> None:
        _seed_chargebacks(in_memory_backend)
        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/chargebacks/aggregate",
            params={
                "group_by": "product_type",
                "time_bucket": "month",
                "start_date": "2026-02-01",
                "end_date": "2026-02-28",
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert data["total_rows"] >= 1
        assert data["buckets"][0]["dimensions"]["product_type"] == "kafka"

    def test_aggregate_total_amount_sums(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        _seed_chargebacks(in_memory_backend)
        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/chargebacks/aggregate",
            params={
                "group_by": "product_type",
                "time_bucket": "day",
                "start_date": "2026-02-01",
                "end_date": "2026-02-28",
            },
        )
        assert response.status_code == 200
        data = response.json()
        # 3 rows of 10.00 each
        assert Decimal(data["total_amount"]) == Decimal("30.0")
        assert data["total_rows"] == 3
