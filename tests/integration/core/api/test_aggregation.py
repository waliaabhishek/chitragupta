from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from fastapi.testclient import TestClient  # noqa: TC002

from core.models.chargeback import ChargebackRow, CostType
from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend  # noqa: TC001


def _seed_chargebacks(backend: SQLModelBackend) -> None:
    """Insert multiple chargeback rows for aggregation tests."""
    rows = [
        # (identity_id, product_type, resource_id, cost_type, amount)
        ("user-1", "kafka", "r-0", CostType.USAGE, Decimal("10.00")),
        ("user-1", "kafka", "r-1", CostType.USAGE, Decimal("10.00")),
        ("user-2", "connect", "r-2", CostType.SHARED, Decimal("10.00")),
    ]
    with backend.create_unit_of_work() as uow:
        for i, (uid, ptype, rid, ctype, amount) in enumerate(rows):
            uow.chargebacks.upsert(
                ChargebackRow(
                    ecosystem="test-eco",
                    tenant_id="test-tenant",
                    timestamp=datetime(2026, 2, 15, i, tzinfo=UTC),
                    resource_id=rid,
                    product_category="compute",
                    product_type=ptype,
                    identity_id=uid,
                    cost_type=ctype,
                    amount=amount,
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
        # 2 identities × 1 day = 2 buckets; total_rows = 3 (2 for user-1, 1 for user-2)
        assert data["total_rows"] == 3
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
        # 2 product types × 1 month bucket = 2 buckets; total_rows = 3
        assert data["total_rows"] == 3
        product_types = {b["dimensions"]["product_type"] for b in data["buckets"]}
        assert "kafka" in product_types

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


class TestAggregateWithFilters:
    _COMMON_PARAMS = {
        "group_by": "identity_id",
        "time_bucket": "day",
        "start_date": "2026-02-01",
        "end_date": "2026-02-28",
    }

    def test_aggregate_with_identity_filter(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        _seed_chargebacks(in_memory_backend)
        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/chargebacks/aggregate",
            params={**self._COMMON_PARAMS, "identity_id": "user-1"},
        )
        assert response.status_code == 200
        data = response.json()
        identity_ids = {b["dimensions"]["identity_id"] for b in data["buckets"]}
        assert identity_ids == {"user-1"}
        assert "user-2" not in identity_ids

    def test_aggregate_with_product_type_filter(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        _seed_chargebacks(in_memory_backend)
        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/chargebacks/aggregate",
            params={**self._COMMON_PARAMS, "product_type": "connect"},
        )
        assert response.status_code == 200
        data = response.json()
        # Only user-2 row has product_type="connect"
        assert data["total_rows"] == 1
        assert Decimal(data["total_amount"]) == Decimal("10.00")

    def test_aggregate_with_resource_id_filter(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        _seed_chargebacks(in_memory_backend)
        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/chargebacks/aggregate",
            params={**self._COMMON_PARAMS, "resource_id": "r-0"},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["total_rows"] == 1
        assert Decimal(data["total_amount"]) == Decimal("10.00")

    def test_aggregate_with_cost_type_filter(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        _seed_chargebacks(in_memory_backend)
        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/chargebacks/aggregate",
            params={**self._COMMON_PARAMS, "cost_type": "shared"},
        )
        assert response.status_code == 200
        data = response.json()
        # Only user-2 row has cost_type=SHARED
        assert data["total_rows"] == 1

    def test_aggregate_with_multiple_filters(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        _seed_chargebacks(in_memory_backend)
        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/chargebacks/aggregate",
            params={**self._COMMON_PARAMS, "identity_id": "user-1", "product_type": "kafka"},
        )
        assert response.status_code == 200
        data = response.json()
        # user-1 has 2 kafka rows
        assert data["total_rows"] == 2
        assert Decimal(data["total_amount"]) == Decimal("20.00")

    def test_aggregate_with_no_matching_filter(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        _seed_chargebacks(in_memory_backend)
        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/chargebacks/aggregate",
            params={**self._COMMON_PARAMS, "identity_id": "nonexistent-user"},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["buckets"] == []
        assert data["total_rows"] == 0
