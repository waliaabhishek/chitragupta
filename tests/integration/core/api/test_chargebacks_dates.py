from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from fastapi.testclient import TestClient  # noqa: TC002

from core.models.chargeback import ChargebackRow, CostType
from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend  # noqa: TC001


def _make_row(
    *,
    tenant_id: str,
    ecosystem: str,
    timestamp: datetime,
    resource_id: str = "r-1",
    identity_id: str = "user-1",
    product_type: str = "kafka",
) -> ChargebackRow:
    return ChargebackRow(
        ecosystem=ecosystem,
        tenant_id=tenant_id,
        timestamp=timestamp,
        resource_id=resource_id,
        product_category="compute",
        product_type=product_type,
        identity_id=identity_id,
        cost_type=CostType.USAGE,
        amount=Decimal("10.00"),
        allocation_method="direct",
        allocation_detail=None,
        tags=[],
        metadata={},
    )


class TestChargebackDatesEndpoint:
    def test_multi_date_tenant_isolation(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        """Only dates for the requested tenant are returned; other-tenant dates excluded."""
        with in_memory_backend.create_unit_of_work() as uow:
            for day in [1, 2, 3]:
                uow.chargebacks.upsert(
                    _make_row(
                        tenant_id="test-tenant",
                        ecosystem="test-eco",
                        timestamp=datetime(2026, 1, day, tzinfo=UTC),
                        resource_id=f"r-{day}",
                    )
                )
            # Different tenant — should NOT appear
            uow.chargebacks.upsert(
                _make_row(
                    tenant_id="other-tenant",
                    ecosystem="test-eco",
                    timestamp=datetime(2026, 1, 4, tzinfo=UTC),
                    resource_id="r-other",
                )
            )
            uow.commit()

        response = app_with_backend.get("/api/v1/tenants/test-tenant/chargebacks/dates")
        assert response.status_code == 200
        data = response.json()
        assert data == {"dates": ["2026-01-01", "2026-01-02", "2026-01-03"]}

    def test_empty_response(self, app_with_backend: TestClient) -> None:
        """Tenant with no facts returns empty dates list."""
        response = app_with_backend.get("/api/v1/tenants/test-tenant/chargebacks/dates")
        assert response.status_code == 200
        data = response.json()
        assert data == {"dates": []}

    def test_deduplication(self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend) -> None:
        """Multiple rows on the same date produce a single entry."""
        ts = datetime(2026, 1, 15, tzinfo=UTC)
        with in_memory_backend.create_unit_of_work() as uow:
            for uid, rid in [("user-1", "r-1"), ("user-2", "r-2"), ("user-3", "r-3")]:
                uow.chargebacks.upsert(
                    _make_row(
                        tenant_id="test-tenant",
                        ecosystem="test-eco",
                        timestamp=ts,
                        resource_id=rid,
                        identity_id=uid,
                    )
                )
            uow.commit()

        response = app_with_backend.get("/api/v1/tenants/test-tenant/chargebacks/dates")
        assert response.status_code == 200
        dates = response.json()["dates"]
        assert dates.count("2026-01-15") == 1
        assert len(dates) == 1

    def test_route_not_captured_by_dynamic_route(self, app_with_backend: TestClient) -> None:
        """Static /dates route must not be swallowed by /{dimension_id}."""
        response = app_with_backend.get("/api/v1/tenants/test-tenant/chargebacks/dates")
        assert response.status_code == 200
        assert "dates" in response.json()

    def test_sorted_ascending(self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend) -> None:
        """Dates returned in ascending order regardless of insertion order."""
        with in_memory_backend.create_unit_of_work() as uow:
            for day in [3, 1, 5, 2, 4]:
                uow.chargebacks.upsert(
                    _make_row(
                        tenant_id="test-tenant",
                        ecosystem="test-eco",
                        timestamp=datetime(2026, 1, day, tzinfo=UTC),
                        resource_id=f"r-{day}",
                    )
                )
            uow.commit()

        response = app_with_backend.get("/api/v1/tenants/test-tenant/chargebacks/dates")
        assert response.status_code == 200
        dates = response.json()["dates"]
        assert dates == sorted(dates)
        assert len(dates) == 5
