from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from typing import TYPE_CHECKING
from unittest.mock import patch

from core.models.billing import BillingLineItem
from core.models.chargeback import ChargebackRow, CostType
from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend  # noqa: TC001

if TYPE_CHECKING:
    from fastapi.testclient import TestClient

# Fixed UTC "today" used across all route tests.
# window: effective_start = date(2025, 12, 16), effective_end = date(2026, 1, 15)
_FIXED_TODAY = date(2026, 1, 15)

# A timestamp inside the 30-day window relative to _FIXED_TODAY.
_IN_WINDOW_TS = datetime(2026, 1, 10, tzinfo=UTC)

# A timestamp outside the window (after _FIXED_TODAY).
_OUT_WINDOW_TS = datetime(2026, 1, 20, tzinfo=UTC)


def _make_billing(ts: datetime) -> BillingLineItem:
    return BillingLineItem(
        ecosystem="test-eco",
        tenant_id="test-tenant",
        timestamp=ts,
        resource_id="resource-1",
        product_category="compute",
        product_type="kafka",
        quantity=Decimal("1"),
        unit_price=Decimal("1"),
        total_cost=Decimal("1"),
        currency="USD",
        granularity="daily",
        metadata={},
    )


def _make_chargeback(ts: datetime) -> ChargebackRow:
    return ChargebackRow(
        ecosystem="test-eco",
        tenant_id="test-tenant",
        timestamp=ts,
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


class TestBillingUsesUtcToday:
    def test_default_dates_use_utc_today(
        self,
        app_with_backend: TestClient,
        in_memory_backend: SQLModelBackend,
    ) -> None:
        """Without explicit dates, billing must use utc_today() to compute the window.

        Data at _IN_WINDOW_TS is inside [_FIXED_TODAY - 30d, _FIXED_TODAY].
        Data at _OUT_WINDOW_TS is after _FIXED_TODAY and must be excluded.
        """
        with in_memory_backend.create_unit_of_work() as uow:
            uow.billing.upsert(_make_billing(_IN_WINDOW_TS))
            uow.billing.upsert(_make_billing(_OUT_WINDOW_TS))
            uow.commit()

        with patch("core.api.dependencies.utc_today", return_value=_FIXED_TODAY):
            response = app_with_backend.get("/api/v1/tenants/test-tenant/billing")

        assert response.status_code == 200
        data = response.json()
        # Only the in-window item is returned.
        assert data["total"] == 1
        assert data["items"][0]["resource_id"] == "resource-1"


class TestAggregationUsesUtcToday:
    def test_default_dates_use_utc_today(
        self,
        app_with_backend: TestClient,
        in_memory_backend: SQLModelBackend,
    ) -> None:
        """Without explicit dates, aggregation must use utc_today() for the window."""
        with in_memory_backend.create_unit_of_work() as uow:
            uow.chargebacks.upsert(_make_chargeback(_IN_WINDOW_TS))
            uow.chargebacks.upsert(_make_chargeback(_OUT_WINDOW_TS))
            uow.commit()

        with patch("core.api.dependencies.utc_today", return_value=_FIXED_TODAY):
            response = app_with_backend.get("/api/v1/tenants/test-tenant/chargebacks/aggregate")

        assert response.status_code == 200
        data = response.json()
        # Only the in-window chargeback contributes to total_rows.
        assert data["total_rows"] == 1


class TestChargebacksUsesUtcToday:
    def test_default_dates_use_utc_today(
        self,
        app_with_backend: TestClient,
        in_memory_backend: SQLModelBackend,
    ) -> None:
        """Without explicit dates, chargebacks must use utc_today() for the window."""
        with in_memory_backend.create_unit_of_work() as uow:
            uow.chargebacks.upsert(_make_chargeback(_IN_WINDOW_TS))
            uow.chargebacks.upsert(_make_chargeback(_OUT_WINDOW_TS))
            uow.commit()

        with patch("core.api.dependencies.utc_today", return_value=_FIXED_TODAY):
            response = app_with_backend.get("/api/v1/tenants/test-tenant/chargebacks")

        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 1
        assert data["items"][0]["identity_id"] == "user-1"


class TestExportUsesUtcToday:
    def test_default_dates_use_utc_today(
        self,
        app_with_backend: TestClient,
        in_memory_backend: SQLModelBackend,
    ) -> None:
        """Without explicit dates, CSV export must use utc_today() for the window."""
        with in_memory_backend.create_unit_of_work() as uow:
            uow.chargebacks.upsert(_make_chargeback(_IN_WINDOW_TS))
            uow.chargebacks.upsert(_make_chargeback(_OUT_WINDOW_TS))
            uow.commit()

        with patch("core.api.dependencies.utc_today", return_value=_FIXED_TODAY):
            response = app_with_backend.post(
                "/api/v1/tenants/test-tenant/export",
                json={},
            )

        assert response.status_code == 200
        lines = response.text.strip().split("\n")
        # header row + exactly 1 data row (the in-window chargeback).
        assert len(lines) == 2


class TestTagsBulkByFilterUsesUtcToday:
    def test_default_dates_use_utc_today(
        self,
        app_with_backend: TestClient,
        in_memory_backend: SQLModelBackend,
    ) -> None:
        """Without explicit dates, bulk-by-filter must use utc_today() for the window."""
        with in_memory_backend.create_unit_of_work() as uow:
            uow.chargebacks.upsert(_make_chargeback(_IN_WINDOW_TS))
            uow.chargebacks.upsert(_make_chargeback(_OUT_WINDOW_TS))
            uow.commit()

        with patch("core.api.dependencies.utc_today", return_value=_FIXED_TODAY):
            response = app_with_backend.post(
                "/api/v1/tenants/test-tenant/tags/bulk-by-filter",
                json={
                    "tag_key": "env",
                    "display_name": "Production",
                    "created_by": "admin",
                },
            )

        assert response.status_code == 200
        data = response.json()
        # Only the in-window chargeback's dimension gets tagged.
        assert data["created_count"] == 1
        assert data["errors"] == []
