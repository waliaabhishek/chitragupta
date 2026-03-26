from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from fastapi.testclient import TestClient  # noqa: TC002
from sqlmodel import Session  # noqa: TC002

from core.models.billing import CoreBillingLineItem
from core.models.chargeback import AllocationDetail, ChargebackRow, CostType
from core.storage.backends.sqlmodel.tables import ChargebackDimensionTable, ChargebackFactTable
from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend  # noqa: TC001

# Denver (MST) = UTC-7 in December.
# _NORMAL_TS:   Dec 15 12:00 UTC  → Dec 15 05:00 Denver  (inside Dec window in both UTC and Denver)
# _BOUNDARY_TS: Jan  1 03:00 UTC  → Dec 31 20:00 Denver  (outside UTC Dec window; inside Denver Dec window)
#
# Querying start_date=2025-12-01, end_date=2025-12-31:
#   UTC window:    [2025-12-01T00:00Z, 2026-01-01T00:00Z) → excludes _BOUNDARY_TS
#   Denver window: [2025-12-01T07:00Z, 2026-01-01T07:00Z) → includes _BOUNDARY_TS
_NORMAL_TS = datetime(2025, 12, 15, 12, 0, 0, tzinfo=UTC)
_BOUNDARY_TS = datetime(2026, 1, 1, 3, 0, 0, tzinfo=UTC)

_DEC_START = "2025-12-01"
_DEC_END = "2025-12-31"
_DENVER = "America/Denver"
_INVALID_TZ = "Not/A_Timezone"


# ---------------------------------------------------------------------------
# Seed helpers
# ---------------------------------------------------------------------------


def _billing_row(timestamp: datetime, resource_id: str) -> CoreBillingLineItem:
    return CoreBillingLineItem(
        ecosystem="test-eco",
        tenant_id="test-tenant",
        timestamp=timestamp,
        resource_id=resource_id,
        product_category="compute",
        product_type="kafka",
        quantity=Decimal("1"),
        unit_price=Decimal("10.00"),
        total_cost=Decimal("10.00"),
        currency="USD",
        granularity="daily",
        metadata={},
    )


def _chargeback_row(timestamp: datetime, resource_id: str) -> ChargebackRow:
    return ChargebackRow(
        ecosystem="test-eco",
        tenant_id="test-tenant",
        timestamp=timestamp,
        resource_id=resource_id,
        product_category="compute",
        product_type="kafka",
        identity_id="user-1",
        cost_type=CostType.USAGE,
        amount=Decimal("10.00"),
        allocation_method="direct",
        allocation_detail=None,
        tags={},
        metadata={},
    )


def _insert_allocation_issue_fact(session: Session, *, timestamp: datetime, resource_id: str) -> None:
    """Insert a chargeback row that qualifies as an allocation issue."""
    dim = ChargebackDimensionTable(
        ecosystem="test-eco",
        tenant_id="test-tenant",
        resource_id=resource_id,
        product_category="compute",
        product_type="kafka",
        identity_id="sa-1",
        cost_type=CostType.USAGE.value,
        allocation_method="direct",
        allocation_detail=AllocationDetail.NO_IDENTITIES_LOCATED.value,
    )
    session.add(dim)
    session.flush()
    fact = ChargebackFactTable(
        dimension_id=dim.dimension_id,
        timestamp=timestamp,
        amount="10.00",
        tags_json="[]",
    )
    session.add(fact)
    session.flush()


# ---------------------------------------------------------------------------
# Billing
# ---------------------------------------------------------------------------


class TestBillingTimezone:
    def test_billing_list_timezone_denver_includes_boundary_rows(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        with in_memory_backend.create_unit_of_work() as uow:
            uow.billing.upsert(_billing_row(_NORMAL_TS, "r-normal"))
            uow.billing.upsert(_billing_row(_BOUNDARY_TS, "r-boundary"))
            uow.commit()

        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/billing",
            params={"start_date": _DEC_START, "end_date": _DEC_END, "timezone": _DENVER, "page_size": 100},
        )
        assert response.status_code == 200
        assert response.json()["total"] == 2

    def test_billing_list_timezone_utc_excludes_boundary_rows(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        with in_memory_backend.create_unit_of_work() as uow:
            uow.billing.upsert(_billing_row(_NORMAL_TS, "r-normal"))
            uow.billing.upsert(_billing_row(_BOUNDARY_TS, "r-boundary"))
            uow.commit()

        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/billing",
            params={"start_date": _DEC_START, "end_date": _DEC_END, "timezone": "UTC", "page_size": 100},
        )
        assert response.status_code == 200
        assert response.json()["total"] == 1

    def test_billing_list_no_timezone_backward_compat(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        """Omitting timezone should behave identically to timezone=UTC."""
        with in_memory_backend.create_unit_of_work() as uow:
            uow.billing.upsert(_billing_row(_NORMAL_TS, "r-normal"))
            uow.billing.upsert(_billing_row(_BOUNDARY_TS, "r-boundary"))
            uow.commit()

        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/billing",
            params={"start_date": _DEC_START, "end_date": _DEC_END, "page_size": 100},
        )
        assert response.status_code == 200
        assert response.json()["total"] == 1

    def test_billing_list_invalid_timezone_returns_400(self, app_with_backend: TestClient) -> None:
        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/billing",
            params={"start_date": _DEC_START, "end_date": _DEC_END, "timezone": _INVALID_TZ},
        )
        assert response.status_code == 400


# ---------------------------------------------------------------------------
# Chargebacks
# ---------------------------------------------------------------------------


class TestChargebacksTimezone:
    def test_chargebacks_list_timezone_denver_includes_boundary_rows(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        with in_memory_backend.create_unit_of_work() as uow:
            uow.chargebacks.upsert(_chargeback_row(_NORMAL_TS, "r-normal"))
            uow.chargebacks.upsert(_chargeback_row(_BOUNDARY_TS, "r-boundary"))
            uow.commit()

        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/chargebacks",
            params={"start_date": _DEC_START, "end_date": _DEC_END, "timezone": _DENVER, "page_size": 100},
        )
        assert response.status_code == 200
        assert response.json()["total"] == 2

    def test_chargebacks_list_no_timezone_backward_compat(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        """Omitting timezone should behave identically to timezone=UTC."""
        with in_memory_backend.create_unit_of_work() as uow:
            uow.chargebacks.upsert(_chargeback_row(_NORMAL_TS, "r-normal"))
            uow.chargebacks.upsert(_chargeback_row(_BOUNDARY_TS, "r-boundary"))
            uow.commit()

        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/chargebacks",
            params={"start_date": _DEC_START, "end_date": _DEC_END, "page_size": 100},
        )
        assert response.status_code == 200
        assert response.json()["total"] == 1

    def test_chargebacks_list_invalid_timezone_returns_400(self, app_with_backend: TestClient) -> None:
        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/chargebacks",
            params={"start_date": _DEC_START, "end_date": _DEC_END, "timezone": _INVALID_TZ},
        )
        assert response.status_code == 400


# ---------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------


class TestAggregationTimezone:
    def test_aggregation_timezone_denver_includes_boundary_rows(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        with in_memory_backend.create_unit_of_work() as uow:
            uow.chargebacks.upsert(_chargeback_row(_NORMAL_TS, "r-normal"))
            uow.chargebacks.upsert(_chargeback_row(_BOUNDARY_TS, "r-boundary"))
            uow.commit()

        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/chargebacks/aggregate",
            params={"start_date": _DEC_START, "end_date": _DEC_END, "timezone": _DENVER},
        )
        assert response.status_code == 200
        assert response.json()["total_rows"] == 2

    def test_aggregation_no_timezone_backward_compat(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        """Omitting timezone should behave identically to timezone=UTC."""
        with in_memory_backend.create_unit_of_work() as uow:
            uow.chargebacks.upsert(_chargeback_row(_NORMAL_TS, "r-normal"))
            uow.chargebacks.upsert(_chargeback_row(_BOUNDARY_TS, "r-boundary"))
            uow.commit()

        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/chargebacks/aggregate",
            params={"start_date": _DEC_START, "end_date": _DEC_END},
        )
        assert response.status_code == 200
        assert response.json()["total_rows"] == 1

    def test_aggregation_invalid_timezone_returns_400(self, app_with_backend: TestClient) -> None:
        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/chargebacks/aggregate",
            params={"start_date": _DEC_START, "end_date": _DEC_END, "timezone": _INVALID_TZ},
        )
        assert response.status_code == 400


# ---------------------------------------------------------------------------
# Allocation issues
# ---------------------------------------------------------------------------


class TestAllocationIssuesTimezone:
    def test_allocation_issues_timezone_denver_includes_boundary_rows(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        with in_memory_backend.create_unit_of_work() as uow:
            _insert_allocation_issue_fact(uow._session, timestamp=_NORMAL_TS, resource_id="r-normal")  # type: ignore[attr-defined]
            _insert_allocation_issue_fact(uow._session, timestamp=_BOUNDARY_TS, resource_id="r-boundary")  # type: ignore[attr-defined]
            uow.commit()

        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/chargebacks/allocation-issues",
            params={"start_date": _DEC_START, "end_date": _DEC_END, "timezone": _DENVER, "page_size": 100},
        )
        assert response.status_code == 200
        assert response.json()["total"] == 2

    def test_allocation_issues_no_timezone_backward_compat(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        """Omitting timezone should behave identically to timezone=UTC."""
        with in_memory_backend.create_unit_of_work() as uow:
            _insert_allocation_issue_fact(uow._session, timestamp=_NORMAL_TS, resource_id="r-normal")  # type: ignore[attr-defined]
            _insert_allocation_issue_fact(uow._session, timestamp=_BOUNDARY_TS, resource_id="r-boundary")  # type: ignore[attr-defined]
            uow.commit()

        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/chargebacks/allocation-issues",
            params={"start_date": _DEC_START, "end_date": _DEC_END, "page_size": 100},
        )
        assert response.status_code == 200
        assert response.json()["total"] == 1

    def test_allocation_issues_invalid_timezone_returns_400(self, app_with_backend: TestClient) -> None:
        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/chargebacks/allocation-issues",
            params={"start_date": _DEC_START, "end_date": _DEC_END, "timezone": _INVALID_TZ},
        )
        assert response.status_code == 400


# ---------------------------------------------------------------------------
# Export (POST body)
# ---------------------------------------------------------------------------


class TestExportTimezone:
    def test_export_timezone_denver_includes_boundary_rows(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        with in_memory_backend.create_unit_of_work() as uow:
            uow.chargebacks.upsert(_chargeback_row(_NORMAL_TS, "r-normal"))
            uow.chargebacks.upsert(_chargeback_row(_BOUNDARY_TS, "r-boundary"))
            uow.commit()

        response = app_with_backend.post(
            "/api/v1/tenants/test-tenant/export",
            json={"start_date": _DEC_START, "end_date": _DEC_END, "timezone": _DENVER},
        )
        assert response.status_code == 200
        lines = response.text.strip().split("\n")
        # header + 2 data rows
        assert len(lines) == 3

    def test_export_no_timezone_backward_compat(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        """Omitting timezone from request body should behave identically to UTC."""
        with in_memory_backend.create_unit_of_work() as uow:
            uow.chargebacks.upsert(_chargeback_row(_NORMAL_TS, "r-normal"))
            uow.chargebacks.upsert(_chargeback_row(_BOUNDARY_TS, "r-boundary"))
            uow.commit()

        response = app_with_backend.post(
            "/api/v1/tenants/test-tenant/export",
            json={"start_date": _DEC_START, "end_date": _DEC_END},
        )
        assert response.status_code == 200
        lines = response.text.strip().split("\n")
        # header + 1 data row
        assert len(lines) == 2

    def test_export_invalid_timezone_returns_400(self, app_with_backend: TestClient) -> None:
        response = app_with_backend.post(
            "/api/v1/tenants/test-tenant/export",
            json={"start_date": _DEC_START, "end_date": _DEC_END, "timezone": _INVALID_TZ},
        )
        assert response.status_code == 400


# ---------------------------------------------------------------------------
# Bulk tag by filter (POST body)
# ---------------------------------------------------------------------------


class TestBulkTagByFilterTimezone:
    def test_bulk_tag_by_filter_timezone_denver_includes_boundary_rows(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        with in_memory_backend.create_unit_of_work() as uow:
            uow.chargebacks.upsert(_chargeback_row(_NORMAL_TS, "r-normal"))
            uow.chargebacks.upsert(_chargeback_row(_BOUNDARY_TS, "r-boundary"))
            uow.commit()

        response = app_with_backend.post(
            "/api/v1/tenants/test-tenant/tags/bulk-by-filter",
            json={
                "start_date": _DEC_START,
                "end_date": _DEC_END,
                "timezone": _DENVER,
                "tag_key": "env",
                "display_name": "Environment",
                "created_by": "admin",
            },
        )
        assert response.status_code == 200
        assert response.json()["created_count"] == 2

    def test_bulk_tag_by_filter_no_timezone_backward_compat(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        """Omitting timezone from request body should behave identically to UTC."""
        with in_memory_backend.create_unit_of_work() as uow:
            uow.chargebacks.upsert(_chargeback_row(_NORMAL_TS, "r-normal"))
            uow.chargebacks.upsert(_chargeback_row(_BOUNDARY_TS, "r-boundary"))
            uow.commit()

        response = app_with_backend.post(
            "/api/v1/tenants/test-tenant/tags/bulk-by-filter",
            json={
                "start_date": _DEC_START,
                "end_date": _DEC_END,
                "tag_key": "env",
                "display_name": "Environment",
                "created_by": "admin",
            },
        )
        assert response.status_code == 200
        assert response.json()["created_count"] == 1

    def test_bulk_tag_by_filter_invalid_timezone_returns_400(self, app_with_backend: TestClient) -> None:
        response = app_with_backend.post(
            "/api/v1/tenants/test-tenant/tags/bulk-by-filter",
            json={
                "start_date": _DEC_START,
                "end_date": _DEC_END,
                "timezone": _INVALID_TZ,
                "tag_key": "env",
                "display_name": "Environment",
                "created_by": "admin",
            },
        )
        assert response.status_code == 400
