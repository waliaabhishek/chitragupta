from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

from core.api.schemas import (
    BillingLineResponse,
    ChargebackDimensionResponse,
    ChargebackResponse,
    HealthResponse,
    IdentityResponse,
    PaginatedResponse,
    PipelineStateResponse,
    ResourceResponse,
    TenantListResponse,
    TenantStatusDetailResponse,
    TenantStatusSummary,
)


class TestPaginatedResponse:
    def test_generic_with_strings(self) -> None:
        resp = PaginatedResponse[str](items=["a", "b"], total=2, page=1, page_size=10, pages=1)
        assert resp.items == ["a", "b"]
        assert resp.total == 2

    def test_generic_with_dict(self) -> None:
        resp = PaginatedResponse[dict](items=[{"k": "v"}], total=1, page=1, page_size=10, pages=1)
        assert len(resp.items) == 1

    def test_empty_items(self) -> None:
        resp = PaginatedResponse[str](items=[], total=0, page=1, page_size=10, pages=0)
        assert resp.items == []
        assert resp.pages == 0


class TestTenantSchemas:
    def test_tenant_status_summary(self) -> None:
        s = TenantStatusSummary(
            tenant_name="t1",
            tenant_id="id1",
            ecosystem="eco",
            dates_pending=5,
            dates_calculated=10,
            last_calculated_date=date(2026, 1, 15),
        )
        assert s.tenant_name == "t1"
        assert s.last_calculated_date == date(2026, 1, 15)

    def test_tenant_status_summary_null_date(self) -> None:
        s = TenantStatusSummary(
            tenant_name="t1",
            tenant_id="id1",
            ecosystem="eco",
            dates_pending=0,
            dates_calculated=0,
            last_calculated_date=None,
        )
        assert s.last_calculated_date is None

    def test_tenant_list_response(self) -> None:
        resp = TenantListResponse(tenants=[])
        assert resp.tenants == []

    def test_pipeline_state_response(self) -> None:
        ps = PipelineStateResponse(
            tracking_date=date(2026, 1, 15),
            billing_gathered=True,
            resources_gathered=False,
            chargeback_calculated=False,
        )
        assert ps.billing_gathered is True

    def test_tenant_status_detail(self) -> None:
        resp = TenantStatusDetailResponse(
            tenant_name="t1",
            tenant_id="id1",
            ecosystem="eco",
            states=[],
        )
        assert resp.states == []


class TestResourceResponse:
    def test_all_fields(self) -> None:
        r = ResourceResponse(
            ecosystem="eco",
            tenant_id="t1",
            resource_id="r1",
            resource_type="kafka",
            display_name="My Cluster",
            parent_id=None,
            owner_id=None,
            status="active",
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
            deleted_at=None,
            last_seen_at=None,
            metadata={"cloud": "aws"},
        )
        assert r.resource_id == "r1"
        assert r.metadata["cloud"] == "aws"


class TestIdentityResponse:
    def test_all_fields(self) -> None:
        i = IdentityResponse(
            ecosystem="eco",
            tenant_id="t1",
            identity_id="u1",
            identity_type="user",
            display_name=None,
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
            deleted_at=None,
            last_seen_at=None,
            metadata={},
        )
        assert i.identity_id == "u1"


class TestBillingLineResponse:
    def test_decimal_serialization(self) -> None:
        b = BillingLineResponse(
            ecosystem="eco",
            tenant_id="t1",
            timestamp=datetime(2026, 1, 15, tzinfo=UTC),
            resource_id="r1",
            product_category="compute",
            product_type="kafka",
            quantity=Decimal("100.5"),
            unit_price=Decimal("0.01"),
            total_cost=Decimal("1.005"),
            currency="USD",
            granularity="daily",
            metadata={},
        )
        data = b.model_dump(mode="json")
        # Pydantic v2 serializes Decimal as string in JSON mode
        assert data["quantity"] == "100.5"
        assert data["total_cost"] == "1.005"


class TestChargebackResponse:
    def test_all_fields(self) -> None:
        c = ChargebackResponse(
            dimension_id=42,
            ecosystem="eco",
            tenant_id="t1",
            timestamp=datetime(2026, 1, 15, tzinfo=UTC),
            resource_id="r1",
            product_category="compute",
            product_type="kafka",
            identity_id="u1",
            cost_type="usage",
            amount=Decimal("50.00"),
            allocation_method="direct",
            allocation_detail=None,
            tags={"tag1": "val1"},
            metadata={},
        )
        assert c.cost_type == "usage"
        assert c.tags == {"tag1": "val1"}


class TestChargebackDimensionResponse:
    def test_all_fields(self) -> None:
        d = ChargebackDimensionResponse(
            dimension_id=42,
            ecosystem="ccloud",
            env_id="env-001",
            tenant_id="t-001",
            resource_id="r-001",
            product_category="KAFKA",
            product_type="KAFKA_NUM_BYTES",
            identity_id="user@example.com",
            cost_type="usage",
            allocation_method="ratio",
            allocation_detail=None,
            tags={"env": "prod"},
        )
        assert d.dimension_id == 42
        assert d.product_type == "KAFKA_NUM_BYTES"
        assert len(d.tags) == 1
        assert d.tags["env"] == "prod"

    def test_empty_tags(self) -> None:
        d = ChargebackDimensionResponse(
            dimension_id=1,
            ecosystem="ccloud",
            env_id="",
            tenant_id="t-001",
            resource_id=None,
            product_category="KAFKA",
            product_type="KAFKA_NUM_BYTES",
            identity_id="user@example.com",
            cost_type="usage",
            allocation_method=None,
            allocation_detail=None,
            tags={},
        )
        assert d.tags == {}
        assert d.resource_id is None
        assert d.allocation_method is None


class TestHealthResponse:
    def test_basic(self) -> None:
        h = HealthResponse(status="ok", version="1.0.0")
        assert h.status == "ok"

    def test_datetime_serialization_utc(self) -> None:
        r = ResourceResponse(
            ecosystem="eco",
            tenant_id="t1",
            resource_id="r1",
            resource_type="kafka",
            display_name=None,
            parent_id=None,
            owner_id=None,
            status="active",
            created_at=datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC),
            deleted_at=None,
            last_seen_at=None,
            metadata={},
        )
        data = r.model_dump(mode="json")
        assert "2026-01-01" in data["created_at"]
