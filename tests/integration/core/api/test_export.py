from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from fastapi.testclient import TestClient  # noqa: TC002

from core.models.chargeback import ChargebackRow, CostType
from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend  # noqa: TC001


def _seed_chargeback(backend: SQLModelBackend, *, with_custom_tag: bool = False) -> None:
    with backend.create_unit_of_work() as uow:
        row = uow.chargebacks.upsert(
            ChargebackRow(
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
        )
        if with_custom_tag and row.dimension_id is not None:
            uow.tags.add_tag(row.dimension_id, "env", "prod", "test")
        uow.commit()


class TestExportChargebacks:
    def test_export_empty(self, app_with_backend: TestClient) -> None:
        response = app_with_backend.post(
            "/api/v1/tenants/test-tenant/export",
            json={"start_date": "2026-02-01", "end_date": "2026-02-28"},
        )
        assert response.status_code == 200
        assert response.headers["content-type"] == "text/csv; charset=utf-8"
        assert "Content-Disposition" in response.headers
        lines = response.text.strip().split("\n")
        assert len(lines) == 1  # header only

    def test_export_with_data(self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend) -> None:
        _seed_chargeback(in_memory_backend)
        response = app_with_backend.post(
            "/api/v1/tenants/test-tenant/export",
            json={"start_date": "2026-02-01", "end_date": "2026-02-28"},
        )
        assert response.status_code == 200
        lines = response.text.strip().split("\n")
        assert len(lines) == 2  # header + 1 data row
        header = lines[0]
        assert "timestamp" in header
        assert "identity_id" in header

    def test_export_custom_columns(self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend) -> None:
        _seed_chargeback(in_memory_backend, with_custom_tag=True)
        response = app_with_backend.post(
            "/api/v1/tenants/test-tenant/export",
            json={
                "start_date": "2026-02-01",
                "end_date": "2026-02-28",
                "columns": ["identity_id", "amount", "tags"],
            },
        )
        assert response.status_code == 200
        lines = response.text.strip().split("\n")
        assert lines[0] == "identity_id,amount,tags"
        assert "user-1" in lines[1]
        assert "prod" in lines[1]  # custom tag display_name

    def test_export_with_filters(self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend) -> None:
        _seed_chargeback(in_memory_backend)
        response = app_with_backend.post(
            "/api/v1/tenants/test-tenant/export",
            json={
                "start_date": "2026-02-01",
                "end_date": "2026-02-28",
                "filters": {"identity_id": "user-1"},
            },
        )
        assert response.status_code == 200
        lines = response.text.strip().split("\n")
        assert len(lines) == 2  # header + 1 match

    def test_export_with_filters_no_match(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        _seed_chargeback(in_memory_backend)
        response = app_with_backend.post(
            "/api/v1/tenants/test-tenant/export",
            json={
                "start_date": "2026-02-01",
                "end_date": "2026-02-28",
                "filters": {"identity_id": "no-such-user"},
            },
        )
        assert response.status_code == 200
        lines = response.text.strip().split("\n")
        assert len(lines) == 1  # header only

    def test_export_default_dates(self, app_with_backend: TestClient) -> None:
        """Export with no dates defaults to last 30 days."""
        response = app_with_backend.post("/api/v1/tenants/test-tenant/export", json={})
        assert response.status_code == 200

    def test_export_invalid_columns(self, app_with_backend: TestClient) -> None:
        response = app_with_backend.post(
            "/api/v1/tenants/test-tenant/export",
            json={
                "start_date": "2026-02-01",
                "end_date": "2026-02-28",
                "columns": ["identity_id", "nonexistent"],
            },
        )
        assert response.status_code == 400
        assert "nonexistent" in response.json()["detail"]

    def test_export_invalid_filter_key(self, app_with_backend: TestClient) -> None:
        response = app_with_backend.post(
            "/api/v1/tenants/test-tenant/export",
            json={
                "start_date": "2026-02-01",
                "end_date": "2026-02-28",
                "filters": {"bad_key": "value"},
            },
        )
        assert response.status_code == 400
        assert "bad_key" in response.json()["detail"]

    def test_export_invalid_date_range(self, app_with_backend: TestClient) -> None:
        response = app_with_backend.post(
            "/api/v1/tenants/test-tenant/export",
            json={"start_date": "2026-02-28", "end_date": "2026-02-01"},
        )
        assert response.status_code == 400

    def test_export_nonexistent_tenant(self, app_with_backend: TestClient) -> None:
        response = app_with_backend.post(
            "/api/v1/tenants/no-such-tenant/export",
            json={"start_date": "2026-02-01", "end_date": "2026-02-28"},
        )
        assert response.status_code == 404
