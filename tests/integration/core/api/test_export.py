from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from fastapi.testclient import TestClient  # noqa: TC002

from core.models.chargeback import ChargebackRow, CostType
from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend  # noqa: TC001


def _seed_chargeback(backend: SQLModelBackend, *, with_custom_tag: bool = False) -> None:
    with backend.create_unit_of_work() as uow:
        uow.chargebacks.upsert(
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
                tags={},
                metadata={},
            )
        )
        if with_custom_tag:
            uow.tags.add_tag("test-tenant", "resource", "resource-1", "env", "prod", "test")
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


class TestExportStreaming:
    """Tests that verify the streaming path (iter_by_filters) is used for export."""

    def test_export_streaming_all_rows_returned_past_old_limit(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        """Export returns all rows even when count would exceed the old find_by_filters default limit."""
        row_count = 1050  # exceeds find_by_filters default limit=1000
        with in_memory_backend.create_unit_of_work() as uow:
            for i in range(row_count):
                uow.chargebacks.upsert(
                    ChargebackRow(
                        ecosystem="test-eco",
                        tenant_id="test-tenant",
                        timestamp=datetime(2026, 2, 1, tzinfo=UTC),
                        resource_id=f"resource-{i}",
                        product_category="compute",
                        product_type="kafka",
                        identity_id=f"user-{i}",
                        cost_type=CostType.USAGE,
                        amount=Decimal("1.00"),
                        allocation_method="direct",
                        allocation_detail=None,
                        tags=[],
                        metadata={},
                    )
                )
            uow.commit()

        response = app_with_backend.post(
            "/api/v1/tenants/test-tenant/export",
            json={"start_date": "2026-02-01", "end_date": "2026-02-28"},
        )
        assert response.status_code == 200
        lines = [ln for ln in response.text.strip().split("\n") if ln]
        assert len(lines) == row_count + 1  # header + all data rows

    def test_export_streaming_tag_overlay_per_row(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        """Custom tag display_names are applied correctly to each row via streaming path."""
        _seed_chargeback(in_memory_backend, with_custom_tag=True)

        response = app_with_backend.post(
            "/api/v1/tenants/test-tenant/export",
            json={
                "start_date": "2026-02-01",
                "end_date": "2026-02-28",
                "columns": ["identity_id", "tags"],
            },
        )
        assert response.status_code == 200
        lines = response.text.strip().split("\n")
        assert len(lines) == 2  # header + 1 data row
        assert lines[0] == "identity_id,tags"
        # display_name from CustomTagTable should appear, not the raw tag_value UUID
        assert "prod" in lines[1]
        assert "user-1" in lines[1]

    def test_export_streaming_no_rows_returns_header_only(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        """Streaming export with filters matching zero rows returns only CSV header, no error."""
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
        lines = [ln for ln in response.text.strip().split("\n") if ln]
        assert len(lines) == 1  # header only, no exception raised

    def test_export_streaming_identity_filter_row_count_matches_db(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        """Export with identity_id filter returns only matching rows; count matches direct DB query."""
        target_identity = "user-target"
        other_count = 5
        target_count = 3

        with in_memory_backend.create_unit_of_work() as uow:
            for i in range(other_count):
                uow.chargebacks.upsert(
                    ChargebackRow(
                        ecosystem="test-eco",
                        tenant_id="test-tenant",
                        timestamp=datetime(2026, 2, i + 1, tzinfo=UTC),
                        resource_id="resource-1",
                        product_category="compute",
                        product_type="kafka",
                        identity_id=f"other-user-{i}",
                        cost_type=CostType.USAGE,
                        amount=Decimal("1.00"),
                        allocation_method="direct",
                        allocation_detail=None,
                        tags=[],
                        metadata={},
                    )
                )
            for i in range(target_count):
                uow.chargebacks.upsert(
                    ChargebackRow(
                        ecosystem="test-eco",
                        tenant_id="test-tenant",
                        timestamp=datetime(2026, 2, i + 1, tzinfo=UTC),
                        resource_id=f"resource-target-{i}",
                        product_category="compute",
                        product_type="kafka",
                        identity_id=target_identity,
                        cost_type=CostType.USAGE,
                        amount=Decimal("2.00"),
                        allocation_method="direct",
                        allocation_detail=None,
                        tags=[],
                        metadata={},
                    )
                )
            uow.commit()

        # Verify via direct DB query
        with in_memory_backend.create_unit_of_work() as uow:
            db_rows, db_total = uow.chargebacks.find_by_filters(
                ecosystem="test-eco",
                tenant_id="test-tenant",
                identity_id=target_identity,
                limit=10000,
            )
        assert db_total == target_count

        # Verify export matches
        response = app_with_backend.post(
            "/api/v1/tenants/test-tenant/export",
            json={
                "start_date": "2026-02-01",
                "end_date": "2026-02-28",
                "filters": {"identity_id": target_identity},
            },
        )
        assert response.status_code == 200
        lines = [ln for ln in response.text.strip().split("\n") if ln]
        assert len(lines) == db_total + 1  # header + matching data rows
