from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from fastapi.testclient import TestClient  # noqa: TC002

from core.models.chargeback import ChargebackRow, CostType
from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend  # noqa: TC001


def _seed_dimension(backend: SQLModelBackend, identity_id: str = "user-1", product_type: str = "kafka") -> int:
    """Insert a chargeback row and return its dimension_id."""
    with backend.create_unit_of_work() as uow:
        uow.chargebacks.upsert(
            ChargebackRow(
                ecosystem="test-eco",
                tenant_id="test-tenant",
                timestamp=datetime(2026, 2, 15, tzinfo=UTC),
                resource_id="resource-1",
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
        )
        uow.commit()

    with backend.create_unit_of_work() as uow:
        # Find dimension by identity_id + product_type
        rows, _ = uow.chargebacks.find_by_filters(
            ecosystem="test-eco",
            tenant_id="test-tenant",
            identity_id=identity_id,
            product_type=product_type,
            limit=1,
        )
        assert rows, "Expected at least one chargeback row"
        assert rows[0].dimension_id is not None
        return rows[0].dimension_id


class TestListTags:
    def test_list_tags_empty(self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend) -> None:
        dim_id = _seed_dimension(in_memory_backend)
        response = app_with_backend.get(f"/api/v1/tenants/test-tenant/chargebacks/{dim_id}/tags")
        assert response.status_code == 200
        assert response.json() == []

    def test_list_tags_wrong_tenant_returns_404(self, app_with_backend: TestClient) -> None:
        response = app_with_backend.get("/api/v1/tenants/test-tenant/chargebacks/99999/tags")
        assert response.status_code == 404

    def test_list_tags_nonexistent_tenant_returns_404(self, app_with_backend: TestClient) -> None:
        response = app_with_backend.get("/api/v1/tenants/no-such-tenant/chargebacks/1/tags")
        assert response.status_code == 404


class TestCreateTag:
    def test_create_tag(self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend) -> None:
        dim_id = _seed_dimension(in_memory_backend)
        response = app_with_backend.post(
            f"/api/v1/tenants/test-tenant/chargebacks/{dim_id}/tags",
            json={"tag_key": "env", "display_name": "Production", "created_by": "admin"},
        )
        assert response.status_code == 201
        data = response.json()
        assert data["tag_key"] == "env"
        assert data["display_name"] == "Production"
        # tag_value is auto-generated uuid
        assert data["tag_value"] is not None
        assert len(data["tag_value"]) == 36  # UUID format
        assert data["created_by"] == "admin"
        assert data["dimension_id"] == dim_id
        assert data["tag_id"] is not None

    def test_create_tag_wrong_dimension_returns_404(self, app_with_backend: TestClient) -> None:
        response = app_with_backend.post(
            "/api/v1/tenants/test-tenant/chargebacks/99999/tags",
            json={"tag_key": "env", "display_name": "prod", "created_by": "admin"},
        )
        assert response.status_code == 404

    def test_create_tag_then_list(self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend) -> None:
        dim_id = _seed_dimension(in_memory_backend)
        app_with_backend.post(
            f"/api/v1/tenants/test-tenant/chargebacks/{dim_id}/tags",
            json={"tag_key": "env", "display_name": "Staging", "created_by": "admin"},
        )
        response = app_with_backend.get(f"/api/v1/tenants/test-tenant/chargebacks/{dim_id}/tags")
        assert response.status_code == 200
        tags = response.json()
        assert len(tags) == 1
        assert tags[0]["tag_key"] == "env"
        assert tags[0]["display_name"] == "Staging"

    def test_create_tag_validation_empty_key(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        dim_id = _seed_dimension(in_memory_backend)
        response = app_with_backend.post(
            f"/api/v1/tenants/test-tenant/chargebacks/{dim_id}/tags",
            json={"tag_key": "", "display_name": "prod", "created_by": "admin"},
        )
        assert response.status_code == 422


class TestDeleteTag:
    def test_delete_tag(self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend) -> None:
        dim_id = _seed_dimension(in_memory_backend)
        create_resp = app_with_backend.post(
            f"/api/v1/tenants/test-tenant/chargebacks/{dim_id}/tags",
            json={"tag_key": "env", "display_name": "Staging", "created_by": "admin"},
        )
        tag_id = create_resp.json()["tag_id"]

        delete_resp = app_with_backend.delete(f"/api/v1/tenants/test-tenant/tags/{tag_id}")
        assert delete_resp.status_code == 204

        # Verify it's gone
        list_resp = app_with_backend.get(f"/api/v1/tenants/test-tenant/chargebacks/{dim_id}/tags")
        assert list_resp.json() == []

    def test_delete_tag_not_found(self, app_with_backend: TestClient) -> None:
        response = app_with_backend.delete("/api/v1/tenants/test-tenant/tags/99999")
        assert response.status_code == 404


class TestListTagsForTenant:
    def test_list_tags_for_tenant_empty(self, app_with_backend: TestClient) -> None:
        response = app_with_backend.get("/api/v1/tenants/test-tenant/tags")
        assert response.status_code == 200
        data = response.json()
        assert data["items"] == []
        assert data["total"] == 0

    def test_list_tags_for_tenant_with_data(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        dim_id = _seed_dimension(in_memory_backend)
        app_with_backend.post(
            f"/api/v1/tenants/test-tenant/chargebacks/{dim_id}/tags",
            json={"tag_key": "team", "display_name": "Platform Team", "created_by": "admin"},
        )
        response = app_with_backend.get("/api/v1/tenants/test-tenant/tags")
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 1
        item = data["items"][0]
        assert item["tag_key"] == "team"
        assert item["display_name"] == "Platform Team"
        assert item["identity_id"] == "user-1"
        assert item["product_type"] == "kafka"

    def test_list_tags_search_by_key(self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend) -> None:
        dim_id = _seed_dimension(in_memory_backend)
        app_with_backend.post(
            f"/api/v1/tenants/test-tenant/chargebacks/{dim_id}/tags",
            json={"tag_key": "team", "display_name": "Platform", "created_by": "admin"},
        )
        app_with_backend.post(
            f"/api/v1/tenants/test-tenant/chargebacks/{dim_id}/tags",
            json={"tag_key": "env", "display_name": "Production", "created_by": "admin"},
        )
        response = app_with_backend.get("/api/v1/tenants/test-tenant/tags?search=team")
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 1
        assert data["items"][0]["tag_key"] == "team"

    def test_list_tags_search_by_display_name(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        dim_id = _seed_dimension(in_memory_backend)
        app_with_backend.post(
            f"/api/v1/tenants/test-tenant/chargebacks/{dim_id}/tags",
            json={"tag_key": "team", "display_name": "Platform Team", "created_by": "admin"},
        )
        app_with_backend.post(
            f"/api/v1/tenants/test-tenant/chargebacks/{dim_id}/tags",
            json={"tag_key": "env", "display_name": "Production", "created_by": "admin"},
        )
        response = app_with_backend.get("/api/v1/tenants/test-tenant/tags?search=platform")
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 1
        assert data["items"][0]["display_name"] == "Platform Team"

    def test_list_tags_search_no_match(self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend) -> None:
        dim_id = _seed_dimension(in_memory_backend)
        app_with_backend.post(
            f"/api/v1/tenants/test-tenant/chargebacks/{dim_id}/tags",
            json={"tag_key": "env", "display_name": "Production", "created_by": "admin"},
        )
        response = app_with_backend.get("/api/v1/tenants/test-tenant/tags?search=zzznomatch")
        data = response.json()
        assert data["total"] == 0
        assert data["items"] == []

    def test_list_tags_pagination(self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend) -> None:
        dim_id = _seed_dimension(in_memory_backend)
        for i in range(5):
            app_with_backend.post(
                f"/api/v1/tenants/test-tenant/chargebacks/{dim_id}/tags",
                json={"tag_key": f"key{i}", "display_name": f"Tag {i}", "created_by": "admin"},
            )
        response = app_with_backend.get("/api/v1/tenants/test-tenant/tags?page=1&page_size=2")
        data = response.json()
        assert data["total"] == 5
        assert len(data["items"]) == 2
        assert data["pages"] == 3


class TestUpdateTag:
    def test_update_display_name(self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend) -> None:
        dim_id = _seed_dimension(in_memory_backend)
        create_resp = app_with_backend.post(
            f"/api/v1/tenants/test-tenant/chargebacks/{dim_id}/tags",
            json={"tag_key": "env", "display_name": "Old Name", "created_by": "admin"},
        )
        tag_id = create_resp.json()["tag_id"]
        original_value = create_resp.json()["tag_value"]

        patch_resp = app_with_backend.patch(
            f"/api/v1/tenants/test-tenant/tags/{tag_id}",
            json={"display_name": "New Name"},
        )
        assert patch_resp.status_code == 200
        data = patch_resp.json()
        assert data["display_name"] == "New Name"
        # tag_value must remain unchanged
        assert data["tag_value"] == original_value

    def test_update_tag_not_found(self, app_with_backend: TestClient) -> None:
        response = app_with_backend.patch(
            "/api/v1/tenants/test-tenant/tags/99999",
            json={"display_name": "New Name"},
        )
        assert response.status_code == 404

    def test_update_tag_wrong_tenant(self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend) -> None:
        dim_id = _seed_dimension(in_memory_backend)
        create_resp = app_with_backend.post(
            f"/api/v1/tenants/test-tenant/chargebacks/{dim_id}/tags",
            json={"tag_key": "env", "display_name": "Old Name", "created_by": "admin"},
        )
        tag_id = create_resp.json()["tag_id"]

        # Try to update using a nonexistent tenant
        response = app_with_backend.patch(
            f"/api/v1/tenants/no-such-tenant/tags/{tag_id}",
            json={"display_name": "New Name"},
        )
        assert response.status_code == 404


class TestBulkAddTags:
    def test_bulk_tag_success(self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend) -> None:
        dim_id1 = _seed_dimension(in_memory_backend, identity_id="user-1", product_type="kafka")
        dim_id2 = _seed_dimension(in_memory_backend, identity_id="user-2", product_type="flink")

        response = app_with_backend.post(
            "/api/v1/tenants/test-tenant/tags/bulk",
            json={
                "dimension_ids": [dim_id1, dim_id2],
                "tag_key": "cost_center",
                "display_name": "Engineering",
                "created_by": "admin",
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert data["created_count"] == 2
        assert data["updated_count"] == 0
        assert data["skipped_count"] == 0
        assert data["errors"] == []

    def test_bulk_tag_skips_existing_key(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        dim_id = _seed_dimension(in_memory_backend)
        # Create tag first
        app_with_backend.post(
            f"/api/v1/tenants/test-tenant/chargebacks/{dim_id}/tags",
            json={"tag_key": "env", "display_name": "Old", "created_by": "admin"},
        )
        # Bulk without override
        response = app_with_backend.post(
            "/api/v1/tenants/test-tenant/tags/bulk",
            json={
                "dimension_ids": [dim_id],
                "tag_key": "env",
                "display_name": "New",
                "created_by": "admin",
                "override_existing": False,
            },
        )
        data = response.json()
        assert data["created_count"] == 0
        assert data["skipped_count"] == 1

    def test_bulk_tag_override_existing(self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend) -> None:
        dim_id = _seed_dimension(in_memory_backend)
        app_with_backend.post(
            f"/api/v1/tenants/test-tenant/chargebacks/{dim_id}/tags",
            json={"tag_key": "env", "display_name": "Old", "created_by": "admin"},
        )
        response = app_with_backend.post(
            "/api/v1/tenants/test-tenant/tags/bulk",
            json={
                "dimension_ids": [dim_id],
                "tag_key": "env",
                "display_name": "New",
                "created_by": "admin",
                "override_existing": True,
            },
        )
        data = response.json()
        assert data["updated_count"] == 1
        assert data["skipped_count"] == 0

        # Verify display_name was updated
        tags_resp = app_with_backend.get(f"/api/v1/tenants/test-tenant/chargebacks/{dim_id}/tags")
        tags = tags_resp.json()
        assert len(tags) == 1
        assert tags[0]["display_name"] == "New"

    def test_bulk_tag_partial_failure(self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend) -> None:
        dim_id = _seed_dimension(in_memory_backend)
        response = app_with_backend.post(
            "/api/v1/tenants/test-tenant/tags/bulk",
            json={
                "dimension_ids": [dim_id, 99999],
                "tag_key": "env",
                "display_name": "Test",
                "created_by": "admin",
            },
        )
        data = response.json()
        assert data["created_count"] == 1
        assert "99999" in data["errors"]

    def test_bulk_tag_all_invalid(self, app_with_backend: TestClient) -> None:
        response = app_with_backend.post(
            "/api/v1/tenants/test-tenant/tags/bulk",
            json={
                "dimension_ids": [99998, 99999],
                "tag_key": "env",
                "display_name": "Test",
                "created_by": "admin",
            },
        )
        data = response.json()
        assert data["created_count"] == 0
        assert len(data["errors"]) == 2


class TestBulkAddTagsByFilter:
    def test_bulk_tag_by_filter_success(self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend) -> None:
        dim_id1 = _seed_dimension(in_memory_backend, identity_id="user-1", product_type="kafka")
        dim_id2 = _seed_dimension(in_memory_backend, identity_id="user-2", product_type="flink")
        assert dim_id1 != dim_id2

        response = app_with_backend.post(
            "/api/v1/tenants/test-tenant/tags/bulk-by-filter",
            json={
                "start_date": "2026-02-01",
                "end_date": "2026-02-28",
                "tag_key": "team",
                "display_name": "Platform",
                "created_by": "admin",
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert data["created_count"] == 2
        assert data["errors"] == []

    def test_bulk_tag_by_filter_empty(self, app_with_backend: TestClient) -> None:
        response = app_with_backend.post(
            "/api/v1/tenants/test-tenant/tags/bulk-by-filter",
            json={
                "start_date": "2025-01-01",
                "end_date": "2025-01-31",
                "tag_key": "team",
                "display_name": "Platform",
                "created_by": "admin",
            },
        )
        data = response.json()
        assert data["created_count"] == 0

    def test_bulk_tag_by_filter_skips_existing(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        dim_id = _seed_dimension(in_memory_backend)
        app_with_backend.post(
            f"/api/v1/tenants/test-tenant/chargebacks/{dim_id}/tags",
            json={"tag_key": "team", "display_name": "Old", "created_by": "admin"},
        )
        response = app_with_backend.post(
            "/api/v1/tenants/test-tenant/tags/bulk-by-filter",
            json={
                "start_date": "2026-02-01",
                "end_date": "2026-02-28",
                "tag_key": "team",
                "display_name": "New",
                "created_by": "admin",
                "override_existing": False,
            },
        )
        data = response.json()
        assert data["created_count"] == 0
        assert data["skipped_count"] == 1

    def test_bulk_tag_by_filter_with_identity_filter(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        dim_id1 = _seed_dimension(in_memory_backend, identity_id="user-1", product_type="kafka")
        dim_id2 = _seed_dimension(in_memory_backend, identity_id="user-2", product_type="flink")
        assert dim_id1 != dim_id2

        response = app_with_backend.post(
            "/api/v1/tenants/test-tenant/tags/bulk-by-filter",
            json={
                "start_date": "2026-02-01",
                "end_date": "2026-02-28",
                "identity_id": "user-1",
                "tag_key": "team",
                "display_name": "Platform",
                "created_by": "admin",
            },
        )
        data = response.json()
        assert data["created_count"] == 1


class TestBulkAddTagsByFilterDateValidation:
    def test_start_date_after_end_date_returns_400(self, app_with_backend: TestClient) -> None:
        response = app_with_backend.post(
            "/api/v1/tenants/test-tenant/tags/bulk-by-filter",
            json={
                "start_date": "2026-02-01",
                "end_date": "2026-01-01",
                "tag_key": "team",
                "display_name": "Team A",
                "created_by": "admin",
            },
        )
        assert response.status_code == 400
