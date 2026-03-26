from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from fastapi.testclient import TestClient  # noqa: TC002

from core.models.chargeback import ChargebackRow, CostType
from core.models.identity import CoreIdentity
from core.models.resource import CoreResource, ResourceStatus
from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend  # noqa: TC001

_RECENT_TS = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)


def _seed_resource(
    backend: SQLModelBackend,
    resource_id: str,
    tenant_id: str = "test-tenant",
) -> None:
    with backend.create_unit_of_work() as uow:
        uow.resources.upsert(
            CoreResource(
                ecosystem="test-eco",
                tenant_id=tenant_id,
                resource_id=resource_id,
                resource_type="kafka_cluster",
                status=ResourceStatus.ACTIVE,
                created_at=datetime(2026, 1, 1, tzinfo=UTC),
                metadata={},
            )
        )
        uow.commit()


def _seed_identity(
    backend: SQLModelBackend,
    identity_id: str,
    tenant_id: str = "test-tenant",
) -> None:
    with backend.create_unit_of_work() as uow:
        uow.identities.upsert(
            CoreIdentity(
                ecosystem="test-eco",
                tenant_id=tenant_id,
                identity_id=identity_id,
                identity_type="user",
                created_at=datetime(2026, 1, 1, tzinfo=UTC),
                metadata={},
            )
        )
        uow.commit()


def _seed_chargeback(
    backend: SQLModelBackend,
    *,
    identity_id: str,
    resource_id: str,
    product_type: str = "kafka",
) -> None:
    with backend.create_unit_of_work() as uow:
        uow.chargebacks.upsert(
            ChargebackRow(
                ecosystem="test-eco",
                tenant_id="test-tenant",
                timestamp=_RECENT_TS,
                resource_id=resource_id,
                product_category="compute",
                product_type=product_type,
                identity_id=identity_id,
                cost_type=CostType.USAGE,
                amount=Decimal("10.00"),
                tags={},
                metadata={},
            )
        )
        uow.commit()


class TestListEntityTags:
    def test_list_entity_tags_empty(self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend) -> None:
        _seed_resource(in_memory_backend, "r1")
        response = app_with_backend.get("/api/v1/tenants/test-tenant/entities/resource/r1/tags")
        assert response.status_code == 200
        assert response.json() == []

    def test_list_entity_tags_wrong_tenant_returns_404(self, app_with_backend: TestClient) -> None:
        response = app_with_backend.get("/api/v1/tenants/no-such-tenant/entities/resource/r1/tags")
        assert response.status_code == 404

    def test_list_entity_tags_nonexistent_resource_returns_empty(self, app_with_backend: TestClient) -> None:
        response = app_with_backend.get("/api/v1/tenants/test-tenant/entities/resource/no-such/tags")
        assert response.status_code == 200
        assert response.json() == []


class TestCreateEntityTag:
    def test_create_tag_on_resource(self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend) -> None:
        _seed_resource(in_memory_backend, "r1")
        response = app_with_backend.post(
            "/api/v1/tenants/test-tenant/entities/resource/r1/tags",
            json={"tag_key": "env", "tag_value": "Production", "created_by": "admin"},
        )
        assert response.status_code == 201
        data = response.json()
        assert data["tag_key"] == "env"
        assert data["tag_value"] == "Production"
        assert data["entity_type"] == "resource"
        assert data["entity_id"] == "r1"
        assert data["tag_id"] is not None

    def test_create_tag_wrong_resource_returns_404(self, app_with_backend: TestClient) -> None:
        response = app_with_backend.post(
            "/api/v1/tenants/test-tenant/entities/resource/no-such/tags",
            json={"tag_key": "env", "tag_value": "prod", "created_by": "admin"},
        )
        assert response.status_code == 404

    def test_create_tag_then_list(self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend) -> None:
        _seed_resource(in_memory_backend, "r1")
        app_with_backend.post(
            "/api/v1/tenants/test-tenant/entities/resource/r1/tags",
            json={"tag_key": "env", "tag_value": "Staging", "created_by": "admin"},
        )
        response = app_with_backend.get("/api/v1/tenants/test-tenant/entities/resource/r1/tags")
        assert response.status_code == 200
        tags = response.json()
        assert len(tags) == 1
        assert tags[0]["tag_key"] == "env"
        assert tags[0]["tag_value"] == "Staging"

    def test_create_tag_validation_empty_key(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        _seed_resource(in_memory_backend, "r1")
        response = app_with_backend.post(
            "/api/v1/tenants/test-tenant/entities/resource/r1/tags",
            json={"tag_key": "", "tag_value": "prod", "created_by": "admin"},
        )
        assert response.status_code == 422


class TestDeleteEntityTag:
    def test_delete_tag(self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend) -> None:
        _seed_resource(in_memory_backend, "r1")
        app_with_backend.post(
            "/api/v1/tenants/test-tenant/entities/resource/r1/tags",
            json={"tag_key": "env", "tag_value": "Staging", "created_by": "admin"},
        )

        delete_resp = app_with_backend.delete("/api/v1/tenants/test-tenant/entities/resource/r1/tags/env")
        assert delete_resp.status_code == 204

        list_resp = app_with_backend.get("/api/v1/tenants/test-tenant/entities/resource/r1/tags")
        assert list_resp.json() == []

    def test_delete_tag_not_found(self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend) -> None:
        _seed_resource(in_memory_backend, "r1")
        response = app_with_backend.delete("/api/v1/tenants/test-tenant/entities/resource/r1/tags/no-such-key")
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
        _seed_resource(in_memory_backend, "r1")
        _seed_identity(in_memory_backend, "u1")
        app_with_backend.post(
            "/api/v1/tenants/test-tenant/entities/resource/r1/tags",
            json={"tag_key": "team", "tag_value": "Platform Team", "created_by": "admin"},
        )
        response = app_with_backend.get("/api/v1/tenants/test-tenant/tags")
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 1
        item = data["items"][0]
        assert item["tag_key"] == "team"
        assert item["tag_value"] == "Platform Team"
        assert item["entity_type"] == "resource"
        assert item["entity_id"] == "r1"

    def test_list_tags_search_by_key(self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend) -> None:
        _seed_resource(in_memory_backend, "r1")
        app_with_backend.post(
            "/api/v1/tenants/test-tenant/entities/resource/r1/tags",
            json={"tag_key": "team", "tag_value": "Platform", "created_by": "admin"},
        )
        app_with_backend.post(
            "/api/v1/tenants/test-tenant/entities/resource/r1/tags",
            json={"tag_key": "env", "tag_value": "Production", "created_by": "admin"},
        )
        response = app_with_backend.get("/api/v1/tenants/test-tenant/tags?tag_key=team")
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 1
        assert data["items"][0]["tag_key"] == "team"

    def test_list_tags_pagination(self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend) -> None:
        for i in range(5):
            _seed_resource(in_memory_backend, f"r{i}")
            app_with_backend.post(
                f"/api/v1/tenants/test-tenant/entities/resource/r{i}/tags",
                json={"tag_key": f"key{i}", "tag_value": f"Tag {i}", "created_by": "admin"},
            )
        response = app_with_backend.get("/api/v1/tenants/test-tenant/tags?page=1&page_size=2")
        data = response.json()
        assert data["total"] == 5
        assert len(data["items"]) == 2
        assert data["pages"] == 3


class TestUpdateEntityTag:
    def test_update_tag_value(self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend) -> None:
        _seed_resource(in_memory_backend, "r1")
        app_with_backend.post(
            "/api/v1/tenants/test-tenant/entities/resource/r1/tags",
            json={"tag_key": "env", "tag_value": "Old Value", "created_by": "admin"},
        )

        patch_resp = app_with_backend.put(
            "/api/v1/tenants/test-tenant/entities/resource/r1/tags/env",
            json={"tag_value": "New Value"},
        )
        assert patch_resp.status_code == 200
        data = patch_resp.json()
        assert data["tag_value"] == "New Value"
        assert data["tag_key"] == "env"

    def test_update_tag_not_found(self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend) -> None:
        _seed_resource(in_memory_backend, "r1")
        response = app_with_backend.put(
            "/api/v1/tenants/test-tenant/entities/resource/r1/tags/no-such-key",
            json={"tag_value": "New Value"},
        )
        assert response.status_code == 404

    def test_update_tag_wrong_tenant(self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend) -> None:
        _seed_resource(in_memory_backend, "r1")
        app_with_backend.post(
            "/api/v1/tenants/test-tenant/entities/resource/r1/tags",
            json={"tag_key": "env", "tag_value": "Old Value", "created_by": "admin"},
        )
        response = app_with_backend.put(
            "/api/v1/tenants/no-such-tenant/entities/resource/r1/tags/env",
            json={"tag_value": "New Value"},
        )
        assert response.status_code == 404


class TestBulkAddEntityTags:
    def test_bulk_tag_success(self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend) -> None:
        _seed_resource(in_memory_backend, "r1")
        _seed_resource(in_memory_backend, "r2")

        response = app_with_backend.post(
            "/api/v1/tenants/test-tenant/tags/bulk",
            json={
                "items": [
                    {"entity_type": "resource", "entity_id": "r1", "tag_key": "env", "tag_value": "prod"},
                    {"entity_type": "resource", "entity_id": "r2", "tag_key": "env", "tag_value": "prod"},
                ],
                "override_existing": False,
                "created_by": "admin",
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert data["created_count"] == 2
        assert data["updated_count"] == 0
        assert data["skipped_count"] == 0

    def test_bulk_tag_skips_existing_key(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        _seed_resource(in_memory_backend, "r1")
        app_with_backend.post(
            "/api/v1/tenants/test-tenant/entities/resource/r1/tags",
            json={"tag_key": "env", "tag_value": "Old", "created_by": "admin"},
        )
        response = app_with_backend.post(
            "/api/v1/tenants/test-tenant/tags/bulk",
            json={
                "items": [
                    {"entity_type": "resource", "entity_id": "r1", "tag_key": "env", "tag_value": "New"},
                ],
                "override_existing": False,
                "created_by": "admin",
            },
        )
        data = response.json()
        assert data["created_count"] == 0
        assert data["skipped_count"] == 1

    def test_bulk_tag_override_existing(self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend) -> None:
        _seed_resource(in_memory_backend, "r1")
        app_with_backend.post(
            "/api/v1/tenants/test-tenant/entities/resource/r1/tags",
            json={"tag_key": "env", "tag_value": "Old", "created_by": "admin"},
        )
        response = app_with_backend.post(
            "/api/v1/tenants/test-tenant/tags/bulk",
            json={
                "items": [
                    {"entity_type": "resource", "entity_id": "r1", "tag_key": "env", "tag_value": "New"},
                ],
                "override_existing": True,
                "created_by": "admin",
            },
        )
        data = response.json()
        assert data["updated_count"] == 1
        assert data["skipped_count"] == 0

        tags_resp = app_with_backend.get("/api/v1/tenants/test-tenant/entities/resource/r1/tags")
        tags = tags_resp.json()
        assert len(tags) == 1
        assert tags[0]["tag_value"] == "New"


class TestBulkAddTagsByFilter:
    def test_bulk_tag_by_filter_success(self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend) -> None:
        _seed_resource(in_memory_backend, "r1")
        _seed_resource(in_memory_backend, "r2")
        _seed_chargeback(in_memory_backend, identity_id="user-1", resource_id="r1", product_type="kafka")
        _seed_chargeback(in_memory_backend, identity_id="user-2", resource_id="r2", product_type="flink")

        response = app_with_backend.post(
            "/api/v1/tenants/test-tenant/tags/bulk-by-filter",
            json={
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
        _seed_resource(in_memory_backend, "r1")
        _seed_chargeback(in_memory_backend, identity_id="user-1", resource_id="r1")
        app_with_backend.post(
            "/api/v1/tenants/test-tenant/entities/resource/r1/tags",
            json={"tag_key": "team", "tag_value": "Old", "created_by": "admin"},
        )
        response = app_with_backend.post(
            "/api/v1/tenants/test-tenant/tags/bulk-by-filter",
            json={
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
        _seed_resource(in_memory_backend, "r1")
        _seed_resource(in_memory_backend, "r2")
        _seed_chargeback(in_memory_backend, identity_id="user-1", resource_id="r1", product_type="kafka")
        _seed_chargeback(in_memory_backend, identity_id="user-2", resource_id="r2", product_type="flink")

        response = app_with_backend.post(
            "/api/v1/tenants/test-tenant/tags/bulk-by-filter",
            json={
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
