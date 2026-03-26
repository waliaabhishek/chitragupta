from __future__ import annotations

from datetime import UTC, datetime

from fastapi.testclient import TestClient  # noqa: TC002

from core.models.resource import CoreResource, ResourceStatus
from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend  # noqa: TC001


def _seed_resource(backend: SQLModelBackend, resource_id: str = "resource-1") -> None:
    with backend.create_unit_of_work() as uow:
        uow.resources.upsert(
            CoreResource(
                ecosystem="test-eco",
                tenant_id="test-tenant",
                resource_id=resource_id,
                resource_type="kafka_cluster",
                status=ResourceStatus.ACTIVE,
                created_at=datetime(2026, 1, 1, tzinfo=UTC),
                metadata={},
            )
        )
        uow.commit()


class TestEntityTagWrite:
    def test_create_two_entity_tags(self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend) -> None:
        _seed_resource(in_memory_backend)
        resp1 = app_with_backend.post(
            "/api/v1/tenants/test-tenant/entities/resource/resource-1/tags",
            json={"tag_key": "env", "tag_value": "prod", "created_by": "admin"},
        )
        resp2 = app_with_backend.post(
            "/api/v1/tenants/test-tenant/entities/resource/resource-1/tags",
            json={"tag_key": "team", "tag_value": "platform", "created_by": "admin"},
        )
        assert resp1.status_code == 201
        assert resp2.status_code == 201
        list_resp = app_with_backend.get("/api/v1/tenants/test-tenant/entities/resource/resource-1/tags")
        assert len(list_resp.json()) == 2

    def test_update_entity_tag_value(self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend) -> None:
        _seed_resource(in_memory_backend)
        app_with_backend.post(
            "/api/v1/tenants/test-tenant/entities/resource/resource-1/tags",
            json={"tag_key": "old", "tag_value": "value", "created_by": "admin"},
        )
        update_resp = app_with_backend.put(
            "/api/v1/tenants/test-tenant/entities/resource/resource-1/tags/old",
            json={"tag_value": "new-value"},
        )
        assert update_resp.status_code == 200
        assert update_resp.json()["tag_value"] == "new-value"

    def test_delete_entity_tag(self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend) -> None:
        _seed_resource(in_memory_backend)
        app_with_backend.post(
            "/api/v1/tenants/test-tenant/entities/resource/resource-1/tags",
            json={"tag_key": "env", "tag_value": "prod", "created_by": "admin"},
        )
        app_with_backend.post(
            "/api/v1/tenants/test-tenant/entities/resource/resource-1/tags",
            json={"tag_key": "team", "tag_value": "platform", "created_by": "admin"},
        )
        del_resp = app_with_backend.delete("/api/v1/tenants/test-tenant/entities/resource/resource-1/tags/env")
        assert del_resp.status_code == 204
        list_resp = app_with_backend.get("/api/v1/tenants/test-tenant/entities/resource/resource-1/tags")
        assert len(list_resp.json()) == 1

    def test_create_tag_on_nonexistent_resource_returns_404(self, app_with_backend: TestClient) -> None:
        response = app_with_backend.post(
            "/api/v1/tenants/test-tenant/entities/resource/no-such/tags",
            json={"tag_key": "env", "tag_value": "prod", "created_by": "admin"},
        )
        assert response.status_code == 404

    def test_create_tag_wrong_tenant_returns_404(self, app_with_backend: TestClient) -> None:
        response = app_with_backend.post(
            "/api/v1/tenants/no-such-tenant/entities/resource/resource-1/tags",
            json={"tag_key": "env", "tag_value": "prod", "created_by": "admin"},
        )
        assert response.status_code == 404
