from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from fastapi.testclient import TestClient  # noqa: TC002

from core.models.chargeback import ChargebackRow, CostType
from core.models.identity import CoreIdentity
from core.models.resource import CoreResource, ResourceStatus
from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend  # noqa: TC001


def _seed_resource(
    backend: SQLModelBackend,
    resource_id: str = "r1",
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
    identity_id: str = "u1",
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


class TestEntityTagCRUD:
    def test_create_resource_tag_returns_201(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        _seed_resource(in_memory_backend)
        response = app_with_backend.post(
            "/api/v1/tenants/test-tenant/entities/resource/r1/tags",
            json={"tag_key": "env", "tag_value": "prod", "created_by": "admin"},
        )
        assert response.status_code == 201
        data = response.json()
        assert data["tag_key"] == "env"
        assert data["tag_value"] == "prod"
        assert data["entity_type"] == "resource"
        assert data["entity_id"] == "r1"
        assert data["tag_id"] is not None

    def test_get_entity_tags_after_create(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        _seed_resource(in_memory_backend)
        app_with_backend.post(
            "/api/v1/tenants/test-tenant/entities/resource/r1/tags",
            json={"tag_key": "env", "tag_value": "prod", "created_by": "admin"},
        )
        response = app_with_backend.get("/api/v1/tenants/test-tenant/entities/resource/r1/tags")
        assert response.status_code == 200
        tags = response.json()
        assert len(tags) == 1
        assert tags[0]["tag_key"] == "env"
        assert tags[0]["tag_value"] == "prod"

    def test_update_tag_value(self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend) -> None:
        _seed_resource(in_memory_backend)
        app_with_backend.post(
            "/api/v1/tenants/test-tenant/entities/resource/r1/tags",
            json={"tag_key": "env", "tag_value": "prod", "created_by": "admin"},
        )
        response = app_with_backend.put(
            "/api/v1/tenants/test-tenant/entities/resource/r1/tags/env",
            json={"tag_value": "staging"},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["tag_value"] == "staging"
        assert data["tag_key"] == "env"

    def test_delete_tag_returns_204(self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend) -> None:
        _seed_resource(in_memory_backend)
        app_with_backend.post(
            "/api/v1/tenants/test-tenant/entities/resource/r1/tags",
            json={"tag_key": "env", "tag_value": "prod", "created_by": "admin"},
        )
        response = app_with_backend.delete("/api/v1/tenants/test-tenant/entities/resource/r1/tags/env")
        assert response.status_code == 204

    def test_get_entity_tags_after_delete_is_empty(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        _seed_resource(in_memory_backend)
        app_with_backend.post(
            "/api/v1/tenants/test-tenant/entities/resource/r1/tags",
            json={"tag_key": "env", "tag_value": "prod", "created_by": "admin"},
        )
        app_with_backend.delete("/api/v1/tenants/test-tenant/entities/resource/r1/tags/env")
        response = app_with_backend.get("/api/v1/tenants/test-tenant/entities/resource/r1/tags")
        assert response.status_code == 200
        assert response.json() == []

    def test_create_identity_tag_returns_201(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        _seed_identity(in_memory_backend)
        response = app_with_backend.post(
            "/api/v1/tenants/test-tenant/entities/identity/u1/tags",
            json={"tag_key": "team", "tag_value": "platform", "created_by": "admin"},
        )
        assert response.status_code == 201
        data = response.json()
        assert data["entity_type"] == "identity"
        assert data["entity_id"] == "u1"


class TestEntityTagDuplicateKey:
    def test_duplicate_composite_key_returns_409_or_422(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        _seed_resource(in_memory_backend)
        app_with_backend.post(
            "/api/v1/tenants/test-tenant/entities/resource/r1/tags",
            json={"tag_key": "env", "tag_value": "prod", "created_by": "admin"},
        )
        response = app_with_backend.post(
            "/api/v1/tenants/test-tenant/entities/resource/r1/tags",
            json={"tag_key": "env", "tag_value": "staging", "created_by": "admin"},
        )
        assert response.status_code in {409, 422}


class TestEntityTagTenantIsolation:
    def test_post_tag_on_entity_from_different_tenant_returns_404(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        # Resource belongs to "other-tenant", not "test-tenant"
        with in_memory_backend.create_unit_of_work() as uow:
            uow.resources.upsert(
                CoreResource(
                    ecosystem="test-eco",
                    tenant_id="other-tenant",
                    resource_id="r-other",
                    resource_type="kafka_cluster",
                    status=ResourceStatus.ACTIVE,
                    created_at=datetime(2026, 1, 1, tzinfo=UTC),
                    metadata={},
                )
            )
            uow.commit()

        response = app_with_backend.post(
            "/api/v1/tenants/test-tenant/entities/resource/r-other/tags",
            json={"tag_key": "env", "tag_value": "prod", "created_by": "admin"},
        )
        assert response.status_code == 404

    def test_post_tag_on_nonexistent_resource_returns_404(self, app_with_backend: TestClient) -> None:
        response = app_with_backend.post(
            "/api/v1/tenants/test-tenant/entities/resource/no-such-resource/tags",
            json={"tag_key": "env", "tag_value": "prod", "created_by": "admin"},
        )
        assert response.status_code == 404

    def test_post_tag_on_nonexistent_identity_returns_404(self, app_with_backend: TestClient) -> None:
        response = app_with_backend.post(
            "/api/v1/tenants/test-tenant/entities/identity/no-such-identity/tags",
            json={"tag_key": "team", "tag_value": "platform", "created_by": "admin"},
        )
        assert response.status_code == 404


class TestEntityTagInvalidEntityType:
    def test_post_invalid_entity_type_returns_422(self, app_with_backend: TestClient) -> None:
        response = app_with_backend.post(
            "/api/v1/tenants/test-tenant/entities/dimension/d1/tags",
            json={"tag_key": "env", "tag_value": "prod", "created_by": "admin"},
        )
        assert response.status_code == 422

    def test_get_invalid_entity_type_returns_422(self, app_with_backend: TestClient) -> None:
        response = app_with_backend.get("/api/v1/tenants/test-tenant/entities/dimension/d1/tags")
        assert response.status_code == 422

    def test_put_invalid_entity_type_returns_422(self, app_with_backend: TestClient) -> None:
        response = app_with_backend.put(
            "/api/v1/tenants/test-tenant/entities/dimension/d1/tags/env",
            json={"tag_value": "prod"},
        )
        assert response.status_code == 422


class TestOldRoutesRemoved:
    def test_old_post_chargeback_tags_route_returns_404(self, app_with_backend: TestClient) -> None:
        response = app_with_backend.post(
            "/api/v1/tenants/test-tenant/chargebacks/1/tags",
            json={"tag_key": "env", "display_name": "prod", "created_by": "admin"},
        )
        assert response.status_code == 404

    def test_old_patch_chargeback_dimension_route_returns_404_or_405(self, app_with_backend: TestClient) -> None:
        response = app_with_backend.patch(
            "/api/v1/tenants/test-tenant/chargebacks/1",
            json={"tags": []},
        )
        assert response.status_code in {404, 405}


class TestChargebackResponseTagsIsDict:
    def test_chargeback_response_tags_field_is_dict(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        with in_memory_backend.create_unit_of_work() as uow:
            uow.chargebacks.upsert(
                ChargebackRow(
                    ecosystem="test-eco",
                    tenant_id="test-tenant",
                    timestamp=datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1),
                    resource_id="r1",
                    product_category="compute",
                    product_type="kafka",
                    identity_id="u1",
                    cost_type=CostType.USAGE,
                    amount=Decimal("10.00"),
                    tags={},
                    metadata={},
                )
            )
            uow.commit()

        response = app_with_backend.get("/api/v1/tenants/test-tenant/chargebacks")
        assert response.status_code == 200
        items = response.json()["items"]
        assert len(items) == 1
        assert isinstance(items[0]["tags"], dict)


class TestListTagsForTenant:
    def test_list_tags_for_tenant_empty(self, app_with_backend: TestClient) -> None:
        response = app_with_backend.get("/api/v1/tenants/test-tenant/tags")
        assert response.status_code == 200
        data = response.json()
        assert data["items"] == []
        assert data["total"] == 0

    def test_list_tags_with_entity_type_filter(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        _seed_resource(in_memory_backend)
        _seed_identity(in_memory_backend)
        app_with_backend.post(
            "/api/v1/tenants/test-tenant/entities/resource/r1/tags",
            json={"tag_key": "env", "tag_value": "prod", "created_by": "admin"},
        )
        app_with_backend.post(
            "/api/v1/tenants/test-tenant/entities/identity/u1/tags",
            json={"tag_key": "team", "tag_value": "platform", "created_by": "admin"},
        )
        response = app_with_backend.get("/api/v1/tenants/test-tenant/tags?entity_type=resource")
        data = response.json()
        assert data["total"] == 1
        assert data["items"][0]["entity_type"] == "resource"

    def test_list_tags_with_tag_key_filter(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        _seed_resource(in_memory_backend, resource_id="r1")
        _seed_resource(in_memory_backend, resource_id="r2")
        app_with_backend.post(
            "/api/v1/tenants/test-tenant/entities/resource/r1/tags",
            json={"tag_key": "env", "tag_value": "prod", "created_by": "admin"},
        )
        app_with_backend.post(
            "/api/v1/tenants/test-tenant/entities/resource/r2/tags",
            json={"tag_key": "team", "tag_value": "platform", "created_by": "admin"},
        )
        response = app_with_backend.get("/api/v1/tenants/test-tenant/tags?tag_key=env")
        data = response.json()
        assert data["total"] == 1
        assert data["items"][0]["tag_key"] == "env"

    def test_list_tags_pagination(self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend) -> None:
        for i in range(5):
            _seed_resource(in_memory_backend, resource_id=f"r{i}")
            app_with_backend.post(
                f"/api/v1/tenants/test-tenant/entities/resource/r{i}/tags",
                json={"tag_key": "env", "tag_value": "prod", "created_by": "admin"},
            )
        response = app_with_backend.get("/api/v1/tenants/test-tenant/tags?page=1&page_size=2")
        data = response.json()
        assert data["total"] == 5
        assert len(data["items"]) == 2
        assert data["pages"] == 3

    def test_list_tags_invalid_entity_type_filter_returns_422(self, app_with_backend: TestClient) -> None:
        response = app_with_backend.get("/api/v1/tenants/test-tenant/tags?entity_type=dimension")
        assert response.status_code == 422


class TestBulkEntityTags:
    def test_bulk_create_three_items_returns_created_3(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        _seed_resource(in_memory_backend, resource_id="r1")
        _seed_resource(in_memory_backend, resource_id="r2")
        _seed_resource(in_memory_backend, resource_id="r3")
        response = app_with_backend.post(
            "/api/v1/tenants/test-tenant/tags/bulk",
            json={
                "items": [
                    {"entity_type": "resource", "entity_id": "r1", "tag_key": "env", "tag_value": "prod"},
                    {"entity_type": "resource", "entity_id": "r2", "tag_key": "env", "tag_value": "prod"},
                    {"entity_type": "resource", "entity_id": "r3", "tag_key": "env", "tag_value": "prod"},
                ],
                "override_existing": False,
                "created_by": "admin",
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert data["created_count"] == 3
        assert data["updated_count"] == 0
        assert data["skipped_count"] == 0

    def test_bulk_same_call_twice_skips_all(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        _seed_resource(in_memory_backend, resource_id="r1")
        _seed_resource(in_memory_backend, resource_id="r2")
        _seed_resource(in_memory_backend, resource_id="r3")
        payload = {
            "items": [
                {"entity_type": "resource", "entity_id": "r1", "tag_key": "env", "tag_value": "prod"},
                {"entity_type": "resource", "entity_id": "r2", "tag_key": "env", "tag_value": "prod"},
                {"entity_type": "resource", "entity_id": "r3", "tag_key": "env", "tag_value": "prod"},
            ],
            "override_existing": False,
            "created_by": "admin",
        }
        app_with_backend.post("/api/v1/tenants/test-tenant/tags/bulk", json=payload)
        response = app_with_backend.post("/api/v1/tenants/test-tenant/tags/bulk", json=payload)
        data = response.json()
        assert data["created_count"] == 0
        assert data["updated_count"] == 0
        assert data["skipped_count"] == 3

    def test_bulk_override_existing_updates_all(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        _seed_resource(in_memory_backend, resource_id="r1")
        _seed_resource(in_memory_backend, resource_id="r2")
        _seed_resource(in_memory_backend, resource_id="r3")
        create_payload = {
            "items": [
                {"entity_type": "resource", "entity_id": "r1", "tag_key": "env", "tag_value": "prod"},
                {"entity_type": "resource", "entity_id": "r2", "tag_key": "env", "tag_value": "prod"},
                {"entity_type": "resource", "entity_id": "r3", "tag_key": "env", "tag_value": "prod"},
            ],
            "override_existing": False,
            "created_by": "admin",
        }
        app_with_backend.post("/api/v1/tenants/test-tenant/tags/bulk", json=create_payload)

        override_payload = {
            "items": [
                {"entity_type": "resource", "entity_id": "r1", "tag_key": "env", "tag_value": "staging"},
                {"entity_type": "resource", "entity_id": "r2", "tag_key": "env", "tag_value": "staging"},
                {"entity_type": "resource", "entity_id": "r3", "tag_key": "env", "tag_value": "staging"},
            ],
            "override_existing": True,
            "created_by": "admin",
        }
        response = app_with_backend.post("/api/v1/tenants/test-tenant/tags/bulk", json=override_payload)
        data = response.json()
        assert data["created_count"] == 0
        assert data["updated_count"] == 3
        assert data["skipped_count"] == 0

    def test_bulk_invalid_entity_type_returns_422(self, app_with_backend: TestClient) -> None:
        response = app_with_backend.post(
            "/api/v1/tenants/test-tenant/tags/bulk",
            json={
                "items": [
                    {"entity_type": "dimension", "entity_id": "d1", "tag_key": "env", "tag_value": "prod"},
                ],
                "override_existing": False,
                "created_by": "admin",
            },
        )
        assert response.status_code == 422


class TestNoImportErrors:
    def test_create_app_no_crash(self) -> None:
        from core.api.app import create_app  # noqa: PLC0415

        app = create_app()
        assert app is not None
