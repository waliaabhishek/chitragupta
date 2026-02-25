from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from fastapi.testclient import TestClient  # noqa: TC002

from core.models.chargeback import ChargebackRow, CostType
from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend  # noqa: TC001


def _seed_dimension(backend: SQLModelBackend) -> int:
    """Insert a chargeback row and return its dimension_id."""
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
                tags=[],
                metadata={},
            )
        )
        uow.commit()

    with backend.create_unit_of_work() as uow:
        dim = uow.chargebacks.get_dimension(1)
        assert dim is not None
        return dim.dimension_id


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
            json={"tag_key": "env", "tag_value": "production", "created_by": "admin"},
        )
        assert response.status_code == 201
        data = response.json()
        assert data["tag_key"] == "env"
        assert data["tag_value"] == "production"
        assert data["created_by"] == "admin"
        assert data["dimension_id"] == dim_id
        assert data["tag_id"] is not None

    def test_create_tag_wrong_dimension_returns_404(self, app_with_backend: TestClient) -> None:
        response = app_with_backend.post(
            "/api/v1/tenants/test-tenant/chargebacks/99999/tags",
            json={"tag_key": "env", "tag_value": "prod", "created_by": "admin"},
        )
        assert response.status_code == 404

    def test_create_tag_then_list(self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend) -> None:
        dim_id = _seed_dimension(in_memory_backend)
        app_with_backend.post(
            f"/api/v1/tenants/test-tenant/chargebacks/{dim_id}/tags",
            json={"tag_key": "env", "tag_value": "staging", "created_by": "admin"},
        )
        response = app_with_backend.get(f"/api/v1/tenants/test-tenant/chargebacks/{dim_id}/tags")
        assert response.status_code == 200
        tags = response.json()
        assert len(tags) == 1
        assert tags[0]["tag_key"] == "env"

    def test_create_tag_validation_empty_key(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        dim_id = _seed_dimension(in_memory_backend)
        response = app_with_backend.post(
            f"/api/v1/tenants/test-tenant/chargebacks/{dim_id}/tags",
            json={"tag_key": "", "tag_value": "prod", "created_by": "admin"},
        )
        assert response.status_code == 422


class TestDeleteTag:
    def test_delete_tag(self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend) -> None:
        dim_id = _seed_dimension(in_memory_backend)
        create_resp = app_with_backend.post(
            f"/api/v1/tenants/test-tenant/chargebacks/{dim_id}/tags",
            json={"tag_key": "env", "tag_value": "staging", "created_by": "admin"},
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
