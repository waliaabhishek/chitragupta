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


class TestPatchDimension:
    def test_patch_dimension_add_tags(self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend) -> None:
        dim_id = _seed_dimension(in_memory_backend)
        response = app_with_backend.patch(
            f"/api/v1/tenants/test-tenant/chargebacks/{dim_id}",
            json={
                "add_tags": [
                    {"tag_key": "env", "tag_value": "prod", "created_by": "admin"},
                    {"tag_key": "team", "tag_value": "platform", "created_by": "admin"},
                ]
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert len(data["tags"]) == 2
        assert data["dimension_id"] == dim_id

    def test_patch_dimension_replace_tags(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        dim_id = _seed_dimension(in_memory_backend)
        # Add initial tag
        app_with_backend.patch(
            f"/api/v1/tenants/test-tenant/chargebacks/{dim_id}",
            json={"add_tags": [{"tag_key": "old", "tag_value": "value", "created_by": "admin"}]},
        )
        # Replace all tags
        response = app_with_backend.patch(
            f"/api/v1/tenants/test-tenant/chargebacks/{dim_id}",
            json={"tags": [{"tag_key": "new", "tag_value": "value", "created_by": "admin"}]},
        )
        assert response.status_code == 200
        data = response.json()
        assert len(data["tags"]) == 1
        assert data["tags"][0]["tag_key"] == "new"

    def test_patch_dimension_remove_tags(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        dim_id = _seed_dimension(in_memory_backend)
        # Add tags
        resp = app_with_backend.patch(
            f"/api/v1/tenants/test-tenant/chargebacks/{dim_id}",
            json={
                "add_tags": [
                    {"tag_key": "env", "tag_value": "prod", "created_by": "admin"},
                    {"tag_key": "team", "tag_value": "platform", "created_by": "admin"},
                ]
            },
        )
        tag_ids = [t["tag_id"] for t in resp.json()["tags"]]
        # Remove first tag
        response = app_with_backend.patch(
            f"/api/v1/tenants/test-tenant/chargebacks/{dim_id}",
            json={"remove_tag_ids": [tag_ids[0]]},
        )
        assert response.status_code == 200
        assert len(response.json()["tags"]) == 1

    def test_patch_dimension_not_found(self, app_with_backend: TestClient) -> None:
        response = app_with_backend.patch(
            "/api/v1/tenants/test-tenant/chargebacks/99999",
            json={"add_tags": [{"tag_key": "env", "tag_value": "prod", "created_by": "admin"}]},
        )
        assert response.status_code == 404

    def test_patch_dimension_wrong_tenant(self, app_with_backend: TestClient) -> None:
        response = app_with_backend.patch(
            "/api/v1/tenants/no-such-tenant/chargebacks/1",
            json={"add_tags": [{"tag_key": "env", "tag_value": "prod", "created_by": "admin"}]},
        )
        assert response.status_code == 404
