from __future__ import annotations

from datetime import UTC, datetime

from fastapi.testclient import TestClient  # noqa: TC002

from core.models.resource import CoreResource, Resource, ResourceStatus
from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend  # noqa: TC001


class TestListResources:
    def test_list_resources_empty(self, app_with_backend: TestClient) -> None:
        response = app_with_backend.get("/api/v1/tenants/test-tenant/resources")
        assert response.status_code == 200
        data = response.json()
        assert data["items"] == []
        assert data["total"] == 0

    def test_list_resources_with_data(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend, sample_resource: Resource
    ) -> None:
        with in_memory_backend.create_unit_of_work() as uow:
            uow.resources.upsert(sample_resource)
            uow.commit()

        response = app_with_backend.get("/api/v1/tenants/test-tenant/resources")
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 1
        assert data["items"][0]["resource_id"] == "resource-1"

    def test_list_resources_filter_by_type(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        with in_memory_backend.create_unit_of_work() as uow:
            for rt in ["kafka_cluster", "ksql_cluster", "connector"]:
                uow.resources.upsert(
                    CoreResource(
                        ecosystem="test-eco",
                        tenant_id="test-tenant",
                        resource_id=f"r-{rt}",
                        resource_type=rt,
                        status=ResourceStatus.ACTIVE,
                        created_at=datetime(2026, 1, 1, tzinfo=UTC),
                        metadata={},
                    )
                )
            uow.commit()

        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/resources",
            params={"resource_type": "kafka_cluster"},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 1
        assert data["items"][0]["resource_type"] == "kafka_cluster"

    def test_list_resources_temporal_active_at(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        with in_memory_backend.create_unit_of_work() as uow:
            uow.resources.upsert(
                CoreResource(
                    ecosystem="test-eco",
                    tenant_id="test-tenant",
                    resource_id="r1",
                    resource_type="kafka",
                    status=ResourceStatus.ACTIVE,
                    created_at=datetime(2026, 1, 1, tzinfo=UTC),
                    deleted_at=datetime(2026, 1, 20, tzinfo=UTC),
                    metadata={},
                )
            )
            uow.commit()

        # Active at Jan 15 — should find it
        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/resources",
            params={"active_at": "2026-01-15T00:00:00Z"},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 1

        # Active at Feb 1 — deleted, should not find it
        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/resources",
            params={"active_at": "2026-02-01T00:00:00Z"},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 0

    def test_list_resources_temporal_period(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        with in_memory_backend.create_unit_of_work() as uow:
            uow.resources.upsert(
                CoreResource(
                    ecosystem="test-eco",
                    tenant_id="test-tenant",
                    resource_id="r1",
                    resource_type="kafka",
                    status=ResourceStatus.ACTIVE,
                    created_at=datetime(2026, 1, 5, tzinfo=UTC),
                    deleted_at=datetime(2026, 1, 25, tzinfo=UTC),
                    metadata={},
                )
            )
            uow.commit()

        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/resources",
            params={
                "period_start": "2026-01-10T00:00:00Z",
                "period_end": "2026-01-20T00:00:00Z",
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 1

    def test_list_resources_temporal_conflict_error(self, app_with_backend: TestClient) -> None:
        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/resources",
            params={
                "active_at": "2026-01-15T00:00:00Z",
                "period_start": "2026-01-10T00:00:00Z",
            },
        )
        assert response.status_code == 400
        assert "Cannot combine" in response.json()["detail"]

    def test_list_resources_filter_by_status(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        with in_memory_backend.create_unit_of_work() as uow:
            for i, status in enumerate([ResourceStatus.ACTIVE, ResourceStatus.ACTIVE, ResourceStatus.DELETED]):
                uow.resources.upsert(
                    CoreResource(
                        ecosystem="test-eco",
                        tenant_id="test-tenant",
                        resource_id=f"r-{i}",
                        resource_type="kafka_cluster",
                        status=status,
                        created_at=datetime(2026, 1, 1, tzinfo=UTC),
                        deleted_at=datetime(2026, 1, 20, tzinfo=UTC) if status == ResourceStatus.DELETED else None,
                        metadata={},
                    )
                )
            uow.commit()

        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/resources",
            params={"status": "deleted"},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 1
        assert data["items"][0]["status"] == "deleted"

    def test_list_resources_invalid_period_range(self, app_with_backend: TestClient) -> None:
        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/resources",
            params={
                "period_start": "2026-01-20T00:00:00Z",
                "period_end": "2026-01-10T00:00:00Z",
            },
        )
        assert response.status_code == 400
        assert "period_start" in response.json()["detail"]

    def test_list_resources_pagination(self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend) -> None:
        with in_memory_backend.create_unit_of_work() as uow:
            for i in range(15):
                uow.resources.upsert(
                    CoreResource(
                        ecosystem="test-eco",
                        tenant_id="test-tenant",
                        resource_id=f"r-{i}",
                        resource_type="kafka_cluster",
                        status=ResourceStatus.ACTIVE,
                        created_at=datetime(2026, 1, 1, i, tzinfo=UTC),
                        metadata={},
                    )
                )
            uow.commit()

        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/resources",
            params={"page": 1, "page_size": 10},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 15
        assert len(data["items"]) == 10
        assert data["pages"] == 2

        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/resources",
            params={"page": 2, "page_size": 10},
        )
        assert response.status_code == 200
        data = response.json()
        assert len(data["items"]) == 5
