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


# ---------------------------------------------------------------------------
# TASK-182: Tests 8, 9, 10 — resource_type mandatory, count_by_type show-all
# ---------------------------------------------------------------------------


class TestListResourcesShowAll:
    """Test 8: GET /resources with no params returns both billing and overlay types."""

    def test_no_params_returns_all_resource_types(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        """count_by_type must drive type resolution when no resource_type param is given."""
        with in_memory_backend.create_unit_of_work() as uow:
            uow.resources.upsert(
                CoreResource(
                    ecosystem="test-eco",
                    tenant_id="test-tenant",
                    resource_id="kc-1",
                    resource_type="kafka_cluster",
                    status=ResourceStatus.ACTIVE,
                    created_at=datetime(2026, 1, 1, tzinfo=UTC),
                    metadata={},
                )
            )
            uow.resources.upsert(
                CoreResource(
                    ecosystem="test-eco",
                    tenant_id="test-tenant",
                    resource_id="topic-1",
                    resource_type="topic",
                    status=ResourceStatus.ACTIVE,
                    created_at=datetime(2026, 1, 1, tzinfo=UTC),
                    metadata={},
                )
            )
            uow.commit()

        response = app_with_backend.get("/api/v1/tenants/test-tenant/resources")
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 2
        returned_types = {item["resource_type"] for item in data["items"]}
        assert "kafka_cluster" in returned_types
        assert "topic" in returned_types

    def test_no_params_includes_overlay_types_in_count(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        """Total count must include overlay (topic) resources when no filter given."""
        with in_memory_backend.create_unit_of_work() as uow:
            for i in range(3):
                uow.resources.upsert(
                    CoreResource(
                        ecosystem="test-eco",
                        tenant_id="test-tenant",
                        resource_id=f"topic-{i}",
                        resource_type="topic",
                        status=ResourceStatus.ACTIVE,
                        created_at=datetime(2026, 1, 1, tzinfo=UTC),
                        metadata={},
                    )
                )
            uow.resources.upsert(
                CoreResource(
                    ecosystem="test-eco",
                    tenant_id="test-tenant",
                    resource_id="kc-1",
                    resource_type="kafka_cluster",
                    status=ResourceStatus.ACTIVE,
                    created_at=datetime(2026, 1, 1, tzinfo=UTC),
                    metadata={},
                )
            )
            uow.commit()

        response = app_with_backend.get("/api/v1/tenants/test-tenant/resources")
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 4


class TestListResourcesTopicFilter:
    """Test 9: GET /resources?resource_type=topic returns only topic resources."""

    def test_topic_filter_returns_only_topics(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        with in_memory_backend.create_unit_of_work() as uow:
            uow.resources.upsert(
                CoreResource(
                    ecosystem="test-eco",
                    tenant_id="test-tenant",
                    resource_id="kc-1",
                    resource_type="kafka_cluster",
                    status=ResourceStatus.ACTIVE,
                    created_at=datetime(2026, 1, 1, tzinfo=UTC),
                    metadata={},
                )
            )
            uow.resources.upsert(
                CoreResource(
                    ecosystem="test-eco",
                    tenant_id="test-tenant",
                    resource_id="topic-1",
                    resource_type="topic",
                    status=ResourceStatus.ACTIVE,
                    created_at=datetime(2026, 1, 1, tzinfo=UTC),
                    metadata={},
                )
            )
            uow.resources.upsert(
                CoreResource(
                    ecosystem="test-eco",
                    tenant_id="test-tenant",
                    resource_id="topic-2",
                    resource_type="topic",
                    status=ResourceStatus.ACTIVE,
                    created_at=datetime(2026, 1, 1, tzinfo=UTC),
                    metadata={},
                )
            )
            uow.commit()

        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/resources",
            params={"resource_type": "topic"},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 2
        assert all(item["resource_type"] == "topic" for item in data["items"])

    def test_topic_filter_excludes_kafka_cluster(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        with in_memory_backend.create_unit_of_work() as uow:
            uow.resources.upsert(
                CoreResource(
                    ecosystem="test-eco",
                    tenant_id="test-tenant",
                    resource_id="kc-1",
                    resource_type="kafka_cluster",
                    status=ResourceStatus.ACTIVE,
                    created_at=datetime(2026, 1, 1, tzinfo=UTC),
                    metadata={},
                )
            )
            uow.resources.upsert(
                CoreResource(
                    ecosystem="test-eco",
                    tenant_id="test-tenant",
                    resource_id="topic-1",
                    resource_type="topic",
                    status=ResourceStatus.ACTIVE,
                    created_at=datetime(2026, 1, 1, tzinfo=UTC),
                    metadata={},
                )
            )
            uow.commit()

        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/resources",
            params={"resource_type": "topic"},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 1
        assert data["items"][0]["resource_id"] == "topic-1"


class TestListResourcesEmptyTenant:
    """Test 10: GET /resources on empty tenant returns empty list, not 500."""

    def test_empty_tenant_returns_200_empty_list(self, app_with_backend: TestClient) -> None:
        """Empty DB → count_by_type returns {} → effective_rt=[] → literal(False) → zero rows."""
        response = app_with_backend.get("/api/v1/tenants/test-tenant/resources")
        assert response.status_code == 200
        data = response.json()
        assert data["items"] == []
        assert data["total"] == 0

    def test_empty_tenant_with_active_at_returns_empty_not_500(self, app_with_backend: TestClient) -> None:
        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/resources",
            params={"active_at": "2026-01-15T00:00:00Z"},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["items"] == []
        assert data["total"] == 0

    def test_empty_tenant_with_period_returns_empty_not_500(self, app_with_backend: TestClient) -> None:
        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/resources",
            params={
                "period_start": "2026-01-01T00:00:00Z",
                "period_end": "2026-02-01T00:00:00Z",
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert data["items"] == []
        assert data["total"] == 0


class TestResourcesApiCountParamSmoke:
    """Smoke tests: GET /resources still returns correct total after count param refactor (task-043)."""

    def test_list_resources_total_reflects_actual_count(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend, sample_resource: Resource
    ) -> None:
        """API endpoint always returns correct total even after internal callers use count=False."""
        with in_memory_backend.create_unit_of_work() as uow:
            uow.resources.upsert(sample_resource)
            uow.commit()

        response = app_with_backend.get("/api/v1/tenants/test-tenant/resources")
        assert response.status_code == 200
        data = response.json()
        assert "total" in data
        assert data["total"] == 1  # not 0 — API must still use count=True (default)
        assert len(data["items"]) == 1
