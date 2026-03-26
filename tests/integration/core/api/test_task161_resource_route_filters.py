from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING

from core.models.resource import CoreResource, ResourceStatus

if TYPE_CHECKING:
    from fastapi.testclient import TestClient

    from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend


class TestListResourcesSearchParam:
    def test_search_filters_by_resource_id(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        with in_memory_backend.create_unit_of_work() as uow:
            uow.resources.upsert(
                CoreResource(
                    ecosystem="test-eco",
                    tenant_id="test-tenant",
                    resource_id="kafka-prod",
                    resource_type="kafka",
                    status=ResourceStatus.ACTIVE,
                    created_at=datetime(2026, 1, 1, tzinfo=UTC),
                    metadata={},
                )
            )
            uow.resources.upsert(
                CoreResource(
                    ecosystem="test-eco",
                    tenant_id="test-tenant",
                    resource_id="ksql-dev",
                    resource_type="ksql",
                    status=ResourceStatus.ACTIVE,
                    created_at=datetime(2026, 1, 1, tzinfo=UTC),
                    metadata={},
                )
            )
            uow.commit()

        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/resources",
            params={"search": "kafka"},
        )

        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 1
        assert data["items"][0]["resource_id"] == "kafka-prod"

    def test_search_filters_by_display_name(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        with in_memory_backend.create_unit_of_work() as uow:
            uow.resources.upsert(
                CoreResource(
                    ecosystem="test-eco",
                    tenant_id="test-tenant",
                    resource_id="r1",
                    resource_type="kafka",
                    display_name="Production Database",
                    status=ResourceStatus.ACTIVE,
                    created_at=datetime(2026, 1, 1, tzinfo=UTC),
                    metadata={},
                )
            )
            uow.resources.upsert(
                CoreResource(
                    ecosystem="test-eco",
                    tenant_id="test-tenant",
                    resource_id="r2",
                    resource_type="kafka",
                    display_name="Dev Cache",
                    status=ResourceStatus.ACTIVE,
                    created_at=datetime(2026, 1, 1, tzinfo=UTC),
                    metadata={},
                )
            )
            uow.commit()

        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/resources",
            params={"search": "database"},
        )

        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 1
        assert data["items"][0]["resource_id"] == "r1"

    def test_search_is_case_insensitive(self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend) -> None:
        with in_memory_backend.create_unit_of_work() as uow:
            uow.resources.upsert(
                CoreResource(
                    ecosystem="test-eco",
                    tenant_id="test-tenant",
                    resource_id="KAFKA-CLUSTER",
                    resource_type="kafka",
                    status=ResourceStatus.ACTIVE,
                    created_at=datetime(2026, 1, 1, tzinfo=UTC),
                    metadata={},
                )
            )
            uow.resources.upsert(
                CoreResource(
                    ecosystem="test-eco",
                    tenant_id="test-tenant",
                    resource_id="ksql-cluster",
                    resource_type="ksql",
                    status=ResourceStatus.ACTIVE,
                    created_at=datetime(2026, 1, 1, tzinfo=UTC),
                    metadata={},
                )
            )
            uow.commit()

        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/resources",
            params={"search": "kafka"},
        )

        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 1


class TestListResourcesSortParam:
    def test_sort_by_resource_id_asc(self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend) -> None:
        with in_memory_backend.create_unit_of_work() as uow:
            uow.resources.upsert(
                CoreResource(
                    ecosystem="test-eco",
                    tenant_id="test-tenant",
                    resource_id="zzz-resource",
                    resource_type="kafka",
                    status=ResourceStatus.ACTIVE,
                    created_at=datetime(2026, 1, 1, tzinfo=UTC),
                    metadata={},
                )
            )
            uow.resources.upsert(
                CoreResource(
                    ecosystem="test-eco",
                    tenant_id="test-tenant",
                    resource_id="aaa-resource",
                    resource_type="kafka",
                    status=ResourceStatus.ACTIVE,
                    created_at=datetime(2026, 1, 1, tzinfo=UTC),
                    metadata={},
                )
            )
            uow.commit()

        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/resources",
            params={"sort_by": "resource_id", "sort_order": "asc"},
        )

        assert response.status_code == 200
        data = response.json()
        assert data["items"][0]["resource_id"] == "aaa-resource"

    def test_sort_by_resource_id_desc(self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend) -> None:
        with in_memory_backend.create_unit_of_work() as uow:
            uow.resources.upsert(
                CoreResource(
                    ecosystem="test-eco",
                    tenant_id="test-tenant",
                    resource_id="zzz-resource",
                    resource_type="kafka",
                    status=ResourceStatus.ACTIVE,
                    created_at=datetime(2026, 1, 1, tzinfo=UTC),
                    metadata={},
                )
            )
            uow.resources.upsert(
                CoreResource(
                    ecosystem="test-eco",
                    tenant_id="test-tenant",
                    resource_id="aaa-resource",
                    resource_type="kafka",
                    status=ResourceStatus.ACTIVE,
                    created_at=datetime(2026, 1, 1, tzinfo=UTC),
                    metadata={},
                )
            )
            uow.commit()

        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/resources",
            params={"sort_by": "resource_id", "sort_order": "desc"},
        )

        assert response.status_code == 200
        data = response.json()
        assert data["items"][0]["resource_id"] == "zzz-resource"

    def test_sort_by_display_name_asc(self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend) -> None:
        with in_memory_backend.create_unit_of_work() as uow:
            uow.resources.upsert(
                CoreResource(
                    ecosystem="test-eco",
                    tenant_id="test-tenant",
                    resource_id="r1",
                    resource_type="kafka",
                    display_name="Zebra DB",
                    status=ResourceStatus.ACTIVE,
                    created_at=datetime(2026, 1, 1, tzinfo=UTC),
                    metadata={},
                )
            )
            uow.resources.upsert(
                CoreResource(
                    ecosystem="test-eco",
                    tenant_id="test-tenant",
                    resource_id="r2",
                    resource_type="kafka",
                    display_name="Apple Cache",
                    status=ResourceStatus.ACTIVE,
                    created_at=datetime(2026, 1, 1, tzinfo=UTC),
                    metadata={},
                )
            )
            uow.commit()

        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/resources",
            params={"sort_by": "display_name", "sort_order": "asc"},
        )

        assert response.status_code == 200
        data = response.json()
        assert data["items"][0]["display_name"] == "Apple Cache"


class TestListResourcesTagParam:
    def test_tag_key_filter_returns_only_tagged(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        with in_memory_backend.create_unit_of_work() as uow:
            uow.resources.upsert(
                CoreResource(
                    ecosystem="test-eco",
                    tenant_id="test-tenant",
                    resource_id="r-tagged",
                    resource_type="kafka",
                    status=ResourceStatus.ACTIVE,
                    created_at=datetime(2026, 1, 1, tzinfo=UTC),
                    metadata={},
                )
            )
            uow.resources.upsert(
                CoreResource(
                    ecosystem="test-eco",
                    tenant_id="test-tenant",
                    resource_id="r-untagged",
                    resource_type="kafka",
                    status=ResourceStatus.ACTIVE,
                    created_at=datetime(2026, 1, 1, tzinfo=UTC),
                    metadata={},
                )
            )
            uow.tags.add_tag(
                tenant_id="test-tenant",
                entity_type="resource",
                entity_id="r-tagged",
                tag_key="cost_center",
                tag_value="eng",
                created_by="admin",
            )
            uow.commit()

        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/resources",
            params={"tag_key": "cost_center"},
        )

        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 1
        assert data["items"][0]["resource_id"] == "r-tagged"

    def test_tag_key_and_value_filter(self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend) -> None:
        with in_memory_backend.create_unit_of_work() as uow:
            uow.resources.upsert(
                CoreResource(
                    ecosystem="test-eco",
                    tenant_id="test-tenant",
                    resource_id="r-prod",
                    resource_type="kafka",
                    status=ResourceStatus.ACTIVE,
                    created_at=datetime(2026, 1, 1, tzinfo=UTC),
                    metadata={},
                )
            )
            uow.resources.upsert(
                CoreResource(
                    ecosystem="test-eco",
                    tenant_id="test-tenant",
                    resource_id="r-dev",
                    resource_type="kafka",
                    status=ResourceStatus.ACTIVE,
                    created_at=datetime(2026, 1, 1, tzinfo=UTC),
                    metadata={},
                )
            )
            uow.tags.add_tag("test-tenant", "resource", "r-prod", "env", "prod", "admin")
            uow.tags.add_tag("test-tenant", "resource", "r-dev", "env", "dev", "admin")
            uow.commit()

        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/resources",
            params={"tag_key": "env", "tag_value": "prod"},
        )

        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 1
        assert data["items"][0]["resource_id"] == "r-prod"

    def test_tag_key_alone_matches_any_tag_value(
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
                    metadata={},
                )
            )
            uow.resources.upsert(
                CoreResource(
                    ecosystem="test-eco",
                    tenant_id="test-tenant",
                    resource_id="r2",
                    resource_type="kafka",
                    status=ResourceStatus.ACTIVE,
                    created_at=datetime(2026, 1, 1, tzinfo=UTC),
                    metadata={},
                )
            )
            uow.resources.upsert(
                CoreResource(
                    ecosystem="test-eco",
                    tenant_id="test-tenant",
                    resource_id="r3",
                    resource_type="kafka",
                    status=ResourceStatus.ACTIVE,
                    created_at=datetime(2026, 1, 1, tzinfo=UTC),
                    metadata={},
                )
            )
            uow.tags.add_tag("test-tenant", "resource", "r1", "team", "platform", "admin")
            uow.tags.add_tag("test-tenant", "resource", "r2", "team", "data", "admin")
            uow.commit()

        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/resources",
            params={"tag_key": "team"},
        )

        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 2
