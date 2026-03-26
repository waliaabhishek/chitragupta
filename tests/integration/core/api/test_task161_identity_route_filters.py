from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING

from core.models.identity import CoreIdentity

if TYPE_CHECKING:
    from fastapi.testclient import TestClient

    from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend


class TestListIdentitiesSearchParam:
    def test_search_filters_by_identity_id(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        with in_memory_backend.create_unit_of_work() as uow:
            uow.identities.upsert(
                CoreIdentity(
                    ecosystem="test-eco",
                    tenant_id="test-tenant",
                    identity_id="alice-svc",
                    identity_type="user",
                    created_at=datetime(2026, 1, 1, tzinfo=UTC),
                    metadata={},
                )
            )
            uow.identities.upsert(
                CoreIdentity(
                    ecosystem="test-eco",
                    tenant_id="test-tenant",
                    identity_id="bob-svc",
                    identity_type="user",
                    created_at=datetime(2026, 1, 1, tzinfo=UTC),
                    metadata={},
                )
            )
            uow.commit()

        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/identities",
            params={"search": "alice"},
        )

        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 1
        assert data["items"][0]["identity_id"] == "alice-svc"

    def test_search_filters_by_display_name(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        with in_memory_backend.create_unit_of_work() as uow:
            uow.identities.upsert(
                CoreIdentity(
                    ecosystem="test-eco",
                    tenant_id="test-tenant",
                    identity_id="u1",
                    identity_type="user",
                    display_name="Alice Smith",
                    created_at=datetime(2026, 1, 1, tzinfo=UTC),
                    metadata={},
                )
            )
            uow.identities.upsert(
                CoreIdentity(
                    ecosystem="test-eco",
                    tenant_id="test-tenant",
                    identity_id="u2",
                    identity_type="user",
                    display_name="Bob Jones",
                    created_at=datetime(2026, 1, 1, tzinfo=UTC),
                    metadata={},
                )
            )
            uow.commit()

        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/identities",
            params={"search": "smith"},
        )

        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 1
        assert data["items"][0]["identity_id"] == "u1"

    def test_search_is_case_insensitive(self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend) -> None:
        with in_memory_backend.create_unit_of_work() as uow:
            uow.identities.upsert(
                CoreIdentity(
                    ecosystem="test-eco",
                    tenant_id="test-tenant",
                    identity_id="ALICE-SVC",
                    identity_type="user",
                    created_at=datetime(2026, 1, 1, tzinfo=UTC),
                    metadata={},
                )
            )
            uow.identities.upsert(
                CoreIdentity(
                    ecosystem="test-eco",
                    tenant_id="test-tenant",
                    identity_id="bob-svc",
                    identity_type="user",
                    created_at=datetime(2026, 1, 1, tzinfo=UTC),
                    metadata={},
                )
            )
            uow.commit()

        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/identities",
            params={"search": "alice"},
        )

        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 1


class TestListIdentitiesSortParam:
    def test_sort_by_identity_id_asc(self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend) -> None:
        with in_memory_backend.create_unit_of_work() as uow:
            uow.identities.upsert(
                CoreIdentity(
                    ecosystem="test-eco",
                    tenant_id="test-tenant",
                    identity_id="zzz-user",
                    identity_type="user",
                    created_at=datetime(2026, 1, 1, tzinfo=UTC),
                    metadata={},
                )
            )
            uow.identities.upsert(
                CoreIdentity(
                    ecosystem="test-eco",
                    tenant_id="test-tenant",
                    identity_id="aaa-user",
                    identity_type="user",
                    created_at=datetime(2026, 1, 1, tzinfo=UTC),
                    metadata={},
                )
            )
            uow.commit()

        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/identities",
            params={"sort_by": "identity_id", "sort_order": "asc"},
        )

        assert response.status_code == 200
        data = response.json()
        assert data["items"][0]["identity_id"] == "aaa-user"

    def test_sort_by_identity_id_desc(self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend) -> None:
        with in_memory_backend.create_unit_of_work() as uow:
            uow.identities.upsert(
                CoreIdentity(
                    ecosystem="test-eco",
                    tenant_id="test-tenant",
                    identity_id="zzz-user",
                    identity_type="user",
                    created_at=datetime(2026, 1, 1, tzinfo=UTC),
                    metadata={},
                )
            )
            uow.identities.upsert(
                CoreIdentity(
                    ecosystem="test-eco",
                    tenant_id="test-tenant",
                    identity_id="aaa-user",
                    identity_type="user",
                    created_at=datetime(2026, 1, 1, tzinfo=UTC),
                    metadata={},
                )
            )
            uow.commit()

        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/identities",
            params={"sort_by": "identity_id", "sort_order": "desc"},
        )

        assert response.status_code == 200
        data = response.json()
        assert data["items"][0]["identity_id"] == "zzz-user"


class TestListIdentitiesTagParam:
    def test_tag_key_filter_returns_only_tagged(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        with in_memory_backend.create_unit_of_work() as uow:
            uow.identities.upsert(
                CoreIdentity(
                    ecosystem="test-eco",
                    tenant_id="test-tenant",
                    identity_id="tagged-user",
                    identity_type="user",
                    created_at=datetime(2026, 1, 1, tzinfo=UTC),
                    metadata={},
                )
            )
            uow.identities.upsert(
                CoreIdentity(
                    ecosystem="test-eco",
                    tenant_id="test-tenant",
                    identity_id="untagged-user",
                    identity_type="user",
                    created_at=datetime(2026, 1, 1, tzinfo=UTC),
                    metadata={},
                )
            )
            uow.tags.add_tag(
                tenant_id="test-tenant",
                entity_type="identity",
                entity_id="tagged-user",
                tag_key="cost_center",
                tag_value="eng",
                created_by="admin",
            )
            uow.commit()

        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/identities",
            params={"tag_key": "cost_center"},
        )

        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 1
        assert data["items"][0]["identity_id"] == "tagged-user"

    def test_tag_key_and_value_filter(self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend) -> None:
        with in_memory_backend.create_unit_of_work() as uow:
            uow.identities.upsert(
                CoreIdentity(
                    ecosystem="test-eco",
                    tenant_id="test-tenant",
                    identity_id="u-eng",
                    identity_type="user",
                    created_at=datetime(2026, 1, 1, tzinfo=UTC),
                    metadata={},
                )
            )
            uow.identities.upsert(
                CoreIdentity(
                    ecosystem="test-eco",
                    tenant_id="test-tenant",
                    identity_id="u-ops",
                    identity_type="user",
                    created_at=datetime(2026, 1, 1, tzinfo=UTC),
                    metadata={},
                )
            )
            uow.tags.add_tag("test-tenant", "identity", "u-eng", "cost_center", "eng", "admin")
            uow.tags.add_tag("test-tenant", "identity", "u-ops", "cost_center", "ops", "admin")
            uow.commit()

        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/identities",
            params={"tag_key": "cost_center", "tag_value": "eng"},
        )

        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 1
        assert data["items"][0]["identity_id"] == "u-eng"

    def test_tag_key_alone_matches_any_tag_value(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        with in_memory_backend.create_unit_of_work() as uow:
            uow.identities.upsert(
                CoreIdentity(
                    ecosystem="test-eco",
                    tenant_id="test-tenant",
                    identity_id="u1",
                    identity_type="user",
                    created_at=datetime(2026, 1, 1, tzinfo=UTC),
                    metadata={},
                )
            )
            uow.identities.upsert(
                CoreIdentity(
                    ecosystem="test-eco",
                    tenant_id="test-tenant",
                    identity_id="u2",
                    identity_type="user",
                    created_at=datetime(2026, 1, 1, tzinfo=UTC),
                    metadata={},
                )
            )
            uow.identities.upsert(
                CoreIdentity(
                    ecosystem="test-eco",
                    tenant_id="test-tenant",
                    identity_id="u3",
                    identity_type="user",
                    created_at=datetime(2026, 1, 1, tzinfo=UTC),
                    metadata={},
                )
            )
            uow.tags.add_tag("test-tenant", "identity", "u1", "env", "prod", "admin")
            uow.tags.add_tag("test-tenant", "identity", "u2", "env", "staging", "admin")
            uow.commit()

        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/identities",
            params={"tag_key": "env"},
        )

        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 2
