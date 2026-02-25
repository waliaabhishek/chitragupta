from __future__ import annotations

from datetime import UTC, datetime

from fastapi.testclient import TestClient  # noqa: TC002

from core.models.identity import Identity
from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend  # noqa: TC001


class TestListIdentities:
    def test_list_identities_empty(self, app_with_backend: TestClient) -> None:
        response = app_with_backend.get("/api/v1/tenants/test-tenant/identities")
        assert response.status_code == 200
        data = response.json()
        assert data["items"] == []
        assert data["total"] == 0

    def test_list_identities_with_data(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend, sample_identity: Identity
    ) -> None:
        with in_memory_backend.create_unit_of_work() as uow:
            uow.identities.upsert(sample_identity)
            uow.commit()

        response = app_with_backend.get("/api/v1/tenants/test-tenant/identities")
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 1
        assert data["items"][0]["identity_id"] == "user-1"

    def test_list_identities_temporal_active_at(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        with in_memory_backend.create_unit_of_work() as uow:
            uow.identities.upsert(
                Identity(
                    ecosystem="test-eco",
                    tenant_id="test-tenant",
                    identity_id="u1",
                    identity_type="user",
                    created_at=datetime(2026, 1, 1, tzinfo=UTC),
                    deleted_at=datetime(2026, 1, 20, tzinfo=UTC),
                    metadata={},
                )
            )
            uow.commit()

        # Active at Jan 15
        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/identities",
            params={"active_at": "2026-01-15T00:00:00Z"},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 1

        # Active at Feb 1 — deleted
        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/identities",
            params={"active_at": "2026-02-01T00:00:00Z"},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 0

    def test_list_identities_temporal_period(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        with in_memory_backend.create_unit_of_work() as uow:
            uow.identities.upsert(
                Identity(
                    ecosystem="test-eco",
                    tenant_id="test-tenant",
                    identity_id="u1",
                    identity_type="user",
                    created_at=datetime(2026, 1, 5, tzinfo=UTC),
                    deleted_at=datetime(2026, 1, 25, tzinfo=UTC),
                    metadata={},
                )
            )
            uow.commit()

        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/identities",
            params={
                "period_start": "2026-01-10T00:00:00Z",
                "period_end": "2026-01-20T00:00:00Z",
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 1

    def test_list_identities_temporal_conflict_error(self, app_with_backend: TestClient) -> None:
        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/identities",
            params={
                "active_at": "2026-01-15T00:00:00Z",
                "period_start": "2026-01-10T00:00:00Z",
            },
        )
        assert response.status_code == 400
        assert "Cannot combine" in response.json()["detail"]

    def test_list_identities_filter_by_type(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        with in_memory_backend.create_unit_of_work() as uow:
            for itype in ["user", "service_account", "api_key"]:
                uow.identities.upsert(
                    Identity(
                        ecosystem="test-eco",
                        tenant_id="test-tenant",
                        identity_id=f"id-{itype}",
                        identity_type=itype,
                        created_at=datetime(2026, 1, 1, tzinfo=UTC),
                        metadata={},
                    )
                )
            uow.commit()

        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/identities",
            params={"identity_type": "service_account"},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 1
        assert data["items"][0]["identity_type"] == "service_account"

    def test_list_identities_invalid_period_range(self, app_with_backend: TestClient) -> None:
        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/identities",
            params={
                "period_start": "2026-01-20T00:00:00Z",
                "period_end": "2026-01-10T00:00:00Z",
            },
        )
        assert response.status_code == 400
        assert "period_start" in response.json()["detail"]
