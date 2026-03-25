from __future__ import annotations

import tempfile
from datetime import UTC, datetime
from pathlib import Path

from fastapi.testclient import TestClient  # noqa: TC002

from core.api.app import create_app
from core.config.models import ApiConfig, AppSettings, LoggingConfig, StorageConfig, TenantConfig
from core.models.identity import CoreIdentity
from core.models.resource import CoreResource, ResourceStatus
from core.storage.backends.sqlmodel.module import CoreStorageModule
from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend  # noqa: TC001


class TestInventorySummary:
    def test_inventory_summary_empty(self, app_with_backend: TestClient) -> None:
        response = app_with_backend.get("/api/v1/tenants/test-tenant/inventory/summary")
        assert response.status_code == 200
        data = response.json()
        assert data["resource_counts"] == {}
        assert data["identity_counts"] == {}

    def test_inventory_summary_with_resources(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        with in_memory_backend.create_unit_of_work() as uow:
            for i in range(3):
                uow.resources.upsert(
                    CoreResource(
                        ecosystem="test-eco",
                        tenant_id="test-tenant",
                        resource_id=f"kafka-{i}",
                        resource_type="kafka_cluster",
                        status=ResourceStatus.ACTIVE,
                        created_at=datetime(2026, 1, 1, tzinfo=UTC),
                        metadata={},
                    )
                )
            uow.commit()

        response = app_with_backend.get("/api/v1/tenants/test-tenant/inventory/summary")
        assert response.status_code == 200
        data = response.json()
        assert data["resource_counts"]["kafka_cluster"] == {"total": 3, "active": 3, "deleted": 0}
        assert data["identity_counts"] == {}

    def test_inventory_summary_with_resources_mixed_status(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        with in_memory_backend.create_unit_of_work() as uow:
            for i in range(3):
                uow.resources.upsert(
                    CoreResource(
                        ecosystem="test-eco",
                        tenant_id="test-tenant",
                        resource_id=f"kafka-active-{i}",
                        resource_type="kafka_cluster",
                        status=ResourceStatus.ACTIVE,
                        created_at=datetime(2026, 1, 1, tzinfo=UTC),
                        metadata={},
                    )
                )
            for i in range(2):
                uow.resources.upsert(
                    CoreResource(
                        ecosystem="test-eco",
                        tenant_id="test-tenant",
                        resource_id=f"kafka-deleted-{i}",
                        resource_type="kafka_cluster",
                        status=ResourceStatus.DELETED,
                        created_at=datetime(2026, 1, 1, tzinfo=UTC),
                        metadata={},
                    )
                )
            uow.commit()

        response = app_with_backend.get("/api/v1/tenants/test-tenant/inventory/summary")
        assert response.status_code == 200
        data = response.json()
        assert data["resource_counts"]["kafka_cluster"] == {"total": 5, "active": 3, "deleted": 2}

    def test_inventory_summary_with_identities(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        with in_memory_backend.create_unit_of_work() as uow:
            for i in range(2):
                uow.identities.upsert(
                    CoreIdentity(
                        ecosystem="test-eco",
                        tenant_id="test-tenant",
                        identity_id=f"sa-{i}",
                        identity_type="service_account",
                        created_at=datetime(2026, 1, 1, tzinfo=UTC),
                        metadata={},
                    )
                )
            uow.commit()

        response = app_with_backend.get("/api/v1/tenants/test-tenant/inventory/summary")
        assert response.status_code == 200
        data = response.json()
        assert data["resource_counts"] == {}
        assert data["identity_counts"]["service_account"] == {"total": 2, "active": 2, "deleted": 0}

    def test_inventory_summary_identity_deleted_at_status_derivation(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        with in_memory_backend.create_unit_of_work() as uow:
            for i in range(3):
                uow.identities.upsert(
                    CoreIdentity(
                        ecosystem="test-eco",
                        tenant_id="test-tenant",
                        identity_id=f"sa-active-{i}",
                        identity_type="service_account",
                        created_at=datetime(2026, 1, 1, tzinfo=UTC),
                        deleted_at=None,
                        metadata={},
                    )
                )
            for i in range(2):
                uow.identities.upsert(
                    CoreIdentity(
                        ecosystem="test-eco",
                        tenant_id="test-tenant",
                        identity_id=f"sa-deleted-{i}",
                        identity_type="service_account",
                        created_at=datetime(2026, 1, 1, tzinfo=UTC),
                        deleted_at=datetime(2026, 1, 15, tzinfo=UTC),
                        metadata={},
                    )
                )
            uow.commit()

        response = app_with_backend.get("/api/v1/tenants/test-tenant/inventory/summary")
        assert response.status_code == 200
        data = response.json()
        assert data["identity_counts"]["service_account"] == {"total": 5, "active": 3, "deleted": 2}

    def test_inventory_summary_combined(self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend) -> None:
        with in_memory_backend.create_unit_of_work() as uow:
            uow.resources.upsert(
                CoreResource(
                    ecosystem="test-eco",
                    tenant_id="test-tenant",
                    resource_id="env-1",
                    resource_type="environment",
                    status=ResourceStatus.ACTIVE,
                    created_at=datetime(2026, 1, 1, tzinfo=UTC),
                    metadata={},
                )
            )
            for i in range(2):
                uow.resources.upsert(
                    CoreResource(
                        ecosystem="test-eco",
                        tenant_id="test-tenant",
                        resource_id=f"conn-{i}",
                        resource_type="connector",
                        status=ResourceStatus.ACTIVE,
                        created_at=datetime(2026, 1, 1, tzinfo=UTC),
                        metadata={},
                    )
                )
            for i in range(3):
                uow.resources.upsert(
                    CoreResource(
                        ecosystem="test-eco",
                        tenant_id="test-tenant",
                        resource_id=f"kafka-{i}",
                        resource_type="kafka_cluster",
                        status=ResourceStatus.ACTIVE,
                        created_at=datetime(2026, 1, 1, tzinfo=UTC),
                        metadata={},
                    )
                )
            uow.identities.upsert(
                CoreIdentity(
                    ecosystem="test-eco",
                    tenant_id="test-tenant",
                    identity_id="sa-1",
                    identity_type="service_account",
                    created_at=datetime(2026, 1, 1, tzinfo=UTC),
                    metadata={},
                )
            )
            uow.commit()

        response = app_with_backend.get("/api/v1/tenants/test-tenant/inventory/summary")
        assert response.status_code == 200
        data = response.json()
        assert set(data["resource_counts"].keys()) == {"environment", "connector", "kafka_cluster"}
        assert data["resource_counts"]["environment"] == {"total": 1, "active": 1, "deleted": 0}
        assert data["resource_counts"]["connector"] == {"total": 2, "active": 2, "deleted": 0}
        assert data["resource_counts"]["kafka_cluster"] == {"total": 3, "active": 3, "deleted": 0}
        assert data["identity_counts"]["service_account"] == {"total": 1, "active": 1, "deleted": 0}

    def test_inventory_summary_tenant_not_found(self, app_with_backend: TestClient) -> None:
        response = app_with_backend.get("/api/v1/tenants/unknown-tenant/inventory/summary")
        assert response.status_code == 404

    def test_inventory_summary_tenant_isolation(self) -> None:
        """Tenant A's resources must not appear in tenant B's inventory summary."""
        with (
            tempfile.NamedTemporaryFile(suffix=".db", delete=False) as fa,
            tempfile.NamedTemporaryFile(suffix=".db", delete=False) as fb,
        ):
            db_a, db_b = f"sqlite:///{fa.name}", f"sqlite:///{fb.name}"

        try:
            backend_a = SQLModelBackend(db_a, CoreStorageModule(), use_migrations=False)
            backend_b = SQLModelBackend(db_b, CoreStorageModule(), use_migrations=False)
            backend_a.create_tables()
            backend_b.create_tables()

            # Insert 3 resources under tenant-a
            with backend_a.create_unit_of_work() as uow:
                for i in range(3):
                    uow.resources.upsert(
                        CoreResource(
                            ecosystem="eco",
                            tenant_id="tenant-a",
                            resource_id=f"kafka-{i}",
                            resource_type="kafka_cluster",
                            status=ResourceStatus.ACTIVE,
                            created_at=datetime(2026, 1, 1, tzinfo=UTC),
                            metadata={},
                        )
                    )
                uow.commit()

            settings = AppSettings(
                api=ApiConfig(host="127.0.0.1", port=8080),
                logging=LoggingConfig(),
                tenants={
                    "tenant-a": TenantConfig(
                        tenant_id="tenant-a",
                        ecosystem="eco",
                        storage=StorageConfig(connection_string=db_a),
                    ),
                    "tenant-b": TenantConfig(
                        tenant_id="tenant-b",
                        ecosystem="eco",
                        storage=StorageConfig(connection_string=db_b),
                    ),
                },
            )
            app = create_app(settings)
            with TestClient(app) as client:
                app.state.backends["tenant-a"] = backend_a
                app.state.backends["tenant-b"] = backend_b

                response = client.get("/api/v1/tenants/tenant-b/inventory/summary")
                assert response.status_code == 200
                data = response.json()
                assert data["resource_counts"] == {}
                assert data["identity_counts"] == {}

            backend_a.dispose()
            backend_b.dispose()
        finally:
            Path(db_a.removeprefix("sqlite:///")).unlink(missing_ok=True)
            Path(db_b.removeprefix("sqlite:///")).unlink(missing_ok=True)
