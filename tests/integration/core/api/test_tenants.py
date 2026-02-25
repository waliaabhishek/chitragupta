from __future__ import annotations

from datetime import date

from fastapi.testclient import TestClient

from core.api.app import create_app
from core.config.models import ApiConfig, AppSettings, LoggingConfig, StorageConfig, TenantConfig
from core.models.pipeline import PipelineState
from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend


class TestListTenants:
    def test_list_tenants_empty(self) -> None:
        settings = AppSettings(
            api=ApiConfig(host="127.0.0.1", port=8080),
            logging=LoggingConfig(),
            tenants={},
        )
        app = create_app(settings)
        with TestClient(app) as client:
            response = client.get("/api/v1/tenants")
            assert response.status_code == 200
            data = response.json()
            assert data["tenants"] == []

    def test_list_tenants_with_data(self, temp_db_path: str) -> None:
        tenant_config = TenantConfig(
            tenant_id="t-1",
            ecosystem="test-eco",
            storage=StorageConfig(connection_string=temp_db_path),
        )
        # Create tables first
        backend = SQLModelBackend(temp_db_path, use_migrations=False)
        backend.create_tables()
        backend.dispose()

        settings = AppSettings(
            api=ApiConfig(host="127.0.0.1", port=8080),
            logging=LoggingConfig(),
            tenants={"test-tenant": tenant_config},
        )
        app = create_app(settings)
        with TestClient(app) as client:
            response = client.get("/api/v1/tenants")
            assert response.status_code == 200
            data = response.json()
            assert len(data["tenants"]) == 1
            assert data["tenants"][0]["tenant_name"] == "test-tenant"
            assert data["tenants"][0]["tenant_id"] == "t-1"
            assert data["tenants"][0]["ecosystem"] == "test-eco"

    def test_list_tenants_includes_status_summary(self, temp_db_path: str) -> None:
        tenant_config = TenantConfig(
            tenant_id="t-1",
            ecosystem="test-eco",
            storage=StorageConfig(connection_string=temp_db_path),
        )

        # Pre-populate with pipeline state
        backend = SQLModelBackend(temp_db_path, use_migrations=False)
        backend.create_tables()
        with backend.create_unit_of_work() as uow:
            uow.pipeline_state.upsert(
                PipelineState(
                    ecosystem="test-eco",
                    tenant_id="t-1",
                    tracking_date=date(2026, 2, 15),
                    billing_gathered=True,
                    resources_gathered=True,
                    chargeback_calculated=True,
                )
            )
            uow.commit()
        backend.dispose()

        settings = AppSettings(
            api=ApiConfig(host="127.0.0.1", port=8080),
            logging=LoggingConfig(),
            tenants={"test-tenant": tenant_config},
        )
        app = create_app(settings)
        with TestClient(app) as client:
            response = client.get("/api/v1/tenants")
            assert response.status_code == 200
            data = response.json()
            summary = data["tenants"][0]
            assert summary["dates_calculated"] == 1
            assert summary["last_calculated_date"] == "2026-02-15"


class TestGetTenantStatus:
    def test_get_tenant_status_not_found(self) -> None:
        settings = AppSettings(
            api=ApiConfig(host="127.0.0.1", port=8080),
            logging=LoggingConfig(),
            tenants={},
        )
        app = create_app(settings)
        with TestClient(app) as client:
            response = client.get("/api/v1/tenants/nonexistent/status")
            assert response.status_code == 404
            assert "not found" in response.json()["detail"]

    def test_get_tenant_status_success(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend, sample_pipeline_state: PipelineState
    ) -> None:
        # Insert pipeline state
        with in_memory_backend.create_unit_of_work() as uow:
            uow.pipeline_state.upsert(sample_pipeline_state)
            uow.commit()

        response = app_with_backend.get("/api/v1/tenants/test-tenant/status")
        assert response.status_code == 200
        data = response.json()
        assert data["tenant_name"] == "test-tenant"
        assert len(data["states"]) == 1
        assert data["states"][0]["tracking_date"] == "2026-02-15"
        assert data["states"][0]["chargeback_calculated"] is True

    def test_get_tenant_status_with_date_range(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        # Insert multiple pipeline states
        with in_memory_backend.create_unit_of_work() as uow:
            for d in [date(2026, 2, 10), date(2026, 2, 15), date(2026, 2, 20)]:
                uow.pipeline_state.upsert(
                    PipelineState(
                        ecosystem="test-eco",
                        tenant_id="test-tenant",
                        tracking_date=d,
                        billing_gathered=True,
                        resources_gathered=True,
                        chargeback_calculated=True,
                    )
                )
            uow.commit()

        # Query with date range
        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/status",
            params={"start_date": "2026-02-12", "end_date": "2026-02-18"},
        )
        assert response.status_code == 200
        data = response.json()
        assert len(data["states"]) == 1
        assert data["states"][0]["tracking_date"] == "2026-02-15"

    def test_get_tenant_status_with_start_date_only(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        with in_memory_backend.create_unit_of_work() as uow:
            for d in [date(2026, 2, 10), date(2026, 2, 15), date(2026, 2, 20)]:
                uow.pipeline_state.upsert(
                    PipelineState(
                        ecosystem="test-eco",
                        tenant_id="test-tenant",
                        tracking_date=d,
                        billing_gathered=True,
                        resources_gathered=True,
                        chargeback_calculated=True,
                    )
                )
            uow.commit()

        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/status",
            params={"start_date": "2026-02-14"},
        )
        assert response.status_code == 200
        data = response.json()
        assert len(data["states"]) == 2
        dates = [s["tracking_date"] for s in data["states"]]
        assert "2026-02-15" in dates
        assert "2026-02-20" in dates

    def test_get_tenant_status_with_end_date_only(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        with in_memory_backend.create_unit_of_work() as uow:
            for d in [date(2026, 2, 10), date(2026, 2, 15), date(2026, 2, 20)]:
                uow.pipeline_state.upsert(
                    PipelineState(
                        ecosystem="test-eco",
                        tenant_id="test-tenant",
                        tracking_date=d,
                        billing_gathered=True,
                        resources_gathered=True,
                        chargeback_calculated=True,
                    )
                )
            uow.commit()

        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/status",
            params={"end_date": "2026-02-16"},
        )
        assert response.status_code == 200
        data = response.json()
        assert len(data["states"]) == 2
        dates = [s["tracking_date"] for s in data["states"]]
        assert "2026-02-10" in dates
        assert "2026-02-15" in dates
