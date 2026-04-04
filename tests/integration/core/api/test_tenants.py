from __future__ import annotations

from datetime import date, timedelta

from fastapi.testclient import TestClient

from core.api.app import create_app
from core.config.models import ApiConfig, AppSettings, LoggingConfig, StorageConfig, TenantConfig
from core.models.pipeline import PipelineState
from core.storage.backends.sqlmodel.module import CoreStorageModule
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
        backend = SQLModelBackend(temp_db_path, CoreStorageModule(), use_migrations=False)
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
        backend = SQLModelBackend(temp_db_path, CoreStorageModule(), use_migrations=False)
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
        assert data["states"][0]["tracking_date"] == (date.today() - timedelta(days=1)).isoformat()
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


class TestGetTenantStatusTopicAttributionFields:
    """PipelineStateResponse must expose topic_overlay_gathered and topic_attribution_calculated."""

    def test_status_response_includes_topic_overlay_gathered_field(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        with in_memory_backend.create_unit_of_work() as uow:
            uow.pipeline_state.upsert(
                PipelineState(
                    ecosystem="test-eco",
                    tenant_id="test-tenant",
                    tracking_date=date.today() - timedelta(days=1),
                    billing_gathered=True,
                    resources_gathered=True,
                    chargeback_calculated=True,
                    topic_overlay_gathered=False,
                    topic_attribution_calculated=False,
                )
            )
            uow.commit()

        response = app_with_backend.get("/api/v1/tenants/test-tenant/status")
        assert response.status_code == 200
        state = response.json()["states"][0]

        assert "topic_overlay_gathered" in state, (
            "PipelineStateResponse is missing 'topic_overlay_gathered' field — "
            "frontend pipeline status page cannot show topic overlay stage"
        )

    def test_status_response_includes_topic_attribution_calculated_field(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        with in_memory_backend.create_unit_of_work() as uow:
            uow.pipeline_state.upsert(
                PipelineState(
                    ecosystem="test-eco",
                    tenant_id="test-tenant",
                    tracking_date=date.today() - timedelta(days=1),
                    billing_gathered=True,
                    resources_gathered=True,
                    chargeback_calculated=True,
                    topic_overlay_gathered=True,
                    topic_attribution_calculated=False,
                )
            )
            uow.commit()

        response = app_with_backend.get("/api/v1/tenants/test-tenant/status")
        assert response.status_code == 200
        state = response.json()["states"][0]

        assert "topic_attribution_calculated" in state, (
            "PipelineStateResponse is missing 'topic_attribution_calculated' field"
        )

    def test_topic_overlay_gathered_false_before_overlay_runs(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        """topic_overlay_gathered must be False when not yet gathered."""
        with in_memory_backend.create_unit_of_work() as uow:
            uow.pipeline_state.upsert(
                PipelineState(
                    ecosystem="test-eco",
                    tenant_id="test-tenant",
                    tracking_date=date.today() - timedelta(days=1),
                    billing_gathered=True,
                    resources_gathered=True,
                    chargeback_calculated=True,
                    topic_overlay_gathered=False,
                    topic_attribution_calculated=False,
                )
            )
            uow.commit()

        response = app_with_backend.get("/api/v1/tenants/test-tenant/status")
        assert response.status_code == 200
        state = response.json()["states"][0]

        assert state["topic_overlay_gathered"] is False
        assert state["topic_attribution_calculated"] is False

    def test_topic_overlay_gathered_true_after_gather(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        """topic_overlay_gathered must be True after overlay gather runs."""
        with in_memory_backend.create_unit_of_work() as uow:
            uow.pipeline_state.upsert(
                PipelineState(
                    ecosystem="test-eco",
                    tenant_id="test-tenant",
                    tracking_date=date.today() - timedelta(days=1),
                    billing_gathered=True,
                    resources_gathered=True,
                    chargeback_calculated=True,
                    topic_overlay_gathered=True,
                    topic_attribution_calculated=False,
                )
            )
            uow.commit()

        response = app_with_backend.get("/api/v1/tenants/test-tenant/status")
        assert response.status_code == 200
        state = response.json()["states"][0]

        assert state["topic_overlay_gathered"] is True
        assert state["topic_attribution_calculated"] is False

    def test_topic_attribution_calculated_true_after_phase_runs(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        """topic_attribution_calculated must be True after phase completes."""
        with in_memory_backend.create_unit_of_work() as uow:
            uow.pipeline_state.upsert(
                PipelineState(
                    ecosystem="test-eco",
                    tenant_id="test-tenant",
                    tracking_date=date.today() - timedelta(days=1),
                    billing_gathered=True,
                    resources_gathered=True,
                    chargeback_calculated=True,
                    topic_overlay_gathered=True,
                    topic_attribution_calculated=True,
                )
            )
            uow.commit()

        response = app_with_backend.get("/api/v1/tenants/test-tenant/status")
        assert response.status_code == 200
        state = response.json()["states"][0]

        assert state["topic_overlay_gathered"] is True
        assert state["topic_attribution_calculated"] is True

    def test_multiple_dates_all_have_topic_fields(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        """Every state entry must carry topic attribution fields, not just the first."""
        with in_memory_backend.create_unit_of_work() as uow:
            uow.pipeline_state.upsert(
                PipelineState(
                    ecosystem="test-eco",
                    tenant_id="test-tenant",
                    tracking_date=date.today() - timedelta(days=2),
                    billing_gathered=True,
                    resources_gathered=True,
                    chargeback_calculated=True,
                    topic_overlay_gathered=True,
                    topic_attribution_calculated=True,
                )
            )
            uow.pipeline_state.upsert(
                PipelineState(
                    ecosystem="test-eco",
                    tenant_id="test-tenant",
                    tracking_date=date.today() - timedelta(days=1),
                    billing_gathered=True,
                    resources_gathered=True,
                    chargeback_calculated=True,
                    topic_overlay_gathered=False,
                    topic_attribution_calculated=False,
                )
            )
            uow.commit()

        response = app_with_backend.get("/api/v1/tenants/test-tenant/status")
        assert response.status_code == 200
        states = response.json()["states"]
        assert len(states) == 2

        for state in states:
            assert "topic_overlay_gathered" in state
            assert "topic_attribution_calculated" in state


class TestGetTenantStatusDefaultLookbackWindow:
    """Default lookback window behavior when no date params are provided."""

    def test_default_range_excludes_old_records(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        """Records older than lookback_days are excluded from the default response."""
        today = date.today()
        default_lookback = 200  # TenantConfig default

        in_window_date = today - timedelta(days=1)
        out_of_window_date = today - timedelta(days=default_lookback + 1)

        with in_memory_backend.create_unit_of_work() as uow:
            for d in [in_window_date, out_of_window_date]:
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

        response = app_with_backend.get("/api/v1/tenants/test-tenant/status")
        assert response.status_code == 200
        data = response.json()
        returned_dates = [s["tracking_date"] for s in data["states"]]
        assert in_window_date.isoformat() in returned_dates
        assert out_of_window_date.isoformat() not in returned_dates

    def test_default_range_uses_tenant_lookback_days(self, temp_db_path: str) -> None:
        """Default window derives from tenant_config.lookback_days, not a hardcoded constant."""
        lookback_days = 30
        today = date.today()

        in_window_date = today - timedelta(days=25)
        out_of_window_date = today - timedelta(days=35)

        backend = SQLModelBackend(temp_db_path, CoreStorageModule(), use_migrations=False)
        backend.create_tables()
        with backend.create_unit_of_work() as uow:
            for d in [in_window_date, out_of_window_date]:
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
        backend.dispose()

        settings = AppSettings(
            api=ApiConfig(host="127.0.0.1", port=8080),
            logging=LoggingConfig(),
            tenants={
                "test-tenant": TenantConfig(
                    tenant_id="test-tenant",
                    ecosystem="test-eco",
                    lookback_days=lookback_days,
                    storage=StorageConfig(connection_string=temp_db_path),
                )
            },
        )
        app = create_app(settings)
        with TestClient(app) as client:
            response = client.get("/api/v1/tenants/test-tenant/status")

        assert response.status_code == 200
        data = response.json()
        returned_dates = [s["tracking_date"] for s in data["states"]]
        assert in_window_date.isoformat() in returned_dates
        assert out_of_window_date.isoformat() not in returned_dates

    def test_default_range_includes_today(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        """A record dated today is returned — validates the +1 day offset on the exclusive upper bound."""
        today = date.today()

        with in_memory_backend.create_unit_of_work() as uow:
            uow.pipeline_state.upsert(
                PipelineState(
                    ecosystem="test-eco",
                    tenant_id="test-tenant",
                    tracking_date=today,
                    billing_gathered=True,
                    resources_gathered=True,
                    chargeback_calculated=True,
                )
            )
            uow.commit()

        response = app_with_backend.get("/api/v1/tenants/test-tenant/status")
        assert response.status_code == 200
        data = response.json()
        returned_dates = [s["tracking_date"] for s in data["states"]]
        assert today.isoformat() in returned_dates

    def test_explicit_start_end_params_unchanged(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        """Explicit start_date + end_date still filter correctly — no regression from default window change."""
        today = date.today()
        d_before = today - timedelta(days=10)
        d_in = today - timedelta(days=5)
        d_after = today - timedelta(days=1)
        start = today - timedelta(days=7)
        end = today - timedelta(days=3)

        with in_memory_backend.create_unit_of_work() as uow:
            for d in [d_before, d_in, d_after]:
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
            params={"start_date": start.isoformat(), "end_date": end.isoformat()},
        )
        assert response.status_code == 200
        data = response.json()
        assert len(data["states"]) == 1
        assert data["states"][0]["tracking_date"] == d_in.isoformat()

    def test_explicit_start_only_unchanged(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        """elif start_date branch continues working with its sentinel range."""
        today = date.today()
        d_old = today - timedelta(days=10)
        d_mid = today - timedelta(days=5)
        d_recent = today - timedelta(days=1)
        boundary = today - timedelta(days=7)

        with in_memory_backend.create_unit_of_work() as uow:
            for d in [d_old, d_mid, d_recent]:
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

        # start_date only: d_mid and d_recent returned (>= boundary); d_old excluded
        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/status",
            params={"start_date": boundary.isoformat()},
        )
        assert response.status_code == 200
        data = response.json()
        returned_dates = {s["tracking_date"] for s in data["states"]}
        assert d_mid.isoformat() in returned_dates
        assert d_recent.isoformat() in returned_dates
        assert d_old.isoformat() not in returned_dates

    def test_explicit_end_only_unchanged(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        """elif end_date branch continues working with its sentinel range."""
        today = date.today()
        d_old = today - timedelta(days=10)
        d_mid = today - timedelta(days=5)
        d_recent = today - timedelta(days=1)
        boundary = today - timedelta(days=7)

        with in_memory_backend.create_unit_of_work() as uow:
            for d in [d_old, d_mid, d_recent]:
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

        # end_date only: d_old returned (< boundary); d_mid and d_recent excluded
        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/status",
            params={"end_date": boundary.isoformat()},
        )
        assert response.status_code == 200
        data = response.json()
        returned_dates = {s["tracking_date"] for s in data["states"]}
        assert d_old.isoformat() in returned_dates
        assert d_mid.isoformat() not in returned_dates
        assert d_recent.isoformat() not in returned_dates
