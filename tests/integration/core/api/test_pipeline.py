from __future__ import annotations

import asyncio
from unittest.mock import MagicMock

from fastapi.testclient import TestClient  # noqa: TC002


class TestTriggerPipeline:
    def test_trigger_pipeline_starts(self, app_with_backend: TestClient, in_memory_backend) -> None:
        response = app_with_backend.post("/api/v1/tenants/test-tenant/pipeline/run")
        assert response.status_code == 202
        data = response.json()
        assert data["status"] == "started"
        assert data["tenant_name"] == "test-tenant"
        # DB record is created synchronously before background task launches
        with in_memory_backend.create_unit_of_work() as uow:
            latest = uow.pipeline_runs.get_latest_run("test-tenant")
        assert latest is not None
        assert latest.tenant_name == "test-tenant"
        assert latest.status in ("running", "completed")

    def test_trigger_pipeline_nonexistent_tenant(self, app_with_backend: TestClient) -> None:
        response = app_with_backend.post("/api/v1/tenants/no-such-tenant/pipeline/run")
        assert response.status_code == 404

    def test_trigger_pipeline_already_running_returns_409(self, app_with_backend: TestClient) -> None:
        """Second trigger while running returns 409 Conflict."""
        app = app_with_backend.app  # type: ignore[union-attr]
        if not hasattr(app.state, "pipeline_tasks"):
            app.state.pipeline_tasks = {}

        # Inject a fake running (not-done) task
        mock_task = MagicMock(spec=asyncio.Task)
        mock_task.done.return_value = False
        app.state.pipeline_tasks["test-tenant"] = mock_task

        response = app_with_backend.post("/api/v1/tenants/test-tenant/pipeline/run")
        assert response.status_code == 409
        assert "already running" in response.json()["detail"]


class TestPipelineStatus:
    def test_status_no_prior_run(self, app_with_backend: TestClient) -> None:
        response = app_with_backend.get("/api/v1/tenants/test-tenant/pipeline/status")
        assert response.status_code == 200
        data = response.json()
        assert data["tenant_name"] == "test-tenant"
        assert data["is_running"] is False
        assert data["last_run"] is None
        assert data["last_result"] is None

    def test_status_after_run(self, app_with_backend: TestClient) -> None:
        app_with_backend.post("/api/v1/tenants/test-tenant/pipeline/run")
        response = app_with_backend.get("/api/v1/tenants/test-tenant/pipeline/status")
        assert response.status_code == 200
        data = response.json()
        assert data["tenant_name"] == "test-tenant"
        # DB record is created synchronously, so last_run is always set
        assert data["last_run"] is not None
        assert isinstance(data["is_running"], bool)

    def test_status_after_completed_run_in_db(self, app_with_backend: TestClient, in_memory_backend) -> None:
        """Status reflects DB-persisted completed run data."""
        from datetime import UTC, datetime

        with in_memory_backend.create_unit_of_work() as uow:
            run = uow.pipeline_runs.create_run("test-tenant", datetime(2026, 2, 26, 10, 0, tzinfo=UTC))
            run.status = "completed"
            run.ended_at = datetime(2026, 2, 26, 11, 0, tzinfo=UTC)
            run.dates_gathered = 5
            run.dates_calculated = 3
            run.rows_written = 150
            uow.pipeline_runs.update_run(run)
            uow.commit()

        response = app_with_backend.get("/api/v1/tenants/test-tenant/pipeline/status")
        assert response.status_code == 200
        data = response.json()
        assert data["is_running"] is False
        result = data["last_result"]
        assert result is not None
        assert result["dates_gathered"] == 5
        assert result["dates_calculated"] == 3
        assert result["chargeback_rows_written"] == 150
        assert result["errors"] == []

    def test_status_nonexistent_tenant(self, app_with_backend: TestClient) -> None:
        response = app_with_backend.get("/api/v1/tenants/no-such-tenant/pipeline/status")
        assert response.status_code == 404
