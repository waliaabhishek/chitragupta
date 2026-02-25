from __future__ import annotations

from fastapi.testclient import TestClient  # noqa: TC002


class TestTriggerPipeline:
    def test_trigger_pipeline_starts(self, app_with_backend: TestClient) -> None:
        response = app_with_backend.post("/api/v1/tenants/test-tenant/pipeline/run")
        assert response.status_code == 202
        data = response.json()
        assert data["status"] == "started"
        assert data["tenant_name"] == "test-tenant"

    def test_trigger_pipeline_nonexistent_tenant(self, app_with_backend: TestClient) -> None:
        response = app_with_backend.post("/api/v1/tenants/no-such-tenant/pipeline/run")
        assert response.status_code == 404

    def test_trigger_pipeline_already_running_returns_409(self, app_with_backend: TestClient) -> None:
        """Second trigger while running returns 409 Conflict."""
        from core.api.routes.pipeline import PipelineRunState

        # Manually set a running state so the second call sees it
        app = app_with_backend.app  # type: ignore[union-attr]
        if not hasattr(app.state, "pipeline_runs"):
            app.state.pipeline_runs = {}
        app.state.pipeline_runs["test-tenant"] = PipelineRunState(is_running=True)

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
        # No WorkflowRunner in test — pipeline completes with stub result
        response = app_with_backend.get("/api/v1/tenants/test-tenant/pipeline/status")
        assert response.status_code == 200
        data = response.json()
        assert data["tenant_name"] == "test-tenant"
        # In API-only mode (no WorkflowRunner), result includes error about missing runner
        if data["last_result"] is not None:
            assert "completed_at" in data["last_result"]
            assert isinstance(data["last_result"]["errors"], list)

    def test_status_with_last_result(self, app_with_backend: TestClient) -> None:
        """Verify PipelineResultSummary fields after a completed run."""
        from datetime import UTC, datetime

        from core.api.routes.pipeline import PipelineRunState
        from core.api.schemas import PipelineResultSummary

        app = app_with_backend.app  # type: ignore[union-attr]
        if not hasattr(app.state, "pipeline_runs"):
            app.state.pipeline_runs = {}
        app.state.pipeline_runs["test-tenant"] = PipelineRunState(
            is_running=False,
            last_run=datetime(2026, 2, 24, 12, 0, tzinfo=UTC),
            last_result=PipelineResultSummary(
                dates_gathered=5,
                dates_calculated=3,
                chargeback_rows_written=150,
                errors=[],
                completed_at=datetime(2026, 2, 24, 12, 0, tzinfo=UTC),
            ),
        )

        response = app_with_backend.get("/api/v1/tenants/test-tenant/pipeline/status")
        assert response.status_code == 200
        data = response.json()
        assert data["is_running"] is False
        result = data["last_result"]
        assert result["dates_gathered"] == 5
        assert result["dates_calculated"] == 3
        assert result["chargeback_rows_written"] == 150
        assert result["errors"] == []

    def test_status_nonexistent_tenant(self, app_with_backend: TestClient) -> None:
        response = app_with_backend.get("/api/v1/tenants/no-such-tenant/pipeline/status")
        assert response.status_code == 404
