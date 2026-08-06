from __future__ import annotations

import asyncio
import logging
import time
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock

from fastapi.testclient import TestClient  # noqa: TC002

if TYPE_CHECKING:
    from core.engine.orchestrator import PipelineRunResult
    from core.metrics.protocol import MetricsSource
    from core.plugin.protocols import CostAllocator, CostInput, ServiceHandler, StorageModule
    from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend


class _NoopCostInput:
    def gather(self, tenant_id: str, start: datetime, end: datetime, uow: Any) -> tuple[object, ...]:
        del tenant_id, start, end, uow
        return ()


class _TypedNoopPlugin:
    @property
    def ecosystem(self) -> str:
        return "confluent_cloud"

    def initialize(self, config: dict[str, Any]) -> None:
        del config

    def get_service_handlers(self) -> dict[str, ServiceHandler]:
        return {}

    def get_cost_input(self) -> CostInput:
        return _NoopCostInput()

    def get_metrics_source(self) -> MetricsSource | None:
        return None

    def get_fallback_allocator(self) -> CostAllocator | None:
        return None

    def build_shared_context(self, tenant_id: str) -> object | None:
        del tenant_id
        return None

    def get_storage_module(self) -> StorageModule:
        from core.storage.backends.sqlmodel.module import CoreStorageModule

        return CoreStorageModule()

    def close(self) -> None:
        return None


class _FailingOrchestrator:
    def __init__(self, error: BaseException) -> None:
        self._error = error
        self._progress_callback: Any = None

    def run(self, *, calculation_run_id: int | None = None) -> PipelineRunResult:
        del calculation_run_id
        raise self._error


class TestTriggerPipeline:
    def test_trigger_pipeline_starts(self, app_with_mock_runner: TestClient) -> None:
        response = app_with_mock_runner.post("/api/v1/tenants/test-tenant/pipeline/run")
        assert response.status_code == 202
        data = response.json()
        assert data["status"] == "started"
        assert data["tenant_name"] == "test-tenant"

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

    def test_trigger_pipeline_background_failure_logs_once_at_workflow_runner_owner(
        self,
        settings_with_tenant,
        in_memory_backend: SQLModelBackend,
        monkeypatch,
        caplog,
    ) -> None:
        from core.api.app import create_app
        from core.plugin.registry import PluginRegistry
        from tests.integration.core.api.backend_provider import install_backend
        from workflow_runner import TenantRuntime, WorkflowRunner, _config_hash

        async def to_thread_inline(function, *args, **kwargs):
            return function(*args, **kwargs)

        monkeypatch.setattr("core.api.routes.pipeline.asyncio.to_thread", to_thread_inline)

        runner = WorkflowRunner(settings_with_tenant, PluginRegistry())
        runner._bootstrapped = True  # noqa: SLF001
        orchestrator = _FailingOrchestrator(RuntimeError("background failure"))
        runner._tenant_runtimes["test-tenant"] = TenantRuntime(  # noqa: SLF001
            tenant_name="test-tenant",
            plugin=_TypedNoopPlugin(),
            storage=in_memory_backend,
            orchestrator=orchestrator,
            config_hash=_config_hash(settings_with_tenant.tenants["test-tenant"]),
            created_at=datetime(2026, 3, 1, tzinfo=UTC),
        )

        app = create_app(settings_with_tenant, workflow_runner=runner, mode="both")
        with TestClient(app) as client, caplog.at_level(logging.INFO):
            install_backend(app, "test-tenant", in_memory_backend)
            response = client.post("/api/v1/tenants/test-tenant/pipeline/run")
            task = client.app.state.pipeline_tasks["test-tenant"]
            deadline = time.monotonic() + 2.0
            while not task.done() and time.monotonic() < deadline:
                time.sleep(0.01)

        assert response.status_code == 202
        assert task.done() is True
        with in_memory_backend.create_read_only_unit_of_work() as uow:
            run = uow.pipeline_runs.get_latest_run("test-tenant")
        assert run is not None
        assert run.status == "failed"
        assert run.error_message == "Unhandled exception — see logs"
        owner_records = [
            record
            for record in caplog.records
            if record.name == "workflow_runner" and record.getMessage().startswith("pipeline_run_failed")
        ]
        assert len(owner_records) == 1
        assert owner_records[0].exc_info is None
        assert "traceback_frames=" in owner_records[0].getMessage()
        route_completion_records = [
            record
            for record in caplog.records
            if record.name == "core.api.routes.pipeline"
            and record.getMessage().startswith("pipeline_background_completed")
        ]
        assert len(route_completion_records) == 1
        route_message = route_completion_records[0].getMessage()
        assert route_completion_records[0].exc_info is None
        assert "tenant_name=test-tenant" in route_message
        assert "request_id=" in route_message
        assert "stage=pipeline_dispatch" in route_message
        assert "operation=pipeline_run" in route_message
        assert "outcome=failed" in route_message
        assert "traceback_frames=" not in route_message
        assert "error_type=" not in route_message
        assert "background failure" not in route_message


class TestPipelineStatus:
    def test_status_no_prior_run(self, app_with_backend: TestClient) -> None:
        response = app_with_backend.get("/api/v1/tenants/test-tenant/pipeline/status")
        assert response.status_code == 200
        data = response.json()
        assert data["tenant_name"] == "test-tenant"
        assert data["is_running"] is False
        assert data["last_run"] is None
        assert data["last_result"] is None

    def test_status_after_run(self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend) -> None:
        from datetime import UTC, datetime

        with in_memory_backend.create_unit_of_work() as uow:
            run = uow.pipeline_runs.create_run("test-tenant", datetime(2026, 3, 1, 10, 0, tzinfo=UTC))
            run.status = "running"
            uow.pipeline_runs.update_run(run)
            uow.commit()

        response = app_with_backend.get("/api/v1/tenants/test-tenant/pipeline/status")
        assert response.status_code == 200
        data = response.json()
        assert data["tenant_name"] == "test-tenant"
        assert data["last_run"] is not None
        assert isinstance(data["is_running"], bool)

    def test_status_after_completed_run_in_db(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
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
