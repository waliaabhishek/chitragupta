"""Unit tests for the _run_pipeline background task and trigger_pipeline endpoint."""

from __future__ import annotations

import asyncio
from collections.abc import Iterator
from contextlib import contextmanager
from unittest.mock import MagicMock

from fastapi.testclient import TestClient

from core.api.routes.pipeline import _run_pipeline


def _make_request(workflow_runner: object = None) -> MagicMock:
    request = MagicMock()
    request.app.state.workflow_runner = workflow_runner
    return request


class TestRunPipelineBackground:
    def test_workflow_runner_run_tenant_called(self) -> None:
        """WorkflowRunner.run_tenant is called with the tenant name."""
        mock_result = MagicMock()
        mock_result.already_running = False

        mock_runner = MagicMock()
        mock_runner.run_tenant.return_value = mock_result

        asyncio.run(_run_pipeline("my-tenant", _make_request(mock_runner)))

        mock_runner.run_tenant.assert_called_once_with("my-tenant")

    def test_workflow_runner_errors_do_not_raise(self) -> None:
        """Errors in result.errors are logged without raising."""
        mock_result = MagicMock()
        mock_result.already_running = False
        mock_result.errors = ["billing API timeout"]

        mock_runner = MagicMock()
        mock_runner.run_tenant.return_value = mock_result

        asyncio.run(_run_pipeline("my-tenant", _make_request(mock_runner)))
        mock_runner.run_tenant.assert_called_once_with("my-tenant")

    def test_exception_in_runner_does_not_propagate(self) -> None:
        """When workflow_runner.run_tenant raises, exception is logged but not re-raised."""
        mock_runner = MagicMock()
        mock_runner.run_tenant.side_effect = RuntimeError("connection refused")

        # Should not raise
        asyncio.run(_run_pipeline("my-tenant", _make_request(mock_runner)))

    def test_trigger_pipeline_no_runner_returns_400(self) -> None:
        """trigger_pipeline returns 400 when no WorkflowRunner is configured."""
        with _make_app_with_runner(None) as client:
            response = client.post("/api/v1/tenants/t/pipeline/run")
        assert response.status_code == 400

    def test_run_pipeline_already_running_result_logged(self) -> None:
        """When run_tenant() returns already_running=True, _run_pipeline logs and returns without raising."""
        from core.engine.orchestrator import PipelineRunResult

        already_running_result = PipelineRunResult(
            tenant_name="my-tenant",
            tenant_id="tid",
            dates_gathered=0,
            dates_calculated=0,
            chargeback_rows_written=0,
            already_running=True,
        )
        mock_runner = MagicMock()
        mock_runner.run_tenant.return_value = already_running_result

        asyncio.run(_run_pipeline("my-tenant", _make_request(mock_runner)))

        mock_runner.run_tenant.assert_called_once_with("my-tenant")


@contextmanager
def _make_app_with_runner(runner: object) -> Iterator[TestClient]:
    """Create a TestClient with a workflow_runner injected into app state."""
    from core.api.app import create_app
    from core.config.models import ApiConfig, AppSettings, LoggingConfig, StorageConfig, TenantConfig

    tenant_config = TenantConfig(
        tenant_id="t",
        ecosystem="test-eco",
        storage=StorageConfig(connection_string="sqlite:///:memory:"),
    )
    settings = AppSettings(
        api=ApiConfig(host="127.0.0.1", port=8080),
        logging=LoggingConfig(),
        tenants={"t": tenant_config},
    )
    app = create_app(settings, workflow_runner=runner)  # type: ignore[arg-type]
    with TestClient(app) as client:
        yield client


class TestTriggerPipelineAlreadyRunning:
    """TASK-005: trigger_pipeline synchronous guard using WorkflowRunner.is_tenant_running()."""

    def test_trigger_pipeline_already_running_returns_200(self) -> None:
        """When is_tenant_running('t') is True, POST returns 200 with status='already_running' (TASK-005 test 8)."""
        mock_runner = MagicMock()
        mock_runner.is_tenant_running.return_value = True

        with _make_app_with_runner(mock_runner) as client:
            response = client.post("/api/v1/tenants/t/pipeline/run")

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "already_running"
