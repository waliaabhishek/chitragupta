"""Unit tests for the _run_pipeline background task and trigger_pipeline endpoint."""

from __future__ import annotations

import asyncio
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from core.api.routes.pipeline import _run_pipeline
from core.storage.backends.sqlmodel.module import CoreStorageModule
from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend

if TYPE_CHECKING:
    from collections.abc import Iterator


@pytest.fixture
def temp_backend() -> Iterator[SQLModelBackend]:
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        path = f.name
    conn = f"sqlite:///{path}"
    backend = SQLModelBackend(conn, CoreStorageModule(), use_migrations=False)
    backend.create_tables()
    yield backend
    backend.dispose()
    Path(path).unlink(missing_ok=True)


def _make_request(workflow_runner: object = None) -> MagicMock:
    request = MagicMock()
    request.app.state.workflow_runner = workflow_runner
    return request


class TestRunPipelineBackground:
    def test_workflow_runner_result_persisted_to_db(self, temp_backend: SQLModelBackend) -> None:
        """WorkflowRunner result fields are written to the DB record on completion (CT-003)."""
        with temp_backend.create_unit_of_work() as uow:
            run = uow.pipeline_runs.create_run("my-tenant", datetime(2026, 2, 26, 10, 0, tzinfo=UTC))
            uow.commit()
        run_id = run.id
        assert run_id is not None

        mock_result = MagicMock()
        mock_result.already_running = False
        mock_result.dates_gathered = 7
        mock_result.dates_calculated = 5
        mock_result.chargeback_rows_written = 200
        mock_result.errors = []

        mock_runner = MagicMock()
        mock_runner.run_tenant.return_value = mock_result

        asyncio.run(_run_pipeline("my-tenant", MagicMock(), run_id, temp_backend, _make_request(mock_runner)))

        with temp_backend.create_unit_of_work() as uow:
            updated = uow.pipeline_runs.get_run(run_id)

        assert updated is not None
        assert updated.status == "completed"
        assert updated.dates_gathered == 7
        assert updated.dates_calculated == 5
        assert updated.rows_written == 200
        assert updated.ended_at is not None
        assert updated.error_message is None

    def test_workflow_runner_errors_persisted(self, temp_backend: SQLModelBackend) -> None:
        """First error from result.errors is stored as error_message."""
        with temp_backend.create_unit_of_work() as uow:
            run = uow.pipeline_runs.create_run("my-tenant", datetime(2026, 2, 26, 10, 0, tzinfo=UTC))
            uow.commit()
        run_id = run.id
        assert run_id is not None

        mock_result = MagicMock()
        mock_result.already_running = False
        mock_result.dates_gathered = 0
        mock_result.dates_calculated = 0
        mock_result.chargeback_rows_written = 0
        mock_result.errors = ["billing API timeout"]

        mock_runner = MagicMock()
        mock_runner.run_tenant.return_value = mock_result

        asyncio.run(_run_pipeline("my-tenant", MagicMock(), run_id, temp_backend, _make_request(mock_runner)))

        with temp_backend.create_unit_of_work() as uow:
            updated = uow.pipeline_runs.get_run(run_id)

        assert updated is not None
        assert updated.status == "completed"
        assert updated.error_message == "billing API timeout"

    def test_exception_sets_failed_status(self, temp_backend: SQLModelBackend) -> None:
        """When workflow_runner raises, DB record gets status='failed' (CT-002)."""
        with temp_backend.create_unit_of_work() as uow:
            run = uow.pipeline_runs.create_run("my-tenant", datetime(2026, 2, 26, 10, 0, tzinfo=UTC))
            uow.commit()
        run_id = run.id
        assert run_id is not None

        mock_runner = MagicMock()
        mock_runner.run_tenant.side_effect = RuntimeError("connection refused")

        asyncio.run(_run_pipeline("my-tenant", MagicMock(), run_id, temp_backend, _make_request(mock_runner)))

        with temp_backend.create_unit_of_work() as uow:
            updated = uow.pipeline_runs.get_run(run_id)

        assert updated is not None
        assert updated.status == "failed"
        assert updated.error_message == "Pipeline execution failed"
        assert updated.ended_at is not None

    def test_no_workflow_runner_completes_with_error_message(self, temp_backend: SQLModelBackend) -> None:
        """API-only mode (no WorkflowRunner) sets status='completed' with stub error."""
        with temp_backend.create_unit_of_work() as uow:
            run = uow.pipeline_runs.create_run("my-tenant", datetime(2026, 2, 26, 10, 0, tzinfo=UTC))
            uow.commit()
        run_id = run.id
        assert run_id is not None

        asyncio.run(_run_pipeline("my-tenant", MagicMock(), run_id, temp_backend, _make_request(None)))

        with temp_backend.create_unit_of_work() as uow:
            updated = uow.pipeline_runs.get_run(run_id)

        assert updated is not None
        assert updated.status == "completed"
        assert updated.error_message is not None
        assert "WorkflowRunner" in updated.error_message

    def test_run_pipeline_skips_when_already_running(self, temp_backend: SQLModelBackend) -> None:
        """When run_tenant() returns already_running=True, DB status is 'skipped' (TASK-005 test 9)."""
        from core.engine.orchestrator import PipelineRunResult

        with temp_backend.create_unit_of_work() as uow:
            run = uow.pipeline_runs.create_run("my-tenant", datetime(2026, 2, 26, 10, 0, tzinfo=UTC))
            uow.commit()
        run_id = run.id
        assert run_id is not None

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

        asyncio.run(_run_pipeline("my-tenant", MagicMock(), run_id, temp_backend, _make_request(mock_runner)))

        with temp_backend.create_unit_of_work() as uow:
            updated = uow.pipeline_runs.get_run(run_id)

        assert updated is not None
        assert updated.status == "skipped"


def _make_app_with_runner(runner: object) -> TestClient:
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
    return TestClient(app).__enter__()


class TestTriggerPipelineAlreadyRunning:
    """TASK-005: trigger_pipeline synchronous guard using WorkflowRunner.is_tenant_running()."""

    def test_trigger_pipeline_already_running_returns_200(self) -> None:
        """When is_tenant_running('t') is True, POST returns 200 with status='already_running' (TASK-005 test 8)."""
        mock_runner = MagicMock()
        mock_runner.is_tenant_running.return_value = True

        client = _make_app_with_runner(mock_runner)
        response = client.post("/api/v1/tenants/t/pipeline/run")

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "already_running"
