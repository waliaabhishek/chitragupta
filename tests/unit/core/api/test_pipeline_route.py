"""Unit tests for the _run_pipeline background task."""

from __future__ import annotations

import asyncio
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING
from unittest.mock import MagicMock

import pytest

from core.api.routes.pipeline import _run_pipeline
from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend

if TYPE_CHECKING:
    from collections.abc import Iterator


@pytest.fixture
def temp_backend() -> Iterator[SQLModelBackend]:
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        path = f.name
    conn = f"sqlite:///{path}"
    backend = SQLModelBackend(conn, use_migrations=False)
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
