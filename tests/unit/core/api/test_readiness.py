from __future__ import annotations

from datetime import UTC, date, datetime
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient

from core.api.routes.readiness import _check_tenant_readiness
from core.config.models import AppSettings, StorageConfig, TenantConfig

if TYPE_CHECKING:
    from core.api.schemas import TenantReadiness


def _make_app_settings_with_tenant() -> AppSettings:
    return AppSettings(
        tenants={
            "acme": TenantConfig(
                tenant_id="t-001",
                ecosystem="ccloud",
                storage=StorageConfig(connection_string="sqlite:///:memory:"),
            )
        }
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_pipeline_run(
    status: str = "running",
    stage: str | None = "gathering",
    current_date: date | None = None,
) -> MagicMock:
    run = MagicMock()
    run.status = status
    run.stage = stage
    run.current_date = current_date
    run.started_at = datetime.now(UTC)
    run.ended_at = None if status == "running" else datetime.now(UTC)
    return run


def _make_backend(latest_run: MagicMock | None = None, count: int = 0) -> MagicMock:
    mock_uow = MagicMock()
    mock_uow.pipeline_runs.get_latest_run.return_value = latest_run
    mock_uow.pipeline_state.count_calculated.return_value = count
    mock_uow.__enter__ = MagicMock(return_value=mock_uow)
    mock_uow.__exit__ = MagicMock(return_value=False)
    mock_backend = MagicMock()
    mock_backend.create_unit_of_work.return_value = mock_uow
    mock_backend.create_read_only_unit_of_work.return_value = mock_uow
    return mock_backend


def _make_storage_config() -> StorageConfig:
    return StorageConfig(connection_string="sqlite:///:memory:")


def _call_check(
    *,
    latest_run: MagicMock | None = None,
    count: int = 0,
    workflow_runner: MagicMock | None = None,
    failed_tenants: dict[str, str] | None = None,
    tenant_name: str = "t",
    topic_attribution_enabled: bool = False,
) -> TenantReadiness:
    backend = _make_backend(latest_run=latest_run, count=count)
    with patch("core.api.routes.readiness.get_or_create_backend", return_value=backend):
        return _check_tenant_readiness(
            tenant_name=tenant_name,
            ecosystem="eco",
            tenant_id="tid",
            storage_config=_make_storage_config(),
            backends={},
            workflow_runner=workflow_runner,
            failed_tenants=failed_tenants or {},
            topic_attribution_enabled=topic_attribution_enabled,
        )


# ---------------------------------------------------------------------------
# Test 1: API-only + orphaned DB record
# ---------------------------------------------------------------------------


class TestApiOnlyOrphanedRun:
    def test_workflow_runner_none_db_running_returns_not_running(self) -> None:
        """workflow_runner=None and DB status='running' must return pipeline_running=False,
        last_run_status='failed' (orphaned — no runner to confirm it)."""
        run = _make_pipeline_run(status="running")
        result = _call_check(latest_run=run, workflow_runner=None)

        assert result.pipeline_running is False
        assert result.last_run_status == "failed"


# ---------------------------------------------------------------------------
# Test 3: Both mode + actually running
# ---------------------------------------------------------------------------


class TestBothModeActuallyRunning:
    def test_both_mode_running_returns_pipeline_running_true_with_stage(self) -> None:
        """workflow_runner.is_tenant_running=True, DB status='running' → pipeline_running=True
        and pipeline_stage is populated."""
        run = _make_pipeline_run(status="running", stage="gathering")
        mock_runner = MagicMock()
        mock_runner.is_tenant_running.return_value = True

        result = _call_check(latest_run=run, workflow_runner=mock_runner)

        assert result.pipeline_running is True
        assert result.pipeline_stage == "gathering"


# ---------------------------------------------------------------------------
# Test 4: Both mode + orphaned record
# ---------------------------------------------------------------------------


class TestBothModeOrphanedRun:
    def test_both_mode_runner_disagrees_db_running_returns_not_running(self) -> None:
        """workflow_runner.is_tenant_running=False, DB status='running' → orphaned run.
        Must return pipeline_running=False, last_run_status='failed'."""
        run = _make_pipeline_run(status="running")
        mock_runner = MagicMock()
        mock_runner.is_tenant_running.return_value = False

        result = _call_check(latest_run=run, workflow_runner=mock_runner)

        assert result.pipeline_running is False
        assert result.last_run_status == "failed"


# ---------------------------------------------------------------------------
# Test 5: Both mode + no DB run yet
# ---------------------------------------------------------------------------


class TestBothModeNoDbRunYet:
    def test_both_mode_no_db_record_runner_active_returns_running(self) -> None:
        """No DB record but workflow_runner.is_tenant_running=True → pipeline_running=True.
        The secondary check (lines 71-73) covers this case."""
        mock_runner = MagicMock()
        mock_runner.is_tenant_running.return_value = True

        result = _call_check(latest_run=None, workflow_runner=mock_runner)

        assert result.pipeline_running is True


# ---------------------------------------------------------------------------
# Test 6: Dead code removed — app.state.pipeline_runs must not exist
# ---------------------------------------------------------------------------


class TestLifespanDeadCodeRemoved:
    def test_lifespan_does_not_set_pipeline_runs_on_app_state(self) -> None:
        """After lifespan startup, app.state must NOT have a 'pipeline_runs' attribute.
        It was dead code left from an older implementation."""
        from core.api.app import create_app

        settings = AppSettings(tenants={})
        app = create_app(settings)

        with TestClient(app) as client:
            client.get("/health")
            assert not hasattr(app.state, "pipeline_runs"), (
                "pipeline_runs is dead code and must be removed from lifespan"
            )


# ---------------------------------------------------------------------------
# GIT-001: HTTP integration — orphaned run reported correctly end-to-end
# ---------------------------------------------------------------------------


class TestReadinessHttpIntegration:
    def test_api_only_orphaned_run_via_http(self) -> None:
        """GIT-001: Full HTTP wiring test.

        GET /api/v1/readiness with workflow_runner=None and a DB record with
        status='running' must return pipeline_running=false, last_run_status='failed'
        in the JSON response.
        """
        from core.api.app import create_app

        settings = _make_app_settings_with_tenant()
        app = create_app(settings, workflow_runner=None, mode="api")

        run = _make_pipeline_run(status="running", stage="gathering")
        backend = _make_backend(latest_run=run, count=1)

        # Prevent lifespan cleanup from hitting real storage, and mock the
        # readiness backend so we control what the DB returns.
        with (
            patch("workflow_runner.cleanup_orphaned_runs_for_all_tenants"),
            patch("core.api.routes.readiness.get_or_create_backend", return_value=backend),
            TestClient(app) as client,
        ):
            response = client.get("/api/v1/readiness")

        assert response.status_code == 200
        body = response.json()

        assert body["mode"] == "api"
        assert len(body["tenants"]) == 1
        tenant = body["tenants"][0]
        assert tenant["tenant_name"] == "acme"
        assert tenant["pipeline_running"] is False
        assert tenant["last_run_status"] == "failed"
