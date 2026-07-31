from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from importlib import import_module
from pathlib import Path
from typing import TYPE_CHECKING
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text

from core.api.routes.readiness import _check_tenant_readiness
from core.api.topic_attribution_status import TopicAttributionStatus
from core.config.models import (
    AppSettings,
    FocusPreviewTenantConfig,
    StorageConfig,
    TenantConfig,
)
from core.models.pipeline import PipelineState
from core.preview.retention import (
    PreviewRetentionCleanupKind,
    PreviewRetentionOutcome,
    PreviewRetentionOutcomeStatus,
    retention_failure_diagnostic,
)
from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend
from plugins.confluent_cloud.storage.module import CCloudStorageModule
from tests.integration.core.api.backend_provider import FixedTenantBackendProvider, install_backend

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
    topic_attribution_status: TopicAttributionStatus | None = None,
) -> TenantReadiness:
    backend = _make_backend(latest_run=latest_run, count=count)
    tenant_config = TenantConfig(
        ecosystem="eco",
        tenant_id="tid",
        storage=_make_storage_config(),
    )
    return _check_tenant_readiness(
        tenant_name=tenant_name,
        tenant_config=tenant_config,
        backend_provider=FixedTenantBackendProvider({tenant_name: backend}),
        workflow_runner=workflow_runner,
        failed_tenants=failed_tenants or {},
        topic_attribution_status=topic_attribution_status or TopicAttributionStatus(status="disabled"),
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
        import core.api.routes.readiness as readiness_module
        from core.api.app import create_app

        readiness_module._readiness_cache = None  # ensure no stale TTL cache from other tests
        settings = _make_app_settings_with_tenant()
        app = create_app(settings, workflow_runner=None, mode="api")

        run = _make_pipeline_run(status="running", stage="gathering")
        backend = _make_backend(latest_run=run, count=1)

        with TestClient(app) as client:
            install_backend(app, "acme", backend)
            response = client.get("/api/v1/readiness")

        assert response.status_code == 200
        body = response.json()

        assert body["mode"] == "api"
        assert len(body["tenants"]) == 1
        tenant = body["tenants"][0]
        assert tenant["tenant_name"] == "acme"
        assert tenant["pipeline_running"] is False
        assert tenant["last_run_status"] == "failed"
        assert "topic_attribution_status" in tenant
        assert "topic_attribution_error" in tenant
        assert "topic_attribution_enabled" not in tenant


def _preview_tenant(
    tmp_path: Path,
    tenant_id: str = "tenant-1",
    *,
    storage_name: str | None = None,
) -> TenantConfig:
    return TenantConfig(
        ecosystem="confluent_cloud",
        tenant_id=tenant_id,
        storage=StorageConfig(
            connection_string=f"sqlite:///{tmp_path / f'{storage_name or tenant_id}.db'}",
        ),
        focus_preview=FocusPreviewTenantConfig(
            commercial_profile="direct_payg",
            effective_start_date=date(2026, 1, 1),
            effective_end_date=date(2026, 12, 31),
        ),
    )


def _preview_backend(tmp_path: Path, name: str) -> SQLModelBackend:
    backend = SQLModelBackend(
        f"sqlite:///{tmp_path / f'{name}.db'}",
        CCloudStorageModule(),
        use_migrations=False,
        focus_preview_enabled=True,
    )
    backend.create_tables()
    return backend


def _queued_repair(
    repair_id: str,
    *,
    tenant_name: str,
    tenant_id: str,
) -> object:
    repair = import_module("core.preview.repair")
    created_at = datetime(2026, 7, 24, tzinfo=UTC)
    dates = tuple(
        repair.PreviewRepairDate(
            repair_id=repair_id,
            tracking_date=date(2026, 7, 1) + timedelta(days=offset),
            status=repair.PreviewRepairDateStatus.QUEUED,
            started_at=None,
            completed_at=None,
            calculation_id=None,
            calculation_completed_at=None,
            rows_written=None,
            failure_stage=None,
            diagnostic=None,
        )
        for offset in range(3)
    )
    return repair.PreviewRepair(
        repair_id=repair_id,
        tenant_name=tenant_name,
        ecosystem="confluent_cloud",
        tenant_id=tenant_id,
        start_date=date(2026, 7, 1),
        end_date=date(2026, 7, 4),
        status=repair.PreviewRepairStatus.QUEUED,
        created_at=created_at,
        started_at=None,
        completed_at=None,
        diagnostic=None,
        dates=dates,
    )


def _create_repair(backend: SQLModelBackend, repair: object) -> None:
    with backend.create_preview_evidence_unit_of_work() as uow:
        uow.repairs.create_queued(repair)
        uow.commit()


def _write_retention_outcome(
    backend: SQLModelBackend,
    tenant: TenantConfig,
    *,
    kind: PreviewRetentionCleanupKind,
    status: PreviewRetentionOutcomeStatus,
    attempted_at: datetime,
) -> None:
    error = RuntimeError(f"{kind.value} cleanup failed")
    with backend.create_preview_evidence_unit_of_work() as uow:
        uow.retention_outcomes.upsert_latest(
            tenant.ecosystem,
            tenant.tenant_id,
            PreviewRetentionOutcome(
                owner=tenant.tenant_id,
                cleanup_kind=kind,
                attempted_at=attempted_at,
                status=status,
                diagnostic=(
                    retention_failure_diagnostic(kind, error)
                    if status is PreviewRetentionOutcomeStatus.FAILURE
                    else None
                ),
            ),
        )
        uow.commit()


def _set_repair_state(
    backend: SQLModelBackend,
    *,
    repair_id: str,
    parent_status: str,
    date_statuses: tuple[str, str, str],
) -> None:
    now = datetime(2026, 7, 24, tzinfo=UTC)
    with backend._engine.begin() as connection:  # noqa: SLF001
        connection.execute(
            text(
                """
                UPDATE ccloud_focus_preview_repairs
                SET status = :status,
                    started_at = :now,
                    completed_at = CASE WHEN :status IN
                        ('completed', 'completed_with_failures', 'failed')
                        THEN :now ELSE NULL END,
                    diagnostic_code = CASE WHEN :status = 'failed' THEN 'failed' ELSE NULL END,
                    diagnostic_message = CASE WHEN :status = 'failed' THEN 'retry' ELSE NULL END,
                    diagnostic_retryable = CASE WHEN :status = 'failed' THEN 1 ELSE NULL END
                WHERE repair_id = :repair_id
                """
            ),
            {"status": parent_status, "now": now, "repair_id": repair_id},
        )
        rows = tuple(
            connection.execute(
                text(
                    """
                    SELECT tracking_date
                    FROM ccloud_focus_preview_repair_dates
                    WHERE repair_id = :repair_id
                    ORDER BY tracking_date
                    """
                ),
                {"repair_id": repair_id},
            ).scalars()
        )
        for tracking_date, status in zip(rows, date_statuses, strict=True):
            connection.execute(
                text(
                    """
                    UPDATE ccloud_focus_preview_repair_dates
                    SET status = :status
                    WHERE repair_id = :repair_id AND tracking_date = :tracking_date
                    """
                ),
                {
                    "status": status,
                    "repair_id": repair_id,
                    "tracking_date": tracking_date,
                },
            )


def test_tenant_readiness_schema_requires_five_state_preview_contract() -> None:
    from core.api.schemas import TenantReadiness

    schema = TenantReadiness.model_json_schema()

    assert schema["properties"]["focus_preview_state"]["enum"] == [
        "disabled",
        "ready",
        "upgrading",
        "degraded",
        "unavailable",
    ]
    required = set(schema["required"])
    assert {
        "focus_preview_state",
        "focus_preview_completed_repair_dates",
        "focus_preview_total_repair_dates",
        "focus_preview_message",
        "focus_preview_ordinary_retention",
        "focus_preview_evidence_retention",
    } <= required


def test_disabled_preview_exposes_null_structured_retention_outcomes() -> None:
    result = _call_check(
        latest_run=None,
        count=1,
        workflow_runner=None,
    )

    assert result.focus_preview_state == "disabled"
    assert result.focus_preview_ordinary_retention is None
    assert result.focus_preview_evidence_retention is None


def test_disabled_preview_does_not_probe_storage_or_recovery() -> None:
    from core.api.routes.readiness import _focus_preview_readiness

    tenant = TenantConfig(
        ecosystem="confluent_cloud",
        tenant_id="tenant-1",
        focus_preview=None,
    )

    state = _focus_preview_readiness(
        tenant_config=tenant,
        backend=object(),
        recovery_available=False,
    )
    assert (
        state.state,
        state.completed_repair_dates,
        state.total_repair_dates,
        state.message,
    ) == (
        "disabled",
        None,
        None,
        "FOCUS Mapping Preview is not enabled for this tenant.",
    )
    assert state.ordinary_retention is None
    assert state.evidence_retention is None


def test_enabled_preview_without_repair_history_is_ready(tmp_path: Path) -> None:
    from core.api.routes.readiness import _focus_preview_readiness

    backend = _preview_backend(tmp_path, "ready")
    try:
        state = _focus_preview_readiness(
            tenant_config=_preview_tenant(tmp_path),
            backend=backend,
            recovery_available=True,
        )
        assert (
            state.state,
            state.completed_repair_dates,
            state.total_repair_dates,
            state.message,
        ) == ("ready", None, None, None)
        assert state.ordinary_retention is None
        assert state.evidence_retention is None
    finally:
        backend.dispose()


def test_failed_recovery_and_storage_failure_are_feature_unavailable(
    tmp_path: Path,
) -> None:
    from core.api.routes.readiness import _focus_preview_readiness

    tenant = _preview_tenant(tmp_path)
    backend = _preview_backend(tmp_path, "recovery-failure")
    unavailable = (
        "unavailable",
        None,
        None,
        "FOCUS Mapping Preview storage is unavailable. Restore storage availability before retrying.",
    )
    try:
        _create_repair(
            backend,
            _queued_repair(
                "stale-active",
                tenant_name="production",
                tenant_id=tenant.tenant_id,
            ),
        )
        for candidate_backend, recovery_available in (
            (backend, False),
            (object(), True),
        ):
            state = _focus_preview_readiness(
                tenant_config=tenant,
                backend=candidate_backend,
                recovery_available=recovery_available,
            )
            assert (
                state.state,
                state.completed_repair_dates,
                state.total_repair_dates,
                state.message,
            ) == unavailable
            assert state.ordinary_retention is None
            assert state.evidence_retention is None
    finally:
        backend.dispose()


@pytest.mark.parametrize(
    ("parent_status", "date_statuses", "expected"),
    [
        (
            "queued",
            ("queued", "queued", "queued"),
            (
                "upgrading",
                0,
                3,
                "Historical repair is in progress; existing valid Preview data remains available.",
            ),
        ),
        (
            "running",
            ("succeeded", "failed", "daily_validated"),
            (
                "upgrading",
                2,
                3,
                "Historical repair is in progress; existing valid Preview data remains available.",
            ),
        ),
        ("completed", ("succeeded", "succeeded", "succeeded"), ("ready", 3, 3, None)),
        (
            "completed_with_failures",
            ("succeeded", "failed", "succeeded"),
            (
                "degraded",
                3,
                3,
                "Historical repair needs attention. Retry the failed dates with a new bounded repair; "
                "existing valid Preview data remains available.",
            ),
        ),
        (
            "failed",
            ("failed", "failed", "failed"),
            (
                "degraded",
                3,
                3,
                "Historical repair needs attention. Retry the failed dates with a new bounded repair; "
                "existing valid Preview data remains available.",
            ),
        ),
    ],
)
def test_preview_state_and_date_progress_come_from_durable_current_repair(
    tmp_path: Path,
    parent_status: str,
    date_statuses: tuple[str, ...],
    expected: tuple[str, int, int, str | None],
) -> None:
    from core.api.routes.readiness import _focus_preview_readiness

    tenant = _preview_tenant(tmp_path)
    backend = _preview_backend(tmp_path, f"state-{parent_status}")
    try:
        _create_repair(
            backend,
            _queued_repair(
                "repair-1",
                tenant_name="production",
                tenant_id=tenant.tenant_id,
            ),
        )
        with backend._engine.begin() as connection:
            connection.execute(
                text(
                    """
                    UPDATE ccloud_focus_preview_repairs
                    SET status = :status,
                        started_at = CASE WHEN :status = 'queued' THEN NULL ELSE :now END,
                        completed_at = CASE WHEN :status IN
                            ('completed', 'completed_with_failures', 'failed')
                            THEN :now ELSE NULL END,
                        diagnostic_code = CASE WHEN :status = 'failed' THEN 'failed' ELSE NULL END,
                        diagnostic_message = CASE WHEN :status = 'failed' THEN 'retry' ELSE NULL END,
                        diagnostic_retryable = CASE WHEN :status = 'failed' THEN 1 ELSE NULL END
                    WHERE repair_id = 'repair-1'
                    """
                ),
                {"status": parent_status, "now": datetime(2026, 7, 24, tzinfo=UTC)},
            )
            rows = tuple(
                connection.execute(
                    text(
                        """
                        SELECT tracking_date
                        FROM ccloud_focus_preview_repair_dates
                        WHERE repair_id = 'repair-1'
                        ORDER BY tracking_date
                        """
                    )
                ).scalars()
            )
            for tracking_date, status in zip(rows, date_statuses, strict=True):
                connection.execute(
                    text(
                        """
                        UPDATE ccloud_focus_preview_repair_dates
                        SET status = :status
                        WHERE repair_id = 'repair-1' AND tracking_date = :tracking_date
                        """
                    ),
                    {"status": status, "tracking_date": tracking_date},
                )

        state = _focus_preview_readiness(
            tenant_config=tenant,
            backend=backend,
            recovery_available=True,
        )
        assert (
            state.state,
            state.completed_repair_dates,
            state.total_repair_dates,
            state.message,
        ) == expected
        assert state.ordinary_retention is None
        assert state.evidence_retention is None
    finally:
        backend.dispose()


@pytest.mark.parametrize(
    ("repair_status", "date_statuses", "completed_dates"),
    [
        ("queued", ("queued", "queued", "queued"), 0),
        ("running", ("succeeded", "running", "queued"), 1),
    ],
)
def test_retention_failure_degrades_active_repair_without_losing_progress_or_cause(
    tmp_path: Path,
    repair_status: str,
    date_statuses: tuple[str, str, str],
    completed_dates: int,
) -> None:
    from core.api.routes.readiness import _focus_preview_readiness

    tenant = _preview_tenant(tmp_path)
    backend = _preview_backend(tmp_path, f"retention-{repair_status}")
    attempted_at = datetime(2026, 7, 30, 22, 1, tzinfo=UTC)
    try:
        _create_repair(
            backend,
            _queued_repair(
                "repair-active",
                tenant_name="production",
                tenant_id=tenant.tenant_id,
            ),
        )
        if repair_status == "running":
            _set_repair_state(
                backend,
                repair_id="repair-active",
                parent_status=repair_status,
                date_statuses=date_statuses,
            )
        _write_retention_outcome(
            backend,
            tenant,
            kind=PreviewRetentionCleanupKind.ORDINARY,
            status=PreviewRetentionOutcomeStatus.FAILURE,
            attempted_at=attempted_at,
        )

        state = _focus_preview_readiness(
            tenant_config=tenant,
            backend=backend,
            recovery_available=True,
        )

        assert state.state == "degraded"
        assert state.completed_repair_dates == completed_dates
        assert state.total_repair_dates == 3
        assert state.message == (
            "Retention cleanup needs attention while historical repair is in progress. "
            "Existing valid Preview data remains available."
        )
        assert state.ordinary_retention is not None
        assert state.ordinary_retention.cleanup_kind is PreviewRetentionCleanupKind.ORDINARY
        assert state.ordinary_retention.status is PreviewRetentionOutcomeStatus.FAILURE
        assert state.ordinary_retention.attempted_at == attempted_at
        assert state.ordinary_retention.diagnostic == retention_failure_diagnostic(
            PreviewRetentionCleanupKind.ORDINARY,
            RuntimeError("ordinary cleanup failed"),
        )
        assert state.evidence_retention is None
    finally:
        backend.dispose()


def test_retention_recovery_clears_only_matching_cause_and_preserves_failed_repair(
    tmp_path: Path,
) -> None:
    from core.api.routes.readiness import _focus_preview_readiness

    tenant = _preview_tenant(tmp_path)
    backend = _preview_backend(tmp_path, "retention-and-repair-failures")
    ordinary_failure_at = datetime(2026, 7, 30, 22, 1, tzinfo=UTC)
    evidence_failure_at = datetime(2026, 7, 30, 22, 2, tzinfo=UTC)
    try:
        _create_repair(
            backend,
            _queued_repair(
                "repair-failed",
                tenant_name="production",
                tenant_id=tenant.tenant_id,
            ),
        )
        _set_repair_state(
            backend,
            repair_id="repair-failed",
            parent_status="completed_with_failures",
            date_statuses=("succeeded", "failed", "succeeded"),
        )
        _write_retention_outcome(
            backend,
            tenant,
            kind=PreviewRetentionCleanupKind.ORDINARY,
            status=PreviewRetentionOutcomeStatus.FAILURE,
            attempted_at=ordinary_failure_at,
        )
        _write_retention_outcome(
            backend,
            tenant,
            kind=PreviewRetentionCleanupKind.PREVIEW_EVIDENCE,
            status=PreviewRetentionOutcomeStatus.FAILURE,
            attempted_at=evidence_failure_at,
        )

        combined = _focus_preview_readiness(
            tenant_config=tenant,
            backend=backend,
            recovery_available=True,
        )
        assert (
            combined.state,
            combined.completed_repair_dates,
            combined.total_repair_dates,
            combined.message,
        ) == (
            "degraded",
            3,
            3,
            "Historical repair and retention cleanup need attention. Retry failed repair dates after "
            "retention cleanup succeeds; existing valid Preview data remains available.",
        )
        assert combined.ordinary_retention is not None
        assert combined.ordinary_retention.status is PreviewRetentionOutcomeStatus.FAILURE
        assert combined.evidence_retention is not None
        assert combined.evidence_retention.status is PreviewRetentionOutcomeStatus.FAILURE

        ordinary_success_at = datetime(2026, 7, 30, 22, 3, tzinfo=UTC)
        _write_retention_outcome(
            backend,
            tenant,
            kind=PreviewRetentionCleanupKind.ORDINARY,
            status=PreviewRetentionOutcomeStatus.SUCCESS,
            attempted_at=ordinary_success_at,
        )
        evidence_still_failed = _focus_preview_readiness(
            tenant_config=tenant,
            backend=backend,
            recovery_available=True,
        )
        assert (
            evidence_still_failed.state,
            evidence_still_failed.completed_repair_dates,
            evidence_still_failed.total_repair_dates,
        ) == ("degraded", 3, 3)
        assert evidence_still_failed.ordinary_retention is not None
        assert evidence_still_failed.ordinary_retention.status is PreviewRetentionOutcomeStatus.SUCCESS
        assert evidence_still_failed.ordinary_retention.attempted_at == ordinary_success_at
        assert evidence_still_failed.ordinary_retention.diagnostic is None
        assert evidence_still_failed.evidence_retention == combined.evidence_retention

        evidence_success_at = datetime(2026, 7, 30, 22, 4, tzinfo=UTC)
        _write_retention_outcome(
            backend,
            tenant,
            kind=PreviewRetentionCleanupKind.PREVIEW_EVIDENCE,
            status=PreviewRetentionOutcomeStatus.SUCCESS,
            attempted_at=evidence_success_at,
        )
        repair_still_failed = _focus_preview_readiness(
            tenant_config=tenant,
            backend=backend,
            recovery_available=True,
        )
        assert (
            repair_still_failed.state,
            repair_still_failed.completed_repair_dates,
            repair_still_failed.total_repair_dates,
            repair_still_failed.message,
        ) == (
            "degraded",
            3,
            3,
            "Historical repair needs attention. Retry the failed dates with a new bounded repair; "
            "existing valid Preview data remains available.",
        )
        assert repair_still_failed.ordinary_retention == evidence_still_failed.ordinary_retention
        assert repair_still_failed.evidence_retention is not None
        assert repair_still_failed.evidence_retention.status is PreviewRetentionOutcomeStatus.SUCCESS
        assert repair_still_failed.evidence_retention.attempted_at == evidence_success_at
        assert repair_still_failed.evidence_retention.diagnostic is None
    finally:
        backend.dispose()


def test_unresolved_history_is_unavailable(tmp_path: Path) -> None:
    from core.api.routes.readiness import _focus_preview_readiness

    tenant = _preview_tenant(tmp_path)
    backend = _preview_backend(tmp_path, "unresolved")
    try:
        _create_repair(
            backend,
            _queued_repair(
                "repair-1",
                tenant_name="production",
                tenant_id=tenant.tenant_id,
            ),
        )
        with backend._engine.begin() as connection:
            connection.execute(
                text(
                    """
                    UPDATE ccloud_focus_preview_repair_heads
                    SET repair_id = NULL
                    WHERE ecosystem = 'confluent_cloud' AND tenant_id = :tenant_id
                    """
                ),
                {"tenant_id": tenant.tenant_id},
            )

        state = _focus_preview_readiness(
            tenant_config=tenant,
            backend=backend,
            recovery_available=True,
        )
        assert state.state == "unavailable"
        assert state.completed_repair_dates is None
        assert state.total_repair_dates is None
        assert state.ordinary_retention is None
        assert state.evidence_retention is None
    finally:
        backend.dispose()


def test_feature_repair_state_does_not_change_top_level_or_pipeline_fields(
    tmp_path: Path,
) -> None:
    import core.api.routes.readiness as readiness_module
    from core.api.app import create_app

    readiness_module._readiness_cache = None
    tenant = _preview_tenant(tmp_path)
    backend = _preview_backend(tmp_path, "top-level-ready")
    _create_repair(
        backend,
        _queued_repair(
            "repair-1",
            tenant_name="production",
            tenant_id=tenant.tenant_id,
        ),
    )
    app = create_app(
        AppSettings(tenants={"production": tenant}),
        workflow_runner=None,
        mode="api",
    )
    try:
        with backend.create_unit_of_work() as uow:
            uow.pipeline_state.upsert(
                PipelineState(
                    ecosystem=tenant.ecosystem,
                    tenant_id=tenant.tenant_id,
                    tracking_date=date(2026, 7, 1),
                    billing_gathered=True,
                    resources_gathered=True,
                    chargeback_calculated=True,
                    calculation_id="calculation-1",
                    calculation_completed_at=datetime(2026, 7, 2, tzinfo=UTC),
                )
            )
            uow.commit()
        with TestClient(app) as client:
            install_backend(app, "production", backend)
            response = client.get("/api/v1/readiness")

        assert response.status_code == 200
        payload = response.json()
        assert payload["status"] == "ready"
        result = payload["tenants"][0]
        assert result["focus_preview_state"] == "upgrading"
        assert result["focus_preview_completed_repair_dates"] == 0
        assert result["focus_preview_total_repair_dates"] == 3
        assert result["pipeline_running"] is False
        assert result["tables_ready"] is True
        assert result["topic_attribution_status"] == "disabled"
    finally:
        backend.dispose()


@pytest.mark.parametrize("failure_mode", ["storage_unavailable", "read_failure"])
def test_enabled_preview_storage_failure_is_feature_scoped(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure_mode: str,
) -> None:
    tenant = _preview_tenant(tmp_path)
    backend = _preview_backend(tmp_path, f"feature-scoped-{failure_mode}")
    try:
        with backend.create_unit_of_work() as uow:
            uow.pipeline_state.upsert(
                PipelineState(
                    ecosystem=tenant.ecosystem,
                    tenant_id=tenant.tenant_id,
                    tracking_date=date(2026, 7, 1),
                    billing_gathered=True,
                    resources_gathered=True,
                    chargeback_calculated=True,
                    calculation_id="calculation-1",
                    calculation_completed_at=datetime(2026, 7, 2, tzinfo=UTC),
                )
            )
            uow.commit()
        if failure_mode == "storage_unavailable":
            backend.mark_preview_evidence_bootstrap_unavailable("ControlledUnavailable")
        else:

            def fail_preview_read() -> object:
                raise RuntimeError("controlled Preview read failure")

            monkeypatch.setattr(
                backend,
                "create_preview_generation_read_unit_of_work",
                fail_preview_read,
            )

        result = _check_tenant_readiness(
            tenant_name="production",
            tenant_config=tenant,
            backend_provider=FixedTenantBackendProvider({"production": backend}),
            workflow_runner=None,
            failed_tenants={},
            topic_attribution_status=TopicAttributionStatus(status="disabled"),
        )

        assert result.focus_preview_state == "unavailable"
        assert result.focus_preview_completed_repair_dates is None
        assert result.focus_preview_total_repair_dates is None
        assert result.tables_ready is True
        assert result.has_data is True
        assert result.pipeline_running is False
        assert result.topic_attribution_status == "disabled"
    finally:
        backend.dispose()


def test_same_owner_values_on_distinct_backends_remain_tenant_name_isolated(
    tmp_path: Path,
) -> None:
    import core.api.routes.readiness as readiness_module
    from core.api.app import create_app

    readiness_module._readiness_cache = None
    first_tenant = _preview_tenant(
        tmp_path,
        tenant_id="shared",
        storage_name="first-config",
    )
    second_tenant = _preview_tenant(
        tmp_path,
        tenant_id="shared",
        storage_name="second-config",
    )
    first = _preview_backend(tmp_path, "first-backend")
    second = _preview_backend(tmp_path, "second-backend")
    _create_repair(
        first,
        _queued_repair(
            "first-repair",
            tenant_name="first",
            tenant_id="shared",
        ),
    )
    app = create_app(
        AppSettings(tenants={"first": first_tenant, "second": second_tenant}),
        mode="api",
    )
    try:
        with TestClient(app) as client:
            app.state.backend_provider = FixedTenantBackendProvider({"first": first, "second": second})
            response = client.get("/api/v1/readiness")

        by_name = {tenant["tenant_name"]: tenant for tenant in response.json()["tenants"]}
        assert by_name["first"]["focus_preview_state"] == "upgrading"
        assert by_name["second"]["focus_preview_state"] == "ready"
    finally:
        first.dispose()
        second.dispose()
