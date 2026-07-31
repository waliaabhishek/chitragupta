from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, date, datetime
from pathlib import Path
from typing import TYPE_CHECKING
from unittest.mock import MagicMock

from fastapi.testclient import TestClient
from sqlalchemy import text

from core.api.app import create_app
from core.config.models import (
    AppSettings,
    FocusPreviewTenantConfig,
    StorageConfig,
    TenantConfig,
)
from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend
from plugins.confluent_cloud.storage.module import CCloudStorageModule
from tests.integration.core.api.backend_provider import FixedTenantBackendProvider
from workflow_runner import TenantRuntime, WorkflowRunner, _config_hash

if TYPE_CHECKING:
    import pytest

TABLE_NAME = "ccloud_focus_preview_retention_outcomes"


def _tenant(tmp_path: Path, name: str, *, tenant_id: str = "tenant-1") -> TenantConfig:
    return TenantConfig(
        ecosystem="confluent_cloud",
        tenant_id=tenant_id,
        storage=StorageConfig(
            connection_string=f"sqlite:///{tmp_path / f'{name}.db'}",
        ),
        focus_preview=FocusPreviewTenantConfig(
            commercial_profile="direct_payg",
            effective_start_date=date(2026, 1, 1),
            effective_end_date=date(2026, 12, 31),
        ),
    )


def _backend(tmp_path: Path, name: str) -> SQLModelBackend:
    backend = SQLModelBackend(
        f"sqlite:///{tmp_path / f'{name}.db'}",
        CCloudStorageModule(),
        use_migrations=False,
        focus_preview_enabled=True,
    )
    backend.create_tables()
    return backend


def _insert_outcome(
    backend: SQLModelBackend,
    *,
    tenant_id: str,
    cleanup_kind: str,
    status: str,
    attempted_at: str,
    diagnostic_code: str | None,
    diagnostic_message: str | None,
    diagnostic_error_type: str | None,
) -> None:
    with backend._engine.begin() as connection:  # noqa: SLF001
        connection.execute(
            text(
                f"""
                INSERT INTO {TABLE_NAME} (
                    ecosystem,
                    tenant_id,
                    cleanup_kind,
                    attempted_at,
                    status,
                    diagnostic_code,
                    diagnostic_message,
                    diagnostic_error_type
                ) VALUES (
                    'confluent_cloud',
                    :tenant_id,
                    :cleanup_kind,
                    :attempted_at,
                    :status,
                    :diagnostic_code,
                    :diagnostic_message,
                    :diagnostic_error_type
                )
                """
            ),
            {
                "tenant_id": tenant_id,
                "cleanup_kind": cleanup_kind,
                "attempted_at": attempted_at,
                "status": status,
                "diagnostic_code": diagnostic_code,
                "diagnostic_message": diagnostic_message,
                "diagnostic_error_type": diagnostic_error_type,
            },
        )


def test_readiness_http_projects_distinct_retention_outcomes_for_a_degraded_tenant(
    tmp_path: Path,
) -> None:
    tenant = _tenant(tmp_path, "production")
    backend = _backend(tmp_path, "production")
    app = create_app(
        AppSettings(tenants={"production": tenant}),
        workflow_runner=None,
        mode="api",
    )
    try:
        _insert_outcome(
            backend,
            tenant_id=tenant.tenant_id,
            cleanup_kind="ordinary",
            status="failure",
            attempted_at="2026-07-30 23:25:01+00:00",
            diagnostic_code="focus_preview_ordinary_retention_failed",
            diagnostic_message=(
                "Ordinary tenant retention cleanup failed. Review worker logs and "
                "restore tenant storage; existing valid Preview data remains available."
            ),
            diagnostic_error_type="OperationalError",
        )
        _insert_outcome(
            backend,
            tenant_id=tenant.tenant_id,
            cleanup_kind="preview_evidence",
            status="success",
            attempted_at="2026-07-30 23:40:01+00:00",
            diagnostic_code=None,
            diagnostic_message=None,
            diagnostic_error_type=None,
        )

        with TestClient(app) as client:
            import core.api.routes.readiness as readiness_module

            readiness_module._readiness_cache = None  # noqa: SLF001
            app.state.backend_provider = FixedTenantBackendProvider({"production": backend})
            app.state.preview_repair_runtime = None
            response = client.get("/api/v1/readiness")

        assert response.status_code == 200
        body = response.json()["tenants"][0]
        assert body["focus_preview_state"] == "degraded"
        assert body["focus_preview_ordinary_retention"] == {
            "attempted_at": "2026-07-30T23:25:01Z",
            "status": "failure",
            "diagnostic": {
                "code": "focus_preview_ordinary_retention_failed",
                "message": (
                    "Ordinary tenant retention cleanup failed. Review worker logs and "
                    "restore tenant storage; existing valid Preview data remains available."
                ),
                "error_type": "OperationalError",
            },
        }
        assert body["focus_preview_evidence_retention"] == {
            "attempted_at": "2026-07-30T23:40:01Z",
            "status": "success",
            "diagnostic": None,
        }
    finally:
        backend.dispose()


def test_failed_cleanup_is_persisted_by_workflow_and_reconstructed_by_readiness_http(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tenant = _tenant(tmp_path, "workflow")
    backend = _backend(tmp_path, "workflow")
    settings = AppSettings(tenants={"production": tenant})
    cleanup_now = datetime(2026, 7, 30, 23, 25, 1, tzinfo=UTC)
    cleanup_uow = MagicMock()
    cleanup_uow.billing.delete_before.side_effect = RuntimeError("controlled ordinary cleanup failure")

    @contextmanager
    def failing_cleanup_uow() -> Iterator[MagicMock]:
        yield cleanup_uow

    monkeypatch.setattr(backend, "create_unit_of_work", failing_cleanup_uow)
    runner = WorkflowRunner(settings, MagicMock())
    runner._tenant_runtimes["production"] = TenantRuntime(  # noqa: SLF001
        tenant_name="production",
        plugin=MagicMock(),
        storage=backend,
        orchestrator=MagicMock(),
        config_hash=_config_hash(tenant),
        created_at=cleanup_now,
    )
    app = create_app(settings, workflow_runner=None, mode="api")
    try:
        runner._cleanup_retention(now=cleanup_now)  # noqa: SLF001

        with TestClient(app) as client:
            import core.api.routes.readiness as readiness_module

            readiness_module._readiness_cache = None  # noqa: SLF001
            app.state.backend_provider = FixedTenantBackendProvider({"production": backend})
            app.state.preview_repair_runtime = None
            response = client.get("/api/v1/readiness")

        assert response.status_code == 200
        body = response.json()["tenants"][0]
        assert body["focus_preview_state"] == "degraded"
        assert body["focus_preview_ordinary_retention"] == {
            "attempted_at": "2026-07-30T23:25:01Z",
            "status": "failure",
            "diagnostic": {
                "code": "focus_preview_ordinary_retention_failed",
                "message": (
                    "Ordinary tenant retention cleanup failed. Review worker logs and "
                    "restore tenant storage; existing valid Preview data remains available."
                ),
                "error_type": "RuntimeError",
            },
        }
        assert body["focus_preview_evidence_retention"] is None
    finally:
        backend.dispose()


def test_readiness_reconstructs_latest_retention_outcomes_after_restart(
    tmp_path: Path,
) -> None:
    tenant = _tenant(tmp_path, "restart")
    first = _backend(tmp_path, "restart")
    try:
        _insert_outcome(
            first,
            tenant_id=tenant.tenant_id,
            cleanup_kind="preview_evidence",
            status="failure",
            attempted_at="2026-07-30 23:55:01+00:00",
            diagnostic_code="focus_preview_evidence_retention_failed",
            diagnostic_message=(
                "FOCUS Preview evidence retention cleanup failed. Review worker logs and "
                "restore Preview evidence storage; existing valid Preview data remains available."
            ),
            diagnostic_error_type="OSError",
        )
    finally:
        first.dispose()

    reopened = _backend(tmp_path, "restart")
    app = create_app(
        AppSettings(tenants={"restart": tenant}),
        workflow_runner=None,
        mode="api",
    )
    try:
        with TestClient(app) as client:
            import core.api.routes.readiness as readiness_module

            readiness_module._readiness_cache = None  # noqa: SLF001
            app.state.backend_provider = FixedTenantBackendProvider({"restart": reopened})
            app.state.preview_repair_runtime = None
            response = client.get("/api/v1/readiness")

        assert response.status_code == 200
        body = response.json()["tenants"][0]
        assert body["focus_preview_state"] == "degraded"
        assert body["focus_preview_ordinary_retention"] is None
        assert body["focus_preview_evidence_retention"]["status"] == "failure"
        assert (
            body["focus_preview_evidence_retention"]["diagnostic"]["code"] == "focus_preview_evidence_retention_failed"
        )
    finally:
        reopened.dispose()


def test_unavailable_precedence_is_tenant_scoped_even_when_another_tenant_has_persisted_retention_failures(
    tmp_path: Path,
) -> None:
    unavailable_tenant = _tenant(tmp_path, "unavailable", tenant_id="shared")
    degraded_tenant = _tenant(tmp_path, "degraded", tenant_id="shared")
    unavailable_backend = _backend(tmp_path, "unavailable")
    degraded_backend = _backend(tmp_path, "degraded")
    degraded_backend.mark_preview_evidence_bootstrap_unavailable("ControlledUnavailable")
    app = create_app(
        AppSettings(
            tenants={
                "unavailable": unavailable_tenant,
                "degraded": degraded_tenant,
            }
        ),
        workflow_runner=None,
        mode="api",
    )
    try:
        _insert_outcome(
            unavailable_backend,
            tenant_id="shared",
            cleanup_kind="ordinary",
            status="failure",
            attempted_at="2026-07-30 23:25:01+00:00",
            diagnostic_code="focus_preview_ordinary_retention_failed",
            diagnostic_message=(
                "Ordinary tenant retention cleanup failed. Review worker logs and "
                "restore tenant storage; existing valid Preview data remains available."
            ),
            diagnostic_error_type="OperationalError",
        )

        with TestClient(app) as client:
            import core.api.routes.readiness as readiness_module

            readiness_module._readiness_cache = None  # noqa: SLF001
            app.state.backend_provider = FixedTenantBackendProvider(
                {
                    "unavailable": unavailable_backend,
                    "degraded": degraded_backend,
                }
            )
            app.state.preview_repair_runtime = None
            response = client.get("/api/v1/readiness")

        assert response.status_code == 200
        by_name = {tenant_state["tenant_name"]: tenant_state for tenant_state in response.json()["tenants"]}
        assert by_name["unavailable"]["focus_preview_state"] == "degraded"
        assert by_name["unavailable"]["focus_preview_ordinary_retention"]["status"] == "failure"
        assert by_name["degraded"]["focus_preview_state"] == "unavailable"
        assert by_name["degraded"]["focus_preview_ordinary_retention"] is None
        assert by_name["degraded"]["focus_preview_evidence_retention"] is None
    finally:
        unavailable_backend.dispose()
        degraded_backend.dispose()
