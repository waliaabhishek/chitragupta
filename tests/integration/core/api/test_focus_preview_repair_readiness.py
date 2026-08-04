from __future__ import annotations

import threading
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text

from core.api.app import create_app
from core.config.models import (
    AppSettings,
    FocusPreviewTenantConfig,
    PreviewConfig,
    StorageConfig,
    TenantConfig,
)
from core.preview.models import PreviewDiagnostic
from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend
from plugins.confluent_cloud.storage.module import CCloudStorageModule

_REPAIR_ADMISSION_AT = datetime(2026, 7, 24, tzinfo=UTC)


class _RepairAdmissionDatetime(datetime):
    @classmethod
    def now(cls, tz: object = None) -> datetime:
        del tz
        return _REPAIR_ADMISSION_AT


@pytest.fixture(autouse=True)
def _pin_repair_route_admission(monkeypatch: pytest.MonkeyPatch) -> None:
    import core.api.routes.focus_preview as focus_preview_route

    monkeypatch.setattr(focus_preview_route, "datetime", _RepairAdmissionDatetime)


def _tenant(tmp_path: Path, name: str, *, tenant_id: str | None = None) -> TenantConfig:
    return TenantConfig(
        ecosystem="confluent_cloud",
        tenant_id=tenant_id or f"tenant-{name}",
        lookback_days=200,
        cutoff_days=5,
        retention_days=250,
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


class _ToggleEvidenceBackend:
    def __init__(self, backend: SQLModelBackend, *, fail_writes: bool = False) -> None:
        self.backend = backend
        self.fail_writes = fail_writes

    @property
    def preview_evidence_availability(self) -> object:
        return self.backend.preview_evidence_availability

    def create_preview_evidence_unit_of_work(self) -> object:
        if self.fail_writes:
            raise RuntimeError("controlled recovery failure")
        return self.backend.create_preview_evidence_unit_of_work()

    def create_preview_generation_read_unit_of_work(self) -> object:
        return self.backend.create_preview_generation_read_unit_of_work()

    def create_preview_evidence_bootstrap(self) -> object:
        return self.backend.create_preview_evidence_bootstrap()

    def mark_preview_evidence_bootstrap_unavailable(self, error_type: str) -> None:
        self.backend.mark_preview_evidence_bootstrap_unavailable(error_type)

    def create_read_only_unit_of_work(self) -> object:
        return self.backend.create_read_only_unit_of_work()

    def create_unit_of_work(self) -> object:
        return self.backend.create_unit_of_work()


class _ProductionRunner:
    def __init__(self, backends: dict[str, Any]) -> None:
        self.backends = backends
        self.preview_generation_scheduler = None
        self.started: list[str] = []
        self.started_event = threading.Event()
        self.release = threading.Event()
        self.worker_calls = 0
        self.busy_tenants: set[str] = set()

    @contextmanager
    def acquire_backend(
        self,
        tenant_name: str,
        tenant_config: TenantConfig,
    ) -> Iterator[Any]:
        del tenant_config
        yield self.backends[tenant_name]

    def close(self) -> None:
        return None

    def is_tenant_running(self, tenant_name: str) -> bool:
        return tenant_name in self.busy_tenants

    def get_failed_tenants(self) -> dict[str, str]:
        return {}

    def run_focus_preview_repair(
        self,
        repair_id: str,
        tenant_name: str,
        tenant_config: TenantConfig,
    ) -> None:
        self.worker_calls += 1
        self.started.append(tenant_name)
        self.started_event.set()
        if not self.release.wait(timeout=5):
            raise TimeoutError("test did not release production repair")
        backend = self.backends[tenant_name]
        with backend.create_preview_evidence_unit_of_work() as uow:
            started = datetime(2026, 7, 24, 0, 0, 1, tzinfo=UTC)
            completed = datetime(2026, 7, 24, 0, 0, 2, tzinfo=UTC)
            current = uow.repairs.get_for_owner(
                repair_id,
                tenant_config.ecosystem,
                tenant_config.tenant_id,
            )
            assert current is not None
            uow.repairs.mark_running(repair_id, started_at=started)
            for item in current.dates:
                uow.repairs.mark_date_running(
                    repair_id,
                    item.tracking_date,
                    started_at=started,
                )
                uow.repairs.mark_date_succeeded_from_running(
                    repair_id,
                    item.tracking_date,
                    completed_at=completed,
                    calculation_id=f"calculation-{item.tracking_date.isoformat()}",
                    calculation_completed_at=started,
                    rows_written=1,
                )
            uow.repairs.finalize_completed(repair_id, completed_at=completed)
            uow.commit()

    def drain(self, timeout: float) -> None:
        del timeout
        self.release.set()


def _settings(
    tmp_path: Path,
    tenants: dict[str, TenantConfig],
    *,
    workers: int = 1,
    waiting: int = 1,
) -> AppSettings:
    return AppSettings(
        preview=PreviewConfig(
            artifact_root=tmp_path / "artifacts",
            max_workers=workers,
            max_queued_repairs=waiting,
        ),
        tenants=tenants,
    )


def _persist_queued(
    backend: SQLModelBackend,
    tenant_name: str,
    tenant: TenantConfig,
    repair_id: str,
) -> None:
    from core.preview import repair

    operation = repair.PreviewRepair(
        repair_id=repair_id,
        tenant_name=tenant_name,
        ecosystem=tenant.ecosystem,
        tenant_id=tenant.tenant_id,
        start_date=date(2026, 7, 1),
        end_date=date(2026, 7, 2),
        status=repair.PreviewRepairStatus.QUEUED,
        created_at=datetime(2026, 7, 24, tzinfo=UTC),
        started_at=None,
        completed_at=None,
        diagnostic=None,
        dates=(
            repair.PreviewRepairDate(
                repair_id=repair_id,
                tracking_date=date(2026, 7, 1),
                status=repair.PreviewRepairDateStatus.QUEUED,
                started_at=None,
                completed_at=None,
                calculation_id=None,
                calculation_completed_at=None,
                rows_written=None,
                failure_stage=None,
                diagnostic=None,
            ),
        ),
    )
    with backend.create_preview_evidence_unit_of_work() as uow:
        uow.repairs.create_queued(operation)
        uow.commit()


def test_production_post_drives_readiness_from_upgrading_to_ready(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import core.api.routes.readiness as readiness_module

    monkeypatch.setattr(readiness_module, "_READINESS_CACHE_TTL", 0)  # type: ignore[attr-defined]
    tenant = _tenant(tmp_path, "production")
    backend = _backend(tmp_path, "production")
    runner = _ProductionRunner({"production": backend})
    app = create_app(
        _settings(tmp_path, {"production": tenant}),
        workflow_runner=runner,  # type: ignore[arg-type]
        mode="both",
    )
    try:
        with TestClient(app) as client:
            submitted = client.post(
                "/api/v1/tenants/production/focus-preview/repairs",
                json={"start_date": "2026-07-01", "end_date": "2026-07-02"},
            )
            assert submitted.status_code == 202
            assert runner.started_event.wait(timeout=2)

            active = client.get("/api/v1/readiness").json()["tenants"][0]
            runner.release.set()
            app.state.preview_repair_runtime.close(wait=True)
            assert active["focus_preview_state"] == "upgrading"
            assert active["focus_preview_completed_repair_dates"] == 0
            assert active["focus_preview_total_repair_dates"] == 1

            terminal = client.get("/api/v1/readiness").json()["tenants"][0]
            assert terminal["focus_preview_state"] == "ready"
            assert terminal["focus_preview_completed_repair_dates"] == 1
            assert terminal["focus_preview_total_repair_dates"] == 1
    finally:
        runner.release.set()
        backend.dispose()


def test_restart_recovery_reports_interrupted_work_degraded_never_upgrading(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import core.api.routes.readiness as readiness_module

    monkeypatch.setattr(readiness_module, "_READINESS_CACHE_TTL", 0)  # type: ignore[attr-defined]
    tenant = _tenant(tmp_path, "production")
    backend = _backend(tmp_path, "production")
    _persist_queued(backend, "production", tenant, "interrupted")
    runner = _ProductionRunner({"production": backend})
    runner.release.set()
    app = create_app(
        _settings(tmp_path, {"production": tenant}),
        workflow_runner=runner,  # type: ignore[arg-type]
        mode="both",
    )
    try:
        with TestClient(app) as client:
            state = client.get("/api/v1/readiness").json()["tenants"][0]

        assert state["focus_preview_state"] == "degraded"
        assert state["focus_preview_completed_repair_dates"] == 1
        assert state["focus_preview_total_repair_dates"] == 1
        assert "Retry" in state["focus_preview_message"]
    finally:
        backend.dispose()


def test_failed_startup_recovery_is_tenant_scoped_and_post_retries_synchronously(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import core.api.routes.readiness as readiness_module

    monkeypatch.setattr(readiness_module, "_READINESS_CACHE_TTL", 0)  # type: ignore[attr-defined]
    bad_tenant = _tenant(tmp_path, "bad", tenant_id="shared-owner")
    good_tenant = _tenant(tmp_path, "good", tenant_id="shared-owner")
    bad_storage = _backend(tmp_path, "bad")
    good_storage = _backend(tmp_path, "good")
    _persist_queued(bad_storage, "bad", bad_tenant, "bad-interrupted")
    _persist_queued(good_storage, "good", good_tenant, "good-interrupted")
    bad_backend = _ToggleEvidenceBackend(bad_storage, fail_writes=True)
    good_backend = _ToggleEvidenceBackend(good_storage)
    runner = _ProductionRunner({"bad": bad_backend, "good": good_backend})
    app = create_app(
        _settings(tmp_path, {"bad": bad_tenant, "good": good_tenant}),
        workflow_runner=runner,  # type: ignore[arg-type]
        mode="both",
    )
    try:
        with TestClient(app) as client:
            by_name = {item["tenant_name"]: item for item in client.get("/api/v1/readiness").json()["tenants"]}
            assert by_name["bad"]["focus_preview_state"] == "unavailable"
            assert by_name["good"]["focus_preview_state"] == "degraded"

            before = runner.worker_calls
            runner.busy_tenants.add("bad")
            busy = client.post(
                "/api/v1/tenants/bad/focus-preview/repairs",
                json={"start_date": "2026-07-02", "end_date": "2026-07-03"},
            )
            assert busy.status_code == 409
            assert busy.json()["detail"]["code"] == "focus_preview_repair_tenant_busy"
            runner.busy_tenants.clear()

            failed = client.post(
                "/api/v1/tenants/bad/focus-preview/repairs",
                json={"start_date": "2026-07-02", "end_date": "2026-07-03"},
            )
            assert failed.status_code == 503
            assert failed.json() == {"detail": "FOCUS Mapping Preview repair worker is unavailable"}
            assert runner.worker_calls == before
            with bad_storage._engine.connect() as connection:
                assert connection.execute(text("SELECT COUNT(*) FROM ccloud_focus_preview_repairs")).scalar_one() == 1
                assert (
                    connection.execute(text("SELECT COUNT(*) FROM ccloud_focus_preview_repair_dates")).scalar_one() == 1
                )
                assert (
                    connection.execute(text("SELECT COUNT(*) FROM ccloud_focus_preview_repair_heads")).scalar_one() == 1
                )

            bad_backend.fail_writes = False
            runner.release.set()
            admitted = client.post(
                "/api/v1/tenants/bad/focus-preview/repairs",
                json={"start_date": "2026-07-02", "end_date": "2026-07-03"},
            )
            assert admitted.status_code == 202
            with bad_storage.create_preview_generation_read_unit_of_work() as uow:
                interrupted = uow.repairs.get_for_owner(
                    "bad-interrupted",
                    "confluent_cloud",
                    "shared-owner",
                )
            assert interrupted is not None
            assert interrupted.status.value == "failed"
            assert interrupted.diagnostic == PreviewDiagnostic(
                code="focus_preview_repair_interrupted",
                message="The repair was interrupted; submit a new bounded repair to retry.",
                retryable=True,
            )
    finally:
        runner.release.set()
        bad_storage.dispose()
        good_storage.dispose()


def test_production_capacity_429_is_pre_persistence_and_retryable(
    tmp_path: Path,
) -> None:
    first_tenant = _tenant(tmp_path, "first")
    second_tenant = _tenant(tmp_path, "second")
    first_backend = _backend(tmp_path, "first")
    second_backend = _backend(tmp_path, "second")
    runner = _ProductionRunner({"first": first_backend, "second": second_backend})
    app = create_app(
        _settings(
            tmp_path,
            {"first": first_tenant, "second": second_tenant},
            workers=1,
            waiting=0,
        ),
        workflow_runner=runner,  # type: ignore[arg-type]
        mode="both",
    )
    try:
        with TestClient(app) as client:
            first = client.post(
                "/api/v1/tenants/first/focus-preview/repairs",
                json={"start_date": "2026-07-01", "end_date": "2026-07-02"},
            )
            assert first.status_code == 202
            assert runner.started_event.wait(timeout=2)

            rejected = client.post(
                "/api/v1/tenants/second/focus-preview/repairs",
                json={"start_date": "2026-07-01", "end_date": "2026-07-02"},
            )
            runner.release.set()
            assert rejected.status_code == 429
            assert rejected.json() == {
                "detail": {
                    "code": "focus_preview_repair_capacity_exhausted",
                    "message": "FOCUS Mapping Preview repair capacity is exhausted.",
                    "retryable": True,
                }
            }
            with second_backend._engine.connect() as connection:
                assert connection.execute(text("SELECT COUNT(*) FROM ccloud_focus_preview_repairs")).scalar_one() == 0
                assert (
                    connection.execute(text("SELECT COUNT(*) FROM ccloud_focus_preview_repair_dates")).scalar_one() == 0
                )
                assert (
                    connection.execute(text("SELECT COUNT(*) FROM ccloud_focus_preview_repair_heads")).scalar_one() == 0
                )
    finally:
        runner.release.set()
        first_backend.dispose()
        second_backend.dispose()


def test_successful_target_recovery_precedes_capacity_and_persists_no_new_repair(
    tmp_path: Path,
) -> None:
    running_tenant = _tenant(tmp_path, "running")
    recovering_tenant = _tenant(tmp_path, "recovering")
    running_storage = _backend(tmp_path, "running")
    recovering_storage = _backend(tmp_path, "recovering")
    _persist_queued(
        recovering_storage,
        "recovering",
        recovering_tenant,
        "interrupted",
    )
    recovering_backend = _ToggleEvidenceBackend(
        recovering_storage,
        fail_writes=True,
    )
    runner = _ProductionRunner(
        {
            "running": running_storage,
            "recovering": recovering_backend,
        }
    )
    app = create_app(
        _settings(
            tmp_path,
            {
                "running": running_tenant,
                "recovering": recovering_tenant,
            },
            workers=1,
            waiting=0,
        ),
        workflow_runner=runner,  # type: ignore[arg-type]
        mode="both",
    )
    try:
        with TestClient(app) as client:
            first = client.post(
                "/api/v1/tenants/running/focus-preview/repairs",
                json={"start_date": "2026-07-01", "end_date": "2026-07-02"},
            )
            assert first.status_code == 202
            assert runner.started_event.wait(timeout=2)

            recovering_backend.fail_writes = False
            rejected = client.post(
                "/api/v1/tenants/recovering/focus-preview/repairs",
                json={"start_date": "2026-07-02", "end_date": "2026-07-03"},
            )
            runner.release.set()
            assert rejected.status_code == 429
            assert rejected.json()["detail"]["code"] == ("focus_preview_repair_capacity_exhausted")
            with recovering_storage.create_preview_generation_read_unit_of_work() as uow:
                interrupted = uow.repairs.get_for_owner(
                    "interrupted",
                    "confluent_cloud",
                    recovering_tenant.tenant_id,
                )
            assert interrupted is not None
            assert interrupted.status.value == "failed"
            with recovering_storage._engine.connect() as connection:
                assert connection.execute(text("SELECT COUNT(*) FROM ccloud_focus_preview_repairs")).scalar_one() == 1
    finally:
        runner.release.set()
        running_storage.dispose()
        recovering_storage.dispose()


def test_successful_target_recovery_precedes_closed_runtime_and_does_not_clear_other_tenant(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import core.api.routes.readiness as readiness_module

    monkeypatch.setattr(readiness_module, "_READINESS_CACHE_TTL", 0)
    first_tenant = _tenant(tmp_path, "first-unavailable")
    second_tenant = _tenant(tmp_path, "second-unavailable")
    first_storage = _backend(tmp_path, "first-unavailable")
    second_storage = _backend(tmp_path, "second-unavailable")
    _persist_queued(first_storage, "first", first_tenant, "first-interrupted")
    _persist_queued(second_storage, "second", second_tenant, "second-interrupted")
    first_backend = _ToggleEvidenceBackend(first_storage, fail_writes=True)
    second_backend = _ToggleEvidenceBackend(second_storage, fail_writes=True)
    runner = _ProductionRunner({"first": first_backend, "second": second_backend})
    app = create_app(
        _settings(
            tmp_path,
            {"first": first_tenant, "second": second_tenant},
        ),
        workflow_runner=runner,  # type: ignore[arg-type]
        mode="both",
    )
    try:
        with TestClient(app) as client:
            app.state.preview_repair_runtime.close(wait=False)
            first_backend.fail_writes = False

            rejected = client.post(
                "/api/v1/tenants/first/focus-preview/repairs",
                json={"start_date": "2026-07-02", "end_date": "2026-07-03"},
            )
            assert rejected.status_code == 503
            assert rejected.json() == {"detail": "FOCUS Mapping Preview repair worker is unavailable"}
            by_name = {item["tenant_name"]: item for item in client.get("/api/v1/readiness").json()["tenants"]}
            assert by_name["first"]["focus_preview_state"] == "degraded"
            assert by_name["second"]["focus_preview_state"] == "unavailable"
            with first_storage._engine.connect() as connection:
                assert connection.execute(text("SELECT COUNT(*) FROM ccloud_focus_preview_repairs")).scalar_one() == 1
    finally:
        runner.release.set()
        first_storage.dispose()
        second_storage.dispose()
