from __future__ import annotations

import threading
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import anyio.to_thread
import pytest

import workflow_runner
from core.api.app import create_app
from core.config.models import PreviewConfig
from core.preview.artifacts import preview_artifact_owner
from core.preview.capacity import PreviewGenerationScheduler
from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend
from plugins.confluent_cloud.storage.module import CCloudStorageModule
from tests.integration.core.api.backend_provider import FixedTenantBackendProvider
from tests.integration.core.api.test_focus_preview import (
    SameThreadApiClient,
    _body,
    _settings,
    _wait_for_terminal,
)
from tests.unit.core.preview.test_service import _aggregate, _allocation, _seed, _source
from workflow_runner import TenantRuntime, WorkflowRunner


@pytest.fixture(autouse=True)
def _inline_api_threads(monkeypatch: pytest.MonkeyPatch) -> None:
    async def run_inline(function: Callable[..., object], *args: object, **kwargs: object) -> object:
        return function(*args, **kwargs)

    async def run_sync_inline(function: Callable[..., object], *args: object, **_kwargs: object) -> object:
        return function(*args)

    monkeypatch.setattr("core.api.app.asyncio.to_thread", run_inline)
    monkeypatch.setattr(anyio.to_thread, "run_sync", run_sync_inline)


def test_production_zero_queue_capacity_returns_exact_429_before_backend_or_persistence(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path).model_copy(
        update={
            "preview": PreviewConfig(
                artifact_root=tmp_path / "artifacts",
                max_workers=1,
                max_queued_generations=0,
                max_running_generations_per_tenant=1,
                max_queued_generations_per_tenant=0,
                max_generation_spool_bytes=2_147_483_648,
            )
        }
    )
    provider = FixedTenantBackendProvider()
    runtime_kwargs: dict[str, object] = {}

    class CapacityExhaustedRuntime:
        def __init__(self, **kwargs: object) -> None:
            runtime_kwargs.update(kwargs)

        @staticmethod
        def _capacity_error() -> BaseException:
            capacity = __import__("core.preview.capacity", fromlist=["PreviewCapacityUnavailable"])
            return capacity.PreviewCapacityUnavailable()

        def reserve_requested(self, **kwargs: object) -> object:
            del kwargs
            raise self._capacity_error()

        def submit(self, **kwargs: object) -> object:
            del kwargs
            raise self._capacity_error()

        def ensure_owner_recovered(self, **kwargs: object) -> None:
            del kwargs

        def close(self, *, wait: bool = True) -> None:
            del wait

    app = create_app(settings)
    with (
        patch("core.api.app.ApiTenantBackendProvider", return_value=provider),
        patch("core.preview.service.PreviewRuntime", CapacityExhaustedRuntime),
        SameThreadApiClient(app) as client,
    ):
        provider.acquisitions.clear()
        invalid = client.post(
            "/api/v1/tenants/production/focus-preview/requests",
            json={**_body(), "end_date": "2026-07-01"},
        )
        exhausted = client.post(
            "/api/v1/tenants/production/focus-preview/requests",
            json=_body(),
        )

        assert invalid.status_code == 400
        assert exhausted.status_code == 429
        assert exhausted.json() == {
            "detail": {
                "code": "preview_capacity_exhausted",
                "message": "FOCUS Mapping Preview generation capacity is exhausted.",
                "retryable": True,
            }
        }
        assert "request_id" not in exhausted.json()
        assert provider.acquisitions == []
        assert (
            runtime_kwargs.items()
            >= {
                "max_workers": 1,
                "max_queued_generations": 0,
                "max_running_generations_per_tenant": 1,
                "max_queued_generations_per_tenant": 0,
                "max_generation_spool_bytes": 2_147_483_648,
            }.items()
        )


def test_production_manifest_and_file_routes_stream_exact_bytes_without_path_read_bytes(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    backend = SQLModelBackend(
        settings.tenants["production"].storage.connection_string.get_secret_value(),
        CCloudStorageModule(),
        use_migrations=False,
        focus_preview_enabled=True,
    )
    backend.create_tables()
    _seed(backend, source=_source(), aggregate=_aggregate(), allocation=_allocation())
    provider = FixedTenantBackendProvider({"production": backend})
    app = create_app(settings)
    try:
        with (
            patch("core.api.app.ApiTenantBackendProvider", return_value=provider),
            SameThreadApiClient(app) as client,
        ):
            accepted = client.post(
                "/api/v1/tenants/production/focus-preview/requests",
                json=_body(),
            )
            ready = _wait_for_terminal(client, accepted.json()["request_id"])
            manifest_url = ready["package"]["manifest"]["download_url"]
            file_url = ready["package"]["files"][0]["download_url"]
            expected_manifest = client.get(manifest_url).content
            expected_file = client.get(file_url).content

            def forbid_read_bytes(path: Path) -> bytes:
                raise AssertionError(f"whole-artifact read forbidden: {path}")

            with patch("core.preview.artifacts.Path.read_bytes", forbid_read_bytes):
                manifest = client.get(manifest_url)
                data_file = client.get(file_url)

            assert manifest.status_code == 200
            assert manifest.content == expected_manifest
            assert data_file.status_code == 200
            assert data_file.content == expected_file
    finally:
        backend.dispose()


def _scheduled_runner(
    tmp_path: Path,
    *,
    queued: int,
    tenant_queued: int,
) -> tuple[WorkflowRunner, MagicMock, SQLModelBackend]:
    settings = _settings(tmp_path).model_copy(
        update={
            "preview": PreviewConfig(
                artifact_root=tmp_path / "artifacts",
                max_workers=1,
                max_queued_generations=queued,
                max_running_generations_per_tenant=1,
                max_queued_generations_per_tenant=tenant_queued,
            )
        }
    )
    backend = SQLModelBackend(
        settings.tenants["production"].storage.connection_string.get_secret_value(),
        CCloudStorageModule(),
        use_migrations=False,
        focus_preview_enabled=True,
    )
    backend.create_tables()
    manager = MagicMock()
    manager.eligible_months.return_value = ("2026-07",)
    scheduler = PreviewGenerationScheduler(
        max_workers=1,
        max_queued_generations=queued,
        max_running_generations_per_tenant=1,
        max_queued_generations_per_tenant=tenant_queued,
    )
    runner = WorkflowRunner(
        settings,
        MagicMock(),
        revision_manager=manager,
        preview_generation_scheduler=scheduler,
    )
    runner._tenant_runtimes["production"] = TenantRuntime(  # noqa: SLF001
        tenant_name="production",
        plugin=MagicMock(),
        storage=backend,
        orchestrator=MagicMock(),
        config_hash=workflow_runner._config_hash(settings.tenants["production"]),  # noqa: SLF001
        created_at=datetime(2026, 8, 7, tzinfo=UTC),
    )
    return runner, manager, backend


def test_scheduled_publication_defers_under_real_scheduler_saturation_then_reacquires_backend(
    tmp_path: Path,
) -> None:
    runner, manager, backend = _scheduled_runner(tmp_path, queued=0, tenant_queued=0)
    scheduler = runner.preview_generation_scheduler
    assert scheduler is not None
    tenant = runner._settings.tenants["production"]  # noqa: SLF001
    owner = preview_artifact_owner("production", tenant)
    started = threading.Event()
    release = threading.Event()
    reservation = scheduler.reserve_requested(owner=owner)
    reservation.attach(
        work_id="blocking-request",
        run=lambda: (started.set(), release.wait(timeout=5)),
    )
    try:
        assert started.wait(timeout=5)
        runner._publish_scheduled_revisions(  # noqa: SLF001
            {"production": MagicMock(errors=[], already_running=False, fatal=False)},
            now=datetime(2026, 8, 7, tzinfo=UTC),
        )
        manager.publish_eligible_month.assert_not_called()
        assert scheduler.snapshot().global_queued == 0

        release.set()
        scheduler.wait_idle()
        before = datetime.now(UTC)
        runner._publish_scheduled_revisions(  # noqa: SLF001
            {"production": MagicMock(errors=[], already_running=False, fatal=False)},
            now=datetime(2026, 8, 7, tzinfo=UTC),
        )
        scheduler.wait_idle()

        call = manager.publish_eligible_month.call_args
        assert call.kwargs["backend"] is backend
        assert call.kwargs["month"] == "2026-07"
        assert call.kwargs["now"] >= before
    finally:
        release.set()
        runner.close()


def test_busy_pipeline_claim_defers_scheduled_work_without_blocking_scheduler_worker(
    tmp_path: Path,
) -> None:
    runner, manager, _backend = _scheduled_runner(tmp_path, queued=4, tenant_queued=2)
    scheduler = runner.preview_generation_scheduler
    assert scheduler is not None
    try:
        with runner._claim_tenant("production") as claimed:  # noqa: SLF001
            assert claimed
            runner._publish_scheduled_revisions(  # noqa: SLF001
                {"production": MagicMock(errors=[], already_running=False, fatal=False)},
                now=datetime(2026, 8, 7, tzinfo=UTC),
            )
            scheduler.wait_idle()
            manager.publish_eligible_month.assert_not_called()

        runner._publish_scheduled_revisions(  # noqa: SLF001
            {"production": MagicMock(errors=[], already_running=False, fatal=False)},
            now=datetime(2026, 8, 7, tzinfo=UTC),
        )
        scheduler.wait_idle()
        manager.publish_eligible_month.assert_called_once()
    finally:
        runner.close()


def test_periodic_cycle_waits_for_admitted_publication_before_same_cycle_retention(
    tmp_path: Path,
) -> None:
    runner, manager, _backend = _scheduled_runner(tmp_path, queued=4, tenant_queued=2)
    publication_started = threading.Event()
    publication_release = threading.Event()
    pipeline_retention_reached = threading.Event()
    preview_retention_reached = threading.Event()
    shutdown = threading.Event()

    def publish(**_kwargs: object) -> None:
        publication_started.set()
        assert publication_release.wait(timeout=5)

    def cleanup_preview(**_kwargs: object) -> None:
        preview_retention_reached.set()
        shutdown.set()

    manager.publish_eligible_month.side_effect = publish
    manager.cleanup_retention.side_effect = cleanup_preview
    runner.run_once = MagicMock(  # type: ignore[method-assign]
        return_value={"production": MagicMock(errors=[], already_running=False, fatal=False)}
    )
    runner._log_results = MagicMock()  # type: ignore[method-assign]  # noqa: SLF001
    runner._cleanup_retention = MagicMock(  # type: ignore[method-assign]  # noqa: SLF001
        side_effect=lambda **_kwargs: pipeline_retention_reached.set()
    )
    worker = threading.Thread(target=runner.run_loop, args=(shutdown,))
    try:
        worker.start()
        assert publication_started.wait(timeout=5)
        assert not pipeline_retention_reached.wait(timeout=0.2)
        assert not preview_retention_reached.is_set()

        publication_release.set()
        assert preview_retention_reached.wait(timeout=5)
        worker.join(timeout=5)
        assert pipeline_retention_reached.is_set()
        assert not worker.is_alive()
    finally:
        publication_release.set()
        shutdown.set()
        worker.join(timeout=5)
        runner.close()


def test_runner_shutdown_drops_promoted_scheduled_publication_before_backend_reacquisition(
    tmp_path: Path,
) -> None:
    runner, manager, _backend = _scheduled_runner(tmp_path, queued=4, tenant_queued=2)
    scheduler = runner.preview_generation_scheduler
    assert scheduler is not None
    tenant = runner._settings.tenants["production"]  # noqa: SLF001
    owner = preview_artifact_owner("production", tenant)
    started = threading.Event()
    release = threading.Event()
    reservation = scheduler.reserve_requested(owner=owner)
    reservation.attach(
        work_id="blocking-request",
        run=lambda: (started.set(), release.wait(timeout=5)),
    )
    closer = threading.Thread(target=runner.close)
    try:
        assert started.wait(timeout=5)
        runner._publish_scheduled_revisions(  # noqa: SLF001
            {"production": MagicMock(errors=[], already_running=False, fatal=False)},
            now=datetime(2026, 8, 7, tzinfo=UTC),
        )
        assert scheduler.snapshot().global_queued == 1
        closer.start()
        release.set()
        closer.join(timeout=5)
        assert not closer.is_alive()
        manager.publish_eligible_month.assert_not_called()
    finally:
        release.set()
        closer.join(timeout=5)
        runner.close()
