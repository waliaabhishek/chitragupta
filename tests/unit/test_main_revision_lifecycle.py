from __future__ import annotations

import threading
from unittest.mock import MagicMock, patch

import pytest

from core.config.models import AppSettings


@pytest.mark.parametrize("run_once", [True, False])
def test_run_worker_leaves_injected_runner_open_after_normal_return(run_once: bool) -> None:
    from main import run_worker

    runner = MagicMock()
    runner.run_once.return_value = {}

    run_worker(
        AppSettings(),
        run_once=run_once,
        runner=runner,
        shutdown_event=threading.Event(),
    )

    runner.close.assert_not_called()


@pytest.mark.parametrize(("method", "run_once"), [("run_once", True), ("run_loop", False)])
def test_run_worker_leaves_injected_runner_open_when_execution_raises(method: str, run_once: bool) -> None:
    from main import run_worker

    runner = MagicMock()
    getattr(runner, method).side_effect = RuntimeError("sentinel")

    with pytest.raises(RuntimeError, match="sentinel"):
        run_worker(
            AppSettings(),
            run_once=run_once,
            runner=runner,
            shutdown_event=threading.Event(),
        )

    runner.close.assert_not_called()


def test_create_runner_wires_one_owned_worker_store_generator_and_revision_service() -> None:
    from core.config.models import FocusPreviewTenantConfig, TenantConfig
    from main import _create_runner

    settings = AppSettings(
        tenants={
            "production": TenantConfig(
                ecosystem="confluent_cloud",
                tenant_id="tenant-1",
                focus_preview=FocusPreviewTenantConfig(
                    commercial_profile="direct_payg",
                    effective_start_date="2026-01-01",
                    effective_end_date="2027-01-01",
                ),
            )
        }
    )
    store = MagicMock()
    generator = MagicMock()
    publisher = MagicMock()
    scheduler = MagicMock()
    registry = MagicMock()
    runner = MagicMock()

    with (
        patch("main._build_registry", return_value=registry),
        patch("core.preview.artifacts.LocalPreviewArtifactStore", return_value=store) as store_type,
        patch("core.preview.generator.PreviewPackageGenerator", return_value=generator) as generator_type,
        patch("core.preview.capacity.PreviewGenerationScheduler", return_value=scheduler) as scheduler_type,
        patch("core.preview.revisions.PreviewRevisionService", return_value=publisher) as service_type,
        patch("main.WorkflowRunner", return_value=runner) as runner_type,
    ):
        assert _create_runner(settings) is runner

    store_type.assert_called_once_with(settings.preview.artifact_root)
    generator_type.assert_called_once_with(
        max_csv_file_bytes=settings.preview.max_csv_file_bytes,
        max_generation_spool_bytes=settings.preview.max_generation_spool_bytes,
    )
    scheduler_type.assert_called_once_with(
        max_workers=settings.preview.max_workers,
        max_queued_generations=settings.preview.max_queued_generations,
        max_running_generations_per_tenant=settings.preview.max_running_generations_per_tenant,
        max_queued_generations_per_tenant=settings.preview.max_queued_generations_per_tenant,
    )
    service_type.assert_called_once()
    assert service_type.call_args.kwargs["artifact_store"] is store
    assert service_type.call_args.kwargs["package_generator"] is generator
    runner_type.assert_called_once_with(
        settings,
        registry,
        revision_manager=publisher,
        owned_preview_artifact_store=store,
        preview_generation_scheduler=scheduler,
    )
