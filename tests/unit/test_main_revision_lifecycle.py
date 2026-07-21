from __future__ import annotations

import threading
from unittest.mock import MagicMock, patch

import pytest

from core.config.models import AppSettings


@pytest.mark.parametrize("run_once", [True, False])
def test_run_worker_closes_runner_after_normal_return(run_once: bool) -> None:
    from main import run_worker

    runner = MagicMock()
    runner.run_once.return_value = {}

    run_worker(
        AppSettings(),
        run_once=run_once,
        runner=runner,
        shutdown_event=threading.Event(),
    )

    runner.close.assert_called_once()


@pytest.mark.parametrize(("method", "run_once"), [("run_once", True), ("run_loop", False)])
def test_run_worker_closes_runner_when_execution_raises(method: str, run_once: bool) -> None:
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

    runner.close.assert_called_once()


def test_create_runner_wires_one_owned_worker_store_generator_and_revision_service() -> None:
    from main import _create_runner

    settings = AppSettings()
    store = MagicMock()
    generator = MagicMock()
    publisher = MagicMock()
    registry = MagicMock()
    runner = MagicMock()

    with (
        patch("main._build_registry", return_value=registry),
        patch("core.preview.artifacts.LocalPreviewArtifactStore", return_value=store) as store_type,
        patch("core.preview.generator.PreviewPackageGenerator", return_value=generator) as generator_type,
        patch("core.preview.revisions.PreviewRevisionService", return_value=publisher) as service_type,
        patch("main.WorkflowRunner", return_value=runner) as runner_type,
    ):
        assert _create_runner(settings) is runner

    store_type.assert_called_once_with(settings.preview.artifact_root)
    generator_type.assert_called_once_with(max_csv_file_bytes=settings.preview.max_csv_file_bytes)
    service_type.assert_called_once()
    assert service_type.call_args.kwargs["artifact_store"] is store
    assert service_type.call_args.kwargs["package_generator"] is generator
    runner_type.assert_called_once_with(
        settings,
        registry,
        revision_manager=publisher,
        owned_preview_artifact_store=store,
    )
