from __future__ import annotations

import inspect
from importlib import import_module
from pathlib import Path


def test_repair_repository_exposes_only_guarded_transition_methods() -> None:
    repair = import_module("core.preview.repair")

    expected = {
        "create_queued",
        "get_for_owner",
        "find_active_for_owner",
        "get_current_progress_for_owner",
        "mark_running",
        "mark_date_running",
        "mark_date_daily_validated",
        "mark_date_succeeded_from_running",
        "mark_date_failed_from_running",
        "finalize_month_dates",
        "fail_queued_before_execution",
        "fail_running_worker",
        "finalize_completed",
        "finalize_completed_with_failures",
        "fail_interrupted_for_owner",
    }
    public_methods = {
        name
        for name, value in inspect.getmembers(repair.PreviewRepairRepository, inspect.isfunction)
        if not name.startswith("_")
    }

    assert public_methods == expected
    assert "set_status" not in public_methods


def test_repair_runner_protocol_requires_owner_scoped_production_entrypoint() -> None:
    repair = import_module("core.preview.repair")

    signature = inspect.signature(repair.PreviewRepairRunner.run_focus_preview_repair)

    assert list(signature.parameters) == [
        "self",
        "repair_id",
        "tenant_name",
        "tenant_config",
    ]


def test_core_repair_module_has_no_confluent_repository_or_artifact_dependency() -> None:
    repair = import_module("core.preview.repair")
    source = Path(repair.__file__).read_text(encoding="utf-8")

    assert "plugins.confluent_cloud" not in source
    assert "CCloudBillingRepository" not in source
    assert "PreviewArtifact" not in source
    assert "PreviewRevision" not in source


def test_native_source_capture_exposes_write_without_finalization_beside_existing_persist() -> None:
    capture = import_module("core.preview.evidence_capture")

    write = inspect.signature(capture.NativeSourceEvidenceCapture.write)
    persist = inspect.signature(capture.NativeSourceEvidenceCapture.persist)

    assert list(write.parameters) == [
        "self",
        "source_windows",
        "source_readiness",
        "attempt_sequence",
        "captured_at",
    ]
    assert list(persist.parameters) == list(write.parameters)
