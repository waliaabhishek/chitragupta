from __future__ import annotations

import inspect
import tomllib
from collections.abc import Iterable
from datetime import datetime, timedelta
from importlib import import_module
from pathlib import Path
from typing import get_type_hints

import pytest
from pydantic import ValidationError

from core.config.models import AppSettings
from tests.unit.core.preview.conftest import preview_module


def test_preview_config_defaults_and_app_ownership() -> None:
    config = import_module("core.config.models")
    settings = AppSettings()

    assert isinstance(settings.preview, config.PreviewConfig)
    assert settings.preview.artifact_root == Path("data/focus-preview")
    assert settings.preview.max_workers == 2
    assert settings.preview.max_csv_file_bytes is None


@pytest.mark.parametrize("max_workers", [0, 17])
def test_preview_config_rejects_invalid_worker_counts(max_workers: int) -> None:
    config = import_module("core.config.models")

    with pytest.raises(ValidationError):
        config.PreviewConfig(max_workers=max_workers)


@pytest.mark.parametrize("max_csv_file_bytes", [0, -1])
def test_preview_config_rejects_nonpositive_csv_limits(max_csv_file_bytes: int) -> None:
    config = import_module("core.config.models")

    with pytest.raises(ValidationError):
        config.PreviewConfig(max_csv_file_bytes=max_csv_file_bytes)


def test_preview_cli_entry_point_is_registered() -> None:
    project = tomllib.loads(Path("pyproject.toml").read_text(encoding="utf-8"))

    assert project["project"]["scripts"]["chitragupta-preview"] == "core.preview.cli:main"


@pytest.mark.parametrize(
    "module_name",
    [
        "__init__",
        "models",
        "eligibility",
        "evidence",
        "persistence",
        "mapping",
        "artifacts",
        "service",
        "cli",
    ],
)
def test_every_preview_module_uses_postponed_annotations(module_name: str) -> None:
    module = preview_module(module_name)
    source = inspect.getsource(module)
    meaningful_lines = [line.strip() for line in source.splitlines() if line.strip() and not line.startswith("#")]

    assert meaningful_lines[0] == "from __future__ import annotations"


def test_focus_preview_route_uses_postponed_annotations() -> None:
    route = import_module("core.api.routes.focus_preview")
    source = inspect.getsource(route)
    meaningful_lines = [line.strip() for line in source.splitlines() if line.strip() and not line.startswith("#")]

    assert meaningful_lines[0] == "from __future__ import annotations"


def test_migration_019_uses_postponed_annotations() -> None:
    migration = import_module("core.storage.migrations.versions.019_add_focus_preview_and_calculation_identity")
    source = inspect.getsource(migration)
    meaningful_lines = [line.strip() for line in source.splitlines() if line.strip() and not line.startswith("#")]

    assert meaningful_lines[0] == "from __future__ import annotations"


def test_migration_020_uses_postponed_annotations() -> None:
    migration = import_module("core.storage.migrations.versions.020_add_preview_diagnostic_correlations")
    source = inspect.getsource(migration)
    meaningful_lines = [line.strip() for line in source.splitlines() if line.strip() and not line.startswith("#")]

    assert meaningful_lines[0] == "from __future__ import annotations"


def test_mapping_exposes_split_typed_data_and_manifest_contracts() -> None:
    mapping = preview_module("mapping")
    models = preview_module("models")
    data_hints = get_type_hints(mapping.build_preview_data_package)
    manifest_hints = get_type_hints(mapping.build_preview_manifest)

    assert data_hints == {
        "request": models.PreviewRequest,
        "snapshot": models.PreviewSourceSnapshot,
        "full_rows": Iterable[mapping.PreviewFullRow],
        "reconciliation": mapping.PreviewPackageReconciliation,
        "max_csv_file_bytes": int | None,
        "return": mapping.PreviewDataPackageDraft,
    }
    assert manifest_hints == {
        "request": models.PreviewRequest,
        "snapshot": models.PreviewSourceSnapshot,
        "draft": mapping.PreviewDataPackageDraft,
        "files": tuple[models.PreviewArtifactMetadata, ...],
        "ready_at": datetime,
        "expires_at": datetime,
        "return": bytes,
    }
    assert mapping.PreviewDataPackageDraft.__module__ == "core.preview.mapping"
    assert not hasattr(models, "PreviewDataPackageDraft")


def test_artifact_store_exposes_staged_publication_contract_without_mapping_dependency() -> None:
    artifacts = preview_module("artifacts")
    models = preview_module("models")
    hints = get_type_hints(artifacts.PreviewArtifactStore.stage_data_files)
    publish_hints = get_type_hints(artifacts.PreviewStagedPackage.publish)

    assert hints == {
        "request_id": str,
        "data_files": tuple[models.PreviewArtifactPayload, ...],
        "return": artifacts.PreviewStagedPackage,
    }
    assert publish_hints == {
        "manifest_body": bytes,
        "return": models.PreviewStoredPackage,
    }
    assert "core.preview.mapping" not in inspect.getsource(artifacts)
    assert "core.preview.mapping" not in inspect.getsource(models)
    assert not hasattr(models, "PreviewPackagePayload")
    assert not hasattr(artifacts.PreviewArtifactStore, "finalize_package")


def test_mapping_helpers_expose_the_typed_v4_boundary_contracts() -> None:
    mapping = preview_module("mapping")

    assert list(inspect.signature(mapping.classify_daily_full_source).parameters) == [
        "request_start",
        "request_end",
        "source",
    ]
    assert list(inspect.signature(mapping.project_financials).parameters) == [
        "source",
        "semantics",
        "billed_share",
    ]
    assert list(inspect.signature(mapping.reconcile_selected_evidence).parameters) == [
        "selected",
        "aggregate",
        "allocation",
    ]
    assert list(inspect.signature(mapping.resolve_provider_resource_context).parameters) == [
        "source",
        "semantics",
        "origin_resource",
        "resources",
    ]
    assert list(inspect.signature(mapping.validate_preview_row).parameters) == [
        "row",
        "target_rules",
        "custom_rules",
    ]
    assert get_type_hints(mapping.validate_preview_row)["return"] is type(None)


def test_focus_preview_route_imports_timedelta_at_module_scope() -> None:
    route = import_module("core.api.routes.focus_preview")
    source = inspect.getsource(route.submit_preview)

    assert "from datetime import timedelta" not in source
    assert route.timedelta is timedelta
