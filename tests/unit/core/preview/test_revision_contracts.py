from __future__ import annotations

import inspect
from collections.abc import Callable
from datetime import date, datetime
from importlib import import_module
from pathlib import Path
from typing import Any, get_type_hints

import pytest


def _module(name: str) -> Any:
    return import_module(f"core.preview.{name}")


def test_generator_is_a_one_way_module_with_exact_public_contract() -> None:
    generator = _module("generator")
    persistence = _module("persistence")
    models = _module("models")
    eligibility = _module("eligibility")
    mapping = _module("mapping")
    source = inspect.getsource(generator)

    assert source.lstrip().startswith("from __future__ import annotations")
    assert "core.preview.service" not in source
    assert "core.preview.revisions" not in source
    assert hasattr(generator, "utc_now")
    assert get_type_hints(generator.utc_now) == {"return": datetime}
    assert get_type_hints(generator.PreviewPackageGenerator.__init__) == {
        "max_csv_file_bytes": int | None,
        "clock": Callable[[], datetime],
        "return": type(None),
    }
    assert get_type_hints(generator.PreviewPackageGenerator.generate) == {
        "backend": persistence.PreviewStorageBackend,
        "request": models.PreviewRequest,
        "policy": eligibility.PreviewEligibilityPolicy,
        "return": tuple[models.PreviewSourceSnapshot, mapping.PreviewDataPackageDraft],
    }
    assert generator.PreviewGenerationError.__name__.endswith("Error")


def test_generator_owns_generation_helpers_and_service_has_no_compatibility_wrapper() -> None:
    generator = _module("generator")
    service = _module("service")

    for name in ("_failure", "_mapping_failure", "_calculation_failure", "_source_correlations"):
        assert hasattr(generator, name)
        assert not hasattr(service, name)
    assert not hasattr(service.PreviewRuntime, "_generate")


@pytest.mark.parametrize("grain", ["daily", "monthly"])
def test_generator_preserves_single_use_projection_iterables(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    grain: str,
) -> None:
    generator = _module("generator")
    models = _module("models")
    from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend
    from plugins.confluent_cloud.storage.module import CCloudStorageModule
    from tests.unit.core.preview.test_lifecycle_snapshot_v5 import _request
    from tests.unit.core.preview.test_service import _aggregate, _allocation, _seed, _source
    from tests.unit.core.preview.test_service_profiles_v5 import _policy

    backend = SQLModelBackend(
        f"sqlite:///{tmp_path / f'{grain}.db'}",
        CCloudStorageModule(),
        use_migrations=False,
    )
    backend.create_tables()
    _seed(backend, source=_source(), aggregate=_aggregate(), allocation=_allocation())
    build = generator.build_preview_data_package
    aggregate = generator.aggregate_monthly_full_rows
    observations: list[str] = []

    def capture_build(**kwargs: Any) -> Any:
        rows = kwargs["full_rows"]
        first = tuple(rows)
        assert tuple(rows) == ()
        observations.append("package-single-use")
        return build(**{**kwargs, "full_rows": first})

    def capture_monthly(*, rows: Any, month_start: Any, month_end: Any) -> Any:
        first = tuple(rows)
        assert tuple(rows) == ()
        observations.append("monthly-input-single-use")
        return iter(aggregate(rows=first, month_start=month_start, month_end=month_end))

    monkeypatch.setattr(generator, "build_preview_data_package", capture_build)
    if grain == "monthly":
        monkeypatch.setattr(generator, "aggregate_monthly_full_rows", capture_monthly)
        monkeypatch.setattr(
            generator,
            "resolve_preview_evidence_interval",
            lambda **kwargs: models.PreviewEvidenceInterval(date(2026, 7, 1), date(2026, 7, 2), "provisional"),
        )
    try:
        generator.PreviewPackageGenerator(max_csv_file_bytes=None).generate(
            backend=backend,
            request=_request(grain=grain),
            policy=_policy(cutoff=date(2026, 7, 2)),
        )
    finally:
        backend.dispose()

    assert observations == (
        ["package-single-use"] if grain == "daily" else ["monthly-input-single-use", "package-single-use"]
    )


def test_revision_protocols_have_the_exact_current_only_contracts() -> None:
    revisions = _module("revisions")
    persistence = _module("persistence")
    models = _module("models")
    artifacts = _module("artifacts")
    config = import_module("core.config.models")

    assert get_type_hints(revisions.PreviewScheduledRevisionPublisher.publish_eligible_months) == {
        "tenant_name": str,
        "tenant_config": config.TenantConfig,
        "backend": persistence.PreviewStorageBackend,
        "now": datetime,
        "return": tuple[models.PreviewRevision, ...],
    }
    assert get_type_hints(revisions.PreviewCurrentRevisionReader.get_current) == {
        "backend": persistence.PreviewStorageBackend,
        "ecosystem": str,
        "tenant_id": str,
        "month_start": import_module("datetime").date,
        "return": models.PreviewRevision | None,
    }
    assert get_type_hints(revisions.PreviewCurrentRevisionReader.read_manifest) == {
        "revision": models.PreviewRevision,
        "return": bytes,
    }
    assert get_type_hints(revisions.PreviewCurrentRevisionReader.read_file) == {
        "revision": models.PreviewRevision,
        "file_name": str,
        "return": tuple[models.PreviewArtifactMetadata, bytes],
    }
    assert get_type_hints(revisions.PreviewCurrentRevisionReader.open_archive) == {
        "revision": models.PreviewRevision,
        "return": artifacts.PreviewArchiveStream,
    }
    assert not hasattr(revisions.PreviewCurrentRevisionReader, "get_by_revision_id")
    assert not hasattr(revisions.PreviewRevisionReadService, "close")


def test_revision_repository_and_uow_contracts_are_current_only_and_runtime_checkable() -> None:
    persistence = _module("persistence")
    models = _module("models")

    assert get_type_hints(persistence.PreviewRevisionRepository.get_current_for_owner) == {
        "ecosystem": str,
        "tenant_id": str,
        "month_start": import_module("datetime").date,
        "return": models.PreviewRevision | None,
    }
    assert get_type_hints(persistence.PreviewRevisionRepository.replace_current) == {
        "candidate": models.PreviewRevisionCandidate,
        "package": models.PreviewStoredPackage,
        "expected_current_revision_id": str | None,
        "return": models.PreviewRevision,
    }
    assert "revisions" in persistence.PreviewWriteUnitOfWork.__annotations__
    assert "revisions" in persistence.PreviewReadUnitOfWork.__annotations__
    assert not hasattr(persistence.PreviewRevisionRepository, "list")
    assert not hasattr(persistence.PreviewRevisionRepository, "get_by_revision_id")


def test_revision_exceptions_follow_error_suffix_convention() -> None:
    persistence = _module("persistence")
    revisions = _module("revisions")

    assert persistence.PreviewRevisionConflictError.__name__.endswith("Error")
    assert revisions.PreviewRevisionArtifactUnavailableError.__name__.endswith("Error")


def test_current_revision_api_schema_has_the_exact_public_fields() -> None:
    schemas = import_module("core.api.schemas")

    assert set(schemas.FocusPreviewRevisionResponse.model_fields) == {
        "revision_id",
        "tenant_name",
        "month",
        "start_date",
        "end_date",
        "monthly_status",
        "published_at",
        "supersedes_revision_id",
        "material_sha256",
        "source_snapshot",
        "self_url",
        "package",
    }
