from __future__ import annotations

import ast
import inspect
import textwrap
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


def test_revision_protocols_have_the_exact_history_and_retention_contracts() -> None:
    revisions = _module("revisions")
    persistence = _module("persistence")
    models = _module("models")
    artifacts = _module("artifacts")
    config = import_module("core.config.models")

    assert get_type_hints(revisions.PreviewScheduledRevisionManager.publish_eligible_months) == {
        "tenant_name": str,
        "tenant_config": config.TenantConfig,
        "backend": persistence.PreviewStorageBackend,
        "now": datetime,
        "return": tuple[models.PreviewRevision, ...],
    }
    assert get_type_hints(revisions.PreviewScheduledRevisionManager.cleanup_retention) == {
        "tenant_name": str,
        "tenant_config": config.TenantConfig,
        "backend": persistence.PreviewStorageBackend,
        "now": datetime,
        "return": revisions.PreviewRevisionCleanupResult,
    }
    assert get_type_hints(revisions.PreviewRevisionReader.get_current) == {
        "backend": persistence.PreviewStorageBackend,
        "ecosystem": str,
        "tenant_id": str,
        "month_start": import_module("datetime").date,
        "return": models.PreviewRevision | None,
    }
    assert get_type_hints(revisions.PreviewRevisionReader.get_for_owner) == {
        "backend": persistence.PreviewStorageBackend,
        "ecosystem": str,
        "tenant_id": str,
        "revision_id": str,
        "return": models.PreviewRevision | None,
    }
    assert get_type_hints(revisions.PreviewRevisionReader.list_for_owner_month) == {
        "backend": persistence.PreviewStorageBackend,
        "ecosystem": str,
        "tenant_id": str,
        "month_start": import_module("datetime").date,
        "limit": int,
        "cursor_revision_id": str | None,
        "return": persistence.PreviewRevisionPage,
    }
    assert get_type_hints(revisions.PreviewRevisionReader.validation_summary) == {
        "revision": models.PreviewRevision,
        "return": models.PreviewRevisionValidationSummary,
    }
    assert get_type_hints(revisions.PreviewRevisionReader.read_manifest) == {
        "revision": models.PreviewRevision,
        "return": bytes,
    }
    assert get_type_hints(revisions.PreviewRevisionReader.read_file) == {
        "revision": models.PreviewRevision,
        "file_name": str,
        "return": tuple[models.PreviewArtifactMetadata, bytes],
    }
    assert get_type_hints(revisions.PreviewRevisionReader.open_archive) == {
        "revision": models.PreviewRevision,
        "return": artifacts.PreviewArchiveStream,
    }
    assert not hasattr(revisions.PreviewRevisionReadService, "close")


def test_revision_repository_and_uow_contracts_expose_history_and_retention() -> None:
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
    expected = {
        "get_current_for_publication",
        "get_for_owner",
        "list_for_owner_month",
        "mark_retention_due",
        "list_retention_pending",
        "get_retention_pending_tail",
        "defer_retention_pending",
        "delete_retention_pending",
    }
    assert expected <= set(persistence.PreviewRevisionRepository.__dict__)
    assert get_type_hints(persistence.PreviewRevisionRepository.get_current_for_publication) == {
        "ecosystem": str,
        "tenant_id": str,
        "month_start": date,
        "return": models.PreviewRevision | None,
    }
    assert get_type_hints(persistence.PreviewRevisionRepository.get_for_owner) == {
        "ecosystem": str,
        "tenant_id": str,
        "revision_id": str,
        "return": models.PreviewRevision | None,
    }
    assert get_type_hints(persistence.PreviewRevisionRepository.list_for_owner_month) == {
        "ecosystem": str,
        "tenant_id": str,
        "month_start": date,
        "limit": int,
        "cursor_revision_id": str | None,
        "return": persistence.PreviewRevisionPage,
    }
    assert get_type_hints(persistence.PreviewRevisionRepository.mark_retention_due) == {
        "ecosystem": str,
        "tenant_id": str,
        "cutoff_date": date,
        "pending_at": datetime,
        "limit": int,
        "return": tuple[persistence.PreviewRetentionCandidate, ...],
    }
    assert get_type_hints(persistence.PreviewRevisionRepository.list_retention_pending) == {
        "ecosystem": str,
        "tenant_id": str,
        "limit": int,
        "return": tuple[persistence.PreviewRetentionCandidate, ...],
    }
    assert get_type_hints(persistence.PreviewRevisionRepository.get_retention_pending_tail) == {
        "ecosystem": str,
        "tenant_id": str,
        "return": datetime | None,
    }
    assert "revisions" in persistence.PreviewWriteUnitOfWork.__annotations__
    assert "revisions" in persistence.PreviewReadUnitOfWork.__annotations__


def test_revision_exceptions_follow_error_suffix_convention() -> None:
    persistence = _module("persistence")
    revisions = _module("revisions")

    assert persistence.PreviewRevisionConflictError.__name__.endswith("Error")
    assert persistence.PreviewRevisionCursorError.__name__.endswith("Error")
    assert revisions.PreviewRevisionArtifactUnavailableError.__name__.endswith("Error")


@pytest.mark.parametrize(
    "values",
    [
        (-1, 0, 0),
        (0, -1, 0),
        (0, 0, -1),
        (True, 0, 0),
        (0, False, 0),
        (0, 0, 1.5),
    ],
)
def test_revision_cleanup_result_requires_exact_nonnegative_integers(
    values: tuple[object, object, object],
) -> None:
    revisions = _module("revisions")

    with pytest.raises(ValueError):
        revisions.PreviewRevisionCleanupResult(*values)


def test_revision_api_schemas_have_the_exact_public_fields() -> None:
    schemas = import_module("core.api.schemas")

    summary_fields = {
        "revision_id",
        "tenant_name",
        "month",
        "start_date",
        "end_date",
        "monthly_status",
        "published_at",
        "supersedes_revision_id",
        "superseded_by_revision_id",
        "lifecycle",
        "material_sha256",
        "source_snapshot",
        "validation",
        "replacement_semantics",
        "consumer_action",
        "detail_url",
    }
    assert set(schemas.FocusPreviewRevisionSummaryResponse.model_fields) == summary_fields
    assert set(schemas.FocusPreviewRevisionResponse.model_fields) == summary_fields | {
        "self_url",
        "package",
    }
    assert set(schemas.FocusPreviewRevisionListResponse.model_fields) == {
        "items",
        "next_cursor",
        "replacement_semantics",
        "consumer_action",
    }


_READER_METHODS = {
    "get_current",
    "get_for_owner",
    "list_for_owner_month",
    "validation_summary",
    "read_manifest",
    "read_file",
    "open_archive",
}


def _annotation_is_revision_reader(annotation: ast.expr | None) -> bool:
    if annotation is None:
        return False
    if isinstance(annotation, ast.Constant) and isinstance(annotation.value, str):
        try:
            return _annotation_is_revision_reader(ast.parse(annotation.value, mode="eval").body)
        except SyntaxError:
            return False
    if isinstance(annotation, ast.Name):
        return annotation.id == "PreviewRevisionReadService"
    if isinstance(annotation, ast.Attribute):
        return annotation.attr == "PreviewRevisionReadService"
    if isinstance(annotation, ast.BinOp) and isinstance(annotation.op, ast.BitOr):
        return _annotation_is_revision_reader(annotation.left) or _annotation_is_revision_reader(annotation.right)
    if isinstance(annotation, ast.Subscript):
        container = annotation.value
        name = container.id if isinstance(container, ast.Name) else getattr(container, "attr", "")
        values = annotation.slice.elts if isinstance(annotation.slice, ast.Tuple) else (annotation.slice,)
        if name == "Annotated":
            return bool(values) and _annotation_is_revision_reader(values[0])
        if name in {"Optional", "Union"}:
            return any(_annotation_is_revision_reader(value) for value in values)
    return False


def _is_reader_constructor(node: ast.AST) -> bool:
    if not isinstance(node, ast.Call):
        return False
    return (
        isinstance(node.func, ast.Name)
        and node.func.id == "PreviewRevisionReadService"
        or isinstance(node.func, ast.Attribute)
        and node.func.attr == "PreviewRevisionReadService"
    )


def _reader_positional_violations(source: str, *, path: str = "snippet.py") -> list[str]:
    tree = ast.parse(textwrap.dedent(source), filename=path)
    violations: list[str] = []

    def inspect_scope(node: ast.Module | ast.FunctionDef | ast.AsyncFunctionDef, inherited: set[str]) -> None:
        tracked = set(inherited)
        body = node.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            parameters = (*node.args.posonlyargs, *node.args.args, *node.args.kwonlyargs)
            tracked.update(arg.arg for arg in parameters if _annotation_is_revision_reader(arg.annotation))
        for statement in body:
            if isinstance(statement, (ast.FunctionDef, ast.AsyncFunctionDef)):
                inspect_scope(statement, tracked)
                continue
            for child in ast.walk(statement):
                if isinstance(child, ast.Assign) and len(child.targets) == 1 and isinstance(child.targets[0], ast.Name):
                    name = child.targets[0].id
                    if (
                        _is_reader_constructor(child.value)
                        or isinstance(child.value, ast.Name)
                        and child.value.id in tracked
                    ):
                        tracked.add(name)
                    else:
                        tracked.discard(name)
                if not isinstance(child, ast.Call) or not child.args or not isinstance(child.func, ast.Attribute):
                    continue
                if child.func.attr not in _READER_METHODS:
                    continue
                receiver = child.func.value
                binding = receiver.id if isinstance(receiver, ast.Name) else "inline"
                if not (isinstance(receiver, ast.Name) and receiver.id in tracked or _is_reader_constructor(receiver)):
                    continue
                violations.append(
                    f"{path}:{child.lineno}: {binding}.{child.func.attr} has {len(child.args)} positional arguments"
                )

    inspect_scope(tree, set())
    return violations


@pytest.mark.parametrize(
    "source",
    [
        "reader = PreviewRevisionReadService(artifact_store=store)\nreader.read_manifest(revision)",
        "reader = PreviewRevisionReadService(artifact_store=store)\nalias = reader\nalias.read_file(revision, name)",
        "PreviewRevisionReadService(artifact_store=store).open_archive(revision)",
        (
            "def helper(reader: PreviewRevisionReadService):\n"
            "    reader.get_for_owner(backend, ecosystem, tenant, revision)"
        ),
        "def helper(reader: 'pkg.PreviewRevisionReadService | None'):\n    reader.validation_summary(revision)",
        "def helper(reader: Annotated[PreviewRevisionReadService, 'x']):\n    reader.get_current(backend)",
    ],
)
def test_reader_consumer_checker_rejects_positional_reader_calls(source: str) -> None:
    assert _reader_positional_violations(source)


def test_reader_consumer_checker_accepts_keyword_calls_and_unrelated_receivers() -> None:
    source = """
    def helper(reader: PreviewRevisionReadService, artifact_store: Any, delegate: Any):
        reader.get_current(backend=backend, ecosystem='confluent_cloud', tenant_id='tenant', month_start=month)
        reader.get_for_owner(backend=backend, ecosystem='confluent_cloud', tenant_id='tenant', revision_id='revision')
        reader.list_for_owner_month(
            backend=backend,
            ecosystem='confluent_cloud',
            tenant_id='tenant',
            month_start=month,
            limit=20,
            cursor_revision_id=None,
        )
        reader.validation_summary(revision=revision)
        reader.read_manifest(revision=revision)
        reader.read_file(revision=revision, file_name='cost.csv')
        reader.open_archive(revision=revision)
        artifact_store.read_manifest(storage_key, metadata)
        delegate.open_archive(storage_key, manifest, files)
        local_store.read_file(storage_key, metadata)
        store.read_manifest(storage_key, metadata)
        self.delegate.open_archive(storage_key, manifest, files)
    """

    assert _reader_positional_violations(source) == []


def test_direct_reader_consumers_use_keyword_only_calls() -> None:
    root = Path(__file__).parents[4]
    paths = (
        root / "tests/unit/core/preview/test_revision_reader.py",
        root / "tests/integration/core/api/test_focus_preview_revision_publication.py",
    )

    violations = [
        violation
        for path in paths
        for violation in _reader_positional_violations(path.read_text(), path=str(path.relative_to(root)))
    ]

    assert violations == []


def test_old_revision_publisher_contract_has_no_consumers() -> None:
    root = Path(__file__).parents[4]
    stale: list[str] = []
    for base in (root / "src", root / "tests"):
        for path in base.rglob("*.py"):
            if path == Path(__file__):
                continue
            body = path.read_text()
            if "PreviewScheduledRevisionPublisher" in body or "revision_publisher" in body:
                stale.append(str(path.relative_to(root)))

    assert stale == []
