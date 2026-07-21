from __future__ import annotations

import hashlib
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from importlib import import_module
from pathlib import Path
from typing import Any, Self
from unittest.mock import MagicMock

import pytest


def _modules() -> tuple[Any, Any, Any, Any]:
    return (
        import_module("core.preview.revisions"),
        import_module("core.preview.mapping"),
        import_module("core.preview.models"),
        import_module("core.preview.generator"),
    )


def _snapshot(start: date, end: date, *, status: str) -> Any:
    models = import_module("core.preview.models")
    effective_end = end if status == "settled" else min(end, start + timedelta(days=5))
    coverage = tuple(
        models.PreviewCalculationCoverageEntry(
            tracking_date=tracking_date,
            calculation_id=f"calculation-{tracking_date.isoformat()}",
            calculation_completed_at=datetime.combine(
                tracking_date + timedelta(days=2), datetime.min.time(), tzinfo=UTC
            ),
            calculation_run_id=1,
        )
        for tracking_date in (start + timedelta(days=offset) for offset in range((effective_end - start).days))
    )
    return models.PreviewSourceSnapshot(
        calculation_timestamp=max((item.calculation_completed_at for item in coverage), default=None),
        calculation_coverage=coverage,
        source_through=None,
        effective_coverage_start_date=start,
        effective_coverage_end_date=effective_end,
        availability_cutoff_end_date=end if status == "settled" else effective_end,
        monthly_status=status,
    )


def _draft(marker: str) -> Any:
    _revisions, mapping, models, _generator = _modules()
    body = (",".join(mapping.FOCUS_1_4_FULL_PROFILE_COLUMNS) + f"\n{marker}\n").encode()
    return mapping.PreviewDataPackageDraft(
        data_files=(models.PreviewArtifactPayload("cost-and-usage.csv", "text/csv", 1, body),),
        source_records=0 if not marker else 1,
        rows=0 if not marker else 1,
        reconciliation=mapping.PreviewPackageReconciliation(
            0 if not marker else 1,
            Decimal(0),
            Decimal(0),
            Decimal(0),
            Decimal(0),
        ),
        logical_data_sha256=hashlib.sha256(body).hexdigest(),
    )


class _Generator:
    def __init__(self, states: list[tuple[str, str] | BaseException]) -> None:
        self.states = states
        self.requests: list[Any] = []

    def generate(self, *, backend: Any, request: Any, policy: Any) -> tuple[Any, Any]:
        del backend, policy
        self.requests.append(request)
        state = self.states.pop(0)
        if isinstance(state, BaseException):
            raise state
        status, marker = state
        return _snapshot(request.start_date, request.end_date, status=status), _draft(marker)


class _RevisionRepository:
    def __init__(self) -> None:
        self.current: dict[tuple[str, str, date], Any] = {}
        self.rows: list[Any] = []
        self.fail_next: BaseException | None = None

    def get_current_for_owner(self, *, ecosystem: str, tenant_id: str, month_start: date) -> Any | None:
        return self.current.get((ecosystem, tenant_id, month_start))

    def replace_current(
        self,
        *,
        candidate: Any,
        package: Any,
        expected_current_revision_id: str | None,
    ) -> Any:
        revisions = import_module("core.preview.revisions")
        models = import_module("core.preview.models")
        if self.fail_next is not None:
            failure, self.fail_next = self.fail_next, None
            raise failure
        key = (candidate.ecosystem, candidate.tenant_id, candidate.start_date)
        prior = self.current.get(key)
        if (None if prior is None else prior.revision_id) != expected_current_revision_id:
            raise revisions.PreviewRevisionConflictError("lost race")
        if candidate.supersedes_revision_id != expected_current_revision_id:
            raise ValueError("candidate supersedes identity does not match expected current revision")
        if prior is not None:
            superseded = replace(
                prior,
                superseded_by_revision_id=candidate.revision_id,
                is_current=False,
            )
            self.rows[self.rows.index(prior)] = superseded
        revision = models.PreviewRevision(
            **candidate.__dict__,
            superseded_by_revision_id=None,
            is_current=True,
            package=package,
        )
        self.rows.append(revision)
        self.current[key] = revision
        return revision


class _WriteUow:
    def __init__(self, revisions: _RevisionRepository, requests: Any) -> None:
        self.revisions = revisions
        self.requests = requests
        self.commits = 0
        self.rollbacks = 0

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
        del exc_type, exc_value, traceback

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1


class _Backend:
    def __init__(self) -> None:
        self.repository = _RevisionRepository()
        self.request_repository = _RecordingRequestRepository()
        self.write_uows: list[_WriteUow] = []
        self.read_uows: list[_ReadUow] = []

    def create_preview_write_unit_of_work(self) -> _WriteUow:
        uow = _WriteUow(self.repository, self.request_repository)
        self.write_uows.append(uow)
        return uow

    def create_preview_read_unit_of_work(self) -> _ReadUow:
        uow = _ReadUow(self.repository, self.request_repository)
        self.read_uows.append(uow)
        return uow


class _RecordingRequestRepository:
    def __init__(self) -> None:
        self.created: list[Any] = []

    def create(self, request: Any) -> Any:
        self.created.append(request)
        return request

    def __getattr__(self, name: str) -> Any:
        def unexpected(*args: Any, **kwargs: Any) -> Any:
            del args, kwargs
            raise AssertionError(f"scheduled publication must not use request repository method {name}")

        return unexpected


class _ReadUow:
    def __init__(self, revisions: _RevisionRepository, requests: Any) -> None:
        persistence = import_module("core.preview.persistence")
        self.requests = requests
        self.revisions = revisions
        self.calculations = MagicMock(spec=persistence.PreviewCalculationRepository)
        self.cost_evidence = MagicMock(spec=persistence.PreviewCostEvidenceReader)
        self.allocation_evidence = MagicMock(spec=persistence.PreviewAllocationEvidenceReader)
        storage = import_module("core.storage.interface")
        self.resources = MagicMock(spec=storage.ResourceRepository)
        self.identities = MagicMock(spec=storage.IdentityRepository)
        self.tags = MagicMock(spec=storage.EntityTagRepository)

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
        del exc_type, exc_value, traceback


def _tenant_config(connection_string: str = "sqlite:///unused.db") -> Any:
    config = import_module("core.config.models")
    return config.TenantConfig(
        ecosystem="confluent_cloud",
        tenant_id="tenant-1",
        lookback_days=40,
        cutoff_days=5,
        storage=config.StorageConfig(connection_string=connection_string),
        focus_preview=config.FocusPreviewTenantConfig(
            commercial_profile="direct_payg",
            billing_currency="USD",
            effective_start_date=date(2026, 7, 1),
            effective_end_date=date(2026, 8, 1),
        ),
    )


def _service(tmp_path: Path, generator: _Generator) -> tuple[Any, Any]:
    revisions = import_module("core.preview.revisions")
    artifacts = import_module("core.preview.artifacts")
    store = artifacts.LocalPreviewArtifactStore(tmp_path)
    identifiers = iter(f"revision-{index}" for index in range(1, 50))
    return (
        revisions.PreviewRevisionService(
            artifact_store=store,
            package_generator=generator,
            clock=lambda: datetime(2026, 8, 4, tzinfo=UTC),
            revision_id_factory=lambda: next(identifiers),
        ),
        store,
    )


def _publish(service: Any, backend: _Backend) -> tuple[Any, ...]:
    return service.publish_eligible_months(
        tenant_name="production",
        tenant_config=_tenant_config(),
        backend=backend,
        now=datetime(2026, 8, 4, tzinfo=UTC),
    )


def test_publication_state_machine_replaces_only_material_or_settlement_changes(tmp_path: Path) -> None:
    generator = _Generator(
        [
            ("provisional", "v1"),
            ("provisional", "v1"),
            ("provisional", "v2"),
            ("settled", "v2"),
            ("settled", "v3"),
            ("provisional", "v4"),
        ]
    )
    service, _store = _service(tmp_path, generator)
    backend = _Backend()

    results = [_publish(service, backend) for _ in range(6)]

    assert [len(item) for item in results] == [1, 0, 1, 1, 1, 0]
    assert [row.revision_id for row in backend.repository.rows if row.is_current] == ["revision-4"]
    assert [row.supersedes_revision_id for row in backend.repository.rows] == [
        None,
        "revision-1",
        "revision-2",
        "revision-3",
    ]
    assert [row.monthly_status for row in backend.repository.rows] == [
        "provisional",
        "provisional",
        "settled",
        "settled",
    ]


def test_generation_and_persistence_failures_preserve_current_revision(tmp_path: Path) -> None:
    revisions, _mapping, models, generator_module = _modules()
    diagnostic = models.PreviewDiagnostic("generation_failed", "safe failure", True)
    generator = _Generator(
        [
            ("settled", "v1"),
            generator_module.PreviewGenerationError(diagnostic),
            ("settled", "v2"),
        ]
    )
    service, store = _service(tmp_path, generator)
    backend = _Backend()
    initial = _publish(service, backend)[0]

    assert _publish(service, backend) == ()
    assert backend.repository.current[("confluent_cloud", "tenant-1", date(2026, 7, 1))] == initial

    backend.repository.fail_next = revisions.PreviewRevisionConflictError("lost race")
    assert _publish(service, backend) == ()
    assert backend.repository.current[("confluent_cloud", "tenant-1", date(2026, 7, 1))] == initial
    assert store.read_manifest(initial.package.storage_key, initial.package.manifest)
    assert [path.name for path in tmp_path.iterdir() if path.is_dir()] == [initial.package.storage_key]


@pytest.mark.parametrize(
    "diagnostic_code",
    [
        "preview_profile_ineligible",
        "calculation_coverage_incomplete",
        "preview_source_classification_failed",
        "preview_allocation_lineage_missing",
        "preview_reconciliation_failed",
        "preview_mapping_validation_failed",
        "preview_package_render_failed",
    ],
)
def test_each_validation_failure_category_publishes_nothing_and_preserves_current(
    tmp_path: Path, diagnostic_code: str
) -> None:
    models = import_module("core.preview.models")
    generator_module = import_module("core.preview.generator")
    generator = _Generator(
        [
            ("settled", "v1"),
            generator_module.PreviewGenerationError(
                models.PreviewDiagnostic(diagnostic_code, "redacted failure", False)
            ),
        ]
    )
    service, store = _service(tmp_path, generator)
    backend = _Backend()
    initial = _publish(service, backend)[0]

    assert _publish(service, backend) == ()
    assert backend.repository.current[("confluent_cloud", "tenant-1", date(2026, 7, 1))] == initial
    assert store.read_manifest(initial.package.storage_key, initial.package.manifest)


def test_artifact_staging_failure_publishes_nothing_and_preserves_current(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    generator = _Generator([("settled", "v1"), ("settled", "v2")])
    service, store = _service(tmp_path, generator)
    backend = _Backend()
    initial = _publish(service, backend)[0]

    def fail_stage(**_kwargs: object) -> object:
        raise OSError("private staging path")

    monkeypatch.setattr(store, "stage_data_files", fail_stage)

    assert _publish(service, backend) == ()
    assert backend.repository.current[("confluent_cloud", "tenant-1", date(2026, 7, 1))] == initial


@pytest.mark.parametrize("scenario", ["initial-non-null", "replacement-wrong"])
def test_service_rejects_supersedes_mismatch_before_staging_or_write_uow(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    scenario: str,
) -> None:
    revisions = import_module("core.preview.revisions")
    generator = _Generator([("settled", "v1"), ("settled", "v2")])
    service, store = _service(tmp_path, generator)
    backend = _Backend()
    if scenario == "replacement-wrong":
        assert _publish(service, backend)
    before_rows = tuple(backend.repository.rows)
    before_artifacts = sorted(path.name for path in tmp_path.iterdir())
    before_write_uows = len(backend.write_uows)
    original_candidate = revisions.PreviewRevisionCandidate
    stage_calls = 0
    original_stage = store.stage_data_files

    def mismatched_candidate(**kwargs: Any) -> Any:
        kwargs["supersedes_revision_id"] = "revision-unexpected" if scenario == "initial-non-null" else "revision-wrong"
        return original_candidate(**kwargs)

    def capture_stage(**kwargs: Any) -> Any:
        nonlocal stage_calls
        stage_calls += 1
        return original_stage(**kwargs)

    monkeypatch.setattr(revisions, "PreviewRevisionCandidate", mismatched_candidate)
    monkeypatch.setattr(store, "stage_data_files", capture_stage)

    assert _publish(service, backend) == ()
    assert stage_calls == 0
    assert len(backend.write_uows) == before_write_uows
    assert tuple(backend.repository.rows) == before_rows
    assert sorted(path.name for path in tmp_path.iterdir()) == before_artifacts


def test_staging_recovery_failure_is_retried_before_publication(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    generator = _Generator([("settled", "v1")])
    service, store = _service(tmp_path, generator)
    backend = _Backend()
    calls = 0
    original = store.cleanup_staging

    def flaky_cleanup() -> int:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise OSError("private path must not escape")
        return original()

    monkeypatch.setattr(store, "cleanup_staging", flaky_cleanup)

    assert _publish(service, backend) == ()
    assert _publish(service, backend)
    assert calls == 2


def test_first_scheduled_pass_seeds_every_eligible_month_including_header_only(tmp_path: Path) -> None:
    config = import_module("core.config.models")
    generator = _Generator([("settled", "april"), ("settled", ""), ("settled", "june")])
    service, _store = _service(tmp_path, generator)
    backend = _Backend()
    tenant = config.TenantConfig(
        ecosystem="confluent_cloud",
        tenant_id="tenant-1",
        lookback_days=130,
        cutoff_days=5,
        focus_preview=config.FocusPreviewTenantConfig(
            commercial_profile="direct_payg",
            effective_start_date=date(2026, 4, 1),
            effective_end_date=date(2026, 7, 1),
        ),
    )

    published = service.publish_eligible_months(
        tenant_name="production",
        tenant_config=tenant,
        backend=backend,
        now=datetime(2026, 8, 4, tzinfo=UTC),
    )

    assert [item.month for item in published] == ["2026-04", "2026-05", "2026-06"]
    assert [request.grain for request in generator.requests] == ["monthly", "monthly", "monthly"]
    assert all(request.column_profile == "full" for request in generator.requests)
    assert backend.repository.rows[1].package.files[0].size_bytes > 0


def test_scheduled_publication_never_creates_preview_request_rows(tmp_path: Path) -> None:
    generator = _Generator([("settled", "v1")])
    service, _store = _service(tmp_path, generator)
    backend = _Backend()

    assert _publish(service, backend)
    assert backend.request_repository.created == []
    assert all(uow.requests is backend.request_repository for uow in (*backend.read_uows, *backend.write_uows))
    persistence = import_module("core.preview.persistence")
    assert all(isinstance(uow, persistence.PreviewReadUnitOfWork) for uow in backend.read_uows)
    assert all(isinstance(uow, persistence.PreviewWriteUnitOfWork) for uow in backend.write_uows)
    assert all(not isinstance(uow, persistence.PreviewWriteUnitOfWork) for uow in backend.read_uows)


def test_revision_service_borrows_generator_and_artifact_store(tmp_path: Path) -> None:
    generator = _Generator([("settled", "v1")])
    service, store = _service(tmp_path, generator)

    assert not hasattr(service, "close")
    assert store.cleanup_staging() == 0
