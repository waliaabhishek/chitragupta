from __future__ import annotations

from contextlib import nullcontext
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from inspect import getsource
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
from alembic import command
from sqlalchemy import MetaData, Table, create_engine, text

from core.preview.evidence import (
    PreviewEvidenceBootstrapReason,
    PreviewEvidenceBootstrapStatus,
    PreviewEvidenceScope,
    PreviewSourceAttempt,
    PreviewSourceEvidence,
    SourceAttemptFailureReason,
    SourceAttemptFinalStatus,
    SourceAttemptStatus,
)
from core.preview.evidence_capture import NativeSourceWindow, PreviewEvidenceBootstrapConflictError
from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend
from plugins.confluent_cloud.preview_bootstrap import (
    CCloudBootstrappedLineageRefresher,
    CCloudPreviewEvidenceBootstrap,
    PreviewEvidenceBootstrapError,
    legacy_capture_id,
)
from plugins.confluent_cloud.storage.module import CCloudStorageModule
from tests.unit.core.storage.test_migration_026_preview_opt_in import _config

NOW = datetime(2026, 7, 22, tzinfo=UTC)
START = NOW - timedelta(days=2)
MID = NOW - timedelta(days=1)


class _UowContext:
    def __init__(self, uow: MagicMock) -> None:
        self.uow = uow
        self.enter_count = 0
        self.exit_count = 0

    def __enter__(self) -> MagicMock:
        self.enter_count += 1
        return self.uow

    def __exit__(self, *args: object) -> None:
        del args
        self.exit_count += 1


class _LineageRefresher:
    def __init__(self) -> None:
        self.capture_ids: list[tuple[str, ...]] = []

    def refresh_bootstrapped_lineage(self, capture_ids: tuple[str, ...]) -> None:
        self.capture_ids.append(capture_ids)


def _attempt() -> PreviewSourceAttempt:
    return PreviewSourceAttempt(
        attempt_sequence=7,
        ecosystem="confluent_cloud",
        tenant_id="tenant-1",
        refresh_token="refresh-1",
        refresh_start=START,
        refresh_end=NOW,
        status=SourceAttemptStatus.PENDING,
        started_at=NOW,
        completed_at=None,
        failure_reason=None,
    )


def _legacy_source(window: NativeSourceWindow) -> PreviewSourceEvidence:
    return PreviewSourceEvidence(
        source_record_id="provider:cost-1",
        identity_scheme="provider_cost_id",
        provider_cost_id="cost-1",
        source_period_start=window.start,
        source_period_end=window.end,
        collection_window_start=window.start,
        collection_window_end=window.end,
        evidence_scope_start=window.start,
        evidence_scope_end=window.end,
        allocation_timestamp=window.start,
        granularity="DAILY",
        native_product="KAFKA",
        native_line_type="KAFKA_STORAGE",
        amount=Decimal("8"),
        original_amount=Decimal("10"),
        discount_amount=Decimal("2"),
        price=Decimal("2"),
        quantity=Decimal("5"),
        unit="GB",
        native_description="Kafka storage usage",
        native_network_access_type="PUBLIC_INTERNET",
        resource_id="lkc-1",
        resource_name="Orders",
        environment_id="env-1",
        native_tier_dimensions=(("tier", "standard"),),
        malformed=False,
        diagnostics=(),
        billing_timestamp=window.start,
        billing_env_id="env-1",
        billing_resource_id="lkc-1",
        billing_product_type="KAFKA_STORAGE",
        billing_product_category="KAFKA",
        ecosystem="confluent_cloud",
        tenant_id="tenant-1",
        retention_timestamp=window.start,
        raw_payload_json='{"id":"cost-1"}',
    )


def _bootstrap(
    *,
    authority: object | None = None,
    windows: tuple[NativeSourceWindow, ...] = (),
    lineage_refresher: object | None = None,
) -> tuple[CCloudPreviewEvidenceBootstrap, MagicMock, _UowContext, _LineageRefresher | object]:
    uow = MagicMock()
    refresher = _LineageRefresher() if lineage_refresher is None else lineage_refresher
    uow.allocation_lineage = refresher
    uow.source_readiness.get_current_authority.return_value = authority
    uow.source_readiness.begin_attempt.return_value = _attempt()
    uow.source_windows.list_unassociated_windows.return_value = windows
    uow.source_windows.iter_unassociated_window.side_effect = lambda *_args: iter((_legacy_source(_args[-1]),))
    uow.source_windows.associate_legacy_window.return_value = 1
    uow.savepoint.return_value = nullcontext()
    context = _UowContext(uow)
    backend = MagicMock()
    backend.create_preview_evidence_unit_of_work.return_value = context
    bootstrap = CCloudPreviewEvidenceBootstrap(
        backend,
        clock=lambda: NOW,
        capture_id_factory=lambda *args: "legacy:capture-1",
        refresh_token_factory=lambda: "refresh-1",
    )
    return bootstrap, uow, context, refresher


def _run(bootstrap: CCloudPreviewEvidenceBootstrap) -> Any:
    return bootstrap.bootstrap_owner(
        ecosystem="confluent_cloud",
        tenant_id="tenant-1",
        policy_start=START,
        policy_end=NOW,
    )


@pytest.mark.parametrize(
    ("authority", "windows", "status", "reason"),
    [
        (object(), (), PreviewEvidenceBootstrapStatus.ALREADY_CURRENT, None),
        (
            None,
            (),
            PreviewEvidenceBootstrapStatus.UNAVAILABLE,
            PreviewEvidenceBootstrapReason.NO_LEGACY_EVIDENCE,
        ),
    ],
)
def test_bootstrap_no_write_outcomes_own_one_uow_and_rollback(
    authority: object | None,
    windows: tuple[NativeSourceWindow, ...],
    status: PreviewEvidenceBootstrapStatus,
    reason: PreviewEvidenceBootstrapReason | None,
) -> None:
    bootstrap, uow, context, refresher = _bootstrap(authority=authority, windows=windows)

    result = _run(bootstrap)

    assert result.status is status
    assert result.reason is reason
    assert context.enter_count == context.exit_count == 1
    uow.rollback.assert_called_once_with()
    uow.commit.assert_not_called()
    uow.source_readiness.begin_attempt.assert_not_called()
    assert isinstance(refresher, _LineageRefresher)
    assert refresher.capture_ids == []


def test_valid_legacy_evidence_commits_once_in_the_single_owned_uow() -> None:
    windows = (NativeSourceWindow(START, MID), NativeSourceWindow(MID, NOW))
    bootstrap, uow, context, refresher = _bootstrap(windows=windows)

    result = _run(bootstrap)

    assert result.status is PreviewEvidenceBootstrapStatus.BOOTSTRAPPED
    assert result.bootstrapped_windows == 2
    assert result.bootstrapped_rows == 2
    assert context.enter_count == context.exit_count == 1
    uow.commit.assert_called_once_with()
    uow.rollback.assert_not_called()
    uow.source_readiness.finalize_attempt.assert_called_once_with(
        7,
        SourceAttemptFinalStatus.COMPLETE,
        completed_at=NOW,
        reason=None,
    )
    assert isinstance(refresher, _LineageRefresher)
    assert refresher.capture_ids == [("legacy:capture-1", "legacy:capture-1")]


def test_bootstrap_requires_typed_lineage_refresh_capability_and_rolls_back() -> None:
    bootstrap, uow, context, _ = _bootstrap(
        windows=(NativeSourceWindow(START, NOW),),
        lineage_refresher=object(),
    )

    with pytest.raises(PreviewEvidenceBootstrapError, match="lineage refresh capability"):
        _run(bootstrap)

    assert context.enter_count == context.exit_count == 1
    uow.rollback.assert_called_once_with()
    uow.commit.assert_not_called()
    uow.source_readiness.finalize_attempt.assert_not_called()


def test_bootstrap_lineage_refresh_failure_rolls_back_without_finalizing() -> None:
    refresher = MagicMock(spec=CCloudBootstrappedLineageRefresher)
    refresher.refresh_bootstrapped_lineage.side_effect = ValueError("lineage unavailable")
    bootstrap, uow, context, _ = _bootstrap(
        windows=(NativeSourceWindow(START, NOW),),
        lineage_refresher=refresher,
    )

    with pytest.raises(PreviewEvidenceBootstrapError, match="legacy Preview evidence bootstrap failed"):
        _run(bootstrap)

    assert context.enter_count == context.exit_count == 1
    refresher.refresh_bootstrapped_lineage.assert_called_once_with(("legacy:capture-1",))
    uow.rollback.assert_called_once_with()
    uow.commit.assert_not_called()
    uow.source_readiness.finalize_attempt.assert_not_called()


def test_bootstrap_window_plan_retains_only_lightweight_metadata() -> None:
    from plugins.confluent_cloud.preview_bootstrap import _BootstrapWindowPlan

    assert set(_BootstrapWindowPlan.__dataclass_fields__) == {
        "window",
        "source_count",
        "capture_id",
    }


def test_bootstrap_streams_ordered_legacy_rows_without_whole_window_materialization() -> None:
    from plugins.confluent_cloud.storage.preview_repositories import (
        SQLModelPreviewSourceWindowRepository,
    )

    repository_source = getsource(SQLModelPreviewSourceWindowRepository.iter_unassociated_window)
    bootstrap_source = getsource(CCloudPreviewEvidenceBootstrap._bootstrap_in_uow)

    assert ".all()" not in repository_source
    assert "evidence_scope_start" in repository_source
    assert "evidence_scope_end" in repository_source
    assert "source_record_id" in repository_source
    assert "identity_scheme" in repository_source
    assert "tuple(source_windows.iter_unassociated_window" not in bootstrap_source
    assert "list(source_windows.iter_unassociated_window" not in bootstrap_source


def test_invalid_legacy_evidence_commits_failed_attempt_without_association() -> None:
    windows = (
        NativeSourceWindow(START, MID),
        NativeSourceWindow(MID + timedelta(seconds=1), NOW),
    )
    bootstrap, uow, context, _ = _bootstrap(windows=windows)

    result = _run(bootstrap)

    assert result.reason is PreviewEvidenceBootstrapReason.INVALID_LEGACY_EVIDENCE
    assert context.enter_count == context.exit_count == 1
    uow.commit.assert_called_once_with()
    uow.source_windows.associate_legacy_window.assert_not_called()
    uow.source_readiness.finalize_attempt.assert_called_once_with(
        7,
        SourceAttemptFinalStatus.FAILED,
        completed_at=NOW,
        reason=SourceAttemptFailureReason.BOOTSTRAP_INVALID,
    )


def test_compare_and_set_conflict_rolls_back_savepoint_and_commits_failed_attempt() -> None:
    bootstrap, uow, context, _ = _bootstrap(windows=(NativeSourceWindow(START, NOW),))
    uow.source_windows.associate_legacy_window.side_effect = PreviewEvidenceBootstrapConflictError("changed")

    result = _run(bootstrap)

    assert result.reason is PreviewEvidenceBootstrapReason.CONCURRENT_CHANGE
    assert context.enter_count == context.exit_count == 1
    uow.commit.assert_called_once_with()
    uow.source_readiness.replace_overlapping.assert_not_called()
    uow.source_readiness.finalize_attempt.assert_called_once_with(
        7,
        SourceAttemptFinalStatus.FAILED,
        completed_at=NOW,
        reason=SourceAttemptFailureReason.BOOTSTRAP_CONCURRENT_CHANGE,
    )


def test_bootstrap_commit_failure_rolls_back_once_closes_and_raises_preview_only_error() -> None:
    bootstrap, uow, context, _ = _bootstrap(windows=(NativeSourceWindow(START, NOW),))
    uow.commit.side_effect = RuntimeError("commit failed")

    with pytest.raises(PreviewEvidenceBootstrapError, match="legacy Preview evidence bootstrap failed"):
        _run(bootstrap)

    assert context.enter_count == context.exit_count == 1
    uow.commit.assert_called_once_with()
    uow.rollback.assert_called_once_with()


def _seed_legacy_source(
    url: str,
    revision: str,
    *,
    overrides: dict[str, object] | None = None,
) -> None:
    command.upgrade(_config(url, selection="confluent_cloud"), revision)
    values: dict[str, object] = {
        "ecosystem": "confluent_cloud",
        "tenant_id": "tenant-1",
        "source_record_id": "provider:cost-1",
        "identity_scheme": "provider_cost_id",
        "provider_cost_id": "cost-1",
        "source_period_start": datetime(2026, 7, 1, tzinfo=UTC),
        "source_period_end": datetime(2026, 7, 2, tzinfo=UTC),
        "collection_window_start": datetime(2026, 7, 1, tzinfo=UTC),
        "collection_window_end": datetime(2026, 7, 2, tzinfo=UTC),
        "evidence_scope_start": datetime(2026, 7, 1, tzinfo=UTC),
        "evidence_scope_end": datetime(2026, 7, 2, tzinfo=UTC),
        "allocation_timestamp": datetime(2026, 7, 1, tzinfo=UTC),
        "retention_timestamp": datetime(2026, 7, 1, tzinfo=UTC),
        "granularity": "DAILY",
        "product": "KAFKA",
        "line_type": "KAFKA_STORAGE",
        "amount": "8.000",
        "original_amount": "10.000",
        "discount_amount": "2.000",
        "price": "2.000",
        "quantity": "5.000",
        "unit": "GB",
        "description": "Kafka storage usage",
        "network_access_type": "PUBLIC_INTERNET",
        "resource_id": "lkc-1",
        "resource_name": "Orders",
        "environment_id": "env-1",
        "tier_dimensions_json": '{"tier":"standard"}',
        "malformed": False,
        "diagnostics_json": "[]",
        "raw_payload_json": '{"id":"cost-1"}',
    }
    if revision == "021":
        values.update(
            {
                "billing_timestamp": datetime(2026, 7, 1, tzinfo=UTC),
                "billing_env_id": "env-1",
                "billing_resource_id": "lkc-1",
                "billing_product_type": "KAFKA_STORAGE",
                "billing_product_category": "KAFKA",
            }
        )
    values.update(overrides or {})
    engine = create_engine(url)
    try:
        source = Table("ccloud_cost_source_records", MetaData(), autoload_with=engine)
        with engine.begin() as connection:
            connection.execute(source.insert().values(**values))
    finally:
        engine.dispose()
    command.upgrade(_config(url, selection="confluent_cloud"), "head")


def _real_backend(tmp_path: Path, revision: str, **overrides: object) -> SQLModelBackend:
    url = f"sqlite:///{tmp_path / f'legacy-{revision}.db'}"
    _seed_legacy_source(url, revision, overrides=overrides)
    backend = SQLModelBackend(
        url,
        CCloudStorageModule(),
        use_migrations=False,
        focus_preview_enabled=True,
    )
    backend.create_tables()
    return backend


@pytest.mark.parametrize("revision", ["018", "021"])
def test_real_legacy_mapper_bootstrap_assigns_capture_from_retained_values(
    tmp_path: Path,
    revision: str,
) -> None:
    backend = _real_backend(tmp_path, revision)
    window = NativeSourceWindow(
        datetime(2026, 7, 1, tzinfo=UTC),
        datetime(2026, 7, 2, tzinfo=UTC),
    )
    try:
        with backend.create_preview_evidence_unit_of_work() as uow:
            mapped = tuple(
                uow.source_windows.iter_unassociated_window(
                    "confluent_cloud",
                    "tenant-1",
                    window,
                )
            )
        assert len(mapped) == 1
        source = mapped[0]
        assert source.amount == Decimal("8.000")
        assert source.original_amount == Decimal("10.000")
        assert source.discount_amount == Decimal("2.000")
        assert source.price == Decimal("2.000")
        assert source.quantity == Decimal("5.000")
        assert source.native_tier_dimensions == (("tier", "standard"),)
        expected_capture_id = legacy_capture_id(
            "confluent_cloud",
            "tenant-1",
            window,
            mapped,
        )
        assert (
            expected_capture_id
            == {
                "018": "legacy:v1:92addbd6e027a11d6d177bbf04d2fcdf74ad2e7f849fba6cbef1c43b08abdadb",
                "021": "legacy:v1:ad27fb72dda6ee6ca6c3a12c8e9da5b5a8baa7a05f8e2d4a8fd04a8b95f54204",
            }[revision]
        )

        result = backend.create_preview_evidence_bootstrap().bootstrap_owner(
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
            policy_start=window.start,
            policy_end=window.end,
        )

        assert result.status is PreviewEvidenceBootstrapStatus.BOOTSTRAPPED
        with backend.create_preview_generation_read_unit_of_work() as uow:
            retained = tuple(
                uow.cost_evidence.iter_preview_sources(
                    PreviewEvidenceScope(
                        ecosystem="confluent_cloud",
                        tenant_id="tenant-1",
                        start=window.start,
                        end=window.end,
                    )
                )
            )
            readiness = uow.source_readiness.list_covering(
                "confluent_cloud",
                "tenant-1",
                window.start,
                window.end,
            )
        assert len(retained) == len(readiness) == 1
        assert retained[0].capture_id == readiness[0].capture_id == expected_capture_id
    finally:
        backend.dispose()


def test_revision_018_bootstrap_before_calculation_is_repaired_by_ordinary_lineage(
    tmp_path: Path,
) -> None:
    from core.engine.allocation_lineage import build_allocation_lineage_capture
    from core.models.pipeline import PipelineState
    from core.storage.interface import AllocationLineageRunCapture
    from tests.unit.core.preview.test_service import _aggregate, _allocation

    backend = _real_backend(tmp_path, "018")
    window = NativeSourceWindow(
        datetime(2026, 7, 1, tzinfo=UTC),
        datetime(2026, 7, 2, tzinfo=UTC),
    )
    aggregate = _aggregate()
    allocation = _allocation()
    completed_at = datetime(2026, 7, 3, 2, tzinfo=UTC)
    try:
        result = backend.create_preview_evidence_bootstrap().bootstrap_owner(
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
            policy_start=window.start,
            policy_end=window.end,
        )
        assert result.status is PreviewEvidenceBootstrapStatus.BOOTSTRAPPED

        with backend.create_unit_of_work() as uow:
            uow.billing.upsert(aggregate)
            uow.pipeline_state.upsert(
                PipelineState(
                    ecosystem="confluent_cloud",
                    tenant_id="tenant-1",
                    tracking_date=aggregate.timestamp.date(),
                    billing_gathered=True,
                    resources_gathered=True,
                    chargeback_calculated=True,
                    calculation_id="calculation-1",
                    calculation_completed_at=completed_at,
                    calculation_run_id=None,
                )
            )
            uow.commit()
        run = AllocationLineageRunCapture(
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
            tracking_date=aggregate.timestamp.date(),
            calculation_id="calculation-1",
            captures=(
                build_allocation_lineage_capture(
                    origin=aggregate,
                    rows=(allocation,),
                ),
            ),
        )
        with backend.create_preview_evidence_unit_of_work() as uow:
            uow.allocation_lineage.replace_calculation_lineage(
                run,
                calculation_completed_at=completed_at,
            )
            uow.commit()

        engine = create_engine(backend._connection_string)
        try:
            with engine.connect() as connection:
                association = connection.execute(
                    text(
                        """
                        SELECT billing_env_id, billing_resource_id,
                               billing_product_type, billing_product_category
                        FROM ccloud_cost_source_records
                        """
                    )
                ).one()
                sidecar_count = connection.execute(
                    text("SELECT COUNT(*) FROM ccloud_preview_source_allocation_lineage_portions")
                ).scalar_one()
        finally:
            engine.dispose()
        assert association == ("env-1", "lkc-1", "KAFKA_STORAGE", "KAFKA")
        assert sidecar_count == 1
    finally:
        backend.dispose()


@pytest.mark.parametrize(
    "overrides",
    [
        {"source_period_end": None},
        {
            "source_period_start": None,
            "source_period_end": None,
            "allocation_timestamp": datetime(1970, 1, 1, tzinfo=UTC),
            "retention_timestamp": datetime(2026, 7, 2, tzinfo=UTC),
            "malformed": True,
            "diagnostics_json": '["invalid_date:start_date"]',
            "evidence_scope_start": datetime(2026, 7, 1, 6, tzinfo=UTC),
        },
    ],
    ids=[
        "unpaired-source-period-bounds",
        "malformed-row-scope-differs-from-collection-window",
    ],
)
def test_real_legacy_structural_invalidity_is_a_durable_unavailable_result(
    tmp_path: Path,
    overrides: dict[str, object],
) -> None:
    backend = _real_backend(tmp_path, "021", **overrides)
    try:
        result = backend.create_preview_evidence_bootstrap().bootstrap_owner(
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
            policy_start=datetime(2026, 7, 1, tzinfo=UTC),
            policy_end=datetime(2026, 7, 2, tzinfo=UTC),
        )

        assert result.status is PreviewEvidenceBootstrapStatus.UNAVAILABLE
        assert result.reason is PreviewEvidenceBootstrapReason.INVALID_LEGACY_EVIDENCE
        with backend.create_preview_evidence_unit_of_work() as uow:
            authority = uow.source_readiness.get_current_authority("confluent_cloud", "tenant-1")
        assert authority is not None
        assert authority.status is SourceAttemptStatus.FAILED
        assert authority.failure_reason is SourceAttemptFailureReason.BOOTSTRAP_INVALID
    finally:
        backend.dispose()


@pytest.mark.parametrize(
    "overrides",
    [
        {"tier_dimensions_json": '["not", "a", "mapping"]'},
        {"diagnostics_json": '{"not":"a-list"}'},
        {"raw_payload_json": "not-json"},
        {"amount": "not-a-decimal"},
    ],
    ids=[
        "malformed-tier-dimensions",
        "malformed-diagnostics",
        "malformed-raw-payload",
        "malformed-decimal",
    ],
)
def test_real_legacy_retained_decode_failure_raises_preview_only_error_and_rolls_back(
    tmp_path: Path,
    overrides: dict[str, object],
) -> None:
    backend = _real_backend(tmp_path, "021", **overrides)
    try:
        with pytest.raises(PreviewEvidenceBootstrapError, match="legacy Preview evidence bootstrap failed"):
            backend.create_preview_evidence_bootstrap().bootstrap_owner(
                ecosystem="confluent_cloud",
                tenant_id="tenant-1",
                policy_start=datetime(2026, 7, 1, tzinfo=UTC),
                policy_end=datetime(2026, 7, 2, tzinfo=UTC),
            )
        with backend.create_preview_evidence_unit_of_work() as uow:
            assert uow.source_readiness.get_current_authority("confluent_cloud", "tenant-1") is None
    finally:
        backend.dispose()


def test_real_legacy_repository_failure_raises_preview_only_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backend = _real_backend(tmp_path, "021")
    repository = __import__(
        "plugins.confluent_cloud.storage.preview_repositories",
        fromlist=["SQLModelPreviewSourceWindowRepository"],
    ).SQLModelPreviewSourceWindowRepository

    def unavailable(*_args: object, **_kwargs: object) -> object:
        raise RuntimeError("database read failed")

    monkeypatch.setattr(repository, "iter_unassociated_window", unavailable)
    try:
        with pytest.raises(PreviewEvidenceBootstrapError, match="legacy Preview evidence bootstrap failed"):
            backend.create_preview_evidence_bootstrap().bootstrap_owner(
                ecosystem="confluent_cloud",
                tenant_id="tenant-1",
                policy_start=datetime(2026, 7, 1, tzinfo=UTC),
                policy_end=datetime(2026, 7, 2, tzinfo=UTC),
            )
    finally:
        backend.dispose()


def test_real_lineage_refresh_failure_rolls_back_bootstrap_association(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from plugins.confluent_cloud.storage.preview_repositories import (
        SQLModelPreviewAllocationLineageRepository,
    )

    backend = _real_backend(tmp_path, "018")

    def fail_refresh(
        _self: SQLModelPreviewAllocationLineageRepository,
        _capture_ids: tuple[str, ...],
    ) -> None:
        raise ValueError("generic lineage is unavailable")

    monkeypatch.setattr(
        SQLModelPreviewAllocationLineageRepository,
        "refresh_bootstrapped_lineage",
        fail_refresh,
    )
    window = NativeSourceWindow(
        datetime(2026, 7, 1, tzinfo=UTC),
        datetime(2026, 7, 2, tzinfo=UTC),
    )
    try:
        with pytest.raises(
            PreviewEvidenceBootstrapError,
            match="legacy Preview evidence bootstrap failed",
        ):
            backend.create_preview_evidence_bootstrap().bootstrap_owner(
                ecosystem="confluent_cloud",
                tenant_id="tenant-1",
                policy_start=window.start,
                policy_end=window.end,
            )

        with backend.create_preview_evidence_unit_of_work() as uow:
            assert uow.source_readiness.get_current_authority("confluent_cloud", "tenant-1") is None
            retained = tuple(
                uow.source_windows.iter_unassociated_window(
                    "confluent_cloud",
                    "tenant-1",
                    window,
                )
            )
        assert len(retained) == 1
        assert retained[0].capture_id is None
    finally:
        backend.dispose()
