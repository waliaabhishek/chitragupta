from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

from core.config.models import AppSettings, StorageConfig, TenantConfig
from core.preview.persistence import LineageDeletionCount
from core.preview.retention import (
    PreviewRetentionCleanupKind,
    PreviewRetentionOutcome,
    PreviewRetentionOutcomeStatus,
    retention_failure_diagnostic,
)
from tests.unit.core.preview.evidence_backend_double import preview_evidence_backend_double
from workflow_runner import TenantRuntime, WorkflowRunner, _config_hash

NOW = datetime(2026, 7, 22, tzinfo=UTC)


class _Context:
    def __init__(self, value: MagicMock, *, rollback_on_error: bool = False) -> None:
        self.value = value
        self.rollback_on_error = rollback_on_error

    def __enter__(self) -> MagicMock:
        return self.value

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        del exc, traceback
        if exc_type is not None and self.rollback_on_error:
            self.value.rollback()


def _tenant(tmp_path: Path, *, enabled: bool) -> TenantConfig:
    return TenantConfig(
        ecosystem="confluent_cloud",
        tenant_id="tenant-1",
        retention_days=30,
        storage=StorageConfig(connection_string=f"sqlite:///{tmp_path / 'retention.db'}"),
        focus_preview=(
            {
                "commercial_profile": "direct_payg",
                "billing_currency": "USD",
                "effective_start_date": "2026-01-01",
                "effective_end_date": "2027-01-01",
            }
            if enabled
            else None
        ),
    )


def _runner(tenant: TenantConfig, storage: Any) -> WorkflowRunner:
    runner = WorkflowRunner(AppSettings(tenants={"production": tenant}), MagicMock())
    runner._tenant_runtimes["production"] = TenantRuntime(
        tenant_name="production",
        plugin=MagicMock(),
        storage=storage,
        orchestrator=MagicMock(),
        config_hash=_config_hash(tenant),
        created_at=NOW,
    )
    return runner


def _generic_uow() -> MagicMock:
    uow = MagicMock()
    uow.billing.delete_before.return_value = 0
    uow.resources.delete_before.return_value = 0
    uow.identities.delete_before.return_value = 0
    uow.chargebacks.delete_before.return_value = 0
    uow.pipeline_state.delete_before.return_value = 0
    return uow


def _retention_record_uow() -> MagicMock:
    uow = MagicMock()
    uow.retention_outcomes.upsert_latest.return_value = None
    return uow


def _assert_recorded_outcome(
    uow: MagicMock,
    *,
    kind: PreviewRetentionCleanupKind,
    status: PreviewRetentionOutcomeStatus,
    error: BaseException | None = None,
) -> PreviewRetentionOutcome:
    uow.retention_outcomes.upsert_latest.assert_called_once()
    ecosystem, tenant_id, outcome = uow.retention_outcomes.upsert_latest.call_args.args
    assert ecosystem == "confluent_cloud"
    assert tenant_id == "tenant-1"
    assert isinstance(outcome, PreviewRetentionOutcome)
    assert outcome.owner == "tenant-1"
    assert outcome.cleanup_kind is kind
    assert outcome.status is status
    assert outcome.attempted_at == NOW
    assert outcome.diagnostic == (None if error is None else retention_failure_diagnostic(kind, error))
    return outcome


def test_disabled_retention_commits_generic_cleanup_without_any_evidence_capability_access(
    tmp_path: Path,
) -> None:
    tenant = _tenant(tmp_path, enabled=False)
    storage = MagicMock()
    generic_uow = _generic_uow()
    storage.create_unit_of_work.return_value = _Context(generic_uow)
    runner = _runner(tenant, storage)

    runner._cleanup_retention(now=NOW)

    generic_uow.commit.assert_called_once_with()
    assert "create_preview_evidence_unit_of_work" not in storage.method_calls


def test_retention_skips_tenant_owned_by_repair_or_pipeline(tmp_path: Path) -> None:
    tenant = _tenant(tmp_path, enabled=True)
    storage = preview_evidence_backend_double()
    runner = _runner(tenant, storage)
    runner._running_tenants.add("production")

    runner._cleanup_retention(now=NOW)

    storage.create_unit_of_work.assert_not_called()
    storage.create_preview_evidence_unit_of_work.assert_not_called()


def test_enabled_retention_runs_all_evidence_deletes_in_documented_order_after_generic_commit(
    tmp_path: Path,
) -> None:
    tenant = _tenant(tmp_path, enabled=True)
    storage = preview_evidence_backend_double()
    generic_uow = _generic_uow()
    evidence_uow = MagicMock()
    evidence_record_uow = _retention_record_uow()
    ordinary_record_uow = _retention_record_uow()
    storage.create_unit_of_work.return_value = _Context(generic_uow)
    storage.create_preview_evidence_unit_of_work.side_effect = [
        _Context(evidence_uow),
        _Context(evidence_record_uow),
        _Context(ordinary_record_uow),
    ]
    events: list[str] = []
    generic_uow.commit.side_effect = lambda: events.append("generic:commit")
    generic_uow.pipeline_state.delete_before.side_effect = lambda *args: events.append("pipeline") or 1
    evidence_uow.source_windows.delete_before.side_effect = lambda *args: events.append("source") or 1
    evidence_uow.source_readiness.delete_orphaned_before.side_effect = lambda *args: events.append("readiness") or 1
    evidence_uow.allocation_lineage.delete_before.return_value = LineageDeletionCount(
        portions=0,
        runs=0,
    )
    evidence_uow.allocation_lineage.delete_unretained.side_effect = lambda *args: (
        events.append("lineage") or LineageDeletionCount(portions=1, runs=1)
    )
    evidence_uow.organization_authority.delete_superseded_before.side_effect = lambda *args: (
        events.append("organization") or 1
    )
    evidence_uow.commit.side_effect = lambda: events.append("evidence:commit")
    runner = _runner(tenant, storage)
    evidence_record_uow.retention_outcomes.upsert_latest.side_effect = lambda *args: events.append(
        f"evidence:outcome:lease={runner._runtime_leases.get('production', 0)}"  # noqa: SLF001
    )
    ordinary_record_uow.retention_outcomes.upsert_latest.side_effect = lambda *args: events.append(
        f"ordinary:outcome:lease={runner._runtime_leases.get('production', 0)}"  # noqa: SLF001
    )

    runner._cleanup_retention(now=NOW)

    assert events == [
        "pipeline",
        "generic:commit",
        "source",
        "readiness",
        "lineage",
        "organization",
        "evidence:commit",
        "evidence:outcome:lease=1",
        "ordinary:outcome:lease=1",
    ]
    _assert_recorded_outcome(
        evidence_record_uow,
        kind=PreviewRetentionCleanupKind.PREVIEW_EVIDENCE,
        status=PreviewRetentionOutcomeStatus.SUCCESS,
    )
    _assert_recorded_outcome(
        ordinary_record_uow,
        kind=PreviewRetentionCleanupKind.ORDINARY,
        status=PreviewRetentionOutcomeStatus.SUCCESS,
    )


def test_evidence_retention_failure_rolls_back_only_evidence_after_generic_cleanup_commits(
    tmp_path: Path,
) -> None:
    tenant = _tenant(tmp_path, enabled=True)
    storage = preview_evidence_backend_double()
    generic_uow = _generic_uow()
    evidence_uow = MagicMock()
    evidence_record_uow = _retention_record_uow()
    ordinary_record_uow = _retention_record_uow()
    storage.create_unit_of_work.return_value = _Context(generic_uow)
    storage.create_preview_evidence_unit_of_work.side_effect = [
        _Context(evidence_uow, rollback_on_error=True),
        _Context(evidence_record_uow),
        _Context(ordinary_record_uow),
    ]
    evidence_uow.source_windows.delete_before.side_effect = RuntimeError("evidence delete failed")
    runner = _runner(tenant, storage)

    runner._cleanup_retention(now=NOW)

    generic_uow.commit.assert_called_once_with()
    evidence_uow.rollback.assert_called_once_with()
    evidence_uow.source_readiness.delete_orphaned_before.assert_not_called()
    evidence_uow.allocation_lineage.delete_unretained.assert_not_called()
    evidence_uow.organization_authority.delete_superseded_before.assert_not_called()
    _assert_recorded_outcome(
        evidence_record_uow,
        kind=PreviewRetentionCleanupKind.PREVIEW_EVIDENCE,
        status=PreviewRetentionOutcomeStatus.FAILURE,
        error=RuntimeError("evidence delete failed"),
    )
    _assert_recorded_outcome(
        ordinary_record_uow,
        kind=PreviewRetentionCleanupKind.ORDINARY,
        status=PreviewRetentionOutcomeStatus.SUCCESS,
    )


def test_retention_uses_whole_utc_calculation_day_and_preserves_exact_noncalculation_cutoffs(
    tmp_path: Path,
) -> None:
    tenant = _tenant(tmp_path, enabled=True)
    storage = preview_evidence_backend_double()
    generic_uow = _generic_uow()
    evidence_uow = MagicMock()
    storage.create_unit_of_work.return_value = _Context(generic_uow)
    storage.create_preview_evidence_unit_of_work.return_value = _Context(evidence_uow)
    evidence_uow.source_windows.delete_before.return_value = 0
    evidence_uow.source_readiness.delete_orphaned_before.return_value = 0
    evidence_uow.organization_authority.delete_superseded_before.return_value = 0
    evidence_uow.allocation_lineage.delete_unretained.return_value = LineageDeletionCount(
        portions=0,
        runs=0,
    )
    evidence_uow.allocation_lineage.delete_before.return_value = LineageDeletionCount(
        portions=0,
        runs=0,
    )
    runner = _runner(tenant, storage)
    cleanup_now = datetime(2026, 7, 22, 15, 30, tzinfo=UTC)
    exact_cutoff = datetime(2026, 6, 22, 15, 30, tzinfo=UTC)
    calculation_cutoff = datetime(2026, 6, 22, tzinfo=UTC)

    runner._cleanup_retention(now=cleanup_now)

    generic_uow.billing.delete_before.assert_called_once_with(
        "confluent_cloud",
        "tenant-1",
        calculation_cutoff,
    )
    generic_uow.chargebacks.delete_before.assert_called_once_with(
        "confluent_cloud",
        "tenant-1",
        calculation_cutoff,
    )
    generic_uow.pipeline_state.delete_before.assert_called_once_with(
        "confluent_cloud",
        "tenant-1",
        calculation_cutoff.date(),
    )
    evidence_uow.source_windows.delete_before.assert_called_once_with(
        "confluent_cloud",
        "tenant-1",
        calculation_cutoff,
    )
    evidence_uow.source_readiness.delete_orphaned_before.assert_called_once_with(
        "confluent_cloud",
        "tenant-1",
        calculation_cutoff,
    )
    evidence_uow.allocation_lineage.delete_unretained.assert_called_once_with(
        "confluent_cloud",
        "tenant-1",
        calculation_cutoff.date(),
    )
    generic_uow.resources.delete_before.assert_called_once_with(
        "confluent_cloud",
        "tenant-1",
        exact_cutoff,
    )
    generic_uow.identities.delete_before.assert_called_once_with(
        "confluent_cloud",
        "tenant-1",
        exact_cutoff,
    )
    evidence_uow.organization_authority.delete_superseded_before.assert_called_once_with(
        "confluent_cloud",
        "tenant-1",
        exact_cutoff,
    )


def test_ordinary_repository_failure_records_latest_retention_failure_when_preview_storage_is_available(
    tmp_path: Path,
) -> None:
    tenant = _tenant(tmp_path, enabled=True)
    storage = preview_evidence_backend_double()
    generic_uow = _generic_uow()
    generic_uow.billing.delete_before.side_effect = RuntimeError("ordinary delete failed")
    record_uow = _retention_record_uow()
    storage.create_unit_of_work.return_value = _Context(generic_uow)
    storage.create_preview_evidence_unit_of_work.return_value = _Context(record_uow)
    runner = _runner(tenant, storage)
    lease_counts: list[int] = []
    record_uow.retention_outcomes.upsert_latest.side_effect = lambda *args: lease_counts.append(
        runner._runtime_leases.get("production", 0)  # noqa: SLF001
    )

    runner._cleanup_retention(now=NOW)

    assert lease_counts == [1]
    _assert_recorded_outcome(
        record_uow,
        kind=PreviewRetentionCleanupKind.ORDINARY,
        status=PreviewRetentionOutcomeStatus.FAILURE,
        error=RuntimeError("ordinary delete failed"),
    )
    record_uow.commit.assert_called_once_with()


def test_post_commit_accounting_failure_records_ordinary_retention_failure_without_entering_evidence_cleanup(
    tmp_path: Path,
) -> None:
    tenant = _tenant(tmp_path, enabled=True)
    storage = preview_evidence_backend_double()
    generic_uow = _generic_uow()
    generic_uow.resources.delete_before.return_value = object()
    record_uow = _retention_record_uow()
    storage.create_unit_of_work.return_value = _Context(generic_uow)
    storage.create_preview_evidence_unit_of_work.return_value = _Context(record_uow)
    runner = _runner(tenant, storage)

    runner._cleanup_retention(now=NOW)

    generic_uow.commit.assert_called_once_with()
    _assert_recorded_outcome(
        record_uow,
        kind=PreviewRetentionCleanupKind.ORDINARY,
        status=PreviewRetentionOutcomeStatus.FAILURE,
        error=TypeError("unsupported operand type"),
    )
    record_uow.commit.assert_called_once_with()


def test_outcome_recording_failure_never_changes_cleanup_success_or_raises(
    tmp_path: Path,
) -> None:
    tenant = _tenant(tmp_path, enabled=True)
    storage = preview_evidence_backend_double()
    generic_uow = _generic_uow()
    evidence_uow = MagicMock()
    evidence_uow.source_windows.delete_before.return_value = 0
    evidence_uow.source_readiness.delete_orphaned_before.return_value = 0
    evidence_uow.organization_authority.delete_superseded_before.return_value = 0
    evidence_uow.allocation_lineage.delete_unretained.return_value = LineageDeletionCount(
        portions=0,
        runs=0,
    )
    record_uow = _retention_record_uow()
    record_uow.retention_outcomes.upsert_latest.side_effect = RuntimeError("record latest outcome failed")
    storage.create_unit_of_work.return_value = _Context(generic_uow)
    storage.create_preview_evidence_unit_of_work.side_effect = [
        _Context(evidence_uow),
        _Context(record_uow),
    ]
    runner = _runner(tenant, storage)

    runner._cleanup_retention(now=NOW)

    generic_uow.commit.assert_called_once_with()
    evidence_uow.commit.assert_called_once_with()
    _assert_recorded_outcome(
        record_uow,
        kind=PreviewRetentionCleanupKind.PREVIEW_EVIDENCE,
        status=PreviewRetentionOutcomeStatus.SUCCESS,
    )


def test_retention_failure_stays_tenant_isolated_and_other_cached_tenant_still_runs(
    tmp_path: Path,
) -> None:
    first = _tenant(tmp_path, enabled=True).model_copy(
        update={
            "tenant_id": "tenant-1",
            "storage": StorageConfig(connection_string=f"sqlite:///{tmp_path / 'tenant-1.db'}"),
        }
    )
    second = _tenant(tmp_path, enabled=True).model_copy(
        update={
            "tenant_id": "tenant-2",
            "storage": StorageConfig(connection_string=f"sqlite:///{tmp_path / 'tenant-2.db'}"),
        }
    )
    first_storage = preview_evidence_backend_double()
    second_storage = preview_evidence_backend_double()
    first_generic = _generic_uow()
    first_generic.billing.delete_before.side_effect = RuntimeError("tenant one failed")
    first_record = _retention_record_uow()
    second_generic = _generic_uow()
    second_evidence = MagicMock()
    second_evidence.source_windows.delete_before.return_value = 0
    second_evidence.source_readiness.delete_orphaned_before.return_value = 0
    second_evidence.organization_authority.delete_superseded_before.return_value = 0
    second_evidence.allocation_lineage.delete_unretained.return_value = LineageDeletionCount(
        portions=0,
        runs=0,
    )
    second_record = _retention_record_uow()
    first_storage.create_unit_of_work.return_value = _Context(first_generic)
    first_storage.create_preview_evidence_unit_of_work.return_value = _Context(first_record)
    second_storage.create_unit_of_work.return_value = _Context(second_generic)
    second_storage.create_preview_evidence_unit_of_work.side_effect = [
        _Context(second_evidence),
        _Context(second_record),
    ]
    runner = WorkflowRunner(
        AppSettings(tenants={"first": first, "second": second}),
        MagicMock(),
    )
    runner._tenant_runtimes["first"] = TenantRuntime(
        tenant_name="first",
        plugin=MagicMock(),
        storage=first_storage,
        orchestrator=MagicMock(),
        config_hash=_config_hash(first),
        created_at=NOW,
    )
    runner._tenant_runtimes["second"] = TenantRuntime(
        tenant_name="second",
        plugin=MagicMock(),
        storage=second_storage,
        orchestrator=MagicMock(),
        config_hash=_config_hash(second),
        created_at=NOW,
    )

    runner._cleanup_retention(now=NOW)

    first_record.retention_outcomes.upsert_latest.assert_called_once()
    second_generic.commit.assert_called_once_with()
    second_evidence.commit.assert_called_once_with()
