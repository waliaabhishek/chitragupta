from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

from core.config.models import AppSettings, StorageConfig, TenantConfig
from core.preview.persistence import LineageDeletionCount
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
    return uow


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
    storage.create_unit_of_work.return_value = _Context(generic_uow)
    storage.create_preview_evidence_unit_of_work.return_value = _Context(evidence_uow)
    events: list[str] = []
    generic_uow.commit.side_effect = lambda: events.append("generic:commit")
    evidence_uow.source_windows.delete_before.side_effect = lambda *args: events.append("source") or 1
    evidence_uow.source_readiness.delete_orphaned_before.side_effect = lambda *args: events.append("readiness") or 1
    evidence_uow.allocation_lineage.delete_before.side_effect = lambda *args: (
        events.append("lineage") or LineageDeletionCount(portions=1, runs=1)
    )
    evidence_uow.organization_authority.delete_superseded_before.side_effect = lambda *args: (
        events.append("organization") or 1
    )
    evidence_uow.commit.side_effect = lambda: events.append("evidence:commit")
    runner = _runner(tenant, storage)

    runner._cleanup_retention(now=NOW)

    assert events == [
        "generic:commit",
        "source",
        "readiness",
        "lineage",
        "organization",
        "evidence:commit",
    ]


def test_evidence_retention_failure_rolls_back_only_evidence_after_generic_cleanup_commits(
    tmp_path: Path,
) -> None:
    tenant = _tenant(tmp_path, enabled=True)
    storage = preview_evidence_backend_double()
    generic_uow = _generic_uow()
    evidence_uow = MagicMock()
    storage.create_unit_of_work.return_value = _Context(generic_uow)
    storage.create_preview_evidence_unit_of_work.return_value = _Context(
        evidence_uow,
        rollback_on_error=True,
    )
    evidence_uow.source_windows.delete_before.side_effect = RuntimeError("evidence delete failed")
    runner = _runner(tenant, storage)

    runner._cleanup_retention(now=NOW)

    generic_uow.commit.assert_called_once_with()
    evidence_uow.rollback.assert_called_once_with()
    evidence_uow.source_readiness.delete_orphaned_before.assert_not_called()
    evidence_uow.allocation_lineage.delete_before.assert_not_called()
    evidence_uow.organization_authority.delete_superseded_before.assert_not_called()
