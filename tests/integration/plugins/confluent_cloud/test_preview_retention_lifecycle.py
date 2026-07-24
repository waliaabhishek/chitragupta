from __future__ import annotations

import logging
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING
from unittest.mock import MagicMock

from sqlalchemy import create_engine, text

import workflow_runner
from core.config.models import AppSettings
from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend
from plugins.confluent_cloud.storage.module import CCloudStorageModule
from tests.integration.core.api.test_focus_preview_revision_publication import _seed_month
from tests.unit.core.preview.test_service import _tenant_config
from workflow_runner import TenantRuntime, WorkflowRunner

if TYPE_CHECKING:
    import pytest


def _counts(connection_string: str) -> dict[str, int]:
    engine = create_engine(connection_string)
    tables = (
        "ccloud_billing",
        "chargeback_facts",
        "pipeline_state",
        "ccloud_cost_source_records",
        "ccloud_allocation_lineage_runs",
        "ccloud_allocation_lineage_portions",
    )
    try:
        with engine.connect() as connection:
            return {
                table: int(connection.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar_one()) for table in tables
            }
    finally:
        engine.dispose()


def _runner(backend: SQLModelBackend, connection_string: str) -> WorkflowRunner:
    tenant = _tenant_config(connection_string).model_copy(update={"retention_days": 15})
    runner = WorkflowRunner(AppSettings(tenants={"production": tenant}), MagicMock())
    runner._tenant_runtimes["production"] = TenantRuntime(  # noqa: SLF001
        tenant_name="production",
        plugin=MagicMock(),
        storage=backend,
        orchestrator=MagicMock(),
        config_hash=workflow_runner._config_hash(tenant),  # noqa: SLF001
        created_at=datetime(2026, 8, 1, tzinfo=UTC),
    )
    return runner


def test_real_retention_uses_whole_calculation_day_reconciles_orphans_and_is_idempotent(
    tmp_path: Path,
) -> None:
    connection_string = f"sqlite:///{tmp_path / 'retention.db'}"
    backend = SQLModelBackend(
        connection_string,
        CCloudStorageModule(),
        use_migrations=False,
        focus_preview_enabled=True,
    )
    backend.create_tables()
    _seed_month(backend, billed_cost=Decimal("8"))
    engine = create_engine(connection_string)
    with engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO ccloud_allocation_lineage_runs "
                "(ecosystem, tenant_id, tracking_date, calculation_id, calculation_completed_at, "
                "capture_status, capture_reason, portion_count) "
                "VALUES ('confluent_cloud', 'tenant-other', :tracking_date, 'foreign-calculation', "
                ":completed_at, 'unavailable', 'source_unavailable', 0)"
            ),
            {
                "tracking_date": date(2026, 7, 1),
                "completed_at": datetime(2026, 7, 3, tzinfo=UTC),
            },
        )
    engine.dispose()
    runner = _runner(backend, connection_string)
    cleanup_now = datetime(2026, 8, 1, 15, 30, tzinfo=UTC)

    try:
        before = _counts(connection_string)
        assert before == {
            "ccloud_billing": 31,
            "chargeback_facts": 31,
            "pipeline_state": 31,
            "ccloud_cost_source_records": 31,
            "ccloud_allocation_lineage_runs": 32,
            "ccloud_allocation_lineage_portions": 31,
        }

        runner._cleanup_retention(now=cleanup_now)  # noqa: SLF001

        after_boundary = _counts(connection_string)
        assert after_boundary == {
            "ccloud_billing": 15,
            "chargeback_facts": 15,
            "pipeline_state": 15,
            "ccloud_cost_source_records": 15,
            "ccloud_allocation_lineage_runs": 16,
            "ccloud_allocation_lineage_portions": 15,
        }
        engine = create_engine(connection_string)
        with engine.connect() as connection:
            retained_dates = (
                connection.execute(
                    text(
                        "SELECT tracking_date FROM ccloud_allocation_lineage_runs "
                        "WHERE tenant_id = 'tenant-1' ORDER BY tracking_date"
                    )
                )
                .scalars()
                .all()
            )
            foreign_rows = connection.execute(
                text("SELECT COUNT(*) FROM ccloud_allocation_lineage_runs WHERE tenant_id = 'tenant-other'")
            ).scalar_one()
        engine.dispose()
        assert retained_dates[0] == "2026-07-17"
        assert retained_dates[-1] == "2026-07-31"
        assert foreign_rows == 1

        engine = create_engine(connection_string)
        with engine.begin() as connection:
            deleted_billing = connection.execute(
                text("DELETE FROM ccloud_billing WHERE tenant_id = 'tenant-1' AND timestamp = '2026-07-20 00:00:00'")
            )
            assert deleted_billing.rowcount == 1
            connection.execute(
                text(
                    "UPDATE pipeline_state SET calculation_id = 'replacement-calculation' "
                    "WHERE tenant_id = 'tenant-1' AND tracking_date = '2026-07-21'"
                )
            )
        engine.dispose()

        runner._cleanup_retention(now=cleanup_now)  # noqa: SLF001

        after_orphan_reconciliation = _counts(connection_string)
        assert after_orphan_reconciliation == {
            "ccloud_billing": 14,
            "chargeback_facts": 15,
            "pipeline_state": 15,
            "ccloud_cost_source_records": 15,
            "ccloud_allocation_lineage_runs": 14,
            "ccloud_allocation_lineage_portions": 13,
        }

        runner._cleanup_retention(now=cleanup_now)  # noqa: SLF001
        assert _counts(connection_string) == after_orphan_reconciliation
    finally:
        runner.close()
        backend.dispose()


def test_preview_evidence_retention_failure_is_visible_and_retry_commits(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    repositories = __import__(
        "plugins.confluent_cloud.storage.preview_repositories",
        fromlist=["SQLModelPreviewAllocationLineageRepository"],
    )
    repository_type = repositories.SQLModelPreviewAllocationLineageRepository
    real_delete_unretained = repository_type.delete_unretained
    calls = 0

    def fail_once(self: object, ecosystem: str, tenant_id: str, before: date) -> object:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise OSError("evidence retention interrupted")
        return real_delete_unretained(self, ecosystem, tenant_id, before)

    monkeypatch.setattr(repository_type, "delete_unretained", fail_once)
    connection_string = f"sqlite:///{tmp_path / 'retention-retry.db'}"
    backend = SQLModelBackend(
        connection_string,
        CCloudStorageModule(),
        use_migrations=False,
        focus_preview_enabled=True,
    )
    backend.create_tables()
    _seed_month(backend, billed_cost=Decimal("8"))
    runner = _runner(backend, connection_string)
    cleanup_now = datetime(2026, 8, 1, 15, 30, tzinfo=UTC)

    try:
        with caplog.at_level(logging.ERROR, logger="workflow_runner"):
            runner._cleanup_retention(now=cleanup_now)  # noqa: SLF001

        assert "Tenant production: Preview evidence retention cleanup failed" in caplog.messages
        assert "Tenant production: retention cleanup failed" not in caplog.messages
        assert _counts(connection_string) == {
            "ccloud_billing": 15,
            "chargeback_facts": 15,
            "pipeline_state": 15,
            "ccloud_cost_source_records": 31,
            "ccloud_allocation_lineage_runs": 31,
            "ccloud_allocation_lineage_portions": 31,
        }

        caplog.clear()
        runner._cleanup_retention(now=cleanup_now)  # noqa: SLF001

        assert calls == 2
        assert "Tenant production: Preview evidence retention cleanup failed" not in caplog.messages
        assert "Tenant production: retention cleanup failed" not in caplog.messages
        assert _counts(connection_string) == {
            "ccloud_billing": 15,
            "chargeback_facts": 15,
            "pipeline_state": 15,
            "ccloud_cost_source_records": 15,
            "ccloud_allocation_lineage_runs": 15,
            "ccloud_allocation_lineage_portions": 15,
        }
    finally:
        runner.close()
        backend.dispose()


def test_retention_preserves_matching_owner_unavailable_zero_portion_lineage(
    tmp_path: Path,
) -> None:
    connection_string = f"sqlite:///{tmp_path / 'retained-unavailable.db'}"
    backend = SQLModelBackend(
        connection_string,
        CCloudStorageModule(),
        use_migrations=False,
        focus_preview_enabled=True,
    )
    backend.create_tables()
    _seed_month(backend, billed_cost=Decimal("8"))
    retained_date = date(2026, 7, 22)
    engine = create_engine(connection_string)
    with engine.begin() as connection:
        connection.execute(
            text(
                "DELETE FROM ccloud_allocation_lineage_portions "
                "WHERE ecosystem = 'confluent_cloud' AND tenant_id = 'tenant-1' "
                "AND tracking_date = :tracking_date"
            ),
            {"tracking_date": retained_date},
        )
        connection.execute(
            text(
                "UPDATE ccloud_allocation_lineage_runs "
                "SET capture_status = 'unavailable', capture_reason = 'source_unavailable', portion_count = 0 "
                "WHERE ecosystem = 'confluent_cloud' AND tenant_id = 'tenant-1' "
                "AND tracking_date = :tracking_date"
            ),
            {"tracking_date": retained_date},
        )
    engine.dispose()
    runner = _runner(backend, connection_string)

    try:
        runner._cleanup_retention(now=datetime(2026, 8, 1, 15, 30, tzinfo=UTC))  # noqa: SLF001

        engine = create_engine(connection_string)
        with engine.connect() as connection:
            retained = (
                connection.execute(
                    text(
                        "SELECT capture_status, capture_reason, portion_count "
                        "FROM ccloud_allocation_lineage_runs "
                        "WHERE ecosystem = 'confluent_cloud' AND tenant_id = 'tenant-1' "
                        "AND tracking_date = :tracking_date"
                    ),
                    {"tracking_date": retained_date},
                )
                .mappings()
                .one()
            )
            matching_authority = connection.execute(
                text(
                    "SELECT COUNT(*) FROM pipeline_state AS state "
                    "JOIN ccloud_allocation_lineage_runs AS lineage "
                    "ON state.ecosystem = lineage.ecosystem "
                    "AND state.tenant_id = lineage.tenant_id "
                    "AND state.tracking_date = lineage.tracking_date "
                    "AND state.calculation_id = lineage.calculation_id "
                    "WHERE lineage.ecosystem = 'confluent_cloud' AND lineage.tenant_id = 'tenant-1' "
                    "AND lineage.tracking_date = :tracking_date AND state.chargeback_calculated = 1"
                ),
                {"tracking_date": retained_date},
            ).scalar_one()
        engine.dispose()
        assert retained == {
            "capture_status": "unavailable",
            "capture_reason": "source_unavailable",
            "portion_count": 0,
        }
        assert matching_authority == 1
    finally:
        runner.close()
        backend.dispose()
