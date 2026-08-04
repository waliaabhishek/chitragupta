from __future__ import annotations

import logging
import threading
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING
from unittest.mock import MagicMock

from sqlalchemy import create_engine, text

import workflow_runner
from core.config.models import AppSettings
from core.models.topic_attribution import TopicAttributionRow
from core.plugin.registry import PluginRegistry
from core.preview.persistence import PreviewEvidenceStorageBackend
from core.preview.retention import PreviewRetentionOutcomeSet, PreviewRetentionOutcomeStatus
from core.preview.storage_availability import PreviewEvidenceAvailabilityState
from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend
from plugins.confluent_cloud.config import CCloudPluginConfig, TopicAttributionConfig
from plugins.confluent_cloud.plugin import ConfluentCloudPlugin
from plugins.confluent_cloud.storage.module import CCloudStorageModule
from tests.integration.core.api.test_focus_preview_revision_publication import _seed_month
from tests.unit.core.preview.test_service import _tenant_config
from workflow_runner import TenantRuntime, WorkflowRunner

if TYPE_CHECKING:
    from typing import Any

    import pytest

    from core.plugin.protocols import OverlayConfig


def _counts(connection_string: str) -> dict[str, int]:
    engine = create_engine(connection_string)
    tables = (
        "ccloud_billing",
        "chargeback_facts",
        "pipeline_state",
        "topic_attribution_facts",
        "ccloud_cost_source_records",
        "ccloud_source_capture_readiness",
        "ccloud_source_evidence_attempts",
        "ccloud_allocation_lineage_runs",
        "ccloud_allocation_lineage_portions",
        "ccloud_focus_preview_retention_outcomes",
    )
    try:
        with engine.connect() as connection:
            return {
                table: int(connection.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar_one()) for table in tables
            }
    finally:
        engine.dispose()


def _topic_names(backend: SQLModelBackend) -> list[str]:
    with backend.create_unit_of_work() as uow:
        return sorted(
            row.topic_name
            for row in uow.topic_attributions.iter_by_filters(
                "confluent_cloud",
                "tenant-1",
            )
        )


def _cwd_sqlite_artifacts(path: Path) -> list[Path]:
    artifacts: list[Path] = []
    for pattern in ("*.db", "*.db-wal", "*.db-shm"):
        artifacts.extend(sorted(path.glob(pattern)))
    return artifacts


def _latest_retention_outcomes(backend: SQLModelBackend) -> PreviewRetentionOutcomeSet:
    with backend.create_preview_generation_read_unit_of_work() as uow:
        return uow.retention_outcomes.get_latest_for_owner("confluent_cloud", "tenant-1")


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
            "topic_attribution_facts": 0,
            "ccloud_cost_source_records": 31,
            "ccloud_source_capture_readiness": 1,
            "ccloud_source_evidence_attempts": 1,
            "ccloud_allocation_lineage_runs": 32,
            "ccloud_allocation_lineage_portions": 31,
            "ccloud_focus_preview_retention_outcomes": 0,
        }

        runner._cleanup_retention(now=cleanup_now)  # noqa: SLF001

        after_boundary = _counts(connection_string)
        assert after_boundary == {
            "ccloud_billing": 15,
            "chargeback_facts": 15,
            "pipeline_state": 15,
            "topic_attribution_facts": 0,
            "ccloud_cost_source_records": 15,
            "ccloud_source_capture_readiness": 1,
            "ccloud_source_evidence_attempts": 1,
            "ccloud_allocation_lineage_runs": 16,
            "ccloud_allocation_lineage_portions": 15,
            "ccloud_focus_preview_retention_outcomes": 2,
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
            "topic_attribution_facts": 0,
            "ccloud_cost_source_records": 15,
            "ccloud_source_capture_readiness": 1,
            "ccloud_source_evidence_attempts": 1,
            "ccloud_allocation_lineage_runs": 14,
            "ccloud_allocation_lineage_portions": 13,
            "ccloud_focus_preview_retention_outcomes": 2,
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
        with caplog.at_level(logging.WARNING, logger="workflow_runner"):
            runner._cleanup_retention(now=cleanup_now)  # noqa: SLF001

        assert "preview_evidence_retention_failed" in caplog.text
        assert "tenant_name=production" in caplog.text
        assert "stage=preview_evidence_retention" in caplog.text
        assert "operation=retention_cleanup" in caplog.text
        assert "outcome=failed" in caplog.text
        assert "Tenant production: retention cleanup failed" not in caplog.messages
        assert _counts(connection_string) == {
            "ccloud_billing": 15,
            "chargeback_facts": 15,
            "pipeline_state": 15,
            "topic_attribution_facts": 0,
            "ccloud_cost_source_records": 31,
            "ccloud_source_capture_readiness": 1,
            "ccloud_source_evidence_attempts": 1,
            "ccloud_allocation_lineage_runs": 31,
            "ccloud_allocation_lineage_portions": 31,
            "ccloud_focus_preview_retention_outcomes": 2,
        }

        caplog.clear()
        runner._cleanup_retention(now=cleanup_now)  # noqa: SLF001

        assert calls == 2
        assert "preview_evidence_retention_failed" not in caplog.text
        assert "Tenant production: retention cleanup failed" not in caplog.messages
        assert _counts(connection_string) == {
            "ccloud_billing": 15,
            "chargeback_facts": 15,
            "pipeline_state": 15,
            "topic_attribution_facts": 0,
            "ccloud_cost_source_records": 15,
            "ccloud_source_capture_readiness": 1,
            "ccloud_source_evidence_attempts": 1,
            "ccloud_allocation_lineage_runs": 15,
            "ccloud_allocation_lineage_portions": 15,
            "ccloud_focus_preview_retention_outcomes": 2,
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


def test_focus_retention_uses_production_constructed_cached_runtime_for_ordinary_topic_and_preview_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    invocation_dir = tmp_path / "invocation"
    invocation_dir.mkdir()
    db_dir = tmp_path / "db"
    db_dir.mkdir()
    monkeypatch.chdir(invocation_dir)
    cleanup_now = datetime(2026, 8, 1, 15, 30, tzinfo=UTC)
    connection_string = f"sqlite:///{db_dir / 'production-path-retention.db'}"
    tenant = _tenant_config(connection_string).model_copy(
        update={
            "retention_days": 15,
            "plugin_settings": CCloudPluginConfig.model_validate(
                {
                    "ccloud_api": {
                        "key": "test-key",
                        "secret": "test-secret",  # pragma: allowlist secret
                    },
                    "metrics": {
                        "type": "prometheus",
                        "url": "http://prometheus.invalid:9090",
                    },
                    "topic_attribution": TopicAttributionConfig(enabled=True, retention_days=20),
                }
            ),
        }
    )
    settings = AppSettings(tenants={"production": tenant})
    created_plugins: list[TrackingConfluentCloudPlugin] = []
    created_backends: list[SQLModelBackend] = []
    storage_calls: list[dict[str, object]] = []
    baseline_threads = {(thread.name, thread.ident) for thread in threading.enumerate() if thread.is_alive()}

    class TrackingConfluentCloudPlugin(ConfluentCloudPlugin):
        def __init__(self) -> None:
            super().__init__()
            self.closed = False
            self.initialize_payloads: list[dict[str, object]] = []
            self.storage_module_calls = 0
            self.overlay_requests: list[str] = []
            created_plugins.append(self)

        def initialize(self, config: dict[str, Any]) -> None:
            self.initialize_payloads.append(config)
            super().initialize(config)

        def get_storage_module(self) -> CCloudStorageModule:
            self.storage_module_calls += 1
            return super().get_storage_module()

        def get_overlay_config(self, name: str) -> OverlayConfig | None:
            self.overlay_requests.append(name)
            return super().get_overlay_config(name)

        def close(self) -> None:
            self.closed = True
            super().close()

    registry = PluginRegistry()
    registry.register("confluent_cloud", TrackingConfluentCloudPlugin)
    real_create_storage_backend = __import__(
        "core.storage.registry",
        fromlist=["create_storage_backend"],
    ).create_storage_backend

    def tracking_create_storage_backend(*args: object, **kwargs: object) -> SQLModelBackend:
        storage_calls.append(
            {
                "config": args[0],
                "storage_module": kwargs.get("storage_module"),
                "use_migrations": kwargs.get("use_migrations", True),
                "focus_preview_enabled": kwargs.get("focus_preview_enabled", False),
            }
        )
        backend = real_create_storage_backend(*args, **kwargs)
        created_backends.append(backend)
        return backend

    monkeypatch.setattr("core.storage.registry.create_storage_backend", tracking_create_storage_backend)
    runner = WorkflowRunner(settings, registry)
    plugin: TrackingConfluentCloudPlugin | None = None

    try:
        assert runner._tenant_runtimes == {}  # noqa: SLF001
        assert _cwd_sqlite_artifacts(invocation_dir) == []

        runner.bootstrap_storage()

        assert len(created_plugins) == 1
        plugin = created_plugins[0]
        assert plugin.initialize_payloads == [tenant.plugin_settings.model_dump()]
        assert plugin.storage_module_calls == 1
        assert len(storage_calls) == 1
        assert storage_calls[0]["config"] == tenant.storage
        assert isinstance(storage_calls[0]["storage_module"], CCloudStorageModule)
        assert storage_calls[0]["use_migrations"] is True
        assert storage_calls[0]["focus_preview_enabled"] is True

        runtime = runner._tenant_runtimes["production"]  # noqa: SLF001
        assert runtime.plugin is plugin
        assert runtime.storage is created_backends[0]
        assert isinstance(runtime.storage, SQLModelBackend)
        assert isinstance(runtime.storage, PreviewEvidenceStorageBackend)
        assert runtime.storage.preview_evidence_availability.state is PreviewEvidenceAvailabilityState.READY
        assert runtime.bootstrap_result is not None
        assert runtime.config_hash == workflow_runner._config_hash(tenant)  # noqa: SLF001
        assert (db_dir / "production-path-retention.db").exists()

        with runner.acquire_backend("production", tenant) as leased_backend:
            assert leased_backend is runtime.storage

        _seed_month(runtime.storage, billed_cost=Decimal("8"))
        with runtime.storage.create_unit_of_work() as uow:
            uow.topic_attributions.upsert_batch(
                [
                    TopicAttributionRow(
                        ecosystem="confluent_cloud",
                        tenant_id="tenant-1",
                        timestamp=datetime(2026, 7, 10, tzinfo=UTC),
                        env_id="env-1",
                        cluster_resource_id="lkc-1",
                        topic_name="expired-topic",
                        product_category="KAFKA",
                        product_type="KAFKA_NETWORK_WRITE",
                        attribution_method="bytes_ratio",
                        amount=Decimal("3"),
                    ),
                    TopicAttributionRow(
                        ecosystem="confluent_cloud",
                        tenant_id="tenant-1",
                        timestamp=datetime(2026, 7, 20, tzinfo=UTC),
                        env_id="env-1",
                        cluster_resource_id="lkc-1",
                        topic_name="retained-topic",
                        product_category="KAFKA",
                        product_type="KAFKA_NETWORK_WRITE",
                        attribution_method="bytes_ratio",
                        amount=Decimal("5"),
                    ),
                ]
            )
            uow.commit()

        overlay_requests_before_cleanup = len(plugin.overlay_requests)
        assert _counts(connection_string) == {
            "ccloud_billing": 31,
            "chargeback_facts": 31,
            "pipeline_state": 31,
            "topic_attribution_facts": 2,
            "ccloud_cost_source_records": 31,
            "ccloud_source_capture_readiness": 1,
            "ccloud_source_evidence_attempts": 1,
            "ccloud_allocation_lineage_runs": 31,
            "ccloud_allocation_lineage_portions": 31,
            "ccloud_focus_preview_retention_outcomes": 0,
        }

        with caplog.at_level(logging.WARNING, logger="workflow_runner"):
            runner._cleanup_retention(now=cleanup_now)  # noqa: SLF001

        assert "Tenant production: retention cleanup failed" not in caplog.messages
        assert "preview_evidence_retention_failed" not in caplog.text
        assert runner._tenant_runtimes["production"] is runtime  # noqa: SLF001
        assert len(created_plugins) == 1
        assert plugin.storage_module_calls == 1
        assert len(storage_calls) == 1
        assert len(plugin.overlay_requests) > overlay_requests_before_cleanup
        assert plugin.overlay_requests[-1] == "topic_attribution"
        assert _counts(connection_string) == {
            "ccloud_billing": 15,
            "chargeback_facts": 15,
            "pipeline_state": 15,
            "topic_attribution_facts": 1,
            "ccloud_cost_source_records": 15,
            "ccloud_source_capture_readiness": 1,
            "ccloud_source_evidence_attempts": 1,
            "ccloud_allocation_lineage_runs": 15,
            "ccloud_allocation_lineage_portions": 15,
            "ccloud_focus_preview_retention_outcomes": 2,
        }
        assert _topic_names(runtime.storage) == ["retained-topic"]

        with runtime.storage.create_preview_generation_read_unit_of_work() as uow:
            assert uow.source_readiness.get_current_authority("confluent_cloud", "tenant-1") is not None

        outcomes = _latest_retention_outcomes(runtime.storage)
        assert outcomes.ordinary is not None
        assert outcomes.preview_evidence is not None
        assert outcomes.ordinary.attempted_at == cleanup_now
        assert outcomes.preview_evidence.attempted_at == cleanup_now
        assert outcomes.ordinary.status is PreviewRetentionOutcomeStatus.SUCCESS
        assert outcomes.preview_evidence.status is PreviewRetentionOutcomeStatus.SUCCESS
    finally:
        runner.close()
        if plugin is not None:
            assert plugin.closed is True
        assert runner._tenant_runtimes == {}  # noqa: SLF001
        assert _cwd_sqlite_artifacts(invocation_dir) == []
        assert {
            (thread.name, thread.ident) for thread in threading.enumerate() if thread.is_alive()
        } == baseline_threads
