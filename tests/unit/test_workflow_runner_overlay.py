from __future__ import annotations

import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock, patch

from core.config.models import AppSettings, FeaturesConfig, StorageConfig, TenantConfig
from core.engine.orchestrator import PipelineRunResult
from workflow_runner import TenantRuntime, WorkflowRunner, _config_hash

if TYPE_CHECKING:
    import pytest


def _make_settings(tenants: dict[str, TenantConfig] | None = None) -> AppSettings:
    return AppSettings(tenants=tenants or {}, features=FeaturesConfig())


def _make_tenant(tmp_path: Path, **overrides: Any) -> TenantConfig:
    unique = uuid.uuid4().hex[:8]
    database_path = tmp_path / f"test_{unique}.db"
    defaults: dict[str, Any] = {
        "ecosystem": "eco",
        "tenant_id": "tid",
        "lookback_days": 30,
        "cutoff_days": 5,
        "storage": StorageConfig(connection_string=f"sqlite:///{database_path}"),
        "retention_days": 30,
    }
    defaults.update(overrides)
    return TenantConfig(**defaults)


def _make_mock_backend_with_uow() -> tuple[MagicMock, MagicMock]:
    mock_backend = MagicMock()
    mock_uow = MagicMock()
    mock_backend.create_unit_of_work.return_value.__enter__ = MagicMock(return_value=mock_uow)
    mock_backend.create_unit_of_work.return_value.__exit__ = MagicMock(return_value=False)
    mock_uow.billing.delete_before.return_value = 0
    mock_uow.resources.delete_before.return_value = 0
    mock_uow.identities.delete_before.return_value = 0
    mock_uow.chargebacks.delete_before.return_value = 0
    mock_uow.topic_attributions.delete_before.return_value = 0
    return mock_backend, mock_uow


def test_real_backend_artifacts_are_confined_to_managed_temp_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from core.storage.registry import create_storage_backend

    invocation_dir = tmp_path / "invocation"
    invocation_dir.mkdir()
    monkeypatch.chdir(invocation_dir)
    artifact_patterns = ("test_*.db", "test_*.db-wal", "test_*.db-shm")

    assert not any(artifact for pattern in artifact_patterns for artifact in invocation_dir.glob(pattern))

    tenant = _make_tenant(tmp_path)
    connection_string = tenant.storage.connection_string.get_secret_value()
    database_path = Path(connection_string.removeprefix("sqlite:///")).resolve()
    possible_artifacts = (
        database_path,
        Path(f"{database_path}-wal"),
        Path(f"{database_path}-shm"),
    )
    backend = create_storage_backend(tenant.storage, use_migrations=False)

    try:
        backend.create_tables()

        assert database_path.exists()
        assert all(path.parent == tmp_path for path in possible_artifacts)
        assert not any(artifact for pattern in artifact_patterns for artifact in invocation_dir.glob(pattern))
    finally:
        backend.dispose()

    assert not any(artifact for pattern in artifact_patterns for artifact in invocation_dir.glob(pattern))


class TestRunTenantOverlayConfigAccess:
    """_run_tenant must access topic attribution config via runtime.plugin, not getattr on config."""

    @patch("workflow_runner.ChargebackOrchestrator")
    @patch("core.storage.registry.create_storage_backend")
    def test_run_tenant_accesses_ta_config_via_plugin_get_overlay_config(
        self, mock_storage: MagicMock, mock_orch_cls: MagicMock, tmp_path: Path
    ) -> None:
        from plugins.confluent_cloud.config import TopicAttributionConfig

        ta_config = TopicAttributionConfig(enabled=True)
        mock_plugin = MagicMock()
        mock_plugin.get_overlay_config = MagicMock(return_value=ta_config)
        mock_plugin.get_metrics_source.return_value = None

        mock_backend = MagicMock()
        mock_storage.return_value = mock_backend

        mock_orch = MagicMock()
        mock_orch.run.return_value = PipelineRunResult(
            tenant_name="t1",
            tenant_id="tid1",
            dates_gathered=0,
            dates_calculated=0,
            chargeback_rows_written=0,
        )
        mock_orch_cls.return_value = mock_orch

        registry = MagicMock()
        registry.create.return_value = mock_plugin

        tenant = _make_tenant(tmp_path, ecosystem="eco", tenant_id="tid1")
        settings = _make_settings(tenants={"t1": tenant})
        runner = WorkflowRunner(settings, registry)

        runner._run_tenant("t1", tenant)

        # New behavior: TA config accessed via plugin.get_overlay_config, not config.plugin_settings
        mock_plugin.get_overlay_config.assert_called_with("topic_attribution")

    @patch("workflow_runner.ChargebackOrchestrator")
    @patch("core.storage.registry.create_storage_backend")
    def test_run_tenant_plugin_without_overlay_plugin_handled_gracefully(
        self, mock_storage: MagicMock, mock_orch_cls: MagicMock, tmp_path: Path
    ) -> None:
        from core.plugin.protocols import OverlayPlugin

        mock_plugin = MagicMock(
            spec=[
                "ecosystem",
                "initialize",
                "get_service_handlers",
                "get_cost_input",
                "get_metrics_source",
                "get_fallback_allocator",
                "build_shared_context",
                "get_storage_module",
                "close",
            ]
        )
        mock_plugin.get_metrics_source.return_value = None
        assert not isinstance(mock_plugin, OverlayPlugin)

        mock_backend = MagicMock()
        mock_storage.return_value = mock_backend

        mock_orch = MagicMock()
        mock_orch.run.return_value = PipelineRunResult(
            tenant_name="t1",
            tenant_id="tid1",
            dates_gathered=0,
            dates_calculated=0,
            chargeback_rows_written=0,
        )
        mock_orch_cls.return_value = mock_orch

        registry = MagicMock()
        registry.create.return_value = mock_plugin

        tenant = _make_tenant(tmp_path, ecosystem="eco", tenant_id="tid1")
        settings = _make_settings(tenants={"t1": tenant})
        runner = WorkflowRunner(settings, registry)

        # Must not raise even though plugin does not implement OverlayPlugin
        result = runner._run_tenant("t1", tenant)
        assert result is not None


class TestCleanupRetentionOverlayConfigAccess:
    """_cleanup_retention must access topic attribution config via runtime.plugin, not getattr on config."""

    def test_cleanup_retention_accesses_ta_config_via_plugin(self, tmp_path: Path) -> None:
        from plugins.confluent_cloud.config import TopicAttributionConfig

        ta_config = TopicAttributionConfig(enabled=True, retention_days=45)

        mock_plugin = MagicMock()
        mock_plugin.get_overlay_config = MagicMock(return_value=ta_config)

        mock_backend, mock_uow = _make_mock_backend_with_uow()

        # Tenant config WITHOUT TA in plugin_settings — old getattr path would skip TA cleanup
        tenant = _make_tenant(tmp_path, ecosystem="eco", tenant_id="tid1", retention_days=30)
        settings = _make_settings(tenants={"t1": tenant})
        runner = WorkflowRunner(settings, MagicMock())

        runtime = TenantRuntime(
            tenant_name="t1",
            plugin=mock_plugin,
            storage=mock_backend,
            orchestrator=MagicMock(),
            config_hash=_config_hash(tenant),
            created_at=datetime.now(UTC),
        )
        runner._tenant_runtimes["t1"] = runtime

        runner._cleanup_retention()

        # New behavior: TA config accessed via plugin, not config.plugin_settings
        mock_plugin.get_overlay_config.assert_called_with("topic_attribution")

    def test_cleanup_retention_plugin_without_overlay_plugin_no_ta_cleanup(self, tmp_path: Path) -> None:
        from core.plugin.protocols import OverlayPlugin

        mock_plugin = MagicMock(
            spec=[
                "ecosystem",
                "initialize",
                "get_service_handlers",
                "get_cost_input",
                "get_metrics_source",
                "get_fallback_allocator",
                "build_shared_context",
                "get_storage_module",
                "close",
            ]
        )
        assert not isinstance(mock_plugin, OverlayPlugin)

        mock_backend, mock_uow = _make_mock_backend_with_uow()
        mock_uow.pipeline_state.delete_before.return_value = 0

        tenant = _make_tenant(tmp_path, ecosystem="eco", tenant_id="tid1", retention_days=30)
        settings = _make_settings(tenants={"t1": tenant})
        plugin_registry = MagicMock()
        runner = WorkflowRunner(settings, plugin_registry)

        runtime = TenantRuntime(
            tenant_name="t1",
            plugin=mock_plugin,
            storage=mock_backend,
            orchestrator=MagicMock(),
            config_hash=_config_hash(tenant),
            created_at=datetime.now(UTC),
        )
        runner._tenant_runtimes["t1"] = runtime

        try:
            runner._cleanup_retention()

            plugin_registry.create.assert_not_called()
            mock_uow.billing.delete_before.assert_called_once()
            mock_uow.topic_attributions.delete_before.assert_not_called()
        finally:
            runner.close()

        mock_backend.dispose.assert_called_once_with()
        mock_plugin.close.assert_called_once_with()


class TestRunTenantTaEmitter:
    """GIT-175-04: _run_tenant runs TA emitter when ta_config.enabled=True and emitters configured."""

    @patch("workflow_runner.EmitterRunner")
    @patch("workflow_runner.ChargebackOrchestrator")
    @patch("core.storage.registry.create_storage_backend")
    def test_run_tenant_runs_ta_emitter_when_emitters_configured(
        self,
        mock_storage: MagicMock,
        mock_orch_cls: MagicMock,
        mock_emitter_cls: MagicMock,
        tmp_path: Path,
    ) -> None:
        """When ta_config.enabled=True and emitters list is non-empty, EmitterRunner.run is called."""
        from core.config.models import EmitterSpec
        from plugins.confluent_cloud.config import TopicAttributionConfig

        ta_config = TopicAttributionConfig(
            enabled=True,
            emitters=[EmitterSpec(type="csv", params={"output_dir": "/tmp/ta"})],
        )

        mock_plugin = MagicMock()
        mock_plugin.get_overlay_config = MagicMock(return_value=ta_config)
        mock_plugin.get_metrics_source.return_value = None

        mock_backend = MagicMock()
        mock_storage.return_value = mock_backend

        mock_orch = MagicMock()
        mock_orch.run.return_value = PipelineRunResult(
            tenant_name="t1",
            tenant_id="tid1",
            dates_gathered=0,
            dates_calculated=0,
            chargeback_rows_written=0,
        )
        mock_orch_cls.return_value = mock_orch

        registry = MagicMock()
        registry.create.return_value = mock_plugin

        tenant = _make_tenant(tmp_path, ecosystem="eco", tenant_id="tid1")
        settings = _make_settings(tenants={"t1": tenant})
        runner = WorkflowRunner(settings, registry)

        runner._run_tenant("t1", tenant)

        mock_emitter_cls.assert_called_once()
        mock_emitter_cls.return_value.run.assert_called_once_with("tid1")
