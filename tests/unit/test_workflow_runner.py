from __future__ import annotations

import threading
import time
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from core.config.models import AppSettings, FeaturesConfig, StorageConfig, TenantConfig
from core.engine.orchestrator import PipelineRunResult
from workflow_runner import WorkflowRunner, _create_storage_backend


class TestCreateStorageBackend:
    def test_sqlmodel_backend(self) -> None:
        config = StorageConfig(backend="sqlmodel", connection_string="sqlite:///:memory:")
        backend = _create_storage_backend(config)
        assert backend is not None
        backend.dispose()

    def test_unknown_backend_raises(self) -> None:
        config = StorageConfig(backend="redis", connection_string="redis://localhost")
        with pytest.raises(ValueError, match="Unknown storage backend"):
            _create_storage_backend(config)


def _make_settings(
    tenants: dict[str, TenantConfig] | None = None,
    **features_kwargs: Any,
) -> AppSettings:
    """Helper to build AppSettings with optional feature overrides."""
    features = FeaturesConfig(**features_kwargs) if features_kwargs else FeaturesConfig()
    return AppSettings(
        tenants=tenants or {},
        features=features,
    )


def _make_tenant(**overrides: Any) -> TenantConfig:
    defaults: dict[str, Any] = {
        "ecosystem": "eco",
        "tenant_id": "tid",
        "lookback_days": 30,
        "cutoff_days": 5,
    }
    defaults.update(overrides)
    return TenantConfig(**defaults)


class TestWorkflowRunnerRunOnce:
    def test_no_tenants_returns_empty(self) -> None:
        settings = _make_settings()
        runner = WorkflowRunner(settings, MagicMock())
        assert runner.run_once() == {}

    @patch("workflow_runner.ChargebackOrchestrator")
    @patch("workflow_runner._create_storage_backend")
    def test_runs_tenant(
        self,
        mock_storage: MagicMock,
        mock_orch_cls: MagicMock,
    ) -> None:
        mock_backend = MagicMock()
        mock_storage.return_value = mock_backend
        mock_orch = MagicMock()
        mock_orch.run.return_value = PipelineRunResult(
            tenant_name="t1",
            tenant_id="tid1",
            dates_gathered=3,
            dates_calculated=2,
            chargeback_rows_written=10,
        )
        mock_orch_cls.return_value = mock_orch

        registry = MagicMock()
        plugin = MagicMock()
        plugin.get_metrics_source.return_value = None
        registry.create.return_value = plugin

        settings = _make_settings(tenants={"t1": _make_tenant(tenant_id="tid1")})
        runner = WorkflowRunner(settings, registry)
        results = runner.run_once()

        assert "t1" in results
        assert results["t1"].dates_gathered == 3
        mock_backend.dispose.assert_called_once()
        mock_backend.create_tables.assert_called_once()  # GAP-003

    @patch("workflow_runner.ChargebackOrchestrator")
    @patch("workflow_runner._create_storage_backend")
    def test_error_isolation(
        self,
        mock_storage: MagicMock,
        mock_orch_cls: MagicMock,
    ) -> None:
        """One tenant failure doesn't affect others."""
        mock_backend = MagicMock()
        mock_storage.return_value = mock_backend

        call_count = 0

        def side_effect(*args: Any, **kwargs: Any) -> Any:
            nonlocal call_count
            call_count += 1
            orch = MagicMock()
            if call_count == 1:
                orch.run.side_effect = RuntimeError("boom")
            else:
                orch.run.return_value = PipelineRunResult(
                    tenant_name="t2",
                    tenant_id="tid2",
                    dates_gathered=1,
                    dates_calculated=1,
                    chargeback_rows_written=5,
                )
            return orch

        mock_orch_cls.side_effect = side_effect

        registry = MagicMock()
        plugin = MagicMock()
        plugin.get_metrics_source.return_value = None
        registry.create.return_value = plugin

        settings = _make_settings(
            tenants={
                "t1": _make_tenant(tenant_id="tid1"),
                "t2": _make_tenant(tenant_id="tid2"),
            }
        )
        runner = WorkflowRunner(settings, registry)
        results = runner.run_once()
        assert len(results) == 2
        error_tenants = [name for name, r in results.items() if r.errors]
        assert len(error_tenants) >= 1


class TestGap003StorageBootstrap:
    """GAP-003: create_tables called before orchestrator runs."""

    @patch("workflow_runner.ChargebackOrchestrator")
    @patch("workflow_runner._create_storage_backend")
    def test_create_tables_called(
        self,
        mock_storage: MagicMock,
        mock_orch_cls: MagicMock,
    ) -> None:
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
        plugin = MagicMock()
        plugin.get_metrics_source.return_value = None
        registry.create.return_value = plugin

        settings = _make_settings(tenants={"t1": _make_tenant(tenant_id="tid1")})
        runner = WorkflowRunner(settings, registry)
        runner.run_once()

        mock_backend.create_tables.assert_called_once()


class TestGap010BoundedConcurrency:
    """GAP-010: max_parallel_tenants bounds thread pool size."""

    def test_max_parallel_tenants_config_default(self) -> None:
        cfg = FeaturesConfig()
        assert cfg.max_parallel_tenants == 4

    def test_max_parallel_tenants_config_bounds(self) -> None:
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            FeaturesConfig(max_parallel_tenants=0)
        with pytest.raises(ValidationError):
            FeaturesConfig(max_parallel_tenants=65)

    @patch("workflow_runner.ThreadPoolExecutor")
    @patch("workflow_runner._create_storage_backend")
    def test_pool_size_capped(
        self,
        mock_storage: MagicMock,
        mock_executor_cls: MagicMock,
    ) -> None:
        """With 10 tenants and max_parallel=3, pool uses 3."""
        mock_backend = MagicMock()
        mock_storage.return_value = mock_backend

        # Make the executor context manager return a mock that submits properly
        mock_executor = MagicMock()
        mock_executor.__enter__ = MagicMock(return_value=mock_executor)
        mock_executor.__exit__ = MagicMock(return_value=False)
        mock_executor.submit.return_value = MagicMock()
        mock_executor_cls.return_value = mock_executor

        tenants = {f"t{i}": _make_tenant(tenant_id=f"tid{i}") for i in range(10)}
        settings = _make_settings(tenants=tenants, max_parallel_tenants=3)
        registry = MagicMock()
        plugin = MagicMock()
        plugin.get_metrics_source.return_value = None
        registry.create.return_value = plugin

        runner = WorkflowRunner(settings, registry)

        with patch("workflow_runner.wait", return_value=(set(), set())):
            runner.run_once()

        mock_executor_cls.assert_called_once_with(max_workers=3)


class TestGap002WaitTimeout:
    """GAP-002: global timeout via wait() deadline."""

    @patch("workflow_runner.ChargebackOrchestrator")
    @patch("workflow_runner._create_storage_backend")
    def test_timeout_zero_means_no_timeout(
        self,
        mock_storage: MagicMock,
        mock_orch_cls: MagicMock,
    ) -> None:
        """timeout=0 → effective_timeout=None (no deadline)."""
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
        plugin = MagicMock()
        plugin.get_metrics_source.return_value = None
        registry.create.return_value = plugin

        settings = _make_settings(
            tenants={"t1": _make_tenant(tenant_id="tid1", tenant_execution_timeout_seconds=0)}
        )
        runner = WorkflowRunner(settings, registry)

        from concurrent.futures import wait as real_wait

        with patch("workflow_runner.wait", wraps=real_wait) as mock_wait:
            runner.run_once()
            _, kwargs = mock_wait.call_args
            assert kwargs.get("timeout") is None

    @patch("workflow_runner.ChargebackOrchestrator")
    @patch("workflow_runner._create_storage_backend")
    def test_not_done_futures_marked_timed_out(
        self,
        mock_storage: MagicMock,
        mock_orch_cls: MagicMock,
    ) -> None:
        """Futures in not_done set get timeout error results."""
        mock_backend = MagicMock()
        mock_storage.return_value = mock_backend

        registry = MagicMock()
        plugin = MagicMock()
        plugin.get_metrics_source.return_value = None
        registry.create.return_value = plugin

        settings = _make_settings(
            tenants={"slow": _make_tenant(tenant_id="tid_slow", tenant_execution_timeout_seconds=1)}
        )
        runner = WorkflowRunner(settings, registry)

        # Mock wait to return all futures as not_done
        def fake_wait(futures: Any, timeout: Any = None) -> tuple[set[Any], set[Any]]:
            return set(), set(futures)

        with patch("workflow_runner.wait", side_effect=fake_wait):
            results = runner.run_once()

        assert "slow" in results
        assert len(results["slow"].errors) == 1
        assert "timed out" in results["slow"].errors[0].lower()


class TestGap005PeriodicRefresh:
    """GAP-005: enable_periodic_refresh flag."""

    def test_disabled_runs_single_cycle(self) -> None:
        settings = _make_settings(enable_periodic_refresh=False)
        registry = MagicMock()
        runner = WorkflowRunner(settings, registry)

        call_count = 0
        original_run_once = runner.run_once

        def counting_run_once() -> dict[str, PipelineRunResult]:
            nonlocal call_count
            call_count += 1
            return original_run_once()

        runner.run_once = counting_run_once  # type: ignore[assignment]
        shutdown = threading.Event()
        runner.run_loop(shutdown)
        assert call_count == 1


class TestGap015017PluginMetrics:
    """GAP-015+017: plugin owns metrics source."""

    @patch("workflow_runner.ChargebackOrchestrator")
    @patch("workflow_runner._create_storage_backend")
    def test_plugin_metrics_passed_to_orchestrator(
        self,
        mock_storage: MagicMock,
        mock_orch_cls: MagicMock,
    ) -> None:
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
        fake_metrics = MagicMock()
        plugin = MagicMock()
        plugin.get_metrics_source.return_value = fake_metrics
        registry.create.return_value = plugin

        settings = _make_settings(tenants={"t1": _make_tenant(tenant_id="tid1")})
        runner = WorkflowRunner(settings, registry)
        runner.run_once()

        # Verify orchestrator was called with the plugin's metrics source
        _, call_kwargs = mock_orch_cls.call_args
        # Positional args: name, config, plugin, storage, metrics
        call_args = mock_orch_cls.call_args[0]
        assert call_args[4] is fake_metrics


class TestWorkflowRunnerRunLoop:
    def test_shutdown_event_stops_loop(self) -> None:
        settings = _make_settings(refresh_interval=1)
        registry = MagicMock()
        runner = WorkflowRunner(settings, registry)

        shutdown = threading.Event()

        def stop_soon() -> None:
            time.sleep(0.5)
            shutdown.set()

        t = threading.Thread(target=stop_soon)
        t.start()
        runner.run_loop(shutdown)
        t.join(timeout=5)
        assert shutdown.is_set()

    @patch("workflow_runner.ChargebackOrchestrator")
    @patch("workflow_runner._create_storage_backend")
    def test_run_loop_processes_tenants(
        self,
        mock_storage: MagicMock,
        mock_orch_cls: MagicMock,
    ) -> None:
        mock_backend = MagicMock()
        mock_storage.return_value = mock_backend
        mock_orch = MagicMock()
        mock_orch.run.return_value = PipelineRunResult(
            tenant_name="t1",
            tenant_id="tid1",
            dates_gathered=1,
            dates_calculated=1,
            chargeback_rows_written=5,
        )
        mock_orch_cls.return_value = mock_orch

        registry = MagicMock()
        plugin = MagicMock()
        plugin.get_metrics_source.return_value = None
        registry.create.return_value = plugin

        settings = _make_settings(
            tenants={"t1": _make_tenant(tenant_id="tid1")},
            refresh_interval=1,
        )
        runner = WorkflowRunner(settings, registry)
        shutdown = threading.Event()

        def stop_soon() -> None:
            time.sleep(0.3)
            shutdown.set()

        t = threading.Thread(target=stop_soon)
        t.start()
        runner.run_loop(shutdown)
        t.join(timeout=5)
        assert mock_orch.run.call_count >= 1

    def test_run_loop_handles_errors_in_results(self) -> None:
        settings = _make_settings(refresh_interval=60)
        registry = MagicMock()
        runner = WorkflowRunner(settings, registry)

        error_result = PipelineRunResult(
            tenant_name="t1",
            tenant_id="tid1",
            dates_gathered=0,
            dates_calculated=0,
            chargeback_rows_written=0,
            errors=["something broke"],
        )
        call_count = 0
        shutdown = threading.Event()

        def mock_run_once() -> dict[str, PipelineRunResult]:
            nonlocal call_count
            call_count += 1
            shutdown.set()
            return {"t1": error_result}

        runner.run_once = mock_run_once  # type: ignore[assignment]

        import logging

        records: list[logging.LogRecord] = []

        class RecordingHandler(logging.Handler):
            def emit(self, record: logging.LogRecord) -> None:
                records.append(record)

        handler = RecordingHandler(level=logging.DEBUG)
        wf_logger = logging.getLogger("workflow_runner")
        wf_logger.addHandler(handler)
        orig_level = wf_logger.level
        orig_disabled = wf_logger.disabled
        wf_logger.setLevel(logging.WARNING)
        wf_logger.disabled = False
        try:
            runner.run_loop(shutdown)
        finally:
            wf_logger.removeHandler(handler)
            wf_logger.setLevel(orig_level)
            wf_logger.disabled = orig_disabled

        assert call_count == 1, "run_once was not called"
        assert len(records) > 0, "Expected log records from workflow_runner"
        logged_messages = [r.getMessage() for r in records]
        assert any("completed with errors" in msg for msg in logged_messages)
