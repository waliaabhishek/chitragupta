from __future__ import annotations

import logging
import threading
import time
import uuid
from datetime import UTC, datetime
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from core.config.models import AppSettings, FeaturesConfig, StorageConfig, TenantConfig
from core.engine.orchestrator import GatherFailureThresholdError, PipelineRunResult
from workflow_runner import TenantRuntime, WorkflowRunner


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
    unique = uuid.uuid4().hex[:8]
    defaults: dict[str, Any] = {
        "ecosystem": "eco",
        "tenant_id": "tid",
        "lookback_days": 30,
        "cutoff_days": 5,
        "storage": StorageConfig(connection_string=f"sqlite:///test_{unique}.db"),
    }
    defaults.update(overrides)
    return TenantConfig(**defaults)


class TestBootstrapStorage:
    """GAP-003: create_tables called once at startup, not per-tenant per-cycle."""

    @patch("core.storage.registry.create_storage_backend")
    def test_bootstrap_creates_tables_for_all_tenants(self, mock_storage: MagicMock) -> None:
        backends: list[MagicMock] = []

        def make_backend(config: Any, **kwargs: Any) -> MagicMock:
            b = MagicMock()
            backends.append(b)
            return b

        mock_storage.side_effect = make_backend

        settings = _make_settings(
            tenants={
                "t1": _make_tenant(tenant_id="tid1"),
                "t2": _make_tenant(tenant_id="tid2"),
            }
        )
        runner = WorkflowRunner(settings, MagicMock())
        runner.bootstrap_storage()

        assert len(backends) == 2
        for b in backends:
            b.create_tables.assert_called_once()
            b.dispose.assert_called_once()

    @patch("core.storage.registry.create_storage_backend")
    def test_bootstrap_only_runs_once(self, mock_storage: MagicMock) -> None:
        mock_storage.return_value = MagicMock()
        settings = _make_settings(tenants={"t1": _make_tenant(tenant_id="tid1")})
        runner = WorkflowRunner(settings, MagicMock())

        runner.bootstrap_storage()
        runner.bootstrap_storage()

        # Only one backend created — second call is a no-op via _bootstrapped flag
        assert mock_storage.call_count == 1

    @patch("workflow_runner.ChargebackOrchestrator")
    @patch("core.storage.registry.create_storage_backend")
    def test_run_once_auto_bootstraps(self, mock_storage: MagicMock, mock_orch_cls: MagicMock) -> None:
        """run_once calls bootstrap_storage on first call if not already done."""
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

        # Bootstrap call: create_tables + dispose
        # _run_tenant: uses cached runtime, does NOT dispose (dispose deferred to close())
        assert mock_backend.create_tables.call_count == 1
        assert mock_backend.dispose.call_count == 1  # only from bootstrap

    @patch("workflow_runner.ChargebackOrchestrator")
    @patch("core.storage.registry.create_storage_backend")
    def test_run_tenant_does_not_call_create_tables(self, mock_storage: MagicMock, mock_orch_cls: MagicMock) -> None:
        """After bootstrap, _run_tenant does not call create_tables."""
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
        runner.bootstrap_storage()
        mock_backend.reset_mock()  # clear bootstrap calls

        runner.run_once()

        # _run_tenant should NOT call create_tables
        mock_backend.create_tables.assert_not_called()


class TestWorkflowRunnerRunOnce:
    def test_no_tenants_returns_empty(self) -> None:
        settings = _make_settings()
        runner = WorkflowRunner(settings, MagicMock())
        assert runner.run_once() == {}

    @patch("workflow_runner.ChargebackOrchestrator")
    @patch("core.storage.registry.create_storage_backend")
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

    @patch("workflow_runner.ChargebackOrchestrator")
    @patch("core.storage.registry.create_storage_backend")
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
    @patch("core.storage.registry.create_storage_backend")
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
    @patch("core.storage.registry.create_storage_backend")
    def test_timeout_zero_means_no_timeout(
        self,
        mock_storage: MagicMock,
        mock_orch_cls: MagicMock,
    ) -> None:
        """timeout=0 → no deadline; tenant completes without timeout error."""
        mock_backend = MagicMock()
        mock_storage.return_value = mock_backend
        mock_orch = MagicMock()
        mock_orch.run.return_value = PipelineRunResult(
            tenant_name="t1",
            tenant_id="tid1",
            dates_gathered=5,
            dates_calculated=5,
            chargeback_rows_written=10,
        )
        mock_orch_cls.return_value = mock_orch

        registry = MagicMock()
        plugin = MagicMock()
        plugin.get_metrics_source.return_value = None
        registry.create.return_value = plugin

        settings = _make_settings(tenants={"t1": _make_tenant(tenant_id="tid1", tenant_execution_timeout_seconds=0)})
        runner = WorkflowRunner(settings, registry)

        results = runner.run_once()

        assert "t1" in results
        assert not results["t1"].errors, f"Expected no errors but got: {results['t1'].errors}"
        assert results["t1"].dates_gathered == 5

    @patch("workflow_runner.ChargebackOrchestrator")
    @patch("core.storage.registry.create_storage_backend")
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
        assert results["slow"].dates_pending_calculation == 0  # GIT-004: timeout path


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

    def test_disabled_single_cycle_logs_results(self) -> None:
        """Single-cycle path logs results at parity with loop path."""
        settings = _make_settings(enable_periodic_refresh=False)
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
        runner.run_once = lambda: {"t1": error_result}  # type: ignore[assignment]

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
            runner.run_loop(threading.Event())
        finally:
            wf_logger.removeHandler(handler)
            wf_logger.setLevel(orig_level)
            wf_logger.disabled = orig_disabled

        logged_messages = [r.getMessage() for r in records]
        assert any("completed with errors" in msg for msg in logged_messages)


class TestGap015017PluginMetrics:
    """GAP-015+017: plugin owns metrics source."""

    @patch("workflow_runner.ChargebackOrchestrator")
    @patch("core.storage.registry.create_storage_backend")
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
        call_args = mock_orch_cls.call_args[0]
        assert call_args[4] is fake_metrics


class TestTd020PluginInitialization:
    """TD-020 regression: plugin.initialize() called before any method calls."""

    @patch("workflow_runner.ChargebackOrchestrator")
    @patch("core.storage.registry.create_storage_backend")
    def test_plugin_initialized_before_get_metrics_source(
        self,
        mock_storage: MagicMock,
        mock_orch_cls: MagicMock,
    ) -> None:
        """TD-020: plugin.initialize() must be called before get_metrics_source()."""
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

        # Track call order
        call_order: list[str] = []

        def track_initialize(*args: Any, **kwargs: Any) -> None:
            call_order.append("initialize")

        def track_get_metrics() -> None:
            call_order.append("get_metrics_source")
            return None

        plugin.initialize.side_effect = track_initialize
        plugin.get_metrics_source.side_effect = track_get_metrics
        registry.create.return_value = plugin

        settings = _make_settings(tenants={"t1": _make_tenant(tenant_id="tid1")})
        runner = WorkflowRunner(settings, registry)
        runner.run_once()

        # Verify initialize() was called before get_metrics_source()
        assert "initialize" in call_order
        assert "get_metrics_source" in call_order
        assert call_order.index("initialize") < call_order.index("get_metrics_source")


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
    @patch("core.storage.registry.create_storage_backend")
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


class TestTd021TenantRuntimeCaching:
    """TD-021: Persistent runtime objects — caching, config change, close()."""

    @patch("workflow_runner.ChargebackOrchestrator")
    @patch("core.storage.registry.create_storage_backend")
    def test_same_tenant_reuses_runtime(self, mock_storage: MagicMock, mock_orch_cls: MagicMock) -> None:
        """Second run for same tenant uses cached plugin/storage/orchestrator."""
        mock_backend = MagicMock()
        mock_storage.return_value = mock_backend
        good_result = PipelineRunResult(
            tenant_name="t1",
            tenant_id="tid1",
            dates_gathered=1,
            dates_calculated=1,
            chargeback_rows_written=0,
        )
        mock_orch = MagicMock()
        mock_orch.run.return_value = good_result
        mock_orch_cls.return_value = mock_orch

        registry = MagicMock()
        plugin = MagicMock()
        plugin.get_metrics_source.return_value = None
        registry.create.return_value = plugin

        settings = _make_settings(tenants={"t1": _make_tenant(tenant_id="tid1")})
        runner = WorkflowRunner(settings, registry)

        runner.run_once()
        runner.run_once()

        # Plugin and storage (runtime) created only once
        assert registry.create.call_count == 1
        assert mock_orch_cls.call_count == 1
        # Storage: 1 for bootstrap (auto-bootstrapped by run_once) + 1 for runtime = 2 total
        # But the runtime storage is the SAME object (cached), so no extra calls on 2nd run_once
        assert mock_storage.call_count == 2  # bootstrap (disposed) + runtime (persistent)

    @patch("workflow_runner.ChargebackOrchestrator")
    @patch("core.storage.registry.create_storage_backend")
    def test_config_change_invalidates_runtime(self, mock_storage: MagicMock, mock_orch_cls: MagicMock) -> None:
        """When tenant config changes, runtime is closed and a new one is created."""
        mock_backend = MagicMock()
        mock_storage.return_value = mock_backend
        good_result = PipelineRunResult(
            tenant_name="t1",
            tenant_id="tid1",
            dates_gathered=1,
            dates_calculated=1,
            chargeback_rows_written=0,
        )
        mock_orch = MagicMock()
        mock_orch.run.return_value = good_result
        mock_orch_cls.return_value = mock_orch

        registry = MagicMock()
        plugin = MagicMock()
        plugin.get_metrics_source.return_value = None
        registry.create.return_value = plugin

        tenant_v1 = _make_tenant(tenant_id="tid1", lookback_days=30)
        settings = _make_settings(tenants={"t1": tenant_v1})
        runner = WorkflowRunner(settings, registry)
        runner.run_once()

        assert mock_orch_cls.call_count == 1

        # Simulate config change: update tenant in settings
        tenant_v2 = _make_tenant(tenant_id="tid1", lookback_days=60)
        runner._settings = _make_settings(tenants={"t1": tenant_v2})
        runner.run_once()

        # New runtime created due to config change
        assert mock_orch_cls.call_count == 2
        # Old runtime was closed (storage disposed)
        assert mock_backend.dispose.call_count >= 1

    @patch("workflow_runner.ChargebackOrchestrator")
    @patch("core.storage.registry.create_storage_backend")
    def test_close_disposes_all_runtimes(self, mock_storage: MagicMock, mock_orch_cls: MagicMock) -> None:
        """close() disposes storage and closes plugin for every cached runtime."""
        backends: dict[str, MagicMock] = {}
        plugins: dict[str, MagicMock] = {}

        def make_backend(config: Any, **kwargs: Any) -> MagicMock:
            b = MagicMock()
            # Use connection string as key (unique per tenant via _make_tenant unique hex)
            backends[config.connection_string.get_secret_value()] = b
            return b

        mock_storage.side_effect = make_backend

        good_result = PipelineRunResult(
            tenant_name="x",
            tenant_id="x",
            dates_gathered=0,
            dates_calculated=0,
            chargeback_rows_written=0,
        )
        mock_orch = MagicMock()
        mock_orch.run.return_value = good_result
        mock_orch_cls.return_value = mock_orch

        registry = MagicMock()

        def make_plugin() -> MagicMock:
            p = MagicMock()
            p.get_metrics_source.return_value = None
            plugins[id(p)] = p
            return p

        registry.create.side_effect = lambda eco: make_plugin()

        settings = _make_settings(
            tenants={
                "t1": _make_tenant(tenant_id="tid1"),
                "t2": _make_tenant(tenant_id="tid2"),
            }
        )
        runner = WorkflowRunner(settings, registry)
        runner.run_once()

        # Two runtimes cached
        assert len(runner._tenant_runtimes) == 2

        runner.close()

        # All runtimes cleared
        assert len(runner._tenant_runtimes) == 0
        # Each backend disposed once
        for backend in backends.values():
            backend.dispose.assert_called_once()

    @patch("workflow_runner.ChargebackOrchestrator")
    @patch("core.storage.registry.create_storage_backend")
    def test_runtime_last_run_at_updated(self, mock_storage: MagicMock, mock_orch_cls: MagicMock) -> None:
        """last_run_at is updated after each successful orchestrator.run()."""
        mock_backend = MagicMock()
        mock_storage.return_value = mock_backend
        good_result = PipelineRunResult(
            tenant_name="t1",
            tenant_id="tid1",
            dates_gathered=1,
            dates_calculated=1,
            chargeback_rows_written=0,
        )
        mock_orch = MagicMock()
        mock_orch.run.return_value = good_result
        mock_orch_cls.return_value = mock_orch

        registry = MagicMock()
        plugin = MagicMock()
        plugin.get_metrics_source.return_value = None
        registry.create.return_value = plugin

        settings = _make_settings(tenants={"t1": _make_tenant(tenant_id="tid1")})
        runner = WorkflowRunner(settings, registry)

        assert "t1" not in runner._tenant_runtimes

        runner.run_once()

        runtime = runner._tenant_runtimes["t1"]
        assert runtime.last_run_at is not None
        assert runtime.last_run_at <= datetime.now(UTC)

    def test_tenant_runtime_close_calls_plugin_close(self) -> None:
        """TenantRuntime.close() calls plugin.close() directly."""
        from workflow_runner import TenantRuntime

        mock_plugin = MagicMock(spec=["close", "get_metrics_source"])
        mock_storage = MagicMock()

        runtime = TenantRuntime(
            tenant_name="t1",
            plugin=mock_plugin,
            storage=mock_storage,
            orchestrator=MagicMock(),
            config_hash="abc",
            created_at=datetime.now(UTC),
        )
        runtime.close()

        mock_storage.dispose.assert_called_once()
        mock_plugin.close.assert_called_once()

    def test_tenant_runtime_is_healthy_default_true(self) -> None:
        """TenantRuntime.is_healthy() returns True (placeholder)."""
        from workflow_runner import TenantRuntime

        runtime = TenantRuntime(
            tenant_name="t1",
            plugin=MagicMock(),
            storage=MagicMock(),
            orchestrator=MagicMock(),
            config_hash="abc",
            created_at=datetime.now(UTC),
        )
        assert runtime.is_healthy() is True


class TestTenantRuntimeClose:
    def test_close_calls_plugin_close_directly(self) -> None:
        """TenantRuntime.close() calls storage.dispose() and plugin.close()."""
        plugin = MagicMock()
        storage = MagicMock()
        runtime = TenantRuntime(
            tenant_name="t1",
            plugin=plugin,
            storage=storage,
            orchestrator=MagicMock(),
            config_hash="abc",
            created_at=datetime.now(UTC),
        )
        runtime.close()
        storage.dispose.assert_called_once()
        plugin.close.assert_called_once()

    def test_close_does_not_call_get_metrics_source(self) -> None:
        """TenantRuntime.close() should NOT call plugin.get_metrics_source().
        Plugin owns its metrics_source cleanup internally."""
        plugin = MagicMock()
        storage = MagicMock()
        runtime = TenantRuntime(
            tenant_name="t1",
            plugin=plugin,
            storage=storage,
            orchestrator=MagicMock(),
            config_hash="abc",
            created_at=datetime.now(UTC),
        )
        runtime.close()
        plugin.get_metrics_source.assert_not_called()


class TestRunTenant:
    """CT-001: run_tenant() tests."""

    def test_unknown_tenant_raises_value_error(self) -> None:
        settings = _make_settings(tenants={"t1": _make_tenant(tenant_id="tid1")})
        runner = WorkflowRunner(settings, MagicMock())
        with pytest.raises(ValueError, match="Unknown tenant"):
            runner.run_tenant("nonexistent")

    @patch("workflow_runner.ChargebackOrchestrator")
    @patch("core.storage.registry.create_storage_backend")
    def test_run_tenant_auto_bootstraps_if_needed(self, mock_storage: MagicMock, mock_orch_cls: MagicMock) -> None:
        """run_tenant() bootstraps storage for the tenant if not already bootstrapped."""
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
        assert not runner._bootstrapped

        runner.run_tenant("t1")

        # create_tables called via bootstrap_storage() for all tenants
        mock_backend.create_tables.assert_called_once()

    @patch("workflow_runner.ChargebackOrchestrator")
    @patch("core.storage.registry.create_storage_backend")
    def test_run_tenant_delegates_to_run_tenant_method(self, mock_storage: MagicMock, mock_orch_cls: MagicMock) -> None:
        """run_tenant() returns the result from the orchestrator."""
        mock_backend = MagicMock()
        mock_storage.return_value = mock_backend
        expected = PipelineRunResult(
            tenant_name="t1",
            tenant_id="tid1",
            dates_gathered=5,
            dates_calculated=3,
            chargeback_rows_written=20,
        )
        mock_orch = MagicMock()
        mock_orch.run.return_value = expected
        mock_orch_cls.return_value = mock_orch

        registry = MagicMock()
        plugin = MagicMock()
        plugin.get_metrics_source.return_value = None
        registry.create.return_value = plugin

        settings = _make_settings(tenants={"t1": _make_tenant(tenant_id="tid1")})
        runner = WorkflowRunner(settings, registry)
        result = runner.run_tenant("t1")

        assert result.dates_gathered == 5
        assert result.dates_calculated == 3
        assert result.chargeback_rows_written == 20

    @patch("workflow_runner.ChargebackOrchestrator")
    @patch("core.storage.registry.create_storage_backend")
    def test_run_tenant_skips_bootstrap_if_already_bootstrapped(
        self, mock_storage: MagicMock, mock_orch_cls: MagicMock
    ) -> None:
        """run_tenant() does not re-bootstrap if already bootstrapped."""
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
        runner.bootstrap_storage()
        mock_backend.reset_mock()

        runner.run_tenant("t1")

        # create_tables NOT called again
        mock_backend.create_tables.assert_not_called()


class TestCleanupRetention:
    """CT-002: _cleanup_retention() tests."""

    @patch("core.storage.registry.create_storage_backend")
    def test_skips_tenant_with_retention_zero(self, mock_storage: MagicMock) -> None:
        """retention_days <= 0 means disabled; no storage operations.
        Since TenantConfig validates retention_days > 0, we patch the value directly."""
        tenant = _make_tenant(tenant_id="tid1")
        # Bypass field validation to set disabled value
        object.__setattr__(tenant, "retention_days", 0)
        settings = _make_settings(tenants={"t1": tenant})
        runner = WorkflowRunner(settings, MagicMock())
        runner._cleanup_retention()
        mock_storage.assert_not_called()

    def test_calls_delete_before_for_enabled_tenant(self) -> None:
        """retention_days > 0 triggers delete_before on all repos via cached runtime storage."""
        mock_backend = MagicMock()
        mock_uow = MagicMock()
        mock_backend.create_unit_of_work.return_value.__enter__ = MagicMock(return_value=mock_uow)
        mock_backend.create_unit_of_work.return_value.__exit__ = MagicMock(return_value=False)
        mock_uow.billing.delete_before.return_value = 0
        mock_uow.resources.delete_before.return_value = 2
        mock_uow.identities.delete_before.return_value = 1
        mock_uow.chargebacks.delete_before.return_value = 0

        tenant = _make_tenant(tenant_id="tid1", retention_days=30)
        settings = _make_settings(tenants={"t1": tenant})
        runner = WorkflowRunner(settings, MagicMock())
        runtime = TenantRuntime(
            tenant_name="t1",
            plugin=MagicMock(),
            storage=mock_backend,
            orchestrator=MagicMock(),
            config_hash="abc123",
            created_at=datetime.now(UTC),
        )
        runner._tenant_runtimes["t1"] = runtime
        runner._cleanup_retention()

        mock_uow.billing.delete_before.assert_called_once()
        mock_uow.resources.delete_before.assert_called_once()
        mock_uow.identities.delete_before.assert_called_once()
        mock_uow.chargebacks.delete_before.assert_called_once()
        mock_uow.commit.assert_called_once()
        mock_backend.dispose.assert_not_called()

    def test_exception_does_not_propagate(self) -> None:
        """Retention cleanup errors are caught and logged, not re-raised."""
        mock_backend = MagicMock()
        mock_backend.create_unit_of_work.side_effect = RuntimeError("DB down")

        tenant = _make_tenant(tenant_id="tid1", retention_days=30)
        settings = _make_settings(tenants={"t1": tenant})
        runner = WorkflowRunner(settings, MagicMock())
        runtime = TenantRuntime(
            tenant_name="t1",
            plugin=MagicMock(),
            storage=mock_backend,
            orchestrator=MagicMock(),
            config_hash="abc123",
            created_at=datetime.now(UTC),
        )
        runner._tenant_runtimes["t1"] = runtime
        runner._cleanup_retention()  # Should not raise


class TestGap021RunTenantBootstrapLatch:
    """GAP-021: run_tenant() must delegate bootstrap to bootstrap_storage() — latches flag and
    bootstraps ALL tenants, not just the requested one."""

    @patch("workflow_runner.ChargebackOrchestrator")
    @patch("core.storage.registry.create_storage_backend")
    def test_consecutive_run_tenant_same_tenant_bootstraps_once(
        self, mock_storage: MagicMock, mock_orch_cls: MagicMock
    ) -> None:
        """Two consecutive run_tenant() calls for the same tenant must not repeat bootstrap.

        Expected (after fix): create_tables() called exactly 2 times total — once per configured
        tenant during the first call — NOT 4 times (which would indicate missing flag latch).
        """
        backends: list[MagicMock] = []

        def make_backend(config: Any, **kwargs: Any) -> MagicMock:
            b = MagicMock()
            backends.append(b)
            return b

        mock_storage.side_effect = make_backend

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
            tenants={
                "t1": _make_tenant(tenant_id="tid1"),
                "t2": _make_tenant(tenant_id="tid2"),
            }
        )
        runner = WorkflowRunner(settings, registry)

        runner.run_tenant("t1")

        # Flag must be latched after first run_tenant — bug: this assertion fails
        assert runner._bootstrapped is True

        create_tables_after_first = sum(b.create_tables.call_count for b in backends)

        runner.run_tenant("t1")

        # No additional create_tables calls on second run_tenant
        create_tables_after_second = sum(b.create_tables.call_count for b in backends)
        assert create_tables_after_second == create_tables_after_first

        # Total: exactly 2 (one per configured tenant bootstrapped during first call)
        assert create_tables_after_second == 2

    @patch("workflow_runner.ChargebackOrchestrator")
    @patch("core.storage.registry.create_storage_backend")
    def test_run_tenant_different_tenants_bootstraps_all_once(
        self, mock_storage: MagicMock, mock_orch_cls: MagicMock
    ) -> None:
        """run_tenant("t1") must bootstrap ALL configured tenants, not only t1.

        After run_tenant("t1"):
          - Both tenants' create_tables() called (not just t1's)
          - _bootstrapped == True

        After run_tenant("t2"):
          - No additional create_tables() calls
        """
        t1_cfg = _make_tenant(tenant_id="tid1")
        t2_cfg = _make_tenant(tenant_id="tid2")

        # Map connection string → per-backend call tracking
        conn_backends: dict[str, list[MagicMock]] = {
            t1_cfg.storage.connection_string.get_secret_value(): [],
            t2_cfg.storage.connection_string.get_secret_value(): [],
        }

        def make_backend(config: Any, **kwargs: Any) -> MagicMock:
            b = MagicMock()
            key = config.connection_string.get_secret_value()
            conn_backends.setdefault(key, []).append(b)
            return b

        mock_storage.side_effect = make_backend

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

        settings = _make_settings(tenants={"t1": t1_cfg, "t2": t2_cfg})
        runner = WorkflowRunner(settings, registry)

        runner.run_tenant("t1")

        # Flag must be latched — bug: this assertion fails
        assert runner._bootstrapped is True

        t1_conn = t1_cfg.storage.connection_string.get_secret_value()
        t2_conn = t2_cfg.storage.connection_string.get_secret_value()

        t1_create_tables_after_first = sum(b.create_tables.call_count for b in conn_backends[t1_conn])
        t2_create_tables_after_first = sum(b.create_tables.call_count for b in conn_backends[t2_conn])

        # t1 bootstrap must have happened
        assert t1_create_tables_after_first >= 1

        # t2 bootstrap must ALSO have happened — bug: this assertion fails
        assert t2_create_tables_after_first >= 1

        runner.run_tenant("t2")

        # No additional create_tables after second run_tenant
        t1_create_tables_after_second = sum(b.create_tables.call_count for b in conn_backends[t1_conn])
        t2_create_tables_after_second = sum(b.create_tables.call_count for b in conn_backends[t2_conn])
        assert t1_create_tables_after_second == t1_create_tables_after_first
        assert t2_create_tables_after_second == t2_create_tables_after_first


class TestPerTenantRunGuard:
    """TASK-005: Per-tenant run guard prevents concurrent duplicate runs."""

    def test_run_tenant_guard_already_running_returns_flag(self) -> None:
        """_run_tenant returns already_running=True when tenant already in _running_tenants (TASK-005 fix test 5)."""
        settings = _make_settings(tenants={"tenant-a": _make_tenant(tenant_id="tid-a")})
        runner = WorkflowRunner(settings, MagicMock())

        # Manually inject tenant into the running set (simulates concurrent run)
        runner._running_tenants.add("tenant-a")

        config = settings.tenants["tenant-a"]
        result = runner._run_tenant("tenant-a", config)

        assert result.already_running is True
        assert result.dates_gathered == 0

    @patch("workflow_runner.ChargebackOrchestrator")
    @patch("core.storage.registry.create_storage_backend")
    def test_run_tenant_guard_clears_on_exception(self, mock_storage: MagicMock, mock_orch_cls: MagicMock) -> None:
        """_running_tenants cleared in finally even when orchestrator.run() raises (TASK-005 test 6)."""
        mock_storage.return_value = MagicMock()
        mock_orch = MagicMock()
        mock_orch.run.side_effect = RuntimeError("orchestrator exploded")
        mock_orch_cls.return_value = mock_orch

        settings = _make_settings(tenants={"tenant-a": _make_tenant(tenant_id="tid-a")})
        runner = WorkflowRunner(settings, MagicMock())
        runner._bootstrapped = True

        config = settings.tenants["tenant-a"]
        with pytest.raises(RuntimeError):
            runner._run_tenant("tenant-a", config)

        # Guard must have been cleared — finally block ran
        assert "tenant-a" not in runner._running_tenants

    @patch("workflow_runner.ChargebackOrchestrator")
    @patch("core.storage.registry.create_storage_backend")
    def test_run_tenant_guard_different_tenants_do_not_block(
        self, mock_storage: MagicMock, mock_orch_cls: MagicMock
    ) -> None:
        """A different tenant in _running_tenants does not block the requested tenant (TASK-005 fix test 7)."""
        mock_storage.return_value = MagicMock()
        mock_orch = MagicMock()
        mock_orch.run.return_value = PipelineRunResult(
            tenant_name="tenant-b",
            tenant_id="tid-b",
            dates_gathered=3,
            dates_calculated=2,
            chargeback_rows_written=10,
        )
        mock_orch_cls.return_value = mock_orch

        settings = _make_settings(
            tenants={
                "tenant-a": _make_tenant(tenant_id="tid-a"),
                "tenant-b": _make_tenant(tenant_id="tid-b"),
            }
        )
        runner = WorkflowRunner(settings, MagicMock())
        runner._bootstrapped = True

        # Inject tenant-a as already running
        runner._running_tenants.add("tenant-a")

        config_b = settings.tenants["tenant-b"]
        result = runner._run_tenant("tenant-b", config_b)

        # tenant-b must proceed normally — not blocked by tenant-a
        assert result.already_running is False
        assert result.dates_gathered == 3

    def test_is_tenant_running_reflects_running_set(self) -> None:
        """is_tenant_running() returns False before, True while in set, False after removal (TASK-005)."""
        settings = _make_settings(tenants={"tenant-a": _make_tenant(tenant_id="tid-a")})
        runner = WorkflowRunner(settings, MagicMock())

        assert runner.is_tenant_running("tenant-a") is False

        runner._running_tenants.add("tenant-a")
        assert runner.is_tenant_running("tenant-a") is True

        runner._running_tenants.discard("tenant-a")
        assert runner.is_tenant_running("tenant-a") is False


class TestDrain:
    """TASK-005: drain() waits for in-progress runs then closes (GIT-002)."""

    def test_drain_calls_close_when_no_tenants_running(self) -> None:
        """drain() calls close() immediately when _running_tenants is empty."""
        settings = _make_settings()
        runner = WorkflowRunner(settings, MagicMock())

        with patch.object(runner, "close") as mock_close:
            runner.drain(timeout=5)

        mock_close.assert_called_once()

    def test_drain_waits_for_running_tenants_to_clear(self) -> None:
        """drain() blocks until _running_tenants empties, then closes."""
        settings = _make_settings()
        runner = WorkflowRunner(settings, MagicMock())
        runner._running_tenants.add("tenant-a")

        cleared_at: list[float] = []

        def clear_after_delay() -> None:
            time.sleep(0.15)
            with runner._running_lock:
                runner._running_tenants.discard("tenant-a")
            cleared_at.append(time.monotonic())

        t = threading.Thread(target=clear_after_delay)
        t.start()

        with patch.object(runner, "close") as mock_close:
            runner.drain(timeout=2)

        t.join()
        mock_close.assert_called_once()
        # drain() must have waited — close() called after tenant was cleared
        assert cleared_at, "tenant was never cleared"

    def test_drain_times_out_and_still_calls_close(self) -> None:
        """drain() calls close() even when _running_tenants never empties (timeout path)."""
        settings = _make_settings()
        runner = WorkflowRunner(settings, MagicMock())
        runner._running_tenants.add("tenant-stuck")

        with patch.object(runner, "close") as mock_close:
            runner.drain(timeout=0.2)  # short timeout — tenant never clears

        mock_close.assert_called_once()


class TestGatherFailureThresholdHandling:
    """TASK-004: GatherFailureThresholdError permanently suspends tenant instead of sys.exit(1)."""

    def _make_orch_mock(self, mock_orch_cls: MagicMock, side_effect: Exception | None = None) -> MagicMock:
        mock_orch = MagicMock()
        if side_effect is not None:
            mock_orch.run.side_effect = side_effect
        mock_orch_cls.return_value = mock_orch
        return mock_orch

    def _make_registry(self) -> MagicMock:
        registry = MagicMock()
        plugin = MagicMock()
        plugin.get_metrics_source.return_value = None
        registry.create.return_value = plugin
        return registry

    # --- Test 1 ---
    @patch("workflow_runner.ChargebackOrchestrator")
    @patch("core.storage.registry.create_storage_backend")
    def test_run_once_marks_tenant_failed_on_threshold_breach(
        self,
        mock_storage: MagicMock,
        mock_orch_cls: MagicMock,
    ) -> None:
        """run_once(): GatherFailureThresholdError → _failed_tenants populated, result fatal=True."""
        mock_storage.return_value = MagicMock()
        exc = GatherFailureThresholdError("consecutive gather failures exceeded threshold")
        self._make_orch_mock(mock_orch_cls, side_effect=exc)

        settings = _make_settings(tenants={"t1": _make_tenant(tenant_id="tid1")})
        runner = WorkflowRunner(settings, self._make_registry())
        runner._bootstrapped = True

        results = runner.run_once()

        assert "t1" in runner._failed_tenants
        result = results["t1"]
        assert result.fatal is True
        assert any("consecutive" in e or "threshold" in e or "gather" in e for e in result.errors)

    # --- Test 2 ---
    @patch("workflow_runner.ChargebackOrchestrator")
    @patch("core.storage.registry.create_storage_backend")
    def test_run_once_skips_failed_tenant_on_subsequent_call(
        self,
        mock_storage: MagicMock,
        mock_orch_cls: MagicMock,
    ) -> None:
        """After threshold breach in run_once(), second run_once() skips the failed tenant."""
        mock_storage.return_value = MagicMock()
        exc = GatherFailureThresholdError("threshold hit")
        mock_orch = self._make_orch_mock(mock_orch_cls, side_effect=exc)

        settings = _make_settings(tenants={"t1": _make_tenant(tenant_id="tid1")})
        runner = WorkflowRunner(settings, self._make_registry())
        runner._bootstrapped = True

        # First call — triggers threshold breach
        runner.run_once()
        assert "t1" in runner._failed_tenants
        assert mock_orch.run.call_count == 1

        # Second call — tenant is permanently failed; orchestrator.run() must NOT be called again
        results = runner.run_once()
        assert mock_orch.run.call_count == 1  # still 1, not 2
        assert results["t1"].fatal is True

    # --- Test 3 ---
    @patch("workflow_runner.ChargebackOrchestrator")
    @patch("core.storage.registry.create_storage_backend")
    def test_run_tenant_marks_failed_on_threshold_breach(
        self,
        mock_storage: MagicMock,
        mock_orch_cls: MagicMock,
    ) -> None:
        """run_tenant(): GatherFailureThresholdError → _failed_tenants populated, result fatal=True."""
        mock_storage.return_value = MagicMock()
        exc = GatherFailureThresholdError("threshold exceeded")
        self._make_orch_mock(mock_orch_cls, side_effect=exc)

        settings = _make_settings(tenants={"t1": _make_tenant(tenant_id="tid1")})
        runner = WorkflowRunner(settings, self._make_registry())
        runner._bootstrapped = True

        result = runner.run_tenant("t1")

        assert "t1" in runner._failed_tenants
        assert result.fatal is True

    # --- Test 4 ---
    @patch("workflow_runner.ChargebackOrchestrator")
    @patch("core.storage.registry.create_storage_backend")
    def test_run_tenant_skips_permanently_failed_tenant(
        self,
        mock_storage: MagicMock,
        mock_orch_cls: MagicMock,
    ) -> None:
        """run_tenant() on a pre-failed tenant returns fatal=True without calling orchestrator."""
        mock_storage.return_value = MagicMock()
        mock_orch = self._make_orch_mock(mock_orch_cls)

        settings = _make_settings(tenants={"t1": _make_tenant(tenant_id="tid1")})
        runner = WorkflowRunner(settings, self._make_registry())
        runner._bootstrapped = True

        # Pre-populate as permanently failed
        runner._failed_tenants["t1"] = "previously exceeded threshold"  # type: ignore[attr-defined]

        result = runner.run_tenant("t1")

        mock_orch.run.assert_not_called()
        assert result.fatal is True

    # --- Test 5 ---
    @patch("workflow_runner.ChargebackOrchestrator")
    @patch("core.storage.registry.create_storage_backend")
    def test_non_fatal_error_does_not_mark_tenant_failed(
        self,
        mock_storage: MagicMock,
        mock_orch_cls: MagicMock,
    ) -> None:
        """Generic RuntimeError does NOT mark tenant as permanently failed; fatal=False."""
        mock_storage.return_value = MagicMock()
        self._make_orch_mock(mock_orch_cls, side_effect=RuntimeError("transient failure"))

        settings = _make_settings(tenants={"t1": _make_tenant(tenant_id="tid1")})
        runner = WorkflowRunner(settings, self._make_registry())
        runner._bootstrapped = True

        results = runner.run_once()

        assert not runner._failed_tenants  # type: ignore[attr-defined]
        result = results["t1"]
        assert result.fatal is False

    # --- Test 6 ---
    @patch("workflow_runner.ChargebackOrchestrator")
    @patch("core.storage.registry.create_storage_backend")
    def test_get_failed_tenants_returns_thread_safe_copy(
        self,
        mock_storage: MagicMock,
        mock_orch_cls: MagicMock,
    ) -> None:
        """get_failed_tenants() returns a copy; mutating it does not affect internal state."""
        mock_storage.return_value = MagicMock()
        exc = GatherFailureThresholdError("breach")
        self._make_orch_mock(mock_orch_cls, side_effect=exc)

        settings = _make_settings(tenants={"t1": _make_tenant(tenant_id="tid1")})
        runner = WorkflowRunner(settings, self._make_registry())
        runner._bootstrapped = True

        runner.run_once()

        failed = runner.get_failed_tenants()  # type: ignore[attr-defined]
        assert "t1" in failed
        assert isinstance(failed["t1"], str)

        # Mutating the returned dict must not affect internal state
        failed["injected"] = "evil"
        assert "injected" not in runner._failed_tenants  # type: ignore[attr-defined]

    # --- Test 7 ---
    def test_sys_not_imported_in_workflow_runner(self) -> None:
        """workflow_runner.py must not use `import sys` after the fix."""
        import pathlib

        source = (pathlib.Path(__file__).parent.parent.parent / "src" / "workflow_runner.py").read_text()
        assert "import sys" not in source, (
            "workflow_runner.py still contains `import sys` — sys.exit(1) must be removed"
        )

    # --- Test 8 (integration) ---
    @patch("workflow_runner.ChargebackOrchestrator")
    @patch("core.storage.registry.create_storage_backend")
    def test_integration_both_mode_failed_tenant_persists_across_threads(
        self,
        mock_storage: MagicMock,
        mock_orch_cls: MagicMock,
    ) -> None:
        """In 'both' mode: daemon thread triggers breach; API thread sees failed tenant; next cycle skips."""
        mock_storage.return_value = MagicMock()
        exc = GatherFailureThresholdError("breach in daemon thread")
        mock_orch = self._make_orch_mock(mock_orch_cls, side_effect=exc)

        settings = _make_settings(tenants={"t1": _make_tenant(tenant_id="tid1")})
        runner = WorkflowRunner(settings, self._make_registry())
        runner._bootstrapped = True

        # Simulate daemon thread calling run_once()
        daemon_results: dict[str, Any] = {}
        daemon_exc: list[Exception] = []

        def daemon_run() -> None:
            try:
                daemon_results.update(runner.run_once())
            except Exception as e:
                daemon_exc.append(e)

        t = threading.Thread(target=daemon_run, daemon=True)
        t.start()
        t.join(timeout=10)

        assert not daemon_exc, f"daemon thread raised: {daemon_exc}"

        # API side: failed tenant is visible
        failed = runner.get_failed_tenants()  # type: ignore[attr-defined]
        assert "t1" in failed

        # Subsequent run_once() must NOT call orchestrator for failed tenant
        mock_orch.run.reset_mock()
        runner.run_once()
        mock_orch.run.assert_not_called()

    # --- Test 9 ---
    @patch("workflow_runner.ChargebackOrchestrator")
    @patch("core.storage.registry.create_storage_backend")
    def test_all_tenants_failed_logs_critical_in_run_loop(
        self,
        mock_storage: MagicMock,
        mock_orch_cls: MagicMock,
    ) -> None:
        """When all tenants are permanently suspended, run_loop logs a CRITICAL alert."""
        mock_storage.return_value = MagicMock()
        exc = GatherFailureThresholdError("threshold breached")
        self._make_orch_mock(mock_orch_cls, side_effect=exc)

        settings = _make_settings(
            tenants={"t1": _make_tenant(tenant_id="tid1")},
            refresh_interval=1,
            enable_periodic_refresh=True,
        )
        runner = WorkflowRunner(settings, self._make_registry())
        runner._bootstrapped = True

        records: list[logging.LogRecord] = []

        class RecordingHandler(logging.Handler):
            def emit(self, record: logging.LogRecord) -> None:
                records.append(record)

        handler = RecordingHandler(level=logging.DEBUG)
        wf_logger = logging.getLogger("workflow_runner")
        wf_logger.addHandler(handler)
        orig_level = wf_logger.level
        orig_disabled = wf_logger.disabled
        wf_logger.setLevel(logging.DEBUG)
        wf_logger.disabled = False

        shutdown = threading.Event()

        # Allow two run_once() iterations: first breaches, second should trigger CRITICAL log
        call_count = [0]
        original_run_once = runner.run_once

        def patched_run_once() -> dict[str, PipelineRunResult]:
            result = original_run_once()
            call_count[0] += 1
            if call_count[0] >= 2:
                shutdown.set()
            return result

        runner.run_once = patched_run_once  # type: ignore[method-assign]

        try:
            runner.run_loop(shutdown)
        finally:
            wf_logger.removeHandler(handler)
            wf_logger.setLevel(orig_level)
            wf_logger.disabled = orig_disabled

        logged_messages = [r.getMessage() for r in records if r.levelno >= logging.CRITICAL]
        assert any("1 tenant" in msg and "permanently suspended" in msg for msg in logged_messages), (
            f"Expected CRITICAL all-tenants-suspended log; got: {logged_messages}"
        )


class TestCleanupRetentionStorageReuse:
    """TASK-020: _cleanup_retention() reuses cached TenantRuntime storage."""

    @patch("core.storage.registry.create_storage_backend")
    def test_no_new_engine_when_runtime_exists(self, mock_create: MagicMock) -> None:
        """With cached runtime, create_storage_backend should NOT be called."""
        mock_storage = MagicMock()
        mock_uow = MagicMock()
        mock_storage.create_unit_of_work.return_value.__enter__ = MagicMock(return_value=mock_uow)
        mock_storage.create_unit_of_work.return_value.__exit__ = MagicMock(return_value=False)
        mock_uow.billing.delete_before.return_value = 0
        mock_uow.resources.delete_before.return_value = 0
        mock_uow.identities.delete_before.return_value = 0
        mock_uow.chargebacks.delete_before.return_value = 0

        tenant = _make_tenant(tenant_id="tid1", retention_days=30)
        settings = _make_settings(tenants={"t1": tenant})
        runner = WorkflowRunner(settings, MagicMock())

        runtime = TenantRuntime(
            tenant_name="t1",
            plugin=MagicMock(),
            storage=mock_storage,
            orchestrator=MagicMock(),
            config_hash="abc123",
            created_at=datetime.now(UTC),
        )
        runner._tenant_runtimes["t1"] = runtime
        runner._cleanup_retention()

        mock_create.assert_not_called()

    def test_uses_runtime_storage_not_new_backend(self) -> None:
        """Runtime's storage.create_unit_of_work() called, dispose() NOT called."""
        mock_storage = MagicMock()
        mock_uow = MagicMock()
        mock_storage.create_unit_of_work.return_value.__enter__ = MagicMock(return_value=mock_uow)
        mock_storage.create_unit_of_work.return_value.__exit__ = MagicMock(return_value=False)
        mock_uow.billing.delete_before.return_value = 0
        mock_uow.resources.delete_before.return_value = 0
        mock_uow.identities.delete_before.return_value = 0
        mock_uow.chargebacks.delete_before.return_value = 0

        tenant = _make_tenant(tenant_id="tid1", retention_days=30)
        settings = _make_settings(tenants={"t1": tenant})
        runner = WorkflowRunner(settings, MagicMock())

        runtime = TenantRuntime(
            tenant_name="t1",
            plugin=MagicMock(),
            storage=mock_storage,
            orchestrator=MagicMock(),
            config_hash="abc123",
            created_at=datetime.now(UTC),
        )
        runner._tenant_runtimes["t1"] = runtime
        runner._cleanup_retention()

        mock_storage.create_unit_of_work.assert_called_once()
        mock_storage.dispose.assert_not_called()

    @patch("core.storage.registry.create_storage_backend")
    def test_skip_tenant_without_runtime(self, mock_create: MagicMock) -> None:
        """Tenant with no cached runtime is skipped — no storage created."""
        mock_storage = MagicMock()
        mock_uow = MagicMock()
        mock_storage.create_unit_of_work.return_value.__enter__ = MagicMock(return_value=mock_uow)
        mock_storage.create_unit_of_work.return_value.__exit__ = MagicMock(return_value=False)
        mock_uow.billing.delete_before.return_value = 0
        mock_uow.resources.delete_before.return_value = 0
        mock_uow.identities.delete_before.return_value = 0
        mock_uow.chargebacks.delete_before.return_value = 0

        tenant1 = _make_tenant(tenant_id="tid1", retention_days=30)
        tenant2 = _make_tenant(tenant_id="tid2", retention_days=30)
        settings = _make_settings(tenants={"t1": tenant1, "t2": tenant2})
        runner = WorkflowRunner(settings, MagicMock())

        # Only populate runtime for t1
        runtime = TenantRuntime(
            tenant_name="t1",
            plugin=MagicMock(),
            storage=mock_storage,
            orchestrator=MagicMock(),
            config_hash="abc123",
            created_at=datetime.now(UTC),
        )
        runner._tenant_runtimes["t1"] = runtime
        runner._cleanup_retention()

        # t1 processed via runtime, t2 skipped (no runtime)
        mock_storage.create_unit_of_work.assert_called_once()
        mock_create.assert_not_called()

    def test_skip_when_retention_days_zero_with_runtime(self) -> None:
        """retention_days=0 disables cleanup even with cached runtime."""
        mock_storage = MagicMock()

        tenant = _make_tenant(tenant_id="tid1")
        object.__setattr__(tenant, "retention_days", 0)
        settings = _make_settings(tenants={"t1": tenant})
        runner = WorkflowRunner(settings, MagicMock())

        runtime = TenantRuntime(
            tenant_name="t1",
            plugin=MagicMock(),
            storage=mock_storage,
            orchestrator=MagicMock(),
            config_hash="abc123",
            created_at=datetime.now(UTC),
        )
        runner._tenant_runtimes["t1"] = runtime
        runner._cleanup_retention()

        mock_storage.create_unit_of_work.assert_not_called()

    def test_exception_isolation_per_tenant(self) -> None:
        """Error in first tenant's cleanup does not block second tenant."""
        mock_storage1 = MagicMock()
        mock_storage1.create_unit_of_work.side_effect = RuntimeError("DB error")

        mock_storage2 = MagicMock()
        mock_uow2 = MagicMock()
        mock_storage2.create_unit_of_work.return_value.__enter__ = MagicMock(return_value=mock_uow2)
        mock_storage2.create_unit_of_work.return_value.__exit__ = MagicMock(return_value=False)
        mock_uow2.billing.delete_before.return_value = 0
        mock_uow2.resources.delete_before.return_value = 0
        mock_uow2.identities.delete_before.return_value = 0
        mock_uow2.chargebacks.delete_before.return_value = 0

        tenant1 = _make_tenant(tenant_id="tid1", retention_days=30)
        tenant2 = _make_tenant(tenant_id="tid2", retention_days=30)
        settings = _make_settings(tenants={"t1": tenant1, "t2": tenant2})
        runner = WorkflowRunner(settings, MagicMock())

        runtime1 = TenantRuntime(
            tenant_name="t1",
            plugin=MagicMock(),
            storage=mock_storage1,
            orchestrator=MagicMock(),
            config_hash="abc123",
            created_at=datetime.now(UTC),
        )
        runtime2 = TenantRuntime(
            tenant_name="t2",
            plugin=MagicMock(),
            storage=mock_storage2,
            orchestrator=MagicMock(),
            config_hash="def456",
            created_at=datetime.now(UTC),
        )
        runner._tenant_runtimes["t1"] = runtime1
        runner._tenant_runtimes["t2"] = runtime2

        runner._cleanup_retention()  # Should not raise

        # t2 still processed despite t1 failure
        mock_storage2.create_unit_of_work.assert_called_once()


class TestGracefulShutdown:
    """GAR-001: Graceful shutdown via threading.Event in run_once() polling loop."""

    def _make_blocking_runner(
        self,
        settings: AppSettings,
        block_event: threading.Event,
    ) -> WorkflowRunner:
        """WorkflowRunner whose _run_tenant blocks until block_event is set."""
        runner = WorkflowRunner(settings, MagicMock())
        runner._bootstrapped = True

        def blocking_run_tenant(name: str, config: Any) -> PipelineRunResult:
            block_event.wait()
            return PipelineRunResult(
                tenant_name=name,
                tenant_id=config.tenant_id,
                dates_gathered=0,
                dates_calculated=0,
                chargeback_rows_written=0,
            )

        runner._run_tenant = blocking_run_tenant  # type: ignore[assignment]
        return runner

    def test_worker_loop_ctrl_c_exits_within_2s(self) -> None:
        """Test 1: Shutdown event causes run_once() to return within 2s with interrupt errors."""
        block_event = threading.Event()
        tenant = _make_tenant(tenant_id="tid1", tenant_execution_timeout_seconds=60)
        settings = _make_settings(tenants={"t1": tenant})
        runner = self._make_blocking_runner(settings, block_event)

        shutdown_event = threading.Event()
        runner.set_shutdown_event(shutdown_event)

        result_holder: dict[str, dict[str, PipelineRunResult]] = {}
        exc_holder: list[Exception] = []

        def run_in_thread() -> None:
            try:
                result_holder["result"] = runner.run_once()
            except Exception as exc:
                exc_holder.append(exc)

        t = threading.Thread(target=run_in_thread)
        t.start()

        time.sleep(0.1)
        shutdown_event.set()

        t.join(timeout=2.0)

        assert not t.is_alive(), "run_once() did not return within 2s after shutdown"
        assert not exc_holder, f"Unexpected exception: {exc_holder}"
        assert "result" in result_holder
        results = result_holder["result"]
        assert "t1" in results
        assert any("Interrupted by shutdown" in e for e in results["t1"].errors), (
            f"Expected 'Interrupted by shutdown' in errors, got: {results['t1'].errors}"
        )
        block_event.set()  # unblock the blocked thread

    def test_timeout_behavior_preserved_no_regression(self) -> None:
        """Test 3: Timeout fires correctly; executor.shutdown(wait=False) releases run_once quickly.

        With the current implementation (wait=True), run_once blocks until the slow thread
        exits (~5s), causing elapsed > 4s → RED. After the fix (wait=False, cancel_futures=True),
        run_once returns right after the 2s deadline → elapsed < 4s → GREEN.
        """
        slow_done = threading.Event()
        tenant = _make_tenant(tenant_id="tid1", tenant_execution_timeout_seconds=2)
        settings = _make_settings(tenants={"t1": tenant})
        runner = WorkflowRunner(settings, MagicMock())
        runner._bootstrapped = True

        def slow_run_tenant(name: str, config: Any) -> PipelineRunResult:
            slow_done.wait(timeout=5)  # blocks ~5s unless released early
            return PipelineRunResult(
                tenant_name=name,
                tenant_id=config.tenant_id,
                dates_gathered=0,
                dates_calculated=0,
                chargeback_rows_written=0,
            )

        runner._run_tenant = slow_run_tenant  # type: ignore[assignment]

        start = time.monotonic()
        results = runner.run_once()
        elapsed = time.monotonic() - start

        slow_done.set()  # release slow thread if still running

        # After fix: run_once exits ~2-3s (timeout + overhead), not after slow thread completes
        assert elapsed < 4.0, f"run_once took {elapsed:.1f}s; expected < 4s (timeout=2s + overhead)"
        assert "t1" in results
        assert any("timed out" in e.lower() for e in results["t1"].errors), (
            f"Expected 'timed out' error, got: {results['t1'].errors}"
        )
        assert not any("shutdown" in e.lower() for e in results["t1"].errors), (
            f"Got unexpected shutdown error: {results['t1'].errors}"
        )

    def test_run_once_normal_completes_no_regression(self) -> None:
        """Test 4: Normal run_once with immediate _run_tenant collects all results correctly."""
        tenant = _make_tenant(tenant_id="tid1")
        settings = _make_settings(tenants={"t1": tenant})
        runner = WorkflowRunner(settings, MagicMock())
        runner._bootstrapped = True

        expected = PipelineRunResult(
            tenant_name="t1",
            tenant_id="tid1",
            dates_gathered=5,
            dates_calculated=3,
            chargeback_rows_written=10,
        )

        def fast_run_tenant(name: str, config: Any) -> PipelineRunResult:
            return expected

        runner._run_tenant = fast_run_tenant  # type: ignore[assignment]

        results = runner.run_once()

        assert "t1" in results
        assert results["t1"].dates_gathered == 5
        assert results["t1"].dates_calculated == 3
        assert results["t1"].chargeback_rows_written == 10
        assert not results["t1"].errors

    def test_pre_set_shutdown_event_skips_all_work(self) -> None:
        """Test 5: Pre-set shutdown event makes run_once() return immediately with interrupt errors."""
        tenants = {f"t{i}": _make_tenant(tenant_id=f"tid{i}") for i in range(3)}
        settings = _make_settings(tenants=tenants)
        runner = WorkflowRunner(settings, MagicMock())
        runner._bootstrapped = True

        call_count = 0

        def slow_run_tenant(name: str, config: Any) -> PipelineRunResult:
            nonlocal call_count
            call_count += 1
            time.sleep(10)
            return PipelineRunResult(
                tenant_name=name,
                tenant_id=config.tenant_id,
                dates_gathered=0,
                dates_calculated=0,
                chargeback_rows_written=0,
            )

        runner._run_tenant = slow_run_tenant  # type: ignore[assignment]

        pre_set = threading.Event()
        pre_set.set()
        runner.set_shutdown_event(pre_set)

        start = time.monotonic()
        results = runner.run_once()
        elapsed = time.monotonic() - start

        assert elapsed <= 0.5, f"run_once took {elapsed:.2f}s with pre-set shutdown; expected ≤0.5s"
        assert len(results) == 3
        for name in tenants:
            assert name in results
            assert any("Interrupted by shutdown" in e for e in results[name].errors), (
                f"Expected 'Interrupted by shutdown' for {name}, got: {results[name].errors}"
            )

    def test_shutdown_event_causes_run_once_to_break(self) -> None:
        """Test 6b: Setting shutdown_event from another thread causes run_once() to break ≤1.5s."""
        block_event = threading.Event()
        tenant = _make_tenant(tenant_id="tid1", tenant_execution_timeout_seconds=60)
        settings = _make_settings(tenants={"t1": tenant})
        runner = self._make_blocking_runner(settings, block_event)

        shutdown_event = threading.Event()
        runner.set_shutdown_event(shutdown_event)

        result_holder: dict[str, dict[str, PipelineRunResult]] = {}

        def run_in_thread() -> None:
            result_holder["result"] = runner.run_once()

        t = threading.Thread(target=run_in_thread)
        t.start()

        time.sleep(0.2)
        shutdown_event.set()

        t.join(timeout=1.5)
        block_event.set()

        assert not t.is_alive(), "run_once() did not return within 1.5s after shutdown event"
        assert "result" in result_holder


class TestShutdownCheckWiring:
    """task-083: WorkflowRunner wires _is_shutdown_requested into ChargebackOrchestrator."""

    @patch("workflow_runner.ChargebackOrchestrator")
    @patch("core.storage.registry.create_storage_backend")
    def test_get_or_create_runtime_passes_shutdown_check(
        self,
        mock_storage: MagicMock,
        mock_orch_cls: MagicMock,
    ) -> None:
        """_get_or_create_runtime passes shutdown_check=runner._is_shutdown_requested."""
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

        mock_orch_cls.assert_called_once()
        _, kwargs = mock_orch_cls.call_args
        assert "shutdown_check" in kwargs


# ---------------------------------------------------------------------------
# Test 7: bootstrap_storage delegates to cleanup_orphaned_runs_for_all_tenants
# Test 8: cleanup_orphaned_runs_for_all_tenants swallow_errors=True
# Test 9: cleanup_orphaned_runs_for_all_tenants swallow_errors=False
# ---------------------------------------------------------------------------


class TestCleanupOrphanedRunsForAllTenants:
    """Tests for the new module-level cleanup_orphaned_runs_for_all_tenants() function."""

    @patch("core.storage.registry.create_storage_backend")
    def test_bootstrap_storage_delegates_to_cleanup_function(self, mock_create_storage: MagicMock) -> None:
        """Test 7: WorkflowRunner.bootstrap_storage() must call
        cleanup_orphaned_runs_for_all_tenants(settings, swallow_errors=False)."""
        from workflow_runner import cleanup_orphaned_runs_for_all_tenants  # noqa: F401

        mock_backend = MagicMock()
        mock_create_storage.return_value = mock_backend

        settings = _make_settings(tenants={"t1": _make_tenant(tenant_id="tid1")})
        runner = WorkflowRunner(settings, MagicMock())

        with patch("workflow_runner.cleanup_orphaned_runs_for_all_tenants") as mock_cleanup:
            runner.bootstrap_storage()

        mock_cleanup.assert_called_once_with(settings, swallow_errors=False)

    def test_swallow_errors_true_does_not_raise_on_storage_failure(self) -> None:
        """Test 8: When swallow_errors=True, create_tables() raising is caught and NOT re-raised."""
        from workflow_runner import cleanup_orphaned_runs_for_all_tenants

        settings = _make_settings(tenants={"t": _make_tenant(tenant_id="tid")})
        mock_storage = MagicMock()
        mock_storage.create_tables.side_effect = RuntimeError("db unavailable")

        with patch("core.storage.registry.create_storage_backend", return_value=mock_storage):
            # Must not raise
            cleanup_orphaned_runs_for_all_tenants(settings, swallow_errors=True)

    def test_swallow_errors_false_propagates_exception(self) -> None:
        """Test 9: When swallow_errors=False, create_tables() raising propagates to caller."""
        from workflow_runner import cleanup_orphaned_runs_for_all_tenants

        settings = _make_settings(tenants={"t": _make_tenant(tenant_id="tid")})
        mock_storage = MagicMock()
        mock_storage.create_tables.side_effect = RuntimeError("db unavailable")

        with (
            patch("core.storage.registry.create_storage_backend", return_value=mock_storage),
            pytest.raises(RuntimeError, match="db unavailable"),
        ):
            cleanup_orphaned_runs_for_all_tenants(settings, swallow_errors=False)

    def test_api_only_startup_cleanup_calls_cleanup_orphaned_runs(self) -> None:
        """Test 2: cleanup_orphaned_runs_for_all_tenants calls PipelineRunTracker.cleanup_orphaned_runs
        for each tenant — verifies the delegation chain works end-to-end."""
        from workflow_runner import PipelineRunTracker, cleanup_orphaned_runs_for_all_tenants

        settings = _make_settings(tenants={"t": _make_tenant(tenant_id="tid")})
        mock_storage = MagicMock()

        with (
            patch("core.storage.registry.create_storage_backend", return_value=mock_storage),
            patch.object(PipelineRunTracker, "cleanup_orphaned_runs") as mock_cleanup,
        ):
            cleanup_orphaned_runs_for_all_tenants(settings, swallow_errors=True)

        mock_cleanup.assert_called_once_with("t")


class TestPipelineRunTrackerCleanupOrphanedRunsRealDb:
    """GIT-002: Verify actual DB mutations via real in-memory SQLite storage."""

    def test_cleanup_orphaned_runs_marks_running_row_as_failed_in_real_db(self) -> None:
        """cleanup_orphaned_runs() on a real in-memory DB must:
        - Set status='failed' on any 'running' row
        - Set ended_at to a non-null datetime
        - Set error_message containing 'Orphaned'
        """
        from core.config.models import StorageConfig
        from core.storage.registry import create_storage_backend
        from workflow_runner import PipelineRunTracker

        storage = create_storage_backend(
            StorageConfig(connection_string="sqlite:///:memory:"),
            use_migrations=False,
        )
        storage.create_tables()

        try:
            tracker = PipelineRunTracker(storage)

            # Insert a 'running' record
            run = tracker.create("acme")
            assert run.status == "running"

            # Act: clean up orphaned runs
            tracker.cleanup_orphaned_runs("acme")

            # Verify the row in the DB
            with storage.create_unit_of_work() as uow:
                updated = uow.pipeline_runs.get_latest_run("acme")

            assert updated is not None
            assert updated.status == "failed"
            assert updated.ended_at is not None
            assert updated.error_message is not None
            assert "Orphaned" in updated.error_message
        finally:
            storage.dispose()


class TestLogResults:
    def test_log_results_includes_pending(self, caplog: pytest.LogCaptureFixture) -> None:
        """_log_results must emit 'pending=N' in the success log line."""
        from unittest.mock import MagicMock

        registry = MagicMock()
        settings = _make_settings(tenants={"t1": _make_tenant(tenant_id="tid1")})
        runner = WorkflowRunner(settings, registry)

        results = {
            "t1": PipelineRunResult(
                tenant_name="t1",
                tenant_id="tid1",
                dates_gathered=3,
                dates_calculated=1,
                chargeback_rows_written=5,
                dates_pending_calculation=3,
            ),
        }

        with caplog.at_level(logging.INFO, logger="workflow_runner"):
            runner._log_results(results)

        log_messages = [r.message for r in caplog.records]
        assert any("pending=3" in m for m in log_messages), (
            f"Expected 'pending=3' in _log_results output, got: {log_messages}"
        )


class TestCleanupRetentionTopicAttribution:
    """AC-12: _cleanup_retention() must call uow.topic_attributions.delete_before() when TA enabled."""

    def _make_mock_backend_with_uow(self) -> tuple[MagicMock, MagicMock]:
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

    def _make_tenant_with_ta(self, *, ta_enabled: bool, ta_retention_days: int = 30) -> TenantConfig:
        from core.metrics.config import MetricsConnectionConfig
        from plugins.confluent_cloud.config import CCloudPluginConfig, TopicAttributionConfig

        metrics = MetricsConnectionConfig(type="prometheus", url="http://prom:9090") if ta_enabled else None
        plugin_settings = CCloudPluginConfig(
            ccloud_api={"key": "k", "secret": "s"},
            metrics=metrics,
            topic_attribution=TopicAttributionConfig(
                enabled=ta_enabled,
                retention_days=ta_retention_days,
            ),
        )
        return _make_tenant(
            ecosystem="confluent_cloud",
            tenant_id="tid1",
            retention_days=30,
            plugin_settings=plugin_settings,
        )

    def test_calls_topic_attributions_delete_before_when_enabled(self) -> None:
        from plugins.confluent_cloud.config import TopicAttributionConfig

        mock_backend, mock_uow = self._make_mock_backend_with_uow()
        tenant = self._make_tenant_with_ta(ta_enabled=True, ta_retention_days=60)
        settings = _make_settings(tenants={"t1": tenant})
        runner = WorkflowRunner(settings, MagicMock())
        ta_config = TopicAttributionConfig(enabled=True, retention_days=60)
        plugin = MagicMock()
        plugin.get_overlay_config = MagicMock(return_value=ta_config)
        runtime = TenantRuntime(
            tenant_name="t1",
            plugin=plugin,
            storage=mock_backend,
            orchestrator=MagicMock(),
            config_hash="abc123",
            created_at=datetime.now(UTC),
        )
        runner._tenant_runtimes["t1"] = runtime
        runner._cleanup_retention()

        mock_uow.topic_attributions.delete_before.assert_called_once()

    def test_uses_topic_attribution_retention_days_not_tenant_retention_days(self) -> None:
        """ta_config.retention_days (60) must be used, not config.retention_days (30)."""
        from datetime import timedelta

        from plugins.confluent_cloud.config import TopicAttributionConfig

        mock_backend, mock_uow = self._make_mock_backend_with_uow()
        tenant = self._make_tenant_with_ta(ta_enabled=True, ta_retention_days=60)
        settings = _make_settings(tenants={"t1": tenant})
        runner = WorkflowRunner(settings, MagicMock())
        ta_config = TopicAttributionConfig(enabled=True, retention_days=60)
        plugin = MagicMock()
        plugin.get_overlay_config = MagicMock(return_value=ta_config)
        runtime = TenantRuntime(
            tenant_name="t1",
            plugin=plugin,
            storage=mock_backend,
            orchestrator=MagicMock(),
            config_hash="abc123",
            created_at=datetime.now(UTC),
        )
        runner._tenant_runtimes["t1"] = runtime
        runner._cleanup_retention()

        call_args = mock_uow.topic_attributions.delete_before.call_args
        cutoff: datetime = call_args[0][2]  # positional: ecosystem, tenant_id, cutoff

        # cutoff must be approximately now() - 60 days (not 30)
        expected_approx = datetime.now(UTC) - timedelta(days=60)
        delta = abs((cutoff - expected_approx).total_seconds())
        assert delta < 5, f"Expected cutoff ~60 days ago, got {cutoff} (delta={delta}s)"

    def test_skips_topic_attribution_cleanup_when_disabled(self) -> None:
        mock_backend, mock_uow = self._make_mock_backend_with_uow()
        tenant = self._make_tenant_with_ta(ta_enabled=False)
        settings = _make_settings(tenants={"t1": tenant})
        runner = WorkflowRunner(settings, MagicMock())
        runtime = TenantRuntime(
            tenant_name="t1",
            plugin=MagicMock(),
            storage=mock_backend,
            orchestrator=MagicMock(),
            config_hash="abc123",
            created_at=datetime.now(UTC),
        )
        runner._tenant_runtimes["t1"] = runtime
        runner._cleanup_retention()

        mock_uow.topic_attributions.delete_before.assert_not_called()

    def test_skips_topic_attribution_cleanup_when_no_plugin_settings(self) -> None:
        """Tenants without TopicAttributionConfig must not touch topic_attributions."""
        mock_backend, mock_uow = self._make_mock_backend_with_uow()
        # Use a plain tenant with no topic_attribution in plugin_settings
        tenant = _make_tenant(tenant_id="tid1", retention_days=30)
        settings = _make_settings(tenants={"t1": tenant})
        runner = WorkflowRunner(settings, MagicMock())
        runtime = TenantRuntime(
            tenant_name="t1",
            plugin=MagicMock(),
            storage=mock_backend,
            orchestrator=MagicMock(),
            config_hash="abc123",
            created_at=datetime.now(UTC),
        )
        runner._tenant_runtimes["t1"] = runtime
        runner._cleanup_retention()

        mock_uow.topic_attributions.delete_before.assert_not_called()
