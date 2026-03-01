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

    @patch("workflow_runner._create_storage_backend")
    def test_bootstrap_creates_tables_for_all_tenants(self, mock_storage: MagicMock) -> None:
        backends: list[MagicMock] = []

        def make_backend(config: Any) -> MagicMock:
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

    @patch("workflow_runner._create_storage_backend")
    def test_bootstrap_only_runs_once(self, mock_storage: MagicMock) -> None:
        mock_storage.return_value = MagicMock()
        settings = _make_settings(tenants={"t1": _make_tenant(tenant_id="tid1")})
        runner = WorkflowRunner(settings, MagicMock())

        runner.bootstrap_storage()
        runner.bootstrap_storage()

        # Only one backend created — second call is a no-op via _bootstrapped flag
        assert mock_storage.call_count == 1

    @patch("workflow_runner.ChargebackOrchestrator")
    @patch("workflow_runner._create_storage_backend")
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
    @patch("workflow_runner._create_storage_backend")
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

        settings = _make_settings(tenants={"t1": _make_tenant(tenant_id="tid1", tenant_execution_timeout_seconds=0)})
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
        call_args = mock_orch_cls.call_args[0]
        assert call_args[4] is fake_metrics


class TestTd020PluginInitialization:
    """TD-020 regression: plugin.initialize() called before any method calls."""

    @patch("workflow_runner.ChargebackOrchestrator")
    @patch("workflow_runner._create_storage_backend")
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
    @patch("workflow_runner._create_storage_backend")
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
    @patch("workflow_runner._create_storage_backend")
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
    @patch("workflow_runner._create_storage_backend")
    def test_close_disposes_all_runtimes(self, mock_storage: MagicMock, mock_orch_cls: MagicMock) -> None:
        """close() disposes storage and closes plugin for every cached runtime."""
        backends: dict[str, MagicMock] = {}
        plugins: dict[str, MagicMock] = {}

        def make_backend(config: Any) -> MagicMock:
            b = MagicMock()
            # Use connection string as key (unique per tenant via _make_tenant unique hex)
            backends[config.connection_string] = b
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
    @patch("workflow_runner._create_storage_backend")
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
        """TenantRuntime.close() calls plugin.close() if it exists."""
        from workflow_runner import TenantRuntime

        mock_plugin = MagicMock(spec=["close", "get_metrics_source"])
        mock_plugin.get_metrics_source.return_value = None
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

    def test_tenant_runtime_close_calls_metrics_source_close(self) -> None:
        """CT-007: TenantRuntime.close() calls metrics_source.close() if it has close()."""
        from workflow_runner import TenantRuntime

        fake_metrics = MagicMock(spec=["close"])
        mock_plugin = MagicMock(spec=["get_metrics_source"])
        mock_plugin.get_metrics_source.return_value = fake_metrics
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
        fake_metrics.close.assert_called_once()

    def test_tenant_runtime_close_skips_metrics_close_if_no_close_attr(self) -> None:
        """TenantRuntime.close() does not fail if metrics source has no close()."""
        from workflow_runner import TenantRuntime

        fake_metrics = MagicMock(spec=[])  # no close() method
        mock_plugin = MagicMock(spec=["get_metrics_source"])
        mock_plugin.get_metrics_source.return_value = fake_metrics
        mock_storage = MagicMock()

        runtime = TenantRuntime(
            tenant_name="t1",
            plugin=mock_plugin,
            storage=mock_storage,
            orchestrator=MagicMock(),
            config_hash="abc",
            created_at=datetime.now(UTC),
        )
        runtime.close()  # Should not raise

        mock_storage.dispose.assert_called_once()


class TestRunTenant:
    """CT-001: run_tenant() tests."""

    def test_unknown_tenant_raises_value_error(self) -> None:
        settings = _make_settings(tenants={"t1": _make_tenant(tenant_id="tid1")})
        runner = WorkflowRunner(settings, MagicMock())
        with pytest.raises(ValueError, match="Unknown tenant"):
            runner.run_tenant("nonexistent")

    @patch("workflow_runner.ChargebackOrchestrator")
    @patch("workflow_runner._create_storage_backend")
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
    @patch("workflow_runner._create_storage_backend")
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
    @patch("workflow_runner._create_storage_backend")
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

    @patch("workflow_runner._create_storage_backend")
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

    @patch("workflow_runner._create_storage_backend")
    def test_calls_delete_before_for_enabled_tenant(self, mock_storage: MagicMock) -> None:
        """retention_days > 0 triggers delete_before on all repos."""
        mock_backend = MagicMock()
        mock_storage.return_value = mock_backend
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
        runner._cleanup_retention()

        mock_uow.billing.delete_before.assert_called_once()
        mock_uow.resources.delete_before.assert_called_once()
        mock_uow.identities.delete_before.assert_called_once()
        mock_uow.chargebacks.delete_before.assert_called_once()
        mock_uow.commit.assert_called_once()
        mock_backend.dispose.assert_called_once()

    @patch("workflow_runner._create_storage_backend")
    def test_exception_does_not_propagate(self, mock_storage: MagicMock) -> None:
        """Retention cleanup errors are caught and logged, not re-raised."""
        mock_backend = MagicMock()
        mock_storage.return_value = mock_backend
        mock_backend.create_unit_of_work.side_effect = RuntimeError("DB down")

        tenant = _make_tenant(tenant_id="tid1", retention_days=30)
        settings = _make_settings(tenants={"t1": tenant})
        runner = WorkflowRunner(settings, MagicMock())
        runner._cleanup_retention()  # Should not raise


class TestGap021RunTenantBootstrapLatch:
    """GAP-021: run_tenant() must delegate bootstrap to bootstrap_storage() — latches flag and
    bootstraps ALL tenants, not just the requested one."""

    @patch("workflow_runner.ChargebackOrchestrator")
    @patch("workflow_runner._create_storage_backend")
    def test_consecutive_run_tenant_same_tenant_bootstraps_once(
        self, mock_storage: MagicMock, mock_orch_cls: MagicMock
    ) -> None:
        """Two consecutive run_tenant() calls for the same tenant must not repeat bootstrap.

        Expected (after fix): create_tables() called exactly 2 times total — once per configured
        tenant during the first call — NOT 4 times (which would indicate missing flag latch).
        """
        backends: list[MagicMock] = []

        def make_backend(config: Any) -> MagicMock:
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
    @patch("workflow_runner._create_storage_backend")
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
            t1_cfg.storage.connection_string: [],
            t2_cfg.storage.connection_string: [],
        }

        def make_backend(config: Any) -> MagicMock:
            b = MagicMock()
            key = config.connection_string
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

        t1_conn = t1_cfg.storage.connection_string
        t2_conn = t2_cfg.storage.connection_string

        t1_create_tables_after_first = sum(
            b.create_tables.call_count for b in conn_backends[t1_conn]
        )
        t2_create_tables_after_first = sum(
            b.create_tables.call_count for b in conn_backends[t2_conn]
        )

        # t1 bootstrap must have happened
        assert t1_create_tables_after_first >= 1

        # t2 bootstrap must ALSO have happened — bug: this assertion fails
        assert t2_create_tables_after_first >= 1

        runner.run_tenant("t2")

        # No additional create_tables after second run_tenant
        t1_create_tables_after_second = sum(
            b.create_tables.call_count for b in conn_backends[t1_conn]
        )
        t2_create_tables_after_second = sum(
            b.create_tables.call_count for b in conn_backends[t2_conn]
        )
        assert t1_create_tables_after_second == t1_create_tables_after_first
        assert t2_create_tables_after_second == t2_create_tables_after_first
