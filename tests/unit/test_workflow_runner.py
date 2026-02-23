from __future__ import annotations

import threading
import time
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from core.config.models import AppSettings, StorageConfig, TenantConfig
from core.engine.orchestrator import PipelineRunResult
from workflow_runner import WorkflowRunner, _create_metrics_source, _create_storage_backend


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


class TestCreateMetricsSource:
    def test_no_metrics_config(self) -> None:
        assert _create_metrics_source({}) is None

    def test_unknown_metrics_type(self) -> None:
        with pytest.raises(ValueError, match="Unknown metrics type"):
            _create_metrics_source({"metrics": {"type": "datadog"}})

    def test_non_dict_metrics_raises(self) -> None:
        with pytest.raises(TypeError, match="metrics config must be a dict"):
            _create_metrics_source({"metrics": "not_a_dict"})

    def test_prometheus_type_creates_source(self) -> None:
        result = _create_metrics_source({"metrics": {"type": "prometheus", "url": "http://localhost:9090"}})
        assert result is not None
        from core.metrics.prometheus import PrometheusMetricsSource

        assert isinstance(result, PrometheusMetricsSource)


class TestWorkflowRunnerRunOnce:
    def test_no_tenants_returns_empty(self) -> None:
        settings = AppSettings(tenants={})
        registry = MagicMock()
        runner = WorkflowRunner(settings, registry)
        assert runner.run_once() == {}

    @patch("workflow_runner.ChargebackOrchestrator")
    @patch("workflow_runner._create_storage_backend")
    @patch("workflow_runner._create_metrics_source")
    def test_runs_tenant(
        self,
        mock_metrics: MagicMock,
        mock_storage: MagicMock,
        mock_orch_cls: MagicMock,
    ) -> None:
        mock_metrics.return_value = None
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

        settings = AppSettings(
            tenants={"t1": TenantConfig(ecosystem="eco", tenant_id="tid1", lookback_days=30, cutoff_days=5)}
        )
        registry = MagicMock()
        registry.create.return_value = MagicMock()

        runner = WorkflowRunner(settings, registry)
        results = runner.run_once()
        assert "t1" in results
        assert results["t1"].dates_gathered == 3
        mock_backend.dispose.assert_called_once()

    @patch("workflow_runner.ChargebackOrchestrator")
    @patch("workflow_runner._create_storage_backend")
    @patch("workflow_runner._create_metrics_source")
    def test_error_isolation(
        self,
        mock_metrics: MagicMock,
        mock_storage: MagicMock,
        mock_orch_cls: MagicMock,
    ) -> None:
        """One tenant failure doesn't affect others."""
        mock_metrics.return_value = None
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

        settings = AppSettings(
            tenants={
                "t1": TenantConfig(ecosystem="eco", tenant_id="tid1", lookback_days=30, cutoff_days=5),
                "t2": TenantConfig(ecosystem="eco", tenant_id="tid2", lookback_days=30, cutoff_days=5),
            }
        )
        registry = MagicMock()
        registry.create.return_value = MagicMock()

        runner = WorkflowRunner(settings, registry)
        results = runner.run_once()
        assert len(results) == 2
        # One should have errors, one should succeed
        error_tenants = [name for name, r in results.items() if r.errors]
        assert len(error_tenants) >= 1


class TestWorkflowRunnerTimeout:
    @patch("workflow_runner.ChargebackOrchestrator")
    @patch("workflow_runner._create_storage_backend")
    @patch("workflow_runner._create_metrics_source")
    def test_tenant_timeout_captured(
        self,
        mock_metrics: MagicMock,
        mock_storage: MagicMock,
        mock_orch_cls: MagicMock,
    ) -> None:
        """Tenant that exceeds timeout gets error result via future.result() TimeoutError."""
        mock_metrics.return_value = None
        mock_backend = MagicMock()
        mock_storage.return_value = mock_backend
        mock_orch = MagicMock()
        mock_orch_cls.return_value = mock_orch

        settings = AppSettings(
            tenants={
                "slow": TenantConfig(
                    ecosystem="eco",
                    tenant_id="tid",
                    lookback_days=30,
                    cutoff_days=5,
                    tenant_execution_timeout_seconds=1,
                )
            }
        )
        registry = MagicMock()
        registry.create.return_value = MagicMock()

        runner = WorkflowRunner(settings, registry)

        # Patch as_completed to return a future that raises TimeoutError
        timeout_future = MagicMock()
        timeout_future.result.side_effect = TimeoutError("timed out")

        def patched_as_completed(fs: Any, **kwargs: Any) -> Any:
            # Map the timeout_future to the same key as the real future
            for real_future in fs:
                fs[timeout_future] = fs.pop(real_future)
                break
            return [timeout_future]

        with patch("workflow_runner.as_completed", side_effect=patched_as_completed):
            results = runner.run_once()

        assert "slow" in results
        assert len(results["slow"].errors) >= 1
        assert "timed out" in results["slow"].errors[0].lower()


class TestWorkflowRunnerRunLoop:
    def test_shutdown_event_stops_loop(self) -> None:
        settings = AppSettings(tenants={})
        settings.features.refresh_interval = 1
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
    @patch("workflow_runner._create_metrics_source")
    def test_run_loop_processes_tenants(
        self,
        mock_metrics: MagicMock,
        mock_storage: MagicMock,
        mock_orch_cls: MagicMock,
    ) -> None:
        """run_loop executes run_once and processes results."""
        mock_metrics.return_value = None
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

        settings = AppSettings(
            tenants={"t1": TenantConfig(ecosystem="eco", tenant_id="tid1", lookback_days=30, cutoff_days=5)}
        )
        settings.features.refresh_interval = 1
        registry = MagicMock()
        registry.create.return_value = MagicMock()

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
        """run_loop logs warnings for tenants with errors."""
        settings = AppSettings(tenants={})
        settings.features.refresh_interval = 60
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
