from __future__ import annotations

import threading
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from core.metrics.protocol import MetricsQueryError
from core.models import MetricQuery, MetricRow
from main import parse_args


class _CheckerSource:
    """MetricsSource double used by production checker CLI-path tests."""

    def __init__(self, *, failure_key: str | None = None) -> None:
        self.failure_key = failure_key
        self.closed = False
        self.calls: list[tuple[MetricQuery, datetime, datetime, timedelta, str | None]] = []

    def query(
        self,
        queries: Sequence[MetricQuery],
        start: datetime,
        end: datetime,
        step: timedelta = timedelta(hours=1),
        resource_id_filter: str | None = None,
    ) -> dict[str, list[MetricRow]]:
        assert len(queries) == 1
        [query] = queries
        self.calls.append((query, start, end, step, resource_id_filter))
        if query.key == self.failure_key:
            raise MetricsQueryError("Prometheus unavailable")
        selector_label = query.resource_label or "deployment"
        selector_value = resource_id_filter or ""
        labels = {
            label: (
                selector_value
                if label == selector_label
                else "1"
                if label == "broker"
                else "orders"
                if label == "topic"
                else "0"
                if label == "partition"
                else "Produce"
                if label == "quota_type"
                else "user"
                if label == "quota_scope"
                else "alice"
                if label == "user"
                else "client-a"
                if label == "client_id"
                else "value"
            )
            for label in query.label_keys
        }
        metric_name = query.query_expression.split("{", 1)[0]
        source_series = tuple(sorted({"__name__": metric_name, **labels}.items()))
        return {
            query.key: [
                MetricRow(
                    timestamp=end,
                    metric_key=query.key,
                    value=1.0,
                    labels=labels,
                    source_series=source_series,
                )
            ]
        }

    def close(self) -> None:
        self.closed = True


def _checker_tenant_settings(cluster_id: str, metrics_identifier: str) -> dict[str, object]:
    return {
        "cluster_id": cluster_id,
        "metrics_identifier": metrics_identifier,
        "metrics_identifier_label": "deployment",
        "broker_count": 3,
        "cost_model": {
            "compute_hourly_rate": "0.10",
            "storage_per_gib_hourly": "0.0001",
            "network_ingress_per_gib": "0.01",
            "network_egress_per_gib": "0.02",
        },
        "metrics": {"url": "http://prometheus:9090"},
        "identity_source": {"source": "prometheus"},
        "topic_attribution": {"enabled": True, "compute_policy": "shared_even_v1"},
    }


class TestParseArgs:
    def test_config_file_optional_in_parser(self) -> None:
        # --config-file is required=False; presence is validated manually in main()
        args = parse_args([])
        assert args.config_file is None

    def test_config_file(self) -> None:
        args = parse_args(["--config-file", "config.yaml"])
        assert args.config_file == "config.yaml"
        assert args.env_file is None
        assert args.run_once is False

    def test_all_flags(self) -> None:
        args = parse_args(["--config-file", "c.yaml", "--env-file", ".env", "--run-once"])
        assert args.config_file == "c.yaml"
        assert args.env_file == ".env"
        assert args.run_once is True


class TestSetupLogging:
    def test_per_module_levels(self) -> None:
        import logging

        from core.config.models import AppSettings

        settings = AppSettings(logging={"level": "WARNING", "per_module_levels": {"test.module": "DEBUG"}})

        from main import setup_logging

        setup_logging(settings)
        assert logging.getLogger("test.module").level == logging.DEBUG


class TestMain:
    @patch("main.WorkflowRunner")
    @patch("main.PluginRegistry")
    @patch("core.plugin.loader.discover_plugins")
    @patch("main.load_config")
    def test_run_once(
        self,
        mock_load: MagicMock,
        mock_discover: MagicMock,
        mock_registry_cls: MagicMock,
        mock_runner_cls: MagicMock,
    ) -> None:
        from core.config.models import AppSettings

        mock_load.return_value = AppSettings()
        mock_discover.return_value = []
        mock_runner = MagicMock()
        mock_runner.run_once.return_value = {}
        mock_runner_cls.return_value = mock_runner

        from main import main

        main(["--config-file", "dummy.yaml", "--run-once"])
        mock_runner.run_once.assert_called_once()
        mock_runner.run_loop.assert_not_called()

    @patch("main.WorkflowRunner")
    @patch("main.PluginRegistry")
    @patch("core.plugin.loader.discover_plugins")
    @patch("main.load_config")
    def test_run_once_with_results(
        self,
        mock_load: MagicMock,
        mock_discover: MagicMock,
        mock_registry_cls: MagicMock,
        mock_runner_cls: MagicMock,
    ) -> None:
        from core.config.models import AppSettings
        from core.engine.orchestrator import PipelineRunResult

        mock_load.return_value = AppSettings()
        mock_discover.return_value = []
        mock_runner = MagicMock()
        mock_runner.run_once.return_value = {
            "t1": PipelineRunResult(
                tenant_name="t1",
                tenant_id="tid1",
                dates_gathered=3,
                dates_calculated=2,
                chargeback_rows_written=10,
            ),
            "t2": PipelineRunResult(
                tenant_name="t2",
                tenant_id="tid2",
                dates_gathered=0,
                dates_calculated=0,
                chargeback_rows_written=0,
                errors=["something failed"],
            ),
        }
        mock_runner_cls.return_value = mock_runner

        from main import main

        main(["--config-file", "dummy.yaml", "--run-once"])
        mock_runner.run_once.assert_called_once()

    @patch("main.WorkflowRunner")
    @patch("main.PluginRegistry")
    @patch("core.plugin.loader.discover_plugins")
    @patch("main.load_config")
    def test_run_loop_mode(
        self,
        mock_load: MagicMock,
        mock_discover: MagicMock,
        mock_registry_cls: MagicMock,
        mock_runner_cls: MagicMock,
    ) -> None:

        from core.config.models import AppSettings

        mock_load.return_value = AppSettings()
        mock_discover.return_value = []
        mock_runner = MagicMock()
        # run_loop should be called; simulate immediate return
        mock_runner.run_loop.return_value = None
        mock_runner_cls.return_value = mock_runner

        from main import main

        # Use a thread to run main without --run-once, patching signal to avoid issues
        with patch("main.signal"):
            main(["--config-file", "dummy.yaml"])
        mock_runner.run_loop.assert_called_once()

    @patch("main.WorkflowRunner")
    @patch("main.PluginRegistry")
    @patch("core.plugin.loader.discover_plugins")
    @patch("main.load_config")
    def test_run_once_logs_pending_count(
        self,
        mock_load: MagicMock,
        mock_discover: MagicMock,
        mock_registry_cls: MagicMock,
        mock_runner_cls: MagicMock,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        import logging

        from core.config.models import AppSettings
        from core.engine.orchestrator import PipelineRunResult

        mock_load.return_value = AppSettings()
        mock_discover.return_value = []
        mock_runner = MagicMock()
        mock_runner.run_once.return_value = {
            "t1": PipelineRunResult(
                tenant_name="t1",
                tenant_id="tid1",
                dates_gathered=3,
                dates_calculated=1,
                chargeback_rows_written=5,
                dates_pending_calculation=3,
            ),
        }
        mock_runner_cls.return_value = mock_runner

        from main import main

        with caplog.at_level(logging.INFO, logger="main"):
            main(["--config-file", "dummy.yaml", "--run-once"])

        log_messages = [r.message for r in caplog.records]
        assert any("pending=3" in m for m in log_messages), f"Expected 'pending=3' in log output, got: {log_messages}"


class TestBothModeSingleRunner:
    """TASK-005: Dual WorkflowRunner Fix — main.py tests."""

    @patch("main.run_api")
    @patch("main.WorkflowRunner")
    @patch("main.PluginRegistry")
    @patch("core.plugin.loader.discover_plugins")
    @patch("main.load_config")
    def test_both_mode_single_runner_created(
        self,
        mock_load: MagicMock,
        mock_discover: MagicMock,
        mock_registry_cls: MagicMock,
        mock_runner_cls: MagicMock,
        mock_run_api: MagicMock,
    ) -> None:
        """Exactly ONE WorkflowRunner is instantiated in --mode both (TASK-005 fix test 1)."""
        from core.config.models import AppSettings

        mock_load.return_value = AppSettings()
        mock_discover.return_value = []
        mock_runner = MagicMock()
        mock_runner.run_once.return_value = {}
        mock_runner_cls.return_value = mock_runner

        from main import main

        main(["--config-file", "dummy.yaml", "--mode", "both", "--run-once"])
        # After fix: exactly one runner — not two (one in main() + one in run_worker())
        assert mock_runner_cls.call_count == 1

    @patch("main.WorkflowRunner")
    @patch("main.PluginRegistry")
    @patch("core.plugin.loader.discover_plugins")
    @patch("main.load_config")
    def test_run_worker_uses_injected_runner(
        self,
        mock_load: MagicMock,
        mock_discover: MagicMock,
        mock_registry_cls: MagicMock,
        mock_runner_cls: MagicMock,
    ) -> None:
        """run_worker uses the injected runner kwarg and does not create a new one (TASK-005 fix test 2)."""
        from core.config.models import AppSettings

        mock_load.return_value = AppSettings()
        settings = AppSettings()
        mock_discover.return_value = []
        mock_runner = MagicMock()
        mock_runner.run_once.return_value = {}

        from main import run_worker

        run_worker(settings, runner=mock_runner, run_once=True)
        # The injected runner's run_once() must be called
        mock_runner.run_once.assert_called_once()
        # No new WorkflowRunner should be constructed
        mock_runner_cls.assert_not_called()

    @patch("main.WorkflowRunner")
    @patch("main.PluginRegistry")
    @patch("core.plugin.loader.discover_plugins")
    def test_run_worker_standalone_creates_runner(
        self,
        mock_discover: MagicMock,
        mock_registry_cls: MagicMock,
        mock_runner_cls: MagicMock,
    ) -> None:
        """run_worker without runner kwarg constructs its own WorkflowRunner (TASK-005 fix test 3)."""
        from core.config.models import AppSettings

        settings = AppSettings()
        mock_discover.return_value = []
        mock_runner = MagicMock()
        mock_runner.run_once.return_value = {}
        mock_runner_cls.return_value = mock_runner

        from main import run_worker

        run_worker(settings, run_once=True)
        # A new WorkflowRunner must be constructed when none is injected
        mock_runner_cls.assert_called_once()
        mock_runner.run_once.assert_called_once()

    @patch("main.signal")
    @patch("main.WorkflowRunner")
    @patch("main.PluginRegistry")
    @patch("core.plugin.loader.discover_plugins")
    def test_run_worker_pre_set_shutdown_event_skips_signals(
        self,
        mock_discover: MagicMock,
        mock_registry_cls: MagicMock,
        mock_runner_cls: MagicMock,
        mock_signal: MagicMock,
    ) -> None:
        """Pre-set shutdown_event exits immediately, no signal registered (TASK-005 test 4)."""
        from core.config.models import AppSettings

        settings = AppSettings()
        mock_discover.return_value = []
        mock_runner = MagicMock()
        # run_loop returns immediately (shutdown already set)
        mock_runner.run_loop.return_value = None
        mock_runner_cls.return_value = mock_runner

        pre_set_event = threading.Event()
        pre_set_event.set()

        from main import run_worker

        run_worker(settings, shutdown_event=pre_set_event)
        # run_loop is called with the provided event
        mock_runner.run_loop.assert_called_once_with(pre_set_event)
        # No signal registrations — event was external
        mock_signal.signal.assert_not_called()

    @patch("main.run_api")
    @patch("main.run_worker")
    @patch("main.WorkflowRunner")
    @patch("main.PluginRegistry")
    @patch("core.plugin.loader.discover_plugins")
    @patch("main.load_config")
    def test_both_mode_api_and_worker_share_same_runner(
        self,
        mock_load: MagicMock,
        mock_discover: MagicMock,
        mock_registry_cls: MagicMock,
        mock_runner_cls: MagicMock,
        mock_run_worker: MagicMock,
        mock_run_api: MagicMock,
    ) -> None:
        """run_api and run_worker share same WorkflowRunner in both mode (TASK-005 test 10)."""
        from core.config.models import AppSettings

        mock_load.return_value = AppSettings()
        mock_discover.return_value = []

        runner_instance = MagicMock()
        mock_runner_cls.return_value = runner_instance

        captured_api_runners: list[object] = []
        captured_worker_runners: list[object] = []

        def capture_run_api(settings: object, runner: object = None, mode: str = "api") -> None:
            captured_api_runners.append(runner)

        def capture_run_worker(settings: object, **kwargs: object) -> None:
            captured_worker_runners.append(kwargs.get("runner"))

        mock_run_api.side_effect = capture_run_api
        mock_run_worker.side_effect = capture_run_worker

        from main import main

        main(["--config-file", "dummy.yaml", "--mode", "both", "--run-once"])

        # run_api and run_worker must both have received the same runner object
        assert len(captured_api_runners) == 1
        assert len(captured_worker_runners) == 1
        assert captured_api_runners[0] is runner_instance
        # After fix: run_worker must receive runner kwarg; currently it doesn't → fails
        assert captured_worker_runners[0] is runner_instance


class TestCreateRunnerPluginPath:
    """TASK-014: Configurable plugins_path for _create_runner."""

    @patch("main.WorkflowRunner")
    @patch("main.PluginRegistry")
    @patch("core.plugin.loader.discover_plugins")
    def test_create_runner_no_override_uses_default_plugins_path(
        self,
        mock_discover: MagicMock,
        mock_registry_cls: MagicMock,
        mock_runner_cls: MagicMock,
    ) -> None:
        """settings.plugins_path=None uses the loader's default plugins path."""
        from core.config.models import AppSettings
        from core.plugin.loader import _DEFAULT_PLUGINS_PATH
        from main import _create_runner

        settings = AppSettings()
        assert settings.plugins_path is None
        mock_discover.return_value = []
        mock_runner_cls.return_value = MagicMock()

        _create_runner(settings)

        mock_discover.assert_called_once_with(_DEFAULT_PLUGINS_PATH)

    @patch("main.WorkflowRunner")
    @patch("main.PluginRegistry")
    @patch("core.plugin.loader.discover_plugins")
    def test_create_runner_absolute_override_resolves_correctly(
        self,
        mock_discover: MagicMock,
        mock_registry_cls: MagicMock,
        mock_runner_cls: MagicMock,
    ) -> None:
        """settings.plugins_path=absolute → discover_plugins called with that absolute path."""
        from core.config.models import AppSettings
        from main import _create_runner

        settings = AppSettings(plugins_path="/abs/path")
        mock_discover.return_value = []
        mock_runner_cls.return_value = MagicMock()

        _create_runner(settings)

        # Path.cwd() / Path("/abs/path") == Path("/abs/path") via pathlib "/" behaviour
        expected = Path.cwd() / Path("/abs/path")
        mock_discover.assert_called_once_with(expected)

    @patch("main.WorkflowRunner")
    @patch("main.PluginRegistry")
    @patch("core.plugin.loader.discover_plugins")
    def test_create_runner_relative_override_joins_cwd(
        self,
        mock_discover: MagicMock,
        mock_registry_cls: MagicMock,
        mock_runner_cls: MagicMock,
    ) -> None:
        """settings.plugins_path=relative → discover_plugins called with Path.cwd() / relative."""
        from core.config.models import AppSettings
        from main import _create_runner

        settings = AppSettings(plugins_path="relative/path")
        mock_discover.return_value = []
        mock_runner_cls.return_value = MagicMock()

        _create_runner(settings)

        expected = Path.cwd() / Path("relative/path")
        mock_discover.assert_called_once_with(expected)

    def test_default_plugins_path_is_absolute_and_cwd_independent(self) -> None:
        """The loader default is absolute and points at the built-in plugins."""
        from core.plugin import loader
        from core.plugin.loader import _DEFAULT_PLUGINS_PATH

        loader_file = Path(loader.__file__).resolve()
        expected = loader_file.parents[2] / "plugins"
        assert _DEFAULT_PLUGINS_PATH.is_absolute()
        assert expected == _DEFAULT_PLUGINS_PATH


class TestGracefulShutdownSignals:
    """GAR-001: run_worker signal handler behavior in standalone vs both mode."""

    def test_run_worker_run_once_catches_keyboard_interrupt(self, caplog: pytest.LogCaptureFixture) -> None:
        """Test 2: Standalone run-once catches KeyboardInterrupt, logs shutdown message, no exception."""
        import logging

        from core.config.models import AppSettings
        from main import run_worker

        settings = AppSettings()
        interrupt_fired = threading.Event()

        def run_once_raises() -> dict:
            interrupt_fired.set()
            raise KeyboardInterrupt

        mock_runner = MagicMock()
        mock_runner.run_once.side_effect = run_once_raises

        keyboard_interrupt_propagated = False
        with patch("main.signal"), caplog.at_level(logging.INFO, logger="main"):
            try:
                run_worker(settings, run_once=True, runner=mock_runner)
            except KeyboardInterrupt:
                keyboard_interrupt_propagated = True

        assert interrupt_fired.is_set(), "mock_runner.run_once() was never called"
        assert not keyboard_interrupt_propagated, (
            "run_worker() propagated KeyboardInterrupt; expected it to be caught internally"
        )
        shutdown_records = [
            r.message for r in caplog.records if "shutdown" in r.message.lower() or "interrupt" in r.message.lower()
        ]
        assert shutdown_records, (
            f"No shutdown/interrupt log message found. All messages: {[r.message for r in caplog.records]}"
        )

    def test_both_mode_shutdown_event_wired_to_runner(self) -> None:
        """Test 6a: run_worker with shutdown_event calls runner.set_shutdown_event(shutdown_event)."""
        from core.config.models import AppSettings
        from main import run_worker

        settings = AppSettings()
        mock_runner = MagicMock()
        mock_runner.run_once.return_value = {}
        shutdown_event = threading.Event()

        run_worker(settings, run_once=True, runner=mock_runner, shutdown_event=shutdown_event)

        mock_runner.set_shutdown_event.assert_called_once_with(shutdown_event)

    def test_run_worker_run_once_no_signal_in_non_main_thread(self) -> None:
        """Test 7: run_worker with shutdown_event from non-main thread does not call signal.signal."""
        from core.config.models import AppSettings
        from main import run_worker

        settings = AppSettings()
        mock_runner = MagicMock()
        mock_runner.run_once.return_value = {}
        shutdown_event = threading.Event()

        signal_call_count: list[int] = [0]
        exc_holder: list[Exception] = []

        def run_in_thread() -> None:
            try:
                with patch("main.signal") as mock_sig:
                    mock_sig.signal.side_effect = lambda *a: signal_call_count.__setitem__(0, signal_call_count[0] + 1)
                    run_worker(settings, run_once=True, runner=mock_runner, shutdown_event=shutdown_event)
            except Exception as exc:
                exc_holder.append(exc)

        t = threading.Thread(target=run_in_thread)
        t.start()
        t.join(timeout=5)

        assert not exc_holder, f"Unexpected exception in non-main thread: {exc_holder}"
        assert signal_call_count[0] == 0, (
            f"signal.signal was called {signal_call_count[0]} times from non-main thread; expected 0"
        )

    def test_run_worker_run_once_standalone_installs_signal_handlers(self) -> None:
        """Test 8: Standalone run-once installs SIGINT and SIGTERM handlers, restores in finally."""
        import signal as _signal

        from core.config.models import AppSettings
        from main import run_worker

        settings = AppSettings()
        mock_runner = MagicMock()
        mock_runner.run_once.return_value = {}

        with patch("main.signal") as mock_sig:
            mock_sig.SIGINT = _signal.SIGINT
            mock_sig.SIGTERM = _signal.SIGTERM

            run_worker(settings, run_once=True, runner=mock_runner)

        registered_sigs = {call.args[0] for call in mock_sig.signal.call_args_list}
        assert _signal.SIGINT in registered_sigs, "SIGINT handler not installed"
        assert _signal.SIGTERM in registered_sigs, "SIGTERM handler not installed"
        assert mock_sig.signal.call_count >= 2, f"signal.signal called {mock_sig.signal.call_count} times; expected ≥2"


# ---------------------------------------------------------------------------
# TASK-106: CLI experience flags — --version, --validate, --show-config
# ---------------------------------------------------------------------------


class TestVersionFlag:
    """TASK-106: --version flag exits 0 via argparse built-in."""

    def test_parse_args_version_exits_zero(self) -> None:
        with pytest.raises(SystemExit) as exc_info:
            parse_args(["--version"])
        assert exc_info.value.code == 0


class TestNewCLIFlags:
    """TASK-106: New flags parsed correctly."""

    def test_parse_args_validate_flag(self) -> None:
        args = parse_args(["--config-file", "x.yaml", "--validate"])
        assert args.validate is True
        assert args.show_config is False

    def test_parse_args_show_config_flag(self) -> None:
        args = parse_args(["--config-file", "x.yaml", "--show-config"])
        assert args.show_config is True
        assert args.validate is False

    def test_parse_args_existing_flags_unchanged(self) -> None:
        """--config-file, --env-file, --run-once, --mode still work after new flags added."""
        args = parse_args(["--config-file", "c.yaml", "--env-file", ".env", "--run-once", "--mode", "api"])
        assert args.config_file == "c.yaml"
        assert args.env_file == ".env"
        assert args.run_once is True
        assert args.mode == "api"


class TestSelfManagedTelemetryCheckCLI:
    @pytest.mark.parametrize(
        ("other_args", "conflicting_option"),
        [
            (["--validate"], "--validate"),
            (["--show-config"], "--show-config"),
            (["--run-once"], "--run-once"),
            (["--emit-once"], "--emit-once"),
            (["--mode", "worker"], "--mode"),
            (["--mode", "api"], "--mode"),
            (["--mode", "both"], "--mode"),
            (["--mode=worker"], "--mode"),
            (["--mode=api"], "--mode"),
            (["--mode=both"], "--mode"),
        ],
    )
    def test_checker_flag_rejects_each_terminal_action_with_argparse_error_contract(
        self,
        other_args: list[str],
        conflicting_option: str,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        with pytest.raises(SystemExit) as raised:
            parse_args(["--config-file", "config.yaml", "--check-self-managed-telemetry", *other_args])

        assert raised.value.code == 2
        assert capsys.readouterr().err.endswith(
            f"error: --check-self-managed-telemetry cannot be combined with {conflicting_option}\n"
        )

    def test_checker_flag_allows_config_and_environment_inputs_without_an_explicit_mode(self) -> None:
        args = parse_args(
            [
                "--config-file",
                "config.yaml",
                "--env-file",
                ".env",
                "--check-self-managed-telemetry",
            ]
        )

        assert args.check_self_managed_telemetry is True
        assert args.config_file == "config.yaml"
        assert args.env_file == ".env"
        assert args.mode == "worker"

    def test_preexisting_flag_combinations_remain_parser_compatible_without_checker_flag(self) -> None:
        args = parse_args(["--config-file", "config.yaml", "--run-once", "--emit-once", "--mode", "both"])

        assert args.run_once is True
        assert args.emit_once is True
        assert args.mode == "both"

    @patch("main.load_config")
    def test_checker_exits_before_pipeline_dependencies_when_no_self_managed_tenant_exists(
        self,
        mock_load: MagicMock,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        from core.config.models import AppSettings, TenantConfig
        from main import main

        mock_load.return_value = AppSettings(
            tenants={"cloud": TenantConfig(ecosystem="confluent_cloud", tenant_id="cloud-tenant")}
        )

        with patch("main._create_runner") as create_runner, pytest.raises(SystemExit) as raised:
            main(["--config-file", "config.yaml", "--check-self-managed-telemetry"])

        assert raised.value.code == 2
        assert capsys.readouterr().err == "error: no self-managed Kafka tenant is configured\n"
        create_runner.assert_not_called()

    @patch("main.load_config")
    def test_checker_validates_every_selected_tenant_before_constructing_a_prometheus_source(
        self,
        mock_load: MagicMock,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        from core.config.models import AppSettings, PluginSettingsBase, StorageConfig, TenantConfig
        from main import main

        invalid = {
            "cluster_id": "cluster-a",
            "metrics_identifier": "kafka-a",
            "metrics_identifier_label": "not-a-label",
            "broker_count": 3,
            "cost_model": {
                "compute_hourly_rate": "0.10",
                "storage_per_gib_hourly": "0.0001",
                "network_ingress_per_gib": "0.01",
                "network_egress_per_gib": "0.02",
            },
            "metrics": {"url": "http://prometheus:9090"},
        }
        valid = {**invalid, "cluster_id": "cluster-b", "metrics_identifier_label": "deployment"}
        mock_load.return_value = AppSettings(
            tenants={
                "first": TenantConfig(
                    ecosystem="self_managed_kafka",
                    tenant_id="tenant-a",
                    storage=StorageConfig(connection_string="sqlite:///first-check.db"),
                    plugin_settings=PluginSettingsBase.model_validate(invalid),
                ),
                "second": TenantConfig(
                    ecosystem="self_managed_kafka",
                    tenant_id="tenant-b",
                    storage=StorageConfig(connection_string="sqlite:///second-check.db"),
                    plugin_settings=PluginSettingsBase.model_validate(valid),
                ),
            }
        )

        with (
            patch("plugins.self_managed_kafka.telemetry_check.create_metrics_source") as create_source,
            patch("main._create_runner") as create_runner,
            pytest.raises(SystemExit) as raised,
        ):
            main(["--config-file", "config.yaml", "--check-self-managed-telemetry"])

        assert raised.value.code == 1
        assert "Telemetry check configuration failed:" in capsys.readouterr().err
        create_source.assert_not_called()
        create_runner.assert_not_called()

    @pytest.mark.parametrize(
        ("selector_label", "field_path", "value", "expected_selector", "category", "reason"),
        [
            (None, "compute_hourly_rate", "-0.125", "kafka_cluster_id=kafka-a", "compute", "negative"),
            (
                "deployment",
                "region_overrides.us-west-2.network_egress_per_gib",
                "Infinity",
                "deployment=kafka-a",
                "network_egress",
                "non_finite",
            ),
        ],
    )
    @patch("plugins.self_managed_kafka.telemetry_check.create_metrics_source")
    @patch("main.load_config")
    def test_checker_invalid_rates_use_sanitized_tenant_diagnostics(
        self,
        mock_load: MagicMock,
        create_source: MagicMock,
        capsys: pytest.CaptureFixture[str],
        selector_label: str | None,
        field_path: str,
        value: str,
        expected_selector: str,
        category: str,
        reason: str,
    ) -> None:
        from core.config.models import AppSettings, PluginSettingsBase, StorageConfig, TenantConfig
        from main import main

        settings = _checker_tenant_settings("cluster-a", "kafka-a")
        if selector_label is None:
            settings.pop("metrics_identifier_label")
        else:
            settings["metrics_identifier_label"] = selector_label
        cost_model = settings["cost_model"]
        assert isinstance(cost_model, dict)
        if field_path.startswith("region_overrides"):
            cost_model["region_overrides"] = {"us-west-2": {field_path.rsplit(".", maxsplit=1)[1]: value}}
        else:
            cost_model[field_path] = value
        mock_load.return_value = AppSettings(
            tenants={
                "kafka-prod": TenantConfig(
                    ecosystem="self_managed_kafka",
                    tenant_id="tenant-check",
                    storage=StorageConfig(connection_string="sqlite:///checker-invalid-rate.db"),
                    plugin_settings=PluginSettingsBase.model_validate(settings),
                )
            }
        )

        with pytest.raises(SystemExit) as raised:
            main(["--config-file", "config.yaml", "--check-self-managed-telemetry"])

        assert raised.value.code == 1
        detail = capsys.readouterr().err
        assert "Telemetry check configuration failed:" in detail
        assert "invalid_self_managed_cost_rate" in detail
        assert "tenant=tenant-check" in detail
        assert "cluster=cluster-a" in detail
        assert f"selector={expected_selector}" in detail
        assert f"field=cost_model.{field_path}" in detail
        assert f"category={category}" in detail
        assert f"reason={reason}" in detail
        assert "date=" not in detail
        assert value not in detail
        assert "http://prometheus:9090" not in detail
        create_source.assert_not_called()

    @patch("main.load_config")
    def test_checker_main_path_continues_after_source_construction_failure_without_starting_runtime(
        self,
        mock_load: MagicMock,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        import json

        from core.config.models import AppSettings, PluginSettingsBase, StorageConfig, TenantConfig
        from main import main

        class Source:
            def __init__(self) -> None:
                self.closed = False
                self.calls: list[object] = []

            def query(
                self,
                queries: Sequence[MetricQuery],
                start: datetime,
                end: datetime,
                step: timedelta = timedelta(hours=1),
                resource_id_filter: str | None = None,
            ) -> dict[str, list[MetricRow]]:
                del start, end, step, resource_id_filter
                assert len(queries) == 1
                [query] = queries
                self.calls.append(query)
                return {query.key: []}

            def close(self) -> None:
                self.closed = True

        plugin_settings = {
            "cluster_id": "cluster-a",
            "metrics_identifier": "kafka-a",
            "metrics_identifier_label": "deployment",
            "broker_count": 3,
            "cost_model": {
                "compute_hourly_rate": "0.10",
                "storage_per_gib_hourly": "0.0001",
                "network_ingress_per_gib": "0.01",
                "network_egress_per_gib": "0.02",
            },
            "metrics": {"url": "http://prometheus:9090"},
            "identity_source": {"source": "static"},
        }
        mock_load.return_value = AppSettings(
            tenants={
                "first": TenantConfig(
                    ecosystem="self_managed_kafka",
                    tenant_id="tenant-a",
                    storage=StorageConfig(connection_string="sqlite:///first-check.db"),
                    plugin_settings=PluginSettingsBase.model_validate(plugin_settings),
                ),
                "second": TenantConfig(
                    ecosystem="self_managed_kafka",
                    tenant_id="tenant-b",
                    storage=StorageConfig(connection_string="sqlite:///second-check.db"),
                    plugin_settings=PluginSettingsBase.model_validate(
                        {**plugin_settings, "cluster_id": "cluster-b", "metrics_identifier": "kafka-b"}
                    ),
                ),
            }
        )
        source = Source()

        with (
            patch(
                "plugins.self_managed_kafka.telemetry_check.create_metrics_source",
                side_effect=[RuntimeError("first unavailable"), source],
            ) as create_source,
            patch("main._create_runner") as create_runner,
            patch("main._build_storage") as build_storage,
            pytest.raises(SystemExit) as raised,
        ):
            main(["--config-file", "config.yaml", "--check-self-managed-telemetry"])

        assert raised.value.code == 1
        report = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
        assert len(report) == 17
        assert [record["tenant"] for record in report[:8]] == ["first"] * 8
        assert [record["tenant"] for record in report[8:16]] == ["second"] * 8
        assert report[-1] == {
            "summary": {"inconclusive": 5, "invalid": 0, "not_observed": 5, "skipped": 6, "valid": 0},
            "tenants": 2,
        }
        assert create_source.call_count == 2
        assert len(source.calls) == 5
        assert source.closed is True
        create_runner.assert_not_called()
        build_storage.assert_not_called()

    @patch("main.load_config")
    def test_checker_success_uses_production_factory_closes_sources_and_shares_one_utc_anchor(
        self,
        mock_load: MagicMock,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        import json

        from core.config.models import AppSettings, PluginSettingsBase, StorageConfig, TenantConfig
        from main import main

        settings_a = _checker_tenant_settings("cluster-a", "kafka-a")
        settings_b = _checker_tenant_settings("cluster-b", "kafka-b")
        mock_load.return_value = AppSettings(
            tenants={
                "first": TenantConfig(
                    ecosystem="self_managed_kafka",
                    tenant_id="tenant-a",
                    storage=StorageConfig(connection_string="sqlite:///first-check.db"),
                    plugin_settings=PluginSettingsBase.model_validate(settings_a),
                ),
                "second": TenantConfig(
                    ecosystem="self_managed_kafka",
                    tenant_id="tenant-b",
                    storage=StorageConfig(connection_string="sqlite:///second-check.db"),
                    plugin_settings=PluginSettingsBase.model_validate(settings_b),
                ),
            }
        )
        sources = [_CheckerSource(), _CheckerSource()]
        anchor = datetime(2026, 8, 23, 12, tzinfo=UTC)

        with (
            patch("plugins.self_managed_kafka.telemetry_check.create_metrics_source", side_effect=sources) as factory,
            patch("main._create_runner") as create_runner,
            patch("main._build_storage") as build_storage,
            patch("main.setup_logging") as setup_logging,
            patch("plugins.self_managed_kafka.gathering.admin_api.create_admin_client") as admin_factory,
            patch("main.datetime") as datetime_cls,
            pytest.raises(SystemExit) as raised,
        ):
            datetime_cls.now.return_value = anchor
            main(["--config-file", "config.yaml", "--check-self-managed-telemetry"])

        assert raised.value.code == 0
        output = capsys.readouterr().out
        report_lines = output.splitlines()
        report = [json.loads(line) for line in report_lines]
        assert len(report) == 17
        assert [record["tenant"] for record in report[:8]] == ["first"] * 8
        assert [record["tenant"] for record in report[8:16]] == ["second"] * 8
        assert [record["canonical_metric"] for record in report[:8]] == [
            "up",
            "kafka_server_brokertopicmetrics_alltopics_bytesin_total",
            "kafka_server_brokertopicmetrics_alltopics_bytesout_total",
            "kafka_log_log_size",
            "kafka_server_brokertopicmetrics_bytesin_total",
            "kafka_server_brokertopicmetrics_bytesout_total",
            "kafka_server_quota_byte_rate",
            "kafka_server_quota_throttle_time_ms",
        ]
        assert report[0] == {
            "affected_feature": ["target_scope"],
            "canonical_metric": "up",
            "corrective_override": None,
            "expected_labels": {},
            "observed_labels": ["deployment"],
            "resolved_metric": "up",
            "selector": 'deployment="kafka-a"',
            "state": "valid",
            "tenant": "first",
            "warning": None,
        }
        assert report[-1] == {
            "summary": {"inconclusive": 0, "invalid": 0, "not_observed": 0, "skipped": 0, "valid": 16},
            "tenants": 2,
        }
        assert report_lines[-1] == (
            '{"summary":{"inconclusive":0,"invalid":0,"not_observed":0,"skipped":0,"valid":16},"tenants":2}'
        )
        assert output.endswith("\n")
        assert factory.call_count == 2
        assert [call.args[0].url for call in factory.call_args_list] == [
            "http://prometheus:9090",
            "http://prometheus:9090",
        ]
        assert all(source.closed for source in sources)
        assert {end for source in sources for _, _, end, _, _ in source.calls} == {anchor}
        datetime_cls.now.assert_called_once_with(UTC)
        create_runner.assert_not_called()
        build_storage.assert_not_called()
        setup_logging.assert_not_called()
        admin_factory.assert_not_called()

    @patch("main.load_config")
    def test_checker_main_path_continues_after_family_query_failure_and_closes_source(
        self,
        mock_load: MagicMock,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        import json

        from core.config.models import AppSettings, PluginSettingsBase, StorageConfig, TenantConfig
        from main import main

        settings = _checker_tenant_settings("cluster-a", "kafka-a")
        mock_load.return_value = AppSettings(
            tenants={
                "first": TenantConfig(
                    ecosystem="self_managed_kafka",
                    tenant_id="tenant-a",
                    storage=StorageConfig(connection_string="sqlite:///first-check.db"),
                    plugin_settings=PluginSettingsBase.model_validate(settings),
                )
            }
        )
        failed_key = "telemetry_check_kafka_server_quota_byte_rate"
        source = _CheckerSource(failure_key=failed_key)
        anchor = datetime(2026, 8, 23, 12, tzinfo=UTC)

        with (
            patch("plugins.self_managed_kafka.telemetry_check.create_metrics_source", return_value=source) as factory,
            patch("main.WorkflowRunner") as workflow_runner,
            patch("main._create_runner") as create_runner,
            patch("main._build_storage") as build_storage,
            patch("main.run_api") as run_api,
            patch("main.run_worker") as run_worker,
            patch("main.setup_logging") as setup_logging,
            patch("plugins.self_managed_kafka.gathering.admin_api.create_admin_client") as admin_factory,
            patch("main.datetime") as datetime_cls,
            pytest.raises(SystemExit) as raised,
        ):
            datetime_cls.now.return_value = anchor
            main(["--config-file", "config.yaml", "--check-self-managed-telemetry"])

        assert raised.value.code == 1
        output = capsys.readouterr().out
        report_lines = output.splitlines()
        report = [json.loads(line) for line in report_lines]
        assert report == [
            {
                "tenant": "first",
                "canonical_metric": "up",
                "state": "valid",
                "resolved_metric": "up",
                "selector": 'deployment="kafka-a"',
                "expected_labels": {},
                "observed_labels": ["deployment"],
                "affected_feature": ["target_scope"],
                "corrective_override": None,
                "warning": None,
            },
            {
                "tenant": "first",
                "canonical_metric": "kafka_server_brokertopicmetrics_alltopics_bytesin_total",
                "state": "valid",
                "resolved_metric": "kafka_server_brokertopicmetrics_alltopics_bytesin_total",
                "selector": 'deployment="kafka-a"',
                "expected_labels": {"broker": "broker"},
                "observed_labels": ["broker", "deployment"],
                "affected_feature": ["cluster_ingress"],
                "corrective_override": None,
                "warning": None,
            },
            {
                "tenant": "first",
                "canonical_metric": "kafka_server_brokertopicmetrics_alltopics_bytesout_total",
                "state": "valid",
                "resolved_metric": "kafka_server_brokertopicmetrics_alltopics_bytesout_total",
                "selector": 'deployment="kafka-a"',
                "expected_labels": {"broker": "broker"},
                "observed_labels": ["broker", "deployment"],
                "affected_feature": ["cluster_egress"],
                "corrective_override": None,
                "warning": None,
            },
            {
                "tenant": "first",
                "canonical_metric": "kafka_log_log_size",
                "state": "valid",
                "resolved_metric": "kafka_log_log_size",
                "selector": 'deployment="kafka-a"',
                "expected_labels": {"broker": "broker", "topic": "topic", "partition": "partition"},
                "observed_labels": ["broker", "deployment", "partition", "topic"],
                "affected_feature": ["cluster_storage", "prometheus_discovery", "topic_storage"],
                "corrective_override": None,
                "warning": None,
            },
            {
                "tenant": "first",
                "canonical_metric": "kafka_server_brokertopicmetrics_bytesin_total",
                "state": "valid",
                "resolved_metric": "kafka_server_brokertopicmetrics_bytesin_total",
                "selector": 'deployment="kafka-a"',
                "expected_labels": {"broker": "broker", "topic": "topic"},
                "observed_labels": ["broker", "deployment", "topic"],
                "affected_feature": ["prometheus_discovery", "topic_ingress"],
                "corrective_override": None,
                "warning": None,
            },
            {
                "tenant": "first",
                "canonical_metric": "kafka_server_brokertopicmetrics_bytesout_total",
                "state": "valid",
                "resolved_metric": "kafka_server_brokertopicmetrics_bytesout_total",
                "selector": 'deployment="kafka-a"',
                "expected_labels": {"broker": "broker", "topic": "topic"},
                "observed_labels": ["broker", "deployment", "topic"],
                "affected_feature": ["prometheus_discovery", "topic_egress"],
                "corrective_override": None,
                "warning": None,
            },
            {
                "tenant": "first",
                "canonical_metric": "kafka_server_quota_byte_rate",
                "state": "inconclusive",
                "resolved_metric": "kafka_server_quota_byte_rate",
                "selector": 'deployment="kafka-a"',
                "expected_labels": {
                    "broker": "broker",
                    "quota_type": "quota_type",
                    "quota_scope": "quota_scope",
                    "user": "user",
                    "client_id": "client_id",
                },
                "observed_labels": [],
                "affected_feature": ["principal_readiness", "principal_attribution"],
                "corrective_override": None,
                "warning": "Prometheus family query failed: MetricsQueryError.",
            },
            {
                "tenant": "first",
                "canonical_metric": "kafka_server_quota_throttle_time_ms",
                "state": "valid",
                "resolved_metric": "kafka_server_quota_throttle_time_ms",
                "selector": 'deployment="kafka-a"',
                "expected_labels": {
                    "broker": "broker",
                    "quota_type": "quota_type",
                    "quota_scope": "quota_scope",
                    "user": "user",
                    "client_id": "client_id",
                },
                "observed_labels": ["broker", "client_id", "deployment", "quota_scope", "quota_type", "user"],
                "affected_feature": ["principal_readiness"],
                "corrective_override": None,
                "warning": None,
            },
            {"summary": {"inconclusive": 1, "invalid": 0, "not_observed": 0, "skipped": 0, "valid": 7}, "tenants": 1},
        ]
        assert report_lines[-1] == (
            '{"summary":{"inconclusive":1,"invalid":0,"not_observed":0,"skipped":0,"valid":7},"tenants":1}'
        )
        assert output.endswith("\n")
        assert factory.call_count == 1
        assert source.closed is True
        assert len(source.calls) == 8
        assert source.calls[6][0].key == failed_key
        assert {end for _, _, end, _, _ in source.calls} == {anchor}
        datetime_cls.now.assert_called_once_with(UTC)
        workflow_runner.assert_not_called()
        create_runner.assert_not_called()
        build_storage.assert_not_called()
        run_api.assert_not_called()
        run_worker.assert_not_called()
        setup_logging.assert_not_called()
        admin_factory.assert_not_called()


class TestMainNoConfigFile:
    """TASK-106: main() without --config-file exits 2."""

    def test_main_no_config_file_exits_2(self, capsys: pytest.CaptureFixture[str]) -> None:
        from main import main

        with pytest.raises(SystemExit) as exc_info:
            main([])
        assert exc_info.value.code == 2
        captured = capsys.readouterr()
        assert "--config-file" in captured.err


class TestValidateFlag:
    """TASK-106: --validate flag behaviour."""

    @patch("main.load_config")
    def test_validate_valid_config_prints_and_exits_0(
        self,
        mock_load: MagicMock,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        from core.config.models import AppSettings
        from main import main

        mock_load.return_value = AppSettings()

        with pytest.raises(SystemExit) as exc_info:
            main(["--config-file", "x.yaml", "--validate"])
        assert exc_info.value.code == 0
        captured = capsys.readouterr()
        assert "Config is valid." in captured.out

    @patch("main.load_config")
    def test_validate_invalid_config_prints_to_stderr_and_exits_1(
        self,
        mock_load: MagicMock,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        from pydantic import BaseModel, ValidationError

        from main import main

        class _Dummy(BaseModel):
            x: int

        try:
            _Dummy(x="not_an_int")  # type: ignore[arg-type]
        except ValidationError as exc:
            validation_error = exc

        mock_load.side_effect = validation_error

        with pytest.raises(SystemExit) as exc_info:
            main(["--config-file", "x.yaml", "--validate"])
        assert exc_info.value.code == 1
        captured = capsys.readouterr()
        assert "Config validation failed" in captured.err


class TestShowConfigFlag:
    """TASK-106: --show-config flag outputs JSON with masked secrets and exits 0."""

    @patch("main.load_config")
    def test_show_config_prints_json_and_exits_0(
        self,
        mock_load: MagicMock,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        import json

        from core.config.models import AppSettings, PluginSettingsBase, StorageConfig, TenantConfig
        from main import main

        mock_load.return_value = AppSettings(
            tenants={
                "prod": TenantConfig(
                    ecosystem="eco",
                    tenant_id="t1",
                    storage=StorageConfig(connection_string="sqlite:///prod.db"),
                    plugin_settings=PluginSettingsBase(api_key="should-be-excluded"),
                )
            }
        )

        with pytest.raises(SystemExit) as exc_info:
            main(["--config-file", "x.yaml", "--show-config"])
        assert exc_info.value.code == 0
        captured = capsys.readouterr()
        # stdout must be valid JSON
        data = json.loads(captured.out)
        assert isinstance(data, dict)
        # plugin_settings must be excluded from all tenant entries
        for tenant_data in data.get("tenants", {}).values():
            assert "plugin_settings" not in tenant_data

    @patch("main.load_config")
    def test_show_config_masks_connection_string(
        self,
        mock_load: MagicMock,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        from core.config.models import AppSettings, StorageConfig, TenantConfig
        from main import main

        mock_load.return_value = AppSettings(
            tenants={
                "prod": TenantConfig(
                    ecosystem="eco",
                    tenant_id="t1",
                    storage=StorageConfig(connection_string="postgresql://u:secret@h/db"),
                )
            }
        )

        with pytest.raises(SystemExit) as exc_info:
            main(["--config-file", "x.yaml", "--show-config"])
        assert exc_info.value.code == 0
        captured = capsys.readouterr()
        assert "secret" not in captured.out
        assert "**********" in captured.out

    @patch("main.load_config")
    def test_show_config_excludes_plugin_settings(
        self,
        mock_load: MagicMock,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        import json

        from core.config.models import AppSettings, PluginSettingsBase, TenantConfig
        from main import main

        mock_load.return_value = AppSettings(
            tenants={
                "prod": TenantConfig(
                    ecosystem="eco",
                    tenant_id="t1",
                    plugin_settings=PluginSettingsBase(ccloud_api_secret="super-secret"),
                )
            }
        )

        with pytest.raises(SystemExit) as exc_info:
            main(["--config-file", "x.yaml", "--show-config"])
        assert exc_info.value.code == 0
        captured = capsys.readouterr()
        data = json.loads(captured.out)
        for tenant_data in data.get("tenants", {}).values():
            assert "plugin_settings" not in tenant_data

    def test_show_config_masks_secret_str_fields(self) -> None:
        """Pydantic v2 serialises SecretStr as '**********' in model_dump_json."""
        from pydantic import BaseModel, SecretStr

        class _Model(BaseModel):
            password: SecretStr
            name: str

        m = _Model(password="s3cr3t", name="alice")
        output = m.model_dump_json()
        assert "s3cr3t" not in output
        assert "**********" in output


class TestValidatePluginConfigs:
    """TASK-132: _validate_plugin_configs() catches plugin-specific config errors."""

    # ------------------------------------------------------------------ test 1
    @patch("core.plugin.loader.discover_plugins")
    @patch("main.load_config")
    def test_cku_ratio_sum_failure_caught(
        self,
        mock_load: MagicMock,
        mock_discover: MagicMock,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        from core.config.models import AppSettings, PluginSettingsBase, TenantConfig
        from main import main
        from plugins.confluent_cloud import ConfluentCloudPlugin

        mock_discover.return_value = [("confluent_cloud", ConfluentCloudPlugin)]
        settings = AppSettings(
            tenants={
                "prod": TenantConfig(
                    ecosystem="confluent_cloud",
                    tenant_id="t-prod",
                    plugin_settings=PluginSettingsBase(
                        ccloud_api={"key": "k", "secret": "s"},
                        allocator_params={
                            "kafka_cku_usage_ratio": 0.7,
                            "kafka_cku_shared_ratio": 0.5,  # sum = 1.2, invalid
                        },
                    ),
                )
            }
        )
        mock_load.return_value = settings

        with pytest.raises(SystemExit) as exc_info:
            main(["--config-file", "x.yaml", "--validate"])
        assert exc_info.value.code == 1
        assert "sum to 1.0" in capsys.readouterr().err

    # ------------------------------------------------------------------ test 2
    @patch("core.plugin.loader.discover_plugins")
    @patch("main.load_config")
    def test_missing_ccloud_api_field_caught(
        self,
        mock_load: MagicMock,
        mock_discover: MagicMock,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        from core.config.models import AppSettings, PluginSettingsBase, TenantConfig
        from main import main
        from plugins.confluent_cloud import ConfluentCloudPlugin

        mock_discover.return_value = [("confluent_cloud", ConfluentCloudPlugin)]
        settings = AppSettings(
            tenants={
                "prod": TenantConfig(
                    ecosystem="confluent_cloud",
                    tenant_id="t-prod",
                    plugin_settings=PluginSettingsBase(),  # no ccloud_api
                )
            }
        )
        mock_load.return_value = settings

        with pytest.raises(SystemExit) as exc_info:
            main(["--config-file", "x.yaml", "--validate"])
        assert exc_info.value.code == 1
        assert "ccloud_api" in capsys.readouterr().err

    # ------------------------------------------------------------------ test 3
    @patch("core.plugin.loader.discover_plugins")
    @patch("main.load_config")
    def test_valid_config_exits_0(
        self,
        mock_load: MagicMock,
        mock_discover: MagicMock,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        from core.config.models import AppSettings, PluginSettingsBase, TenantConfig
        from main import main
        from plugins.confluent_cloud import ConfluentCloudPlugin

        mock_discover.return_value = [("confluent_cloud", ConfluentCloudPlugin)]
        settings = AppSettings(
            tenants={
                "prod": TenantConfig(
                    ecosystem="confluent_cloud",
                    tenant_id="t-prod",
                    plugin_settings=PluginSettingsBase(
                        ccloud_api={"key": "k", "secret": "s"},
                    ),
                )
            }
        )
        mock_load.return_value = settings

        with pytest.raises(SystemExit) as exc_info:
            main(["--config-file", "x.yaml", "--validate"])
        assert exc_info.value.code == 0
        assert "Config is valid." in capsys.readouterr().out

    # ------------------------------------------------------------------ test 4
    @patch("core.plugin.loader.discover_plugins")
    @patch("main.load_config")
    def test_self_managed_kafka_invalid_config_caught(
        self,
        mock_load: MagicMock,
        mock_discover: MagicMock,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        from core.config.models import AppSettings, PluginSettingsBase, TenantConfig
        from main import main
        from plugins.self_managed_kafka import SelfManagedKafkaPlugin

        mock_discover.return_value = [("self_managed_kafka", SelfManagedKafkaPlugin)]
        settings = AppSettings(
            tenants={
                "kafka-prod": TenantConfig(
                    ecosystem="self_managed_kafka",
                    tenant_id="t-kafka",
                    # missing required: cluster_id, broker_count, cost_model, metrics
                    plugin_settings=PluginSettingsBase(),
                )
            }
        )
        mock_load.return_value = settings

        with pytest.raises(SystemExit) as exc_info:
            main(["--config-file", "x.yaml", "--validate"])
        assert exc_info.value.code == 1
        assert "kafka-prod" in capsys.readouterr().err

    # ------------------------------------------------------------------ test 5
    @patch("core.plugin.loader.discover_plugins")
    @patch("main.load_config")
    def test_generic_metrics_only_empty_cost_types_caught(
        self,
        mock_load: MagicMock,
        mock_discover: MagicMock,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        from core.config.models import AppSettings, PluginSettingsBase, TenantConfig
        from main import main
        from plugins.generic_metrics_only import GenericMetricsOnlyPlugin

        mock_discover.return_value = [("generic_metrics_only", GenericMetricsOnlyPlugin)]
        settings = AppSettings(
            tenants={
                "pg-prod": TenantConfig(
                    ecosystem="generic_metrics_only",
                    tenant_id="t-pg",
                    plugin_settings=PluginSettingsBase(
                        ecosystem_name="self_managed_postgres",
                        cluster_id="cluster-1",
                        metrics={"url": "http://prom:9090"},
                        identity_source={"source": "static"},
                        cost_types=[],  # violates min_length=1
                    ),
                )
            }
        )
        mock_load.return_value = settings

        with pytest.raises(SystemExit) as exc_info:
            main(["--config-file", "x.yaml", "--validate"])
        assert exc_info.value.code == 1

    # ------------------------------------------------------------------ test 6
    @patch("core.plugin.loader.discover_plugins")
    @patch("main.load_config")
    def test_unknown_ecosystem_exits_1(
        self,
        mock_load: MagicMock,
        mock_discover: MagicMock,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        from core.config.models import AppSettings, PluginSettingsBase, TenantConfig
        from main import main

        mock_discover.return_value = []  # no plugins registered
        settings = AppSettings(
            tenants={
                "mystery": TenantConfig(
                    ecosystem="nonexistent",
                    tenant_id="t-mystery",
                    plugin_settings=PluginSettingsBase(),
                )
            }
        )
        mock_load.return_value = settings

        with pytest.raises(SystemExit) as exc_info:
            main(["--config-file", "x.yaml", "--validate"])
        assert exc_info.value.code == 1
        assert "unknown ecosystem" in capsys.readouterr().err.lower()

    # ------------------------------------------------------------------ test 7
    @patch("core.plugin.loader.discover_plugins")
    @patch("main.load_config")
    def test_multiple_tenant_errors_all_reported(
        self,
        mock_load: MagicMock,
        mock_discover: MagicMock,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        from core.config.models import AppSettings, PluginSettingsBase, StorageConfig, TenantConfig
        from main import main
        from plugins.confluent_cloud import ConfluentCloudPlugin

        mock_discover.return_value = [("confluent_cloud", ConfluentCloudPlugin)]
        settings = AppSettings(
            tenants={
                "alpha": TenantConfig(
                    ecosystem="confluent_cloud",
                    tenant_id="t-alpha",
                    storage=StorageConfig(connection_string="sqlite:///alpha.db"),
                    plugin_settings=PluginSettingsBase(),  # no ccloud_api
                ),
                "beta": TenantConfig(
                    ecosystem="confluent_cloud",
                    tenant_id="t-beta",
                    storage=StorageConfig(connection_string="sqlite:///beta.db"),
                    plugin_settings=PluginSettingsBase(),  # no ccloud_api
                ),
            }
        )
        mock_load.return_value = settings

        with pytest.raises(SystemExit) as exc_info:
            main(["--config-file", "x.yaml", "--validate"])
        assert exc_info.value.code == 1
        err = capsys.readouterr().err
        assert "alpha" in err
        assert "beta" in err

    # ------------------------------------------------------------------ test 8
    @patch("core.plugin.loader.discover_plugins")
    @patch("main.load_config")
    def test_plugin_without_validate_method_skipped_silently(
        self,
        mock_load: MagicMock,
        mock_discover: MagicMock,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        from core.config.models import AppSettings, PluginSettingsBase, TenantConfig
        from main import main

        class _NoValidatePlugin:
            """Minimal plugin stub — no validate_plugin_settings method."""

        mock_discover.return_value = [("no_validate_eco", _NoValidatePlugin)]
        settings = AppSettings(
            tenants={
                "prod": TenantConfig(
                    ecosystem="no_validate_eco",
                    tenant_id="t-prod",
                    plugin_settings=PluginSettingsBase(),
                )
            }
        )
        mock_load.return_value = settings

        with pytest.raises(SystemExit) as exc_info:
            main(["--config-file", "x.yaml", "--validate"])
        assert exc_info.value.code == 0

    # ------------------------------------------------------------------ test 9
    def test_ecosystem_plugin_protocol_unchanged(self) -> None:
        from core.plugin.protocols import EcosystemPlugin

        assert not hasattr(EcosystemPlugin, "validate_plugin_settings"), (
            "validate_plugin_settings must NOT be added to the EcosystemPlugin protocol"
        )

    # ------------------------------------------------------------------ test 10
    @patch("core.plugin.loader.discover_plugins")
    def test_build_registry_importable_and_populates_registry(
        self,
        mock_discover: MagicMock,
    ) -> None:
        from core.config.models import AppSettings
        from main import _build_registry
        from plugins.confluent_cloud import ConfluentCloudPlugin

        mock_discover.return_value = [("confluent_cloud", ConfluentCloudPlugin)]
        registry = _build_registry(AppSettings())
        assert "confluent_cloud" in registry.list_ecosystems()


# ---------- Case 12: --emit-once standalone ----------


class TestEmitOnce:
    """Case 12: --emit-once runs EmitterRunner for all tenants, no pipeline triggered."""

    @patch("main.EmitterRunner")
    @patch("main.load_config")
    def test_emit_once_runs_emitter_runner_for_all_tenants(
        self,
        mock_load: MagicMock,
        mock_runner_cls: MagicMock,
    ) -> None:
        from core.config.models import AppSettings, StorageConfig, TenantConfig

        settings = AppSettings(
            tenants={
                "tenant-a": TenantConfig(
                    ecosystem="test-eco",
                    tenant_id="t-a",
                    lookback_days=30,
                    storage=StorageConfig(connection_string="sqlite:///tmp/ta.db"),
                    plugin_settings={"emitters": [{"type": "csv", "params": {"output_dir": "/tmp"}}]},
                ),
                "tenant-b": TenantConfig(
                    ecosystem="test-eco",
                    tenant_id="t-b",
                    lookback_days=30,
                    storage=StorageConfig(connection_string="sqlite:///tmp/tb.db"),
                    plugin_settings={"emitters": [{"type": "csv", "params": {"output_dir": "/tmp"}}]},
                ),
            }
        )
        mock_load.return_value = settings

        mock_runner_instance = MagicMock()
        mock_runner_cls.return_value = mock_runner_instance

        mock_plugin = MagicMock()
        mock_plugin.get_service_handlers.return_value = {}
        mock_plugin.get_fallback_allocator.return_value = None
        mock_registry = MagicMock()
        mock_registry.create.return_value = mock_plugin

        from main import main

        with patch("main._build_storage") as mock_storage, patch("main._build_registry") as mock_reg:
            mock_storage.return_value = MagicMock()
            mock_reg.return_value = mock_registry
            main(["--config-file", "dummy.yaml", "--emit-once"])

        # EmitterRunner instantiated once per tenant
        assert mock_runner_cls.call_count == 2
        # run() called once per tenant
        assert mock_runner_instance.run.call_count == 2

    @patch("main.WorkflowRunner")
    @patch("main.load_config")
    def test_emit_once_does_not_trigger_pipeline(
        self,
        mock_load: MagicMock,
        mock_workflow_runner_cls: MagicMock,
    ) -> None:
        from core.config.models import AppSettings

        mock_load.return_value = AppSettings()
        mock_wf = MagicMock()
        mock_workflow_runner_cls.return_value = mock_wf

        from main import main

        with patch("main.EmitterRunner") as mock_er_cls, patch("main._build_storage") as mock_storage:
            mock_storage.return_value = MagicMock()
            mock_er_cls.return_value = MagicMock()
            main(["--config-file", "dummy.yaml", "--emit-once"])

        # WorkflowRunner.run_once() must NOT be called
        mock_wf.run_once.assert_not_called()
        mock_wf.run_loop.assert_not_called()

    def test_emit_once_flag_parsed_correctly(self) -> None:
        from main import parse_args

        args = parse_args(["--config-file", "c.yaml", "--emit-once"])
        assert args.emit_once is True

    def test_emit_once_not_set_by_default(self) -> None:
        from main import parse_args

        args = parse_args(["--config-file", "c.yaml"])
        assert args.emit_once is False

    @patch("main.load_config")
    def test_emit_once_initializes_plugin_before_flagged_storage_and_closes_in_reverse_order(
        self,
        mock_load: MagicMock,
    ) -> None:
        from core.config.models import AppSettings, StorageConfig, TenantConfig
        from main import main

        events: list[str] = []
        module = object()

        class Plugin:
            ecosystem = "confluent_cloud"

            def initialize(self, config: object) -> None:
                del config
                events.append("plugin.initialize")

            def get_storage_module(self) -> object:
                events.append("plugin.storage_module")
                return module

            def get_service_handlers(self) -> dict[str, object]:
                return {}

            def get_fallback_allocator(self) -> None:
                return None

            def close(self) -> None:
                events.append("plugin.close")

        plugin = Plugin()
        registry = MagicMock()
        registry.create.side_effect = lambda _ecosystem: events.append("registry.create") or plugin
        storage = MagicMock()
        storage.create_tables.side_effect = lambda: events.append("storage.create_tables")
        storage.dispose.side_effect = lambda: events.append("storage.dispose")
        mock_load.return_value = AppSettings(
            tenants={
                "production": TenantConfig(
                    ecosystem="confluent_cloud",
                    tenant_id="tenant-1",
                    storage=StorageConfig(connection_string="sqlite:///emit-once.db"),
                )
            }
        )

        def build_storage(*args: object, **kwargs: object) -> object:
            events.append("storage.create")
            assert kwargs["storage_module"] is module
            assert kwargs["focus_preview_enabled"] is False
            return storage

        with (
            patch("main._build_registry", return_value=registry),
            patch("main.create_storage_backend", side_effect=build_storage),
            patch("main.EmitterRunner") as runner_type,
        ):
            runner_type.return_value.run.side_effect = lambda _tenant_id: events.append("emit")
            main(["--config-file", "dummy.yaml", "--emit-once"])

        assert events == [
            "registry.create",
            "plugin.initialize",
            "plugin.storage_module",
            "storage.create",
            "storage.create_tables",
            "emit",
            "storage.dispose",
            "plugin.close",
        ]

    @patch("main.load_config")
    def test_emit_once_preserves_emit_error_and_attempts_storage_and_plugin_cleanup_once(
        self,
        mock_load: MagicMock,
    ) -> None:
        from core.config.models import AppSettings, StorageConfig, TenantConfig
        from main import main

        plugin = MagicMock()
        plugin.get_storage_module.return_value = object()
        plugin.close.side_effect = RuntimeError("plugin cleanup failed")
        registry = MagicMock()
        registry.create.return_value = plugin
        storage = MagicMock()
        storage.dispose.side_effect = RuntimeError("storage cleanup failed")
        mock_load.return_value = AppSettings(
            tenants={
                "production": TenantConfig(
                    ecosystem="confluent_cloud",
                    tenant_id="tenant-1",
                    storage=StorageConfig(connection_string="sqlite:///emit-once-failure.db"),
                )
            }
        )

        with (
            patch("main._build_registry", return_value=registry),
            patch("main.create_storage_backend", return_value=storage),
            patch("main.EmitterRunner") as runner_type,
            pytest.raises(ValueError, match="original emit failure"),
        ):
            runner_type.return_value.run.side_effect = ValueError("original emit failure")
            main(["--config-file", "dummy.yaml", "--emit-once"])

        storage.dispose.assert_called_once_with()
        plugin.close.assert_called_once_with()


class TestSelfManagedCostRateStartupDiagnostics:
    @pytest.mark.parametrize(
        ("selector_label", "field_path", "value", "expected_selector", "category", "reason"),
        [
            (
                None,
                "compute_hourly_rate",
                "-0.125",
                "kafka_cluster_id=kraft-a-001",
                "compute",
                "negative",
            ),
            (
                "deployment",
                "region_overrides.us-west-2.network_egress_per_gib",
                "Infinity",
                "deployment=kraft-a-001",
                "network_egress",
                "non_finite",
            ),
        ],
    )
    @patch("core.plugin.loader.discover_plugins")
    @patch("main.load_config")
    def test_explicit_startup_validation_reports_tenant_and_sanitized_rate_details(
        self,
        mock_load: MagicMock,
        mock_discover: MagicMock,
        capsys: pytest.CaptureFixture[str],
        selector_label: str | None,
        field_path: str,
        value: str,
        expected_selector: str,
        category: str,
        reason: str,
    ) -> None:
        from core.config.models import AppSettings, PluginSettingsBase, TenantConfig
        from main import main
        from plugins.self_managed_kafka import SelfManagedKafkaPlugin

        settings = _checker_tenant_settings("billing-cluster-a", "kraft-a-001")
        if selector_label is None:
            settings.pop("metrics_identifier_label")
        else:
            settings["metrics_identifier_label"] = selector_label
        cost_model = settings["cost_model"]
        assert isinstance(cost_model, dict)
        if field_path.startswith("region_overrides"):
            cost_model["region_overrides"] = {"us-west-2": {field_path.rsplit(".", maxsplit=1)[1]: value}}
        else:
            cost_model[field_path] = value
        mock_discover.return_value = [("self_managed_kafka", SelfManagedKafkaPlugin)]
        mock_load.return_value = AppSettings(
            tenants={
                "kafka-prod": TenantConfig(
                    ecosystem="self_managed_kafka",
                    tenant_id="tenant-273",
                    plugin_settings=PluginSettingsBase.model_validate(settings),
                )
            }
        )

        with pytest.raises(SystemExit) as error:
            main(["--config-file", "task-273.yaml", "--validate"])

        assert error.value.code == 1
        detail = capsys.readouterr().err
        assert "invalid_self_managed_cost_rate" in detail
        assert "tenant=tenant-273" in detail
        assert "cluster=billing-cluster-a" in detail
        assert f"selector={expected_selector}" in detail
        assert f"field=cost_model.{field_path}" in detail
        assert f"category={category}" in detail
        assert f"reason={reason}" in detail
        assert "date=" not in detail
        assert value not in detail
        assert "http://prometheus:9090" not in detail
