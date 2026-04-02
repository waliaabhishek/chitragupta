from __future__ import annotations

import threading
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from main import parse_args


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
    @patch("main.discover_plugins")
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
    @patch("main.discover_plugins")
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
    @patch("main.discover_plugins")
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
    @patch("main.discover_plugins")
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
    @patch("main.discover_plugins")
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
    @patch("main.discover_plugins")
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
    @patch("main.discover_plugins")
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
    @patch("main.discover_plugins")
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
    @patch("main.discover_plugins")
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
    @patch("main.discover_plugins")
    def test_create_runner_no_override_uses_default_plugins_path(
        self,
        mock_discover: MagicMock,
        mock_registry_cls: MagicMock,
        mock_runner_cls: MagicMock,
    ) -> None:
        """settings.plugins_path=None → discover_plugins called with _DEFAULT_PLUGINS_PATH."""
        from core.config.models import AppSettings
        from main import _DEFAULT_PLUGINS_PATH, _create_runner

        settings = AppSettings()
        assert settings.plugins_path is None
        mock_discover.return_value = []
        mock_runner_cls.return_value = MagicMock()

        _create_runner(settings)

        mock_discover.assert_called_once_with(_DEFAULT_PLUGINS_PATH)

    @patch("main.WorkflowRunner")
    @patch("main.PluginRegistry")
    @patch("main.discover_plugins")
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
    @patch("main.discover_plugins")
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
        """_DEFAULT_PLUGINS_PATH is absolute and equals Path(main.__file__).parent / "plugins"."""
        import main as main_module
        from main import _DEFAULT_PLUGINS_PATH

        main_file = Path(main_module.__file__).resolve()
        expected = main_file.parent / "plugins"
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
    @patch("main.discover_plugins")
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
    @patch("main.discover_plugins")
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
    @patch("main.discover_plugins")
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
    @patch("main.discover_plugins")
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
    @patch("main.discover_plugins")
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
    @patch("main.discover_plugins")
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
    @patch("main.discover_plugins")
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
    @patch("main.discover_plugins")
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
    @patch("main.discover_plugins")
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
