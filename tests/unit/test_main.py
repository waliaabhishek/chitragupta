from __future__ import annotations

import threading
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from main import parse_args


class TestParseArgs:
    def test_config_file_required(self) -> None:
        with pytest.raises(SystemExit):
            parse_args([])

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

        def capture_run_api(settings: object, runner: object = None) -> None:
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
