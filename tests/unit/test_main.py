from __future__ import annotations

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
