from __future__ import annotations

import argparse
import logging
import signal
import threading
from pathlib import Path
from typing import TYPE_CHECKING

from core.config.loader import load_config
from core.emitters.registry import register as register_emitter
from core.plugin.loader import discover_plugins
from core.plugin.registry import PluginRegistry
from emitters.csv_emitter import make_csv_emitter
from workflow_runner import WorkflowRunner

logger = logging.getLogger(__name__)


# Register built-in emitters at application startup
register_emitter("csv", make_csv_emitter)

if TYPE_CHECKING:
    from core.config.models import AppSettings
_DEFAULT_PLUGINS_PATH = Path(__file__).parent.parent / "plugins"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Chitragupt")
    parser.add_argument(
        "--config-file",
        required=True,
        help="Path to YAML configuration file",
    )
    parser.add_argument(
        "--env-file",
        default=None,
        help="Path to .env file (optional)",
    )
    parser.add_argument(
        "--run-once",
        action="store_true",
        default=False,
        help="Run pipeline once and exit (no loop)",
    )
    parser.add_argument(
        "--mode",
        choices=["worker", "api", "both"],
        default="worker",
        help="Run mode: worker (pipeline), api (HTTP server), or both",
    )
    return parser.parse_args(argv)


def setup_logging(settings: AppSettings) -> None:
    """Configure root logger and per-module levels from settings."""
    log_cfg = settings.logging
    logging.basicConfig(level=log_cfg.level, format=log_cfg.format)
    for module, level in log_cfg.per_module_levels.items():
        logging.getLogger(module).setLevel(level)


def _create_runner(settings: AppSettings) -> WorkflowRunner:
    """Create a WorkflowRunner with all plugins discovered from configured plugins path."""
    plugins_path = _DEFAULT_PLUGINS_PATH if settings.plugins_path is None else Path.cwd() / settings.plugins_path

    registry = PluginRegistry()
    for ecosystem, factory in discover_plugins(plugins_path):
        registry.register(ecosystem, factory)
    return WorkflowRunner(settings, registry)


def run_api(settings: AppSettings, runner: WorkflowRunner | None = None) -> None:
    """Start the FastAPI server."""
    import uvicorn

    from core.api.app import create_app

    app = create_app(settings, workflow_runner=runner)
    uvicorn.run(app, host=settings.api.host, port=settings.api.port)


def run_worker(
    settings: AppSettings,
    *,
    run_once: bool = False,
    runner: WorkflowRunner | None = None,
    shutdown_event: threading.Event | None = None,
) -> None:
    """Run the pipeline worker.

    Standalone 'worker' mode: creates its own runner, installs signal handlers.
    'both' mode: accepts injected runner and external shutdown_event.
    """
    if runner is None:
        runner = _create_runner(settings)

    if run_once:
        results = runner.run_once()
        for name, result in results.items():
            if result.errors:
                logger.error("Tenant %s errors: %s", name, result.errors)
            else:
                logger.info(
                    "Tenant %s: gathered=%d, calculated=%d, rows=%d",
                    name,
                    result.dates_gathered,
                    result.dates_calculated,
                    result.chargeback_rows_written,
                )
        return

    if shutdown_event is None:
        # Standalone worker: install signal handlers (main thread only)
        shutdown_event = threading.Event()

        def _signal_handler(signum: int, frame: object) -> None:
            logger.info("Received signal %d, shutting down...", signum)
            shutdown_event.set()

        signal.signal(signal.SIGINT, _signal_handler)
        signal.signal(signal.SIGTERM, _signal_handler)

    logger.info("Starting chargeback engine worker...")
    runner.run_loop(shutdown_event)
    logger.info("Chargeback engine worker stopped.")


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    settings = load_config(args.config_file, env_file=args.env_file)
    setup_logging(settings)

    mode = args.mode

    if mode == "api":
        run_api(settings)
    elif mode == "both":
        runner = _create_runner(settings)
        shutdown_event = threading.Event()

        worker_thread = threading.Thread(
            target=run_worker,
            args=(settings,),
            kwargs={
                "run_once": args.run_once,
                "runner": runner,
                "shutdown_event": shutdown_event,
            },
        )
        worker_thread.daemon = True
        worker_thread.start()
        run_api(settings, runner=runner)
        shutdown_event.set()
        worker_thread.join(timeout=30)
    else:
        run_worker(settings, run_once=args.run_once)


if __name__ == "__main__":
    main()
