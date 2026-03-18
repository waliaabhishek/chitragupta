from __future__ import annotations

import argparse
import logging
import signal
import sys
import threading
from pathlib import Path
from typing import TYPE_CHECKING

from core.api import get_version
from core.config.loader import load_config
from core.emitters.registry import register as register_emitter
from core.plugin.loader import discover_plugins
from core.plugin.registry import PluginRegistry
from emitters.csv_emitter import make_csv_emitter
from emitters.prometheus_emitter import make_prometheus_emitter
from workflow_runner import WorkflowRunner

logger = logging.getLogger(__name__)


# Register built-in emitters at application startup
register_emitter("csv", make_csv_emitter)
register_emitter("prometheus", make_prometheus_emitter)

if TYPE_CHECKING:
    from core.config.models import AppSettings
_DEFAULT_PLUGINS_PATH = Path(__file__).parent / "plugins"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Chitragupt")
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {get_version()}",
    )
    parser.add_argument(
        "--config-file",
        required=False,
        default=None,
        help="Path to YAML configuration file",
    )
    parser.add_argument(
        "--env-file",
        default=None,
        help="Path to .env file (optional)",
    )
    parser.add_argument(
        "--validate",
        action="store_true",
        default=False,
        help="Validate config file and exit",
    )
    parser.add_argument(
        "--show-config",
        action="store_true",
        default=False,
        help="Print resolved config (secrets masked) and exit",
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


def run_api(settings: AppSettings, runner: WorkflowRunner | None = None, mode: str = "api") -> None:
    """Start the FastAPI server."""
    import uvicorn

    from core.api.app import create_app

    app = create_app(settings, workflow_runner=runner, mode=mode)
    uvicorn.run(
        app,
        host=settings.api.host,
        port=settings.api.port,
        # Keep workers=1: --mode both requires shared in-process WorkflowRunner state.
        # Multiple workers would each have their own runner and race on the DB.
        workers=1,
        limit_concurrency=100,  # reject new connections with 503 after 100 concurrent
        timeout_keep_alive=10,  # close idle keep-alive sockets after 10s (default: 5)
    )


def run_worker(
    settings: AppSettings,
    *,
    run_once: bool = False,
    runner: WorkflowRunner | None = None,
    shutdown_event: threading.Event | None = None,
) -> None:
    if runner is None:
        runner = _create_runner(settings)

    if run_once:
        if shutdown_event is None:
            # Standalone run-once: main thread — safe to install signal handlers.
            local_event = threading.Event()

            def _once_handler(signum: int, frame: object) -> None:
                logger.info("Received signal %d, shutting down...", signum)
                local_event.set()

            prev_int = signal.signal(signal.SIGINT, _once_handler)
            prev_term = signal.signal(signal.SIGTERM, _once_handler)
            runner.set_shutdown_event(local_event)
            try:
                results = runner.run_once()
            except KeyboardInterrupt:
                logger.info("Shutdown requested.")
                results = {}
            finally:
                signal.signal(signal.SIGINT, prev_int)
                signal.signal(signal.SIGTERM, prev_term)
        else:
            # Injected event (both mode): caller owns signals, we are in a non-main
            # thread — calling signal.signal() here would raise ValueError.
            runner.set_shutdown_event(shutdown_event)
            try:
                results = runner.run_once()
            except KeyboardInterrupt:
                logger.info("Shutdown requested.")
                results = {}
        for name, result in results.items():
            if result.errors:
                logger.error("Tenant %s errors: %s", name, result.errors)
            else:
                logger.info(
                    "Tenant %s: gathered=%d, pending=%d, calculated=%d, rows=%d",
                    name,
                    result.dates_gathered,
                    result.dates_pending_calculation,
                    result.dates_calculated,
                    result.chargeback_rows_written,
                )
        return

    if shutdown_event is None:
        shutdown_event = threading.Event()

        def _signal_handler(signum: int, frame: object) -> None:
            logger.info("Received signal %d, shutting down...", signum)
            shutdown_event.set()

        signal.signal(signal.SIGINT, _signal_handler)
        signal.signal(signal.SIGTERM, _signal_handler)

    runner.set_shutdown_event(shutdown_event)
    logger.info("Starting chargeback engine worker...")
    runner.run_loop(shutdown_event)
    logger.info("Chargeback engine worker stopped.")


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    # --version is handled by argparse (prints and exits before reaching here)

    if args.config_file is None:
        print("error: --config-file is required", file=sys.stderr)
        sys.exit(2)

    try:
        settings = load_config(args.config_file, env_file=args.env_file)
    except Exception as exc:
        if args.validate:
            print(f"Config validation failed:\n{exc}", file=sys.stderr)
            sys.exit(1)
        raise

    if args.validate:
        print("Config is valid.")
        sys.exit(0)

    if args.show_config:
        print(settings.model_dump_json(indent=2))
        sys.exit(0)

    setup_logging(settings)

    mode = args.mode

    if mode == "api":
        run_api(settings, mode=mode)
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
        run_api(settings, runner=runner, mode=mode)
        shutdown_event.set()
        worker_thread.join(timeout=30)
    else:
        run_worker(settings, run_once=args.run_once)


if __name__ == "__main__":
    main()
