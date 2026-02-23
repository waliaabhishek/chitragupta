from __future__ import annotations

import argparse
import logging
import signal
import threading
from pathlib import Path
from typing import TYPE_CHECKING

from core.config.loader import load_config
from core.plugin.loader import discover_plugins
from core.plugin.registry import PluginRegistry
from workflow_runner import WorkflowRunner

if TYPE_CHECKING:
    from core.config.models import AppSettings

logger = logging.getLogger(__name__)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Chargeback Engine")
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
    return parser.parse_args(argv)


def setup_logging(settings: AppSettings) -> None:
    """Configure root logger and per-module levels from settings."""
    log_cfg = settings.logging
    logging.basicConfig(level=log_cfg.level, format=log_cfg.format)
    for module, level in log_cfg.per_module_levels.items():
        logging.getLogger(module).setLevel(level)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    settings = load_config(args.config_file, env_file=args.env_file)
    setup_logging(settings)

    # Discover and register plugins
    plugins_path = Path("plugins")
    registry = PluginRegistry()
    for ecosystem, factory in discover_plugins(plugins_path):
        registry.register(ecosystem, factory)

    runner = WorkflowRunner(settings, registry)

    if args.run_once:
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

    shutdown_event = threading.Event()

    def _signal_handler(signum: int, frame: object) -> None:
        logger.info("Received signal %d, shutting down...", signum)
        shutdown_event.set()

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    logger.info("Starting chargeback engine...")
    runner.run_loop(shutdown_event)
    logger.info("Chargeback engine stopped.")


if __name__ == "__main__":
    main()
