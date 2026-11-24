from __future__ import annotations

import argparse
import json
import logging
import signal
import sys
import threading
from contextlib import ExitStack, contextmanager
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING

from pydantic import ValidationError

from core.api import get_version
from core.config.loader import load_config
from core.config.models import MAX_SOURCE_RETENTION_DAYS
from core.emitters.registry import register as register_emitter
from core.emitters.runner import EmitterRunner
from core.emitters.sources import ChargebackDateSource, ChargebackRowFetcher, RegistryEmitterBuilder
from core.emitters.wiring import create_auxiliary_prometheus_runners
from core.plugin.loader import build_plugin_registry
from core.plugin.registry import EcosystemBundle, PluginRegistry
from core.storage.registry import create_storage_backend
from emitters.csv_emitter import make_csv_emitter
from emitters.prometheus_emitter import make_prometheus_emitter
from workflow_runner import WorkflowRunner

logger = logging.getLogger(__name__)


# Register built-in emitters at application startup
register_emitter("csv", make_csv_emitter)
register_emitter("prometheus", make_prometheus_emitter)

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator
    from contextlib import AbstractContextManager

    from core.config.models import AppSettings, StorageConfig
    from core.plugin.protocols import StorageModule
    from core.storage.interface import StorageBackend


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Chitragupta")
    parser.add_argument(
        "--version",
        action="store_true",
        help="Show the version and exit",
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
        "--emit-once",
        action="store_true",
        default=False,
        help="Re-emit all pending chargebacks from DB and exit (no pipeline run)",
    )
    iceberg_actions = parser.add_mutually_exclusive_group()
    iceberg_actions.add_argument(
        "--iceberg-status",
        nargs=3,
        metavar=("TENANT", "START", "END"),
        help="Show Iceberg export status for an inclusive UTC date range",
    )
    iceberg_actions.add_argument(
        "--iceberg-replay",
        nargs=3,
        metavar=("TENANT", "START", "END"),
        help="With --emit-once, replay Iceberg export for an inclusive UTC date range",
    )
    parser.add_argument(
        "--check-self-managed-telemetry",
        action="store_true",
        default=False,
        help="Check configured self-managed Kafka Prometheus telemetry and exit",
    )
    parser.add_argument(
        "--mode",
        choices=["worker", "api", "both"],
        help="Run mode: worker (pipeline), api (HTTP server), or both",
    )
    args = parser.parse_args(argv)
    if args.iceberg_status is not None or args.iceberg_replay is not None:
        replay = args.iceberg_replay is not None
        command = "--iceberg-replay" if replay else "--iceberg-status"
        for option, present in (
            ("--version", args.version),
            ("--validate", args.validate),
            ("--show-config", args.show_config),
            ("--run-once", args.run_once),
            ("--emit-once", args.emit_once and not replay),
            ("--check-self-managed-telemetry", args.check_self_managed_telemetry),
            ("--mode", args.mode is not None),
        ):
            if present:
                parser.error(f"{command} cannot be combined with {option}")
        if replay and not args.emit_once:
            parser.error("--iceberg-replay requires --emit-once")
        tenant_name, start_text, end_text = args.iceberg_replay if replay else args.iceberg_status
        try:
            start = date.fromisoformat(start_text)
            end = date.fromisoformat(end_text)
        except ValueError:
            parser.error(f"{command} requires dates in YYYY-MM-DD format")
        if start.isoformat() != start_text or end.isoformat() != end_text:
            parser.error(f"{command} requires dates in YYYY-MM-DD format")
        if start > end:
            parser.error(f"{command} start date must be on or before the end date")
        if (end - start).days > MAX_SOURCE_RETENTION_DAYS:
            parser.error(f"{command} accepts at most {MAX_SOURCE_RETENTION_DAYS + 1} dates")
        if replay:
            args.iceberg_replay = (tenant_name, start, end)
        else:
            args.iceberg_status = (tenant_name, start, end)
    if args.version:
        print(f"{parser.prog} {get_version()}")
        parser.exit()
    if args.check_self_managed_telemetry:
        for option, present in (
            ("--validate", args.validate),
            ("--show-config", args.show_config),
            ("--run-once", args.run_once),
            ("--emit-once", args.emit_once),
            ("--mode", args.mode is not None),
        ):
            if present:
                parser.error(f"--check-self-managed-telemetry cannot be combined with {option}")
    if args.mode is None:
        args.mode = "worker"
    return args


def setup_logging(settings: AppSettings) -> None:
    """Configure root logger and per-module levels from settings."""
    log_cfg = settings.logging
    logging.basicConfig(level=log_cfg.level, format=log_cfg.format)
    for module, level in log_cfg.per_module_levels.items():
        logging.getLogger(module).setLevel(level)


def _build_storage(
    storage_config: StorageConfig,
    *,
    storage_module: StorageModule | None = None,
    focus_preview_enabled: bool = False,
) -> StorageBackend:
    """Create a storage backend from a StorageConfig. Extracted for testability."""
    return create_storage_backend(
        storage_config,
        storage_module=storage_module,
        focus_preview_enabled=focus_preview_enabled,
    )


@contextmanager
def _standalone_iceberg_backends(
    settings: AppSettings,
) -> Iterator[Callable[[str], AbstractContextManager[StorageBackend]]]:
    from plugins.storage_modules import get_storage_module_for_ecosystem

    backends: dict[str, StorageBackend] = {}
    with ExitStack() as stack:

        @contextmanager
        def acquire(tenant_name: str) -> Iterator[StorageBackend]:
            backend = backends.get(tenant_name)
            if backend is None:
                config = settings.tenants[tenant_name]
                backend = _build_storage(
                    config.storage,
                    storage_module=get_storage_module_for_ecosystem(config.ecosystem),
                    focus_preview_enabled=config.focus_preview_enabled,
                )
                backends[tenant_name] = backend
                stack.callback(backend.dispose)
            yield backend

        yield acquire


def _emit_iceberg_once(settings: AppSettings) -> bool:
    if not any(config.plugin_settings.iceberg_emitter is not None for config in settings.tenants.values()):
        return True
    try:
        from core.emitters.iceberg import publish_iceberg

        with _standalone_iceberg_backends(settings) as acquire:
            summary = publish_iceberg(settings.tenants, acquire_backend=acquire, now=datetime.now(UTC), limit=None)
        logger.info(
            "Iceberg export attempted=%d confirmed=%d failed=%d unconfirmed=%d errors=%d",
            summary.attempted,
            summary.confirmed,
            summary.failed,
            summary.unconfirmed,
            summary.errors,
        )
        return summary.failed == 0 and summary.unconfirmed == 0 and summary.errors == 0
    except Exception as exc:
        logger.error("Iceberg export pass failed error_type=%s", type(exc).__name__)
        return False


def _run_iceberg_action(settings: AppSettings, scope: tuple[str, date, date], *, replay: bool) -> int:
    tenant_name, start, end = scope
    tenant = settings.tenants.get(tenant_name)
    if tenant is None:
        print(f"error: unknown tenant {tenant_name!r}", file=sys.stderr)
        return 2
    if tenant.plugin_settings.iceberg_emitter is None:
        print(f"error: tenant {tenant_name!r} has no Iceberg emitter", file=sys.stderr)
        return 2

    from core.emitters.iceberg import IcebergExportError, iceberg_replay, iceberg_status

    try:
        with _standalone_iceberg_backends(settings) as acquire:
            now = datetime.now(UTC)
            if replay:
                records, confirmed = iceberg_replay(
                    settings.tenants, tenant_name=tenant_name, start=start, end=end, acquire_backend=acquire, now=now
                )
            else:
                records = iceberg_status(
                    settings.tenants, tenant_name=tenant_name, start=start, end=end, acquire_backend=acquire, now=now
                )
                confirmed = True
    except IcebergExportError as exc:
        print(f"Iceberg action failed for tenant {tenant_name!r}: {exc.code}", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"Iceberg action failed for tenant {tenant_name!r}: {type(exc).__name__}", file=sys.stderr)
        return 1

    for record in records:
        print(json.dumps(record, default=str))
    return 0 if confirmed else 1


def _build_registry(settings: AppSettings) -> PluginRegistry:
    """Delegate application plugin discovery to the canonical loader helper."""
    return build_plugin_registry(settings)


def _create_runner(settings: AppSettings) -> WorkflowRunner:
    """Create a WorkflowRunner with all plugins discovered from configured plugins path."""
    from core.preview.artifacts import LocalPreviewArtifactStore
    from core.preview.capacity import PreviewGenerationScheduler
    from core.preview.generator import PreviewPackageGenerator
    from core.preview.revisions import PreviewRevisionService

    artifact_store = None
    revision_manager = None
    preview_generation_scheduler = None
    if settings.focus_preview_enabled:
        artifact_store = LocalPreviewArtifactStore(settings.preview.artifact_root)
        package_generator = PreviewPackageGenerator(
            max_csv_file_bytes=settings.preview.max_csv_file_bytes,
            max_generation_spool_bytes=settings.preview.max_generation_spool_bytes,
        )
        revision_manager = PreviewRevisionService(
            artifact_store=artifact_store,
            package_generator=package_generator,
        )
        preview_generation_scheduler = PreviewGenerationScheduler(
            max_workers=settings.preview.max_workers,
            max_queued_generations=settings.preview.max_queued_generations,
            max_running_generations_per_tenant=(settings.preview.max_running_generations_per_tenant),
            max_queued_generations_per_tenant=(settings.preview.max_queued_generations_per_tenant),
        )
    return WorkflowRunner(
        settings,
        _build_registry(settings),
        revision_manager=revision_manager,
        owned_preview_artifact_store=artifact_store,
        preview_generation_scheduler=preview_generation_scheduler,
    )


def _validate_plugin_configs(settings: AppSettings) -> None:
    """Instantiate plugin-specific config models for all configured tenants.

    Calls validate_plugin_settings() on each plugin instance if the method
    exists. Plugins without this method are skipped (graceful degradation for
    third-party plugins).

    Raises ValueError with tenant context on the first validation failure.
    """
    registry = _build_registry(settings)
    errors: list[str] = []
    for tenant_name, tenant_config in settings.tenants.items():
        ecosystem = tenant_config.ecosystem
        try:
            plugin = registry.create(ecosystem)
        except KeyError:
            errors.append(f"tenant {tenant_name!r}: unknown ecosystem {ecosystem!r}")
            continue
        validate_fn = getattr(plugin, "validate_plugin_settings", None)
        if validate_fn is None:
            continue
        try:
            validate_fn(tenant_config.plugin_settings.plugin_payload())
        except Exception as exc:
            detail = str(exc)
            if detail.startswith("invalid_self_managed_cost_rate"):
                detail = f"tenant={tenant_config.tenant_id} {detail}"
            errors.append(f"tenant {tenant_name!r} ({ecosystem}): {detail}")
    if errors:
        raise ValueError("\n".join(errors))


def run_api(settings: AppSettings, runner: WorkflowRunner | None = None, mode: str = "api") -> None:
    """Start the FastAPI server."""
    import uvicorn

    from core.api.app import create_app

    registry = None if runner is not None else _build_registry(settings)
    app = create_app(settings, workflow_runner=runner, mode=mode, plugin_registry=registry)
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


def _run_worker_execution(
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


def run_worker(
    settings: AppSettings,
    *,
    run_once: bool = False,
    runner: WorkflowRunner | None = None,
    shutdown_event: threading.Event | None = None,
) -> None:
    owns_runner = runner is None
    active_runner = _create_runner(settings) if runner is None else runner
    try:
        _run_worker_execution(
            settings,
            run_once=run_once,
            runner=active_runner,
            shutdown_event=shutdown_event,
        )
    finally:
        if owns_runner:
            active_runner.close()


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)

    if args.config_file is None:
        print("error: --config-file is required", file=sys.stderr)
        sys.exit(2)

    try:
        settings = load_config(args.config_file, env_file=args.env_file)
    except Exception as exc:
        if args.check_self_managed_telemetry:
            print(f"Telemetry check configuration failed:\n{exc}", file=sys.stderr)
            sys.exit(1)
        if args.validate:
            print(f"Config validation failed:\n{exc}", file=sys.stderr)
            sys.exit(1)
        raise

    if args.check_self_managed_telemetry:
        from plugins.self_managed_kafka import (
            SelfManagedKafkaPlugin,
            TelemetryCheckState,
            TelemetryFamilyCheck,
            check_self_managed_telemetry,
            render_telemetry_check_jsonl,
        )
        from plugins.self_managed_kafka.config import SelfManagedKafkaConfig

        selected = [
            (tenant_name, tenant_config)
            for tenant_name, tenant_config in settings.tenants.items()
            if tenant_config.ecosystem == "self_managed_kafka"
        ]
        if not selected:
            print("error: no self-managed Kafka tenant is configured", file=sys.stderr)
            sys.exit(2)

        validated: list[tuple[str, SelfManagedKafkaConfig]] = []
        config_errors: list[str] = []
        for tenant_name, tenant_config in selected:
            raw_settings = tenant_config.plugin_settings.plugin_payload()
            try:
                SelfManagedKafkaPlugin().validate_plugin_settings(raw_settings)
                validated.append(
                    (
                        tenant_name,
                        SelfManagedKafkaConfig.from_plugin_settings(raw_settings),
                    )
                )
            except (ValidationError, ValueError) as exc:
                detail = str(exc)
                if detail.startswith("invalid_self_managed_cost_rate"):
                    detail = f"tenant={tenant_config.tenant_id} {detail}"
                config_errors.append(f"tenant {tenant_name!r}: {detail}")
        if config_errors:
            print("Telemetry check configuration failed:\n" + "\n".join(config_errors), file=sys.stderr)
            sys.exit(1)

        window_end = datetime.now(UTC)
        records: list[TelemetryFamilyCheck] = []
        for tenant_name, config in validated:
            records.extend(
                check_self_managed_telemetry(
                    tenant_name=tenant_name,
                    config=config,
                    window_end=window_end,
                )
            )
        print(
            render_telemetry_check_jsonl(records, tenant_count=len(validated)),
            end="",
        )
        if any(record.state in {TelemetryCheckState.INVALID, TelemetryCheckState.INCONCLUSIVE} for record in records):
            sys.exit(1)
        sys.exit(0)

    if args.validate:
        try:
            _validate_plugin_configs(settings)
        except Exception as exc:
            print(f"Config validation failed:\n{exc}", file=sys.stderr)
            sys.exit(1)
        print("Config is valid.")
        sys.exit(0)

    if args.show_config:
        print(
            settings.model_dump_json(
                indent=2,
                exclude={"tenants": {"__all__": {"plugin_settings"}}},
            )
        )
        sys.exit(0)

    setup_logging(settings)

    if args.iceberg_status is not None:
        sys.exit(_run_iceberg_action(settings, args.iceberg_status, replay=False))
    if args.iceberg_replay is not None:
        sys.exit(_run_iceberg_action(settings, args.iceberg_replay, replay=True))

    if args.emit_once:
        iceberg_succeeded = _emit_iceberg_once(settings)
        registry = None
        for tenant_name, tenant_config in settings.tenants.items():
            ordinary_specs = tenant_config.plugin_settings.ordinary_emitters
            if tenant_config.plugin_settings.iceberg_emitter is not None and not ordinary_specs:
                continue
            if registry is None:
                registry = _build_registry(settings)
            plugin = registry.create(tenant_config.ecosystem)
            storage = None
            original_error: BaseException | None = None
            try:
                plugin.initialize(tenant_config.plugin_settings.plugin_payload())
                storage = _build_storage(
                    tenant_config.storage,
                    storage_module=plugin.get_storage_module(),
                    focus_preview_enabled=tenant_config.focus_preview_enabled,
                )
                storage.create_tables()
                billing_types = EcosystemBundle.build(plugin).billing_resource_types
                chargeback_date_source = ChargebackDateSource(storage)
                prometheus_specs = [s for s in ordinary_specs if s.type == "prometheus"]

                runners = [
                    EmitterRunner(
                        ecosystem=tenant_config.ecosystem,
                        storage_backend=storage,
                        emitter_specs=ordinary_specs,
                        date_source=chargeback_date_source,
                        row_fetcher=ChargebackRowFetcher(storage),
                        emitter_builder=RegistryEmitterBuilder(),
                        pipeline="chargeback",
                        chargeback_granularity=tenant_config.plugin_settings.chargeback_granularity,
                    ),
                ]
                if prometheus_specs:
                    runners += create_auxiliary_prometheus_runners(
                        ecosystem=tenant_config.ecosystem,
                        storage_backend=storage,
                        prometheus_specs=prometheus_specs,
                        date_source=chargeback_date_source,
                        resource_types=billing_types,
                    )
                for emitter_runner in runners:
                    emitter_runner.run(tenant_config.tenant_id)
            except BaseException as exc:
                original_error = exc
                raise
            finally:
                cleanup_errors: list[BaseException] = []
                if storage is not None:
                    try:
                        storage.dispose()
                    except BaseException as exc:
                        cleanup_errors.append(exc)
                        logger.error(
                            "Emit-once cleanup failed tenant=%s step=storage error_type=%s",
                            tenant_name,
                            type(exc).__name__,
                        )
                try:
                    plugin.close()
                except BaseException as exc:
                    cleanup_errors.append(exc)
                    logger.error(
                        "Emit-once cleanup failed tenant=%s step=plugin error_type=%s",
                        tenant_name,
                        type(exc).__name__,
                    )
                if cleanup_errors and original_error is None:
                    raise cleanup_errors[0]
        if not iceberg_succeeded:
            sys.exit(1)
        return

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
