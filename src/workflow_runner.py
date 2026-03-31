from __future__ import annotations

import hashlib
import json
import logging
import threading
import time
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor, wait
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from core.emitters.runner import EmitterRunner
from core.emitters.sources import ChargebackDateSource, ChargebackRowFetcher, RegistryEmitterBuilder
from core.engine.orchestrator import ChargebackOrchestrator, GatherFailureThresholdError, PipelineRunResult
from core.plugin.protocols import OverlayPlugin

if TYPE_CHECKING:
    from datetime import date as date_type

    from core.config.models import AppSettings, EmitterSpec, TenantConfig
    from core.models.pipeline import PipelineRun
    from core.plugin.protocols import EcosystemPlugin, OverlayConfig
    from core.plugin.registry import PluginRegistry
    from core.storage.interface import StorageBackend

logger = logging.getLogger(__name__)


def _get_overlay_ta_config(plugin: EcosystemPlugin) -> OverlayConfig | None:
    """Return topic attribution overlay config from plugin if available."""
    if isinstance(plugin, OverlayPlugin):
        return plugin.get_overlay_config("topic_attribution")
    return None


@dataclass
class TenantRuntime:
    """Persistent runtime objects for a single tenant."""

    tenant_name: str
    plugin: EcosystemPlugin
    storage: StorageBackend
    orchestrator: ChargebackOrchestrator
    config_hash: str
    created_at: datetime
    last_run_at: datetime | None = field(default=None)

    def is_healthy(self) -> bool:
        """Check if runtime is still usable. Placeholder — always healthy for now."""
        return True

    def close(self) -> None:
        """Clean up all resources."""
        self.storage.dispose()
        self.plugin.close()


def _config_hash(config: TenantConfig) -> str:
    """Stable hash of tenant config for change detection."""
    try:
        raw = json.dumps(config.model_dump(), sort_keys=True, default=str)
    except TypeError, ValueError, AttributeError:
        logger.debug("Failed to JSON-serialize config for hashing; falling back to repr()", exc_info=True)
        raw = repr(config)
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


class PipelineRunTracker:
    """Manages PipelineRun DB records: creation, progress updates, finalization.

    Extracted from WorkflowRunner to separate execution scheduling (WorkflowRunner)
    from audit-record lifecycle (this class).
    """

    def __init__(self, storage: StorageBackend) -> None:
        self._storage = storage

    def _persist(self, pipeline_run: PipelineRun, context: str) -> None:
        """Best-effort persist of PipelineRun state to DB."""
        try:
            with self._storage.create_unit_of_work() as uow:
                uow.pipeline_runs.update_run(pipeline_run)
                uow.commit()
        except Exception:
            logger.warning("Failed to %s pipeline run", context, exc_info=True)

    def create(self, tenant_name: str) -> PipelineRun:
        """Create a PipelineRun record with status='running'."""
        with self._storage.create_unit_of_work() as uow:
            run = uow.pipeline_runs.create_run(tenant_name, datetime.now(UTC))
            uow.commit()
        return run

    def make_progress_callback(self, pipeline_run: PipelineRun) -> Callable[[str | None, date_type | None], None]:
        """Build a callback that updates PipelineRun stage/current_date in DB."""

        def callback(stage: str | None, current_date: date_type | None) -> None:
            pipeline_run.stage = stage
            pipeline_run.current_date = current_date
            self._persist(pipeline_run, "update stage for")

        return callback

    def finalize(self, pipeline_run: PipelineRun, result: PipelineRunResult) -> None:
        """Update PipelineRun with final status and metrics."""
        pipeline_run.status = "failed" if result.errors else "completed"
        pipeline_run.ended_at = datetime.now(UTC)
        pipeline_run.stage = None
        pipeline_run.current_date = None
        pipeline_run.dates_gathered = result.dates_gathered
        pipeline_run.dates_calculated = result.dates_calculated
        pipeline_run.rows_written = result.chargeback_rows_written
        if result.errors:
            pipeline_run.error_message = "; ".join(result.errors)[:2000]
        self._persist(pipeline_run, "finalize")

    def fail(self, pipeline_run: PipelineRun, error_message: str = "Unhandled exception — see logs") -> None:
        """Mark PipelineRun as failed on exception."""
        pipeline_run.status = "failed"
        pipeline_run.ended_at = datetime.now(UTC)
        pipeline_run.stage = None
        pipeline_run.current_date = None
        pipeline_run.error_message = error_message
        self._persist(pipeline_run, "mark as failed")

    def cleanup_orphaned_runs(self, tenant_name: str) -> None:
        """Mark any 'running' PipelineRuns as failed (stale after restart)."""
        try:
            with self._storage.create_unit_of_work() as uow:
                latest = uow.pipeline_runs.get_latest_run(tenant_name)
                if latest is not None and latest.status == "running":
                    latest.status = "failed"
                    latest.ended_at = datetime.now(UTC)
                    latest.stage = None
                    latest.current_date = None
                    latest.error_message = "Orphaned — process restarted before completion"
                    uow.pipeline_runs.update_run(latest)
                    uow.commit()
                    logger.info(
                        "Cleaned up orphaned 'running' PipelineRun for tenant %s (id=%s)",
                        tenant_name,
                        latest.id,
                    )
        except Exception:
            logger.warning("Failed to clean up orphaned runs for %s", tenant_name, exc_info=True)


def cleanup_orphaned_runs_for_all_tenants(
    settings: AppSettings,
    *,
    swallow_errors: bool = False,
) -> None:
    """Iterate all tenants: create tables and clean up orphaned pipeline runs."""
    from core.storage.registry import create_storage_backend

    for tenant_name, config in settings.tenants.items():
        storage = create_storage_backend(config.storage)
        try:
            storage.create_tables()
            PipelineRunTracker(storage).cleanup_orphaned_runs(tenant_name)
        except Exception:
            if not swallow_errors:
                raise
            logger.warning("Failed to clean up orphaned runs for %s", tenant_name, exc_info=True)
        finally:
            storage.dispose()


class TopicAttributionDateSource:
    """PipelineDateSource backed by TopicAttributionRepository (read-only UoW)."""

    def __init__(self, storage_backend: StorageBackend) -> None:
        self._storage_backend = storage_backend

    def get_distinct_dates(self, ecosystem: str, tenant_id: str) -> list[date_type]:
        with self._storage_backend.create_read_only_unit_of_work() as uow:
            return uow.topic_attributions.get_distinct_dates(ecosystem, tenant_id)


class TopicAttributionRowFetcher:
    """PipelineRowFetcher backed by TopicAttributionRepository (read-only UoW).

    Implements PipelineRowFetcher only — NOT PipelineAggregatedRowFetcher.
    No fetch_aggregated method: topic attribution has no aggregation concept.
    """

    def __init__(self, storage_backend: StorageBackend) -> None:
        self._storage_backend = storage_backend

    def fetch_by_date(self, ecosystem: str, tenant_id: str, dt: date_type) -> list[Any]:
        with self._storage_backend.create_read_only_unit_of_work() as uow:
            return uow.topic_attributions.find_by_date(ecosystem, tenant_id, dt)


class TopicAttributionEmitterBuilder:
    """PipelineEmitterBuilder for topic attribution — builds emitters directly, not via registry."""

    def __init__(self, storage_backend: StorageBackend) -> None:
        self._storage_backend = storage_backend

    def build(self, spec: EmitterSpec) -> Any:
        if spec.type == "csv":
            from emitters.topic_attribution_csv_emitter import TopicAttributionCsvEmitter

            output_dir = spec.params.get("output_dir", "/tmp/topic_attribution")
            return TopicAttributionCsvEmitter(output_dir)
        if spec.type == "prometheus":
            from emitters.prometheus_emitter import PrometheusEmitter

            emitter_obj = PrometheusEmitter(
                port=spec.params.get("port", 8000),
                storage_backend=self._storage_backend,
            )
            return emitter_obj.emit_topic_attributions
        raise ValueError(f"TopicAttributionEmitterBuilder: unknown emitter type {spec.type!r}")


class WorkflowRunner:
    """Periodic execution loop. Runs orchestrator for all tenants concurrently."""

    def __init__(self, settings: AppSettings, plugin_registry: PluginRegistry) -> None:
        self._settings = settings
        self._plugin_registry = plugin_registry
        self._bootstrapped = False
        self._tenant_runtimes: dict[str, TenantRuntime] = {}
        self._running_tenants: set[str] = set()
        self._running_lock = threading.Lock()
        self._failed_tenants: dict[str, str] = {}  # name -> error message
        self._failed_tenants_lock = threading.Lock()
        self._shutdown_event: threading.Event | None = None

    def set_shutdown_event(self, event: threading.Event) -> None:
        """Register the shutdown event so run_once() can exit early on signal."""
        self._shutdown_event = event

    def _is_shutdown_requested(self) -> bool:
        return self._shutdown_event is not None and self._shutdown_event.is_set()

    def is_tenant_running(self, tenant_name: str) -> bool:
        """Return True if tenant is currently being processed by any thread."""
        with self._running_lock:
            return tenant_name in self._running_tenants

    def drain(self, timeout: float) -> None:
        """Signal shutdown and wait for in-progress tenant runs to complete, then close.

        Sets the shutdown event so orchestrators abort early, then waits up to
        `timeout` seconds for `_running_tenants` to empty before disposing resources.
        """
        if self._shutdown_event is not None:
            self._shutdown_event.set()
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            with self._running_lock:
                if not self._running_tenants:
                    break
            time.sleep(0.1)
        self.close()

    def close(self) -> None:
        """Clean up all tenant runtimes."""
        for runtime in self._tenant_runtimes.values():
            runtime.close()
        self._tenant_runtimes.clear()

    def _get_or_create_runtime(self, tenant_name: str, config: TenantConfig) -> TenantRuntime:
        """Get cached runtime or create new one. Recreates if unhealthy or config changed."""
        current_hash = _config_hash(config)

        if tenant_name in self._tenant_runtimes:
            runtime = self._tenant_runtimes[tenant_name]
            if runtime.config_hash == current_hash and runtime.is_healthy():
                return runtime
            # Config changed or unhealthy — close and recreate
            logger.info(
                "Tenant %s: recreating runtime (config_changed=%s, healthy=%s)",
                tenant_name,
                runtime.config_hash != current_hash,
                runtime.is_healthy(),
            )
            runtime.close()
            del self._tenant_runtimes[tenant_name]

        plugin = self._plugin_registry.create(config.ecosystem)
        plugin.initialize(config.plugin_settings.model_dump())
        from core.storage.registry import create_storage_backend

        storage = create_storage_backend(config.storage, storage_module=plugin.get_storage_module())
        metrics = plugin.get_metrics_source()
        orchestrator = ChargebackOrchestrator(
            tenant_name,
            config,
            plugin,
            storage,
            metrics,
            shutdown_check=self._is_shutdown_requested,
        )

        runtime = TenantRuntime(
            tenant_name=tenant_name,
            plugin=plugin,
            storage=storage,
            orchestrator=orchestrator,
            config_hash=current_hash,
            created_at=datetime.now(UTC),
        )
        self._tenant_runtimes[tenant_name] = runtime
        logger.debug("Tenant %s: created new runtime", tenant_name)
        return runtime

    def bootstrap_storage(self) -> None:
        """Create tables for all tenant storage backends and clean up orphaned runs.

        Call once at startup. After table creation, marks any PipelineRuns stuck
        in 'running' status (from a previous process crash) as failed.
        """
        if self._bootstrapped:
            return
        cleanup_orphaned_runs_for_all_tenants(self._settings, swallow_errors=False)
        self._bootstrapped = True

    def run_tenant(self, tenant_name: str) -> PipelineRunResult:
        """Run pipeline for a single tenant.

        TD-039: Single-tenant execution to avoid running all tenants
        when API triggers a specific tenant.
        """
        config = self._settings.tenants.get(tenant_name)
        if config is None:
            raise ValueError(f"Unknown tenant: {tenant_name}")

        if not self._bootstrapped:
            self.bootstrap_storage()

        # Return cached fatal result if tenant is permanently failed
        with self._failed_tenants_lock:
            error_msg = self._failed_tenants.get(tenant_name)
        if error_msg is not None:
            return self._build_cached_fatal_result(tenant_name, config, error_msg)

        try:
            return self._run_tenant(tenant_name, config)
        except GatherFailureThresholdError as exc:
            return self._mark_tenant_permanently_failed(tenant_name, config, exc)

    def run_once(self) -> dict[str, PipelineRunResult]:
        if not self._bootstrapped:
            self.bootstrap_storage()

        results: dict[str, PipelineRunResult] = {}
        tenants = self._settings.tenants
        if not tenants:
            return results

        # Skip permanently failed tenants
        with self._failed_tenants_lock:
            failed_snapshot = dict(self._failed_tenants)
        active_tenants = {name: config for name, config in tenants.items() if name not in failed_snapshot}
        for name, error_msg in failed_snapshot.items():
            config = tenants[name]
            results[name] = self._build_cached_fatal_result(name, config, error_msg)

        if not active_tenants:
            return results

        # GAP-010: bounded concurrency
        max_workers = min(
            len(active_tenants),
            self._settings.features.max_parallel_tenants,
        )
        # GAP-002: global timeout = max of all tenant timeouts
        max_timeout = max(
            (c.tenant_execution_timeout_seconds for c in active_tenants.values()),
            default=3600,
        )
        effective_timeout = max_timeout if max_timeout > 0 else None  # 0 means no timeout

        executor = ThreadPoolExecutor(max_workers=max_workers)
        try:
            futures: dict[Future[PipelineRunResult], tuple[str, TenantConfig]] = {
                executor.submit(self._run_tenant, name, config): (name, config)
                for name, config in active_tenants.items()
            }
            deadline = time.monotonic() + effective_timeout if effective_timeout is not None else None
            pending: set[Future[PipelineRunResult]] = set(futures)
            done: set[Future[PipelineRunResult]] = set()

            while pending and not self._is_shutdown_requested():
                poll_timeout = 1.0
                if deadline is not None:
                    time_left = deadline - time.monotonic()
                    if time_left <= 0:
                        break
                    poll_timeout = min(poll_timeout, time_left)
                newly_done, pending = wait(pending, timeout=poll_timeout)
                done.update(newly_done)

            not_done = pending
        finally:
            executor.shutdown(wait=False, cancel_futures=True)

        shutdown_interrupted = self._is_shutdown_requested() and bool(not_done)

        # Collect completed results
        for future in done:
            name, config = futures[future]
            try:
                results[name] = future.result()
            except GatherFailureThresholdError as exc:
                results[name] = self._mark_tenant_permanently_failed(name, config, exc)
            except Exception as exc:
                logger.exception("Tenant %s failed: %s", name, exc)
                results[name] = PipelineRunResult(
                    tenant_name=name,
                    tenant_id=config.tenant_id,
                    dates_gathered=0,
                    dates_calculated=0,
                    chargeback_rows_written=0,
                    dates_pending_calculation=0,
                    errors=[str(exc)],
                )

        # Mark timed-out or shutdown-interrupted tenants
        if shutdown_interrupted:
            logger.info("Shutdown requested — %d tenant(s) did not complete", len(not_done))
        for future in not_done:
            name, config = futures[future]
            future.cancel()
            if shutdown_interrupted:
                reason = "Interrupted by shutdown"
            else:
                timeout = config.tenant_execution_timeout_seconds
                logger.error("Tenant %s timed out after %ds", name, timeout)
                reason = f"Execution timed out after {timeout}s"
            results[name] = PipelineRunResult(
                tenant_name=name,
                tenant_id=config.tenant_id,
                dates_gathered=0,
                dates_calculated=0,
                chargeback_rows_written=0,
                dates_pending_calculation=0,
                errors=[reason],
            )
        return results

    def _run_tenant(self, name: str, config: TenantConfig) -> PipelineRunResult:
        with self._running_lock:
            if name in self._running_tenants:
                logger.info("Tenant %s: run skipped — already in progress", name)
                return PipelineRunResult(
                    tenant_name=name,
                    tenant_id=config.tenant_id,
                    dates_gathered=0,
                    dates_calculated=0,
                    chargeback_rows_written=0,
                    dates_pending_calculation=0,
                    already_running=True,
                )
            self._running_tenants.add(name)

        try:
            runtime = self._get_or_create_runtime(name, config)
            tracker = PipelineRunTracker(runtime.storage)

            pipeline_run = tracker.create(name)
            runtime.orchestrator._progress_callback = tracker.make_progress_callback(pipeline_run)

            try:
                result = runtime.orchestrator.run()  # GatherFailureThresholdError propagates up
                runtime.last_run_at = datetime.now(UTC)
                tracker.finalize(pipeline_run, result)

                # Post-pipeline hook: emit after successful pipeline commit
                if config.plugin_settings.emitters:
                    emitter_runner = EmitterRunner(
                        ecosystem=config.ecosystem,
                        storage_backend=runtime.storage,
                        emitter_specs=config.plugin_settings.emitters,
                        date_source=ChargebackDateSource(runtime.storage),
                        row_fetcher=ChargebackRowFetcher(runtime.storage),
                        emitter_builder=RegistryEmitterBuilder(runtime.storage),
                        pipeline="chargeback",
                        chargeback_granularity=config.plugin_settings.chargeback_granularity,
                    )
                    try:
                        emitter_runner.run(config.tenant_id)
                    except Exception:
                        logger.exception("EmitterRunner failed for tenant=%s — pipeline result unaffected", name)

                # Post-pipeline hook: emit topic attribution after successful pipeline commit
                ta_config = _get_overlay_ta_config(runtime.plugin)
                if ta_config and ta_config.enabled:
                    from core.engine.topic_attribution_models import TopicAttributionConfigProtocol

                    if isinstance(ta_config, TopicAttributionConfigProtocol):
                        emitters = getattr(ta_config, "emitters", None)
                    else:
                        emitters = None
                    if emitters:
                        ta_emitter_runner = EmitterRunner(
                            ecosystem=config.ecosystem,
                            storage_backend=runtime.storage,
                            emitter_specs=emitters,
                            date_source=TopicAttributionDateSource(runtime.storage),
                            row_fetcher=TopicAttributionRowFetcher(runtime.storage),
                            emitter_builder=TopicAttributionEmitterBuilder(runtime.storage),
                            pipeline="topic_attribution",
                        )
                        try:
                            ta_emitter_runner.run(config.tenant_id)
                        except Exception:
                            logger.exception(
                                "EmitterRunner (topic_attribution) failed for tenant=%s — pipeline result unaffected",
                                name,
                            )

                return result
            except Exception:
                tracker.fail(pipeline_run)
                raise
        finally:
            with self._running_lock:
                self._running_tenants.discard(name)

    def _build_cached_fatal_result(self, name: str, config: TenantConfig, error_msg: str) -> PipelineRunResult:
        """Build a PipelineRunResult for an already-failed tenant (no side effects)."""
        return PipelineRunResult(
            tenant_name=name,
            tenant_id=config.tenant_id,
            dates_gathered=0,
            dates_calculated=0,
            chargeback_rows_written=0,
            dates_pending_calculation=0,
            errors=[error_msg],
            fatal=True,
        )

    def _mark_tenant_permanently_failed(
        self, name: str, config: TenantConfig, exc: GatherFailureThresholdError
    ) -> PipelineRunResult:
        """Mark tenant as permanently failed, emit structured alert, return fatal result."""
        error_msg = str(exc)
        with self._failed_tenants_lock:
            self._failed_tenants[name] = error_msg
        logger.critical(
            "ALERT: Tenant %s has been permanently suspended after breaching gather failure threshold. "
            "Manual operator intervention required. Error: %s",
            name,
            error_msg,
        )
        return self._build_cached_fatal_result(name, config, error_msg)

    def get_failed_tenants(self) -> dict[str, str]:
        """Return permanently failed tenants and their error messages."""
        with self._failed_tenants_lock:
            return dict(self._failed_tenants)

    def _log_results(self, results: dict[str, PipelineRunResult]) -> None:
        for name, result in results.items():
            if result.errors:
                logger.warning("Tenant %s completed with errors: %s", name, result.errors)
            else:
                logger.info(
                    "Tenant %s: gathered=%d, pending=%d, calculated=%d, rows=%d",
                    name,
                    result.dates_gathered,
                    result.dates_pending_calculation,
                    result.dates_calculated,
                    result.chargeback_rows_written,
                )

    def _cleanup_retention(self) -> None:
        """Delete data older than retention_days for each tenant.

        Only processes tenants with a cached TenantRuntime (i.e., tenants that ran
        this cycle). Tenants without a cached runtime are skipped — no new storage
        backend is created.
        """
        for name, runtime in self._tenant_runtimes.items():
            config = self._settings.tenants.get(name)
            if config is None or config.retention_days <= 0:
                continue  # tenant removed from config, or retention disabled

            cutoff = datetime.now(UTC) - timedelta(days=config.retention_days)
            try:
                with runtime.storage.create_unit_of_work() as uow:
                    deleted_billing = uow.billing.delete_before(config.ecosystem, config.tenant_id, cutoff)
                    deleted_resources = uow.resources.delete_before(config.ecosystem, config.tenant_id, cutoff)
                    deleted_identities = uow.identities.delete_before(config.ecosystem, config.tenant_id, cutoff)
                    deleted_chargebacks = uow.chargebacks.delete_before(config.ecosystem, config.tenant_id, cutoff)

                    ta_config = _get_overlay_ta_config(runtime.plugin)
                    deleted_ta = 0
                    if ta_config and ta_config.enabled:
                        from core.engine.topic_attribution_models import TopicAttributionConfigProtocol

                        if isinstance(ta_config, TopicAttributionConfigProtocol):
                            retention_days = getattr(ta_config, "retention_days", None)
                            if retention_days:
                                ta_cutoff = datetime.now(UTC) - timedelta(days=retention_days)
                                deleted_ta = uow.topic_attributions.delete_before(
                                    config.ecosystem, config.tenant_id, ta_cutoff
                                )

                    uow.commit()

                total_deleted = (
                    deleted_billing + deleted_resources + deleted_identities + deleted_chargebacks + deleted_ta
                )
                if total_deleted > 0:
                    logger.info(
                        "Tenant %s: retention cleanup deleted %d records (before %s)",
                        name,
                        total_deleted,
                        cutoff.date(),
                    )
            except Exception:
                logger.exception("Tenant %s: retention cleanup failed", name)

    def run_loop(self, shutdown_event: threading.Event) -> None:
        """Run orchestrator loop until shutdown_event is set."""
        # GAP-005: honor enable_periodic_refresh flag
        if not self._settings.features.enable_periodic_refresh:
            logger.info("Periodic refresh disabled — running single cycle")
            self._log_results(self.run_once())
            return

        interval = self._settings.features.refresh_interval
        while not shutdown_event.is_set():
            try:
                self._log_results(self.run_once())
                self._cleanup_retention()  # TD-016: Retention cleanup after each cycle

                # Alert if all configured tenants are permanently failed
                all_tenants = set(self._settings.tenants)
                with self._failed_tenants_lock:
                    failed_set = set(self._failed_tenants)
                if all_tenants and all_tenants == failed_set:
                    logger.critical(
                        "ALERT: All %d tenant(s) have been permanently suspended. "
                        "No work will be performed. Operator intervention required. "
                        "Failed tenants: %s",
                        len(all_tenants),
                        list(failed_set),
                    )
            except Exception:
                logger.exception("Unexpected error in run_loop")

            # Sleep in small increments to check shutdown_event
            for _ in range(interval):
                if shutdown_event.is_set():
                    break
                time.sleep(1)
