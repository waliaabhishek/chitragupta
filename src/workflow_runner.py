from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable, Iterator
from concurrent.futures import Future, ThreadPoolExecutor, wait
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from typing import TYPE_CHECKING, Literal, cast

from core.config.fingerprint import tenant_config_fingerprint
from core.emitters.runner import EmitterRunner
from core.emitters.sources import ChargebackDateSource, ChargebackRowFetcher, RegistryEmitterBuilder
from core.emitters.wiring import create_auxiliary_prometheus_runners
from core.engine.orchestrator import ChargebackOrchestrator, GatherFailureThresholdError, PipelineRunResult
from core.plugin.protocols import OverlayPlugin
from core.plugin.registry import EcosystemBundle
from core.storage.tenant_lifecycle import cleanup_orphaned_pipeline_run, prepare_tenant_backend
from core.time_precision import canonical_utc_second

if TYPE_CHECKING:
    from datetime import date as date_type

    from core.config.models import AppSettings, TenantConfig
    from core.models.pipeline import PipelineRun
    from core.plugin.protocols import EcosystemPlugin, OverlayConfig
    from core.plugin.registry import PluginRegistry
    from core.preview.artifacts import PreviewArtifactStore
    from core.preview.capacity import PreviewGenerationScheduler
    from core.preview.evidence import PreviewEvidenceBootstrapResult
    from core.preview.evidence_capture import PreviewSourceCaptureReceipt
    from core.preview.revisions import PreviewScheduledRevisionManager
    from core.preview.storage_availability import PreviewEvidenceBootstrapUnavailable
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
    bootstrap_result: PreviewEvidenceBootstrapResult | PreviewEvidenceBootstrapUnavailable | None = None
    last_run_at: datetime | None = field(default=None)

    def is_healthy(self) -> bool:
        """Check if runtime is still usable. Placeholder — always healthy for now."""
        return True

    def close(self) -> None:
        """Clean up all resources."""
        failures: list[BaseException] = []
        try:
            self.storage.dispose()
        except BaseException as exc:
            failures.append(exc)
        try:
            self.plugin.close()
        except BaseException as exc:
            failures.append(exc)
        if failures:
            raise failures[0]


def _config_hash(config: TenantConfig) -> str:
    """Stable hash of tenant config for change detection."""
    return tenant_config_fingerprint(config)


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
        cleanup_orphaned_pipeline_run(self._storage, tenant_name)


class WorkflowRunner:
    """Periodic execution loop. Runs orchestrator for all tenants concurrently."""

    def __init__(
        self,
        settings: AppSettings,
        plugin_registry: PluginRegistry,
        *,
        revision_manager: PreviewScheduledRevisionManager | None = None,
        owned_preview_artifact_store: PreviewArtifactStore | None = None,
        preview_generation_scheduler: PreviewGenerationScheduler | None = None,
    ) -> None:
        self._settings = settings
        self._plugin_registry = plugin_registry
        self._bootstrapped = False
        self._tenant_runtimes: dict[str, TenantRuntime] = {}
        self._runtime_condition = threading.Condition(threading.RLock())
        self._runtime_leases: dict[str, int] = {}
        self._retiring_runtime_names: set[str] = set()
        self._running_tenants: set[str] = set()
        self._running_lock = threading.Lock()
        self._failed_tenants: dict[str, str] = {}  # name -> error message
        self._failed_tenants_lock = threading.Lock()
        self._shutdown_event: threading.Event | None = None
        self._revision_manager = revision_manager
        self._owned_preview_artifact_store = owned_preview_artifact_store
        if revision_manager is not None and preview_generation_scheduler is None:
            from core.preview.capacity import PreviewGenerationScheduler

            preview_generation_scheduler = PreviewGenerationScheduler(
                max_workers=settings.preview.max_workers,
                max_queued_generations=settings.preview.max_queued_generations,
                max_running_generations_per_tenant=(settings.preview.max_running_generations_per_tenant),
                max_queued_generations_per_tenant=(settings.preview.max_queued_generations_per_tenant),
            )
        self._preview_generation_scheduler = preview_generation_scheduler
        self._periodic_cycle_lock = threading.Lock()
        self._close_lock = threading.Lock()
        self._closing_event = threading.Event()
        self._closed = False

    def set_shutdown_event(self, event: threading.Event) -> None:
        """Register the shutdown event so run_once() can exit early on signal."""
        self._shutdown_event = event

    def _is_shutdown_requested(self) -> bool:
        return self._closing_event.is_set() or (self._shutdown_event is not None and self._shutdown_event.is_set())

    def is_tenant_running(self, tenant_name: str) -> bool:
        """Return True if tenant is currently being processed by any thread."""
        with self._running_lock:
            return tenant_name in self._running_tenants

    @contextmanager
    def _claim_tenant(self, tenant_name: str) -> Iterator[bool]:
        with self._running_lock:
            claimed = tenant_name not in self._running_tenants
            if claimed:
                self._running_tenants.add(tenant_name)
        if not claimed:
            yield False
            return
        try:
            yield True
        finally:
            with self._running_lock:
                self._running_tenants.discard(tenant_name)

    @contextmanager
    def _claim_tenant_for_scheduled_preview(self, tenant_name: str) -> Iterator[bool]:
        with self._running_lock:
            claimed = not self._is_shutdown_requested() and tenant_name not in self._running_tenants
            if claimed:
                self._running_tenants.add(tenant_name)
        if not claimed:
            yield False
            return
        try:
            yield True
        finally:
            with self._running_lock:
                self._running_tenants.discard(tenant_name)

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
        with self._periodic_cycle_lock, self._close_lock:
            if self._closed:
                return
            self._closing_event.set()
            failures: list[BaseException] = []
            if self._preview_generation_scheduler is not None:
                try:
                    self._preview_generation_scheduler.close(wait=True)
                except BaseException as exc:
                    failures.append(exc)
            with self._runtime_condition:
                self._closed = True
                while any(self._runtime_leases.values()):
                    self._runtime_condition.wait()
            with self._runtime_condition:
                while self._tenant_runtimes:
                    tenant_name, runtime = next(iter(self._tenant_runtimes.items()))
                    if tenant_name in self._retiring_runtime_names:
                        self._runtime_condition.wait()
                        continue
                    self._retiring_runtime_names.add(tenant_name)
                    while self._runtime_leases.get(tenant_name, 0):
                        self._runtime_condition.wait()
                    if self._tenant_runtimes.get(tenant_name) is runtime:
                        del self._tenant_runtimes[tenant_name]
                    try:
                        runtime.close()
                    except BaseException as exc:
                        failures.append(exc)
                    finally:
                        self._retiring_runtime_names.remove(tenant_name)
                        self._runtime_condition.notify_all()
            if self._owned_preview_artifact_store is not None:
                try:
                    self._owned_preview_artifact_store.close()
                except BaseException as exc:
                    failures.append(exc)
            if failures:
                raise failures[0]

    @property
    def preview_generation_scheduler(self) -> PreviewGenerationScheduler | None:
        return self._preview_generation_scheduler

    def _get_or_create_runtime(self, tenant_name: str, config: TenantConfig) -> TenantRuntime:
        """Get cached runtime or create new one. Recreates if unhealthy or config changed."""
        with self._runtime_condition:
            return self._get_or_create_runtime_locked(tenant_name, config)

    def _get_or_create_runtime_locked(self, tenant_name: str, config: TenantConfig) -> TenantRuntime:
        current_hash = _config_hash(config)

        while True:
            if self._closed:
                raise RuntimeError("workflow runner is closed")
            if tenant_name in self._retiring_runtime_names:
                self._runtime_condition.wait()
                continue
            runtime = self._tenant_runtimes.get(tenant_name)
            if runtime is None:
                break
            healthy = runtime.is_healthy()
            if runtime.config_hash == current_hash and healthy:
                return runtime
            logger.info(
                "Tenant %s: recreating runtime (config_changed=%s, healthy=%s)",
                tenant_name,
                runtime.config_hash != current_hash,
                healthy,
            )
            # This thread is the sole owner of retiring this exact cache entry.
            self._retiring_runtime_names.add(tenant_name)
            while self._runtime_leases.get(tenant_name, 0):
                self._runtime_condition.wait()
            if self._tenant_runtimes.get(tenant_name) is runtime:
                del self._tenant_runtimes[tenant_name]
            try:
                runtime.close()
            finally:
                self._retiring_runtime_names.remove(tenant_name)
                self._runtime_condition.notify_all()

        plugin = self._plugin_registry.create(config.ecosystem)
        storage: StorageBackend | None = None
        try:
            plugin.initialize(config.plugin_settings.model_dump())
            from core.storage.registry import create_storage_backend

            storage = create_storage_backend(
                config.storage,
                storage_module=plugin.get_storage_module(),
                focus_preview_enabled=config.focus_preview_enabled,
            )
            bootstrap_result = prepare_tenant_backend(storage, tenant_name, config)
            metrics = plugin.get_metrics_source()
            orchestrator = ChargebackOrchestrator(
                tenant_name,
                config,
                plugin,
                storage,
                metrics,
                shutdown_check=self._is_shutdown_requested,
            )
        except BaseException:
            if storage is not None:
                try:
                    storage.dispose()
                except BaseException as cleanup_error:
                    logger.error(
                        "Tenant runtime construction cleanup failed tenant=%s step=storage error_type=%s",
                        tenant_name,
                        type(cleanup_error).__name__,
                    )
                try:
                    plugin.close()
                except BaseException as cleanup_error:
                    logger.error(
                        "Tenant runtime construction cleanup failed tenant=%s step=plugin error_type=%s",
                        tenant_name,
                        type(cleanup_error).__name__,
                    )
            else:
                try:
                    plugin.close()
                except BaseException as cleanup_error:
                    logger.error(
                        "Tenant runtime construction cleanup failed tenant=%s step=plugin error_type=%s",
                        tenant_name,
                        type(cleanup_error).__name__,
                    )
            raise

        runtime = TenantRuntime(
            tenant_name=tenant_name,
            plugin=plugin,
            storage=storage,
            orchestrator=orchestrator,
            config_hash=current_hash,
            created_at=datetime.now(UTC),
            bootstrap_result=bootstrap_result,
        )
        self._tenant_runtimes[tenant_name] = runtime
        logger.debug("Tenant %s: created new runtime", tenant_name)
        return runtime

    @contextmanager
    def _acquire_runtime(self, tenant_name: str, config: TenantConfig) -> Iterator[TenantRuntime]:
        with self._runtime_condition:
            runtime = self._get_or_create_runtime_locked(tenant_name, config)
            self._runtime_leases[tenant_name] = self._runtime_leases.get(tenant_name, 0) + 1
        try:
            yield runtime
        finally:
            with self._runtime_condition:
                remaining = self._runtime_leases[tenant_name] - 1
                if remaining:
                    self._runtime_leases[tenant_name] = remaining
                else:
                    del self._runtime_leases[tenant_name]
                self._runtime_condition.notify_all()

    @contextmanager
    def acquire_backend(self, tenant_name: str, tenant_config: TenantConfig) -> Iterator[StorageBackend]:
        """Lease the persistent tenant runtime backend for API or worker use."""
        with self._acquire_runtime(tenant_name, tenant_config) as runtime:
            yield runtime.storage

    def bootstrap_storage(self) -> None:
        """Construct and prepare every configured tenant backend once at startup."""
        if self._bootstrapped:
            return
        for tenant_name, config in self._settings.tenants.items():
            with self._acquire_runtime(tenant_name, config):
                pass
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
        with self._claim_tenant(name) as claimed:
            if not claimed:
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
            runtime_lease = None
            runtime_lease_entered = False
            try:
                runtime_lease = self._acquire_runtime(name, config)
                runtime = runtime_lease.__enter__()
                runtime_lease_entered = True
                tracker = PipelineRunTracker(runtime.storage)

                pipeline_run = tracker.create(name)
                runtime.orchestrator._progress_callback = tracker.make_progress_callback(pipeline_run)

                try:
                    result = runtime.orchestrator.run(
                        calculation_run_id=pipeline_run.id
                    )  # GatherFailureThresholdError propagates up
                    runtime.last_run_at = datetime.now(UTC)
                    tracker.finalize(pipeline_run, result)

                    # Post-pipeline hook: emit after successful pipeline commit
                    if config.plugin_settings.emitters:
                        chargeback_date_source = ChargebackDateSource(runtime.storage)
                        # Billing/resource/identity rows are Prometheus-only — only pass prometheus specs
                        # to avoid spurious emission tracking records for CSV emitters.
                        prometheus_specs = [s for s in config.plugin_settings.emitters if s.type == "prometheus"]

                        emitter_runners = [
                            EmitterRunner(
                                ecosystem=config.ecosystem,
                                storage_backend=runtime.storage,
                                emitter_specs=config.plugin_settings.emitters,
                                date_source=chargeback_date_source,
                                row_fetcher=ChargebackRowFetcher(runtime.storage),
                                emitter_builder=RegistryEmitterBuilder(),
                                pipeline="chargeback",
                                chargeback_granularity=config.plugin_settings.chargeback_granularity,
                            ),
                        ]
                        if prometheus_specs:
                            emitter_runners += create_auxiliary_prometheus_runners(
                                ecosystem=config.ecosystem,
                                storage_backend=runtime.storage,
                                prometheus_specs=prometheus_specs,
                                date_source=chargeback_date_source,
                                resource_types=EcosystemBundle.build(runtime.plugin).billing_resource_types,
                            )

                        for emitter_runner in emitter_runners:
                            try:
                                emitter_runner.run(config.tenant_id)
                            except Exception:
                                logger.exception(
                                    "EmitterRunner failed for tenant=%s pipeline=%s — pipeline result unaffected",
                                    name,
                                    emitter_runner._pipeline,
                                )

                    # Post-pipeline hook: emit topic attribution after successful pipeline commit
                    ta_config = _get_overlay_ta_config(runtime.plugin)
                    if ta_config and ta_config.enabled:
                        from core.engine.topic_attribution_models import TopicAttributionConfigProtocol

                        if isinstance(ta_config, TopicAttributionConfigProtocol):
                            emitters = getattr(ta_config, "emitters", None)
                        else:
                            emitters = None
                        if emitters:
                            try:
                                from core.emitters.sources import (
                                    TopicAttributionDateSource,
                                    TopicAttributionRowFetcher,
                                )

                                EmitterRunner(
                                    ecosystem=config.ecosystem,
                                    storage_backend=runtime.storage,
                                    emitter_specs=emitters,
                                    date_source=TopicAttributionDateSource(runtime.storage),
                                    row_fetcher=TopicAttributionRowFetcher(runtime.storage),
                                    emitter_builder=RegistryEmitterBuilder(),
                                    pipeline="topic_attribution",
                                ).run(config.tenant_id)
                            except Exception:
                                logger.exception(
                                    "EmitterRunner (topic_attribution) failed for tenant=%s "
                                    "— pipeline result unaffected",
                                    name,
                                )
                    return result
                except Exception:
                    tracker.fail(pipeline_run)
                    raise
            finally:
                if runtime_lease is not None and runtime_lease_entered:
                    runtime_lease.__exit__(None, None, None)

    def run_focus_preview_repair(
        self,
        repair_id: str,
        tenant_name: str,
        tenant_config: TenantConfig,
    ) -> None:
        from core.engine.orchestrator import HistoricalRepairProviderSourceError
        from core.preview.eligibility import policy_from_tenant_config
        from core.preview.evidence import (
            AllocationLineageRunStatus,
            AllocationLineageUnavailableReason,
            PreviewEvidenceScope,
            SourceAttemptFailureReason,
            SourceAttemptFinalStatus,
            SourceAttemptStatus,
        )
        from core.preview.generator import PreviewGenerationError, PreviewPackageGenerator
        from core.preview.mapping import FOCUS_1_4_FULL_PROFILE_COLUMNS
        from core.preview.models import PreviewDiagnostic, PreviewRequest, PreviewRequestStatus
        from core.preview.persistence import PreviewEvidenceStorageBackend, PreviewStorageBackend
        from core.preview.repair import (
            PreviewRepair,
            PreviewRepairDate,
            PreviewRepairDateStatus,
            PreviewRepairFailureStage,
            PreviewRepairStatus,
            PreviewRepairWorkerConflictError,
        )
        from core.preview.request import canonicalize_monthly_interval

        def diagnostic(code: str, message: str, retryable: bool) -> PreviewDiagnostic:
            return PreviewDiagnostic(code=code, message=message, retryable=retryable)

        def read_operation() -> PreviewRepair | None:
            with evidence_backend.create_preview_generation_read_unit_of_work() as read_uow:
                return read_uow.repairs.get_for_owner(
                    repair_id,
                    tenant_config.ecosystem,
                    tenant_config.tenant_id,
                )

        def require_operation(
            value: PreviewRepair | None,
            *,
            matches: Callable[[PreviewRepair], bool],
            conflict: str,
        ) -> PreviewRepair:
            if value is not None:
                return value
            persisted = read_operation()
            if persisted is not None and matches(persisted):
                return persisted
            raise PreviewRepairWorkerConflictError(conflict)

        def require_date(
            value: PreviewRepairDate | None,
            tracking_date: date,
            *,
            matches: Callable[[PreviewRepairDate], bool],
            conflict: str,
        ) -> PreviewRepairDate:
            if value is not None:
                return value
            persisted = read_operation()
            if persisted is not None:
                current = next(
                    (item for item in persisted.dates if item.tracking_date == tracking_date),
                    None,
                )
                if current is not None and matches(current):
                    return current
            raise PreviewRepairWorkerConflictError(conflict)

        def fail_queued_busy() -> None:
            with self._acquire_runtime(tenant_name, tenant_config) as runtime:
                if not isinstance(runtime.storage, PreviewEvidenceStorageBackend):
                    return
                completed_at = canonical_utc_second(
                    datetime.now(UTC),
                    field="repair.completed_at",
                )
                value = diagnostic(
                    "focus_preview_repair_tenant_busy",
                    "The tenant pipeline is busy; wait for it to finish and retry the repair.",
                    True,
                )
                with runtime.storage.create_preview_evidence_unit_of_work() as uow:
                    failed = uow.repairs.fail_queued_before_execution(
                        repair_id,
                        completed_at=completed_at,
                        diagnostic=value,
                    )
                    uow.commit()
                if failed is None:
                    with runtime.storage.create_preview_generation_read_unit_of_work() as read_uow:
                        persisted = read_uow.repairs.get_for_owner(
                            repair_id,
                            tenant_config.ecosystem,
                            tenant_config.tenant_id,
                        )
                    if (
                        persisted is None
                        or persisted.status is not PreviewRepairStatus.FAILED
                        or persisted.completed_at != completed_at
                        or persisted.diagnostic != value
                    ):
                        raise PreviewRepairWorkerConflictError("queued repair busy transition conflicted")

        with self._claim_tenant(tenant_name) as claimed:
            if not claimed:
                fail_queued_busy()
                return
            with self._acquire_runtime(tenant_name, tenant_config) as runtime:
                storage = runtime.storage
                if not isinstance(storage, PreviewEvidenceStorageBackend) or not isinstance(
                    storage, PreviewStorageBackend
                ):
                    return
                evidence_backend = cast("PreviewEvidenceStorageBackend", storage)
                preview_backend = cast("PreviewStorageBackend", storage)
                started_at = canonical_utc_second(
                    datetime.now(UTC),
                    field="repair.started_at",
                )
                with evidence_backend.create_preview_evidence_unit_of_work() as uow:
                    claimed_operation = uow.repairs.mark_running(
                        repair_id,
                        started_at=started_at,
                    )
                    uow.commit()
                operation = require_operation(
                    claimed_operation,
                    matches=lambda item: (
                        item.status is PreviewRepairStatus.RUNNING
                        and item.started_at == started_at
                        and item.completed_at is None
                        and item.diagnostic is None
                    ),
                    conflict="repair claim transition conflicted",
                )

                generator = PreviewPackageGenerator(max_csv_file_bytes=self._settings.preview.max_csv_file_bytes)

                def fail_date(
                    tracking_date: date,
                    stage: PreviewRepairFailureStage,
                    value: PreviewDiagnostic,
                ) -> None:
                    completed_at = canonical_utc_second(
                        datetime.now(UTC),
                        field="repair_date.completed_at",
                    )
                    with evidence_backend.create_preview_evidence_unit_of_work() as uow:
                        failed = uow.repairs.mark_date_failed_from_running(
                            repair_id,
                            tracking_date,
                            completed_at=completed_at,
                            stage=stage,
                            diagnostic=value,
                        )
                        uow.commit()
                    require_date(
                        failed,
                        tracking_date,
                        matches=lambda item: (
                            item.status is PreviewRepairDateStatus.FAILED
                            and item.completed_at == completed_at
                            and item.failure_stage is stage
                            and item.diagnostic == value
                        ),
                        conflict=f"repair date failure transition conflicted for {tracking_date.isoformat()}",
                    )

                def fail_attempt(
                    tracking_date: date,
                    attempt_sequence: int,
                    reason: SourceAttemptFailureReason,
                ) -> None:
                    token = f"repair:{repair_id}:{tracking_date.isoformat()}"
                    with evidence_backend.create_preview_evidence_unit_of_work() as uow:
                        attempt = uow.source_readiness.get_by_token(
                            tenant_config.ecosystem,
                            tenant_config.tenant_id,
                            token,
                        )
                        if attempt is None or attempt.attempt_sequence != attempt_sequence:
                            raise PreviewRepairWorkerConflictError(
                                f"repair source attempt conflicted for {tracking_date.isoformat()}"
                            )
                        if attempt.status is SourceAttemptStatus.PENDING:
                            failed = uow.source_readiness.finalize_attempt(
                                attempt_sequence,
                                SourceAttemptFinalStatus.FAILED,
                                completed_at=canonical_utc_second(
                                    datetime.now(UTC),
                                    field="source_attempt.completed_at",
                                ),
                                reason=reason,
                            )
                            if failed.status is not SourceAttemptStatus.FAILED or failed.failure_reason is not reason:
                                raise PreviewRepairWorkerConflictError(
                                    f"repair source attempt failure conflicted for {tracking_date.isoformat()}"
                                )
                            uow.commit()
                            return
                        if attempt.status is SourceAttemptStatus.FAILED and attempt.failure_reason is reason:
                            return
                        raise PreviewRepairWorkerConflictError(
                            f"repair source attempt terminal state conflicted for {tracking_date.isoformat()}"
                        )

                def evidence_commit_matches(
                    *,
                    token: str,
                    day_start: datetime,
                    attempt_sequence: int,
                    completed_at: datetime,
                    receipt: PreviewSourceCaptureReceipt | None,
                    calculation_id: str,
                    calculation_completed_at: datetime,
                ) -> bool:
                    if receipt is None:
                        return False
                    scope = PreviewEvidenceScope(
                        tenant_config.ecosystem,
                        tenant_config.tenant_id,
                        day_start,
                        day_start + timedelta(days=1),
                    )
                    with evidence_backend.create_preview_generation_read_unit_of_work() as read_uow:
                        attempt = read_uow.source_readiness.get_by_token(
                            tenant_config.ecosystem,
                            tenant_config.tenant_id,
                            token,
                        )
                        readiness = tuple(
                            item
                            for item in read_uow.source_readiness.list_covering(
                                tenant_config.ecosystem,
                                tenant_config.tenant_id,
                                scope.start,
                                scope.end,
                            )
                            if item.attempt_sequence == attempt_sequence
                        )
                        lineage_runs = tuple(
                            read_uow.allocation_evidence.iter_preview_allocation_runs(
                                scope,
                                (calculation_id,),
                            )
                        )
                    return (
                        attempt is not None
                        and attempt.attempt_sequence == attempt_sequence
                        and attempt.refresh_token == token
                        and attempt.refresh_start == scope.start
                        and attempt.refresh_end == scope.end
                        and attempt.status is SourceAttemptStatus.COMPLETE
                        and attempt.completed_at == completed_at
                        and attempt.failure_reason is None
                        and readiness == receipt.captures
                        and len(lineage_runs) == 1
                        and lineage_runs[0].tracking_date == day_start.date()
                        and lineage_runs[0].calculation_id == calculation_id
                        and lineage_runs[0].calculation_completed_at == calculation_completed_at
                        and lineage_runs[0].capture_status is AllocationLineageRunStatus.COMPLETE
                    )

                def validation_request(
                    *,
                    grain: Literal["daily", "monthly"],
                    start_date: date,
                    end_date: date,
                ) -> PreviewRequest:
                    now = datetime.now(UTC).replace(microsecond=0)
                    return PreviewRequest(
                        request_id=f"repair-validation-{repair_id}-{grain}-{start_date.isoformat()}",
                        tenant_name=tenant_name,
                        ecosystem=tenant_config.ecosystem,
                        tenant_id=tenant_config.tenant_id,
                        grain=grain,
                        start_date=start_date,
                        end_date=end_date,
                        column_profile="full",
                        status=PreviewRequestStatus.RUNNING,
                        created_at=now,
                        started_at=now,
                        completed_at=None,
                        expires_at=None,
                        source_snapshot=None,
                        diagnostic=None,
                        storage_key=None,
                        package=None,
                        effective_columns=FOCUS_1_4_FULL_PROFILE_COLUMNS,
                    )

                policy = policy_from_tenant_config(
                    tenant_config,
                    created_at=datetime.now(UTC),
                )
                for repair_date in operation.dates:
                    tracking_date = repair_date.tracking_date
                    date_started_at = canonical_utc_second(
                        datetime.now(UTC),
                        field="repair_date.started_at",
                    )
                    with evidence_backend.create_preview_evidence_unit_of_work() as uow:
                        running_date = uow.repairs.mark_date_running(
                            repair_id,
                            tracking_date,
                            started_at=date_started_at,
                        )
                        uow.commit()

                    def claimed_date_matches(
                        item: PreviewRepairDate,
                        expected_started_at: datetime = date_started_at,
                    ) -> bool:
                        return (
                            item.status is PreviewRepairDateStatus.RUNNING
                            and item.started_at == expected_started_at
                            and item.completed_at is None
                            and item.calculation_id is None
                            and item.diagnostic is None
                        )

                    require_date(
                        running_date,
                        tracking_date,
                        matches=claimed_date_matches,
                        conflict=f"repair date claim transition conflicted for {tracking_date.isoformat()}",
                    )
                    with storage.create_unit_of_work() as generic_uow:
                        retained_state = generic_uow.pipeline_state.get(
                            tenant_config.ecosystem,
                            tenant_config.tenant_id,
                            tracking_date,
                        )
                    if retained_state is None or not retained_state.chargeback_calculated:
                        fail_date(
                            tracking_date,
                            PreviewRepairFailureStage.RETAINED_STATE,
                            diagnostic(
                                "focus_preview_repair_retained_calculation_unavailable",
                                (
                                    "No retained successful calculation is available for repair "
                                    f"on {tracking_date.isoformat()}."
                                ),
                                False,
                            ),
                        )
                        continue
                    token = f"repair:{repair_id}:{tracking_date.isoformat()}"
                    day_start = datetime.combine(tracking_date, datetime.min.time(), tzinfo=UTC)
                    with evidence_backend.create_preview_evidence_unit_of_work() as uow:
                        attempt = uow.source_readiness.begin_attempt(
                            tenant_config.ecosystem,
                            tenant_config.tenant_id,
                            token,
                            day_start,
                            day_start + timedelta(days=1),
                            datetime.now(UTC),
                        )
                        uow.commit()
                    try:
                        result = runtime.orchestrator.repair_historical_date(tracking_date)
                    except HistoricalRepairProviderSourceError:
                        fail_attempt(
                            tracking_date,
                            attempt.attempt_sequence,
                            SourceAttemptFailureReason.CONSTRUCTION_FAILED,
                        )
                        fail_date(
                            tracking_date,
                            PreviewRepairFailureStage.PROVIDER_SOURCE,
                            diagnostic(
                                "focus_preview_repair_provider_history_unavailable",
                                (
                                    "Authoritative provider history is unavailable for repair "
                                    f"on {tracking_date.isoformat()}."
                                ),
                                True,
                            ),
                        )
                        continue
                    except Exception:
                        fail_attempt(
                            tracking_date,
                            attempt.attempt_sequence,
                            SourceAttemptFailureReason.PERSISTENCE_FAILED,
                        )
                        fail_date(
                            tracking_date,
                            PreviewRepairFailureStage.CALCULATION,
                            diagnostic(
                                "focus_preview_repair_calculation_failed",
                                f"Canonical recalculation failed for {tracking_date.isoformat()}.",
                                True,
                            ),
                        )
                        continue
                    calculation_completed_at = canonical_utc_second(
                        result.calculation.calculation_completed_at,
                        field="repair_date.calculation_completed_at",
                    )
                    receipt: PreviewSourceCaptureReceipt | None = None
                    evidence_completed_at = canonical_utc_second(
                        datetime.now(UTC),
                        field="source_evidence.completed_at",
                    )
                    lineage_unavailable = False
                    try:
                        with evidence_backend.create_preview_evidence_unit_of_work() as uow:
                            receipt = result.source_capture.write(
                                uow.source_windows,
                                uow.source_readiness,
                                attempt_sequence=attempt.attempt_sequence,
                                captured_at=evidence_completed_at,
                            )
                            if result.calculation.lineage_capture is None:
                                lineage_unavailable = True
                                uow.allocation_lineage.mark_calculation_lineage_unavailable(
                                    runtime.orchestrator._lineage_unavailable(
                                        result.calculation,
                                        AllocationLineageUnavailableReason.CAPTURE_FAILED,
                                    )
                                )
                                uow.source_readiness.finalize_attempt(
                                    attempt.attempt_sequence,
                                    SourceAttemptFinalStatus.FAILED,
                                    completed_at=evidence_completed_at,
                                    reason=SourceAttemptFailureReason.CONSTRUCTION_FAILED,
                                )
                                uow.commit()
                                raise RuntimeError("allocation lineage capture unavailable")
                            uow.allocation_lineage.replace_calculation_lineage(
                                result.calculation.lineage_capture,
                                calculation_completed_at=calculation_completed_at,
                            )
                            uow.source_readiness.finalize_attempt(
                                attempt.attempt_sequence,
                                SourceAttemptFinalStatus.COMPLETE,
                                completed_at=evidence_completed_at,
                                reason=None,
                            )
                            uow.commit()
                    except Exception:
                        if evidence_commit_matches(
                            token=token,
                            day_start=day_start,
                            attempt_sequence=attempt.attempt_sequence,
                            completed_at=evidence_completed_at,
                            receipt=receipt,
                            calculation_id=result.calculation.calculation_id,
                            calculation_completed_at=calculation_completed_at,
                        ):
                            pass
                        else:
                            fail_attempt(
                                tracking_date,
                                attempt.attempt_sequence,
                                (
                                    SourceAttemptFailureReason.CONSTRUCTION_FAILED
                                    if lineage_unavailable
                                    else SourceAttemptFailureReason.PERSISTENCE_FAILED
                                ),
                            )
                            fail_date(
                                tracking_date,
                                PreviewRepairFailureStage.EVIDENCE,
                                diagnostic(
                                    "focus_preview_repair_evidence_persistence_failed",
                                    (
                                        "Calculation evidence could not be persisted for repair "
                                        f"on {tracking_date.isoformat()}."
                                    ),
                                    True,
                                ),
                            )
                            continue
                    try:
                        generator.generate(
                            backend=preview_backend,
                            request=validation_request(
                                grain="daily",
                                start_date=tracking_date,
                                end_date=tracking_date + timedelta(days=1),
                            ),
                            policy=policy,
                        )
                    except PreviewGenerationError as exc:
                        fail_date(
                            tracking_date,
                            PreviewRepairFailureStage.PREVIEW_VALIDATION,
                            exc.diagnostic,
                        )
                        continue
                    month_start = tracking_date.replace(day=1)
                    month_end = (
                        date(month_start.year + 1, 1, 1)
                        if month_start.month == 12
                        else date(month_start.year, month_start.month + 1, 1)
                    )
                    date_completed_at = canonical_utc_second(
                        datetime.now(UTC),
                        field="repair_date.completed_at",
                    )
                    with evidence_backend.create_preview_evidence_unit_of_work() as uow:
                        if operation.start_date <= month_start and operation.end_date >= month_end:
                            validated = uow.repairs.mark_date_daily_validated(
                                repair_id,
                                tracking_date,
                                calculation_id=result.calculation.calculation_id,
                                calculation_completed_at=calculation_completed_at,
                                rows_written=result.billing_rows_written,
                            )
                        else:
                            validated = uow.repairs.mark_date_succeeded_from_running(
                                repair_id,
                                tracking_date,
                                completed_at=date_completed_at,
                                calculation_id=result.calculation.calculation_id,
                                calculation_completed_at=calculation_completed_at,
                                rows_written=result.billing_rows_written,
                            )
                        uow.commit()
                    target_status = (
                        PreviewRepairDateStatus.DAILY_VALIDATED
                        if operation.start_date <= month_start and operation.end_date >= month_end
                        else PreviewRepairDateStatus.SUCCEEDED
                    )
                    expected_date_completed_at = (
                        None if target_status is PreviewRepairDateStatus.DAILY_VALIDATED else date_completed_at
                    )
                    calculation_id = result.calculation.calculation_id
                    rows_written = result.billing_rows_written

                    def validated_date_matches(
                        item: PreviewRepairDate,
                        expected_status: PreviewRepairDateStatus = target_status,
                        expected_completed_at: datetime | None = expected_date_completed_at,
                        expected_calculation_id: str = calculation_id,
                        expected_calculation_completed_at: datetime = calculation_completed_at,
                        expected_rows_written: int = rows_written,
                    ) -> bool:
                        return (
                            item.status is expected_status
                            and item.completed_at == expected_completed_at
                            and item.calculation_id == expected_calculation_id
                            and item.calculation_completed_at == expected_calculation_completed_at
                            and item.rows_written == expected_rows_written
                            and item.failure_stage is None
                            and item.diagnostic is None
                        )

                    require_date(
                        validated,
                        tracking_date,
                        matches=validated_date_matches,
                        conflict=f"repair date validation transition conflicted for {tracking_date.isoformat()}",
                    )

                month_cursor = operation.start_date.replace(day=1)
                while month_cursor < operation.end_date:
                    interval = canonicalize_monthly_interval(month=f"{month_cursor.year:04d}-{month_cursor.month:02d}")
                    if operation.start_date <= interval.start_date and operation.end_date >= interval.end_date:
                        with evidence_backend.create_preview_generation_read_unit_of_work() as read_uow:
                            current = read_uow.repairs.get_for_owner(
                                repair_id,
                                tenant_config.ecosystem,
                                tenant_config.tenant_id,
                            )
                        assert current is not None
                        pending_month = tuple(
                            item.tracking_date
                            for item in current.dates
                            if interval.start_date <= item.tracking_date < interval.end_date
                            and item.status is PreviewRepairDateStatus.DAILY_VALIDATED
                        )
                        if pending_month:
                            terminal = PreviewRepairDateStatus.SUCCEEDED
                            stage = None
                            monthly_diagnostic = None
                            try:
                                generator.generate(
                                    backend=preview_backend,
                                    request=validation_request(
                                        grain="monthly",
                                        start_date=interval.start_date,
                                        end_date=interval.end_date,
                                    ),
                                    policy=policy,
                                )
                            except PreviewGenerationError as exc:
                                terminal = PreviewRepairDateStatus.FAILED
                                stage = PreviewRepairFailureStage.PREVIEW_VALIDATION
                                monthly_diagnostic = exc.diagnostic
                            month_completed_at = canonical_utc_second(
                                datetime.now(UTC),
                                field="repair_date.completed_at",
                            )
                            with evidence_backend.create_preview_evidence_unit_of_work() as uow:
                                finalized_month = uow.repairs.finalize_month_dates(
                                    repair_id,
                                    pending_month,
                                    terminal_status=cast(
                                        "Literal[PreviewRepairDateStatus.SUCCEEDED, PreviewRepairDateStatus.FAILED]",
                                        terminal,
                                    ),
                                    completed_at=month_completed_at,
                                    stage=stage,
                                    diagnostic=monthly_diagnostic,
                                )
                                uow.commit()
                            if finalized_month is None:
                                persisted = read_operation()
                                if persisted is None:
                                    raise PreviewRepairWorkerConflictError(
                                        "monthly repair finalization lost its operation"
                                    )
                                persisted_month = tuple(
                                    item for item in persisted.dates if item.tracking_date in pending_month
                                )
                                exact = len(persisted_month) == len(pending_month) and all(
                                    item.status is terminal
                                    and item.completed_at == month_completed_at
                                    and (
                                        (
                                            terminal is PreviewRepairDateStatus.SUCCEEDED
                                            and item.failure_stage is None
                                            and item.diagnostic is None
                                        )
                                        or (
                                            terminal is PreviewRepairDateStatus.FAILED
                                            and item.failure_stage is stage
                                            and item.diagnostic == monthly_diagnostic
                                        )
                                    )
                                    for item in persisted_month
                                )
                                if not exact:
                                    raise PreviewRepairWorkerConflictError("monthly repair finalization conflicted")
                    month_cursor = interval.end_date

                with evidence_backend.create_preview_evidence_unit_of_work() as uow:
                    final = uow.repairs.get_for_owner(
                        repair_id,
                        tenant_config.ecosystem,
                        tenant_config.tenant_id,
                    )
                    assert final is not None
                    operation_completed_at = canonical_utc_second(
                        datetime.now(UTC),
                        field="repair.completed_at",
                    )
                    if all(item.status is PreviewRepairDateStatus.SUCCEEDED for item in final.dates):
                        completed = uow.repairs.finalize_completed(
                            repair_id,
                            completed_at=operation_completed_at,
                        )
                        expected_status = PreviewRepairStatus.COMPLETED
                    else:
                        completed = uow.repairs.finalize_completed_with_failures(
                            repair_id,
                            completed_at=operation_completed_at,
                        )
                        expected_status = PreviewRepairStatus.COMPLETED_WITH_FAILURES
                    uow.commit()
                require_operation(
                    completed,
                    matches=lambda item: (
                        item.status is expected_status
                        and item.completed_at == operation_completed_at
                        and item.diagnostic is None
                    ),
                    conflict="repair operation finalization conflicted",
                )

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

    def _cleanup_retention(self, *, now: datetime | None = None) -> None:
        """Delete data older than retention_days for each tenant.

        Only processes tenants with a cached TenantRuntime (i.e., tenants that ran
        this cycle). Tenants without a cached runtime are skipped — no new storage
        backend is created.
        """
        cleanup_now = datetime.now(UTC) if now is None else now
        with self._runtime_condition:
            cached_names = tuple(self._tenant_runtimes)
        for name in cached_names:
            config = self._settings.tenants.get(name)
            if config is None or config.retention_days <= 0:
                continue  # tenant removed from config, or retention disabled
            with self._claim_tenant(name) as claimed:
                if not claimed:
                    continue
                self._cleanup_tenant_retention(
                    name,
                    config,
                    cleanup_now=cleanup_now,
                )

    def _cleanup_tenant_retention(
        self,
        name: str,
        config: TenantConfig,
        *,
        cleanup_now: datetime,
    ) -> None:
        exact_cutoff = cleanup_now.astimezone(UTC) - timedelta(days=config.retention_days)
        calculation_cutoff_date = exact_cutoff.date()
        calculation_cutoff = datetime.combine(calculation_cutoff_date, datetime.min.time(), tzinfo=UTC)
        try:
            with self._acquire_runtime(name, config) as runtime:
                with runtime.storage.create_unit_of_work() as uow:
                    deleted_billing = uow.billing.delete_before(
                        config.ecosystem,
                        config.tenant_id,
                        calculation_cutoff,
                    )
                    deleted_resources = uow.resources.delete_before(
                        config.ecosystem,
                        config.tenant_id,
                        exact_cutoff,
                    )
                    deleted_identities = uow.identities.delete_before(
                        config.ecosystem,
                        config.tenant_id,
                        exact_cutoff,
                    )
                    deleted_chargebacks = uow.chargebacks.delete_before(
                        config.ecosystem,
                        config.tenant_id,
                        calculation_cutoff,
                    )
                    deleted_pipeline_state = uow.pipeline_state.delete_before(
                        config.ecosystem,
                        config.tenant_id,
                        calculation_cutoff_date,
                    )

                    ta_config = _get_overlay_ta_config(runtime.plugin)
                    deleted_ta = 0
                    if ta_config and ta_config.enabled:
                        from core.engine.topic_attribution_models import (
                            TopicAttributionConfigProtocol,
                        )

                        if isinstance(ta_config, TopicAttributionConfigProtocol):
                            retention_days = getattr(ta_config, "retention_days", None)
                            if retention_days:
                                ta_cutoff = cleanup_now - timedelta(days=retention_days)
                                deleted_ta = uow.topic_attributions.delete_before(
                                    config.ecosystem,
                                    config.tenant_id,
                                    ta_cutoff,
                                )

                    uow.commit()

                total_deleted = (
                    deleted_billing
                    + deleted_resources
                    + deleted_identities
                    + deleted_chargebacks
                    + deleted_pipeline_state
                    + deleted_ta
                )
                if config.focus_preview_enabled:
                    from core.preview.persistence import PreviewEvidenceStorageBackend

                    if isinstance(runtime.storage, PreviewEvidenceStorageBackend):
                        try:
                            with runtime.storage.create_preview_evidence_unit_of_work() as evidence_uow:
                                source_deleted = evidence_uow.source_windows.delete_before(
                                    config.ecosystem,
                                    config.tenant_id,
                                    calculation_cutoff,
                                )
                                readiness_deleted = evidence_uow.source_readiness.delete_orphaned_before(
                                    config.ecosystem,
                                    config.tenant_id,
                                    calculation_cutoff,
                                )
                                lineage_deleted = evidence_uow.allocation_lineage.delete_unretained(
                                    config.ecosystem,
                                    config.tenant_id,
                                    calculation_cutoff_date,
                                )
                                organization_deleted = evidence_uow.organization_authority.delete_superseded_before(
                                    config.ecosystem,
                                    config.tenant_id,
                                    exact_cutoff,
                                )
                                evidence_uow.commit()
                            total_deleted += (
                                source_deleted
                                + readiness_deleted
                                + lineage_deleted.portions
                                + lineage_deleted.runs
                                + organization_deleted
                            )
                        except Exception:
                            logger.exception(
                                "Tenant %s: Preview evidence retention cleanup failed",
                                name,
                            )
            if total_deleted > 0:
                logger.info(
                    "Tenant %s: retention cleanup deleted %d records (before %s)",
                    name,
                    total_deleted,
                    calculation_cutoff_date,
                )
        except Exception:
            logger.exception("Tenant %s: retention cleanup failed", name)

    def _publish_scheduled_revisions(
        self,
        results: dict[str, PipelineRunResult],
        *,
        now: datetime,
    ) -> tuple[threading.Event, ...]:
        manager = self._revision_manager
        scheduler = self._preview_generation_scheduler
        if manager is None or scheduler is None:
            return ()
        from core.preview.artifacts import preview_artifact_owner
        from core.preview.persistence import PreviewStorageBackend

        admitted_completions: list[threading.Event] = []
        for tenant_name, result in results.items():
            if result.errors or result.already_running or result.fatal:
                continue
            config = self._settings.tenants.get(tenant_name)
            with self._runtime_condition:
                runtime = self._tenant_runtimes.get(tenant_name)
            if (
                config is None
                or runtime is None
                or config.ecosystem != "confluent_cloud"
                or config.focus_preview is None
                or config.focus_preview.commercial_profile != "direct_payg"
                or not isinstance(runtime.storage, PreviewStorageBackend)
            ):
                continue
            tenant_config: TenantConfig = config
            owner = preview_artifact_owner(tenant_name, tenant_config)
            for month in manager.eligible_months(tenant_config=tenant_config, now=now):
                month_start = date.fromisoformat(f"{month}-01")
                completion = threading.Event()

                def run_scheduled(
                    tenant_name: str = tenant_name,
                    month: str = month,
                    completion: threading.Event = completion,
                ) -> None:
                    try:
                        self._run_scheduled_revision(
                            tenant_name=tenant_name,
                            month=month,
                        )
                    finally:
                        completion.set()

                admitted = scheduler.admit_scheduled(
                    owner=owner,
                    month=month_start,
                    run=run_scheduled,
                    on_cancel=completion.set,
                )
                if admitted:
                    admitted_completions.append(completion)
        return tuple(admitted_completions)

    def _run_scheduled_revision(
        self,
        *,
        tenant_name: str,
        month: str,
    ) -> None:
        manager = self._revision_manager
        config = self._settings.tenants.get(tenant_name)
        if manager is None or config is None or not config.focus_preview_enabled:
            return
        from core.preview.persistence import PreviewStorageBackend

        with self._claim_tenant_for_scheduled_preview(tenant_name) as claimed:
            if not claimed:
                return
            try:
                with self._acquire_runtime(tenant_name, config) as leased_runtime:
                    if self._is_shutdown_requested() or not isinstance(
                        leased_runtime.storage,
                        PreviewStorageBackend,
                    ):
                        return
                    manager.publish_eligible_month(
                        tenant_name=tenant_name,
                        tenant_config=config,
                        backend=leased_runtime.storage,
                        now=datetime.now(UTC),
                        month=month,
                    )
            except Exception as exc:
                logger.error(
                    "Tenant %s: scheduled FOCUS Mapping Preview publication failed error_type=%s",
                    tenant_name,
                    type(exc).__name__,
                )

    def _cleanup_preview_revision_retention(self, *, now: datetime) -> None:
        manager = self._revision_manager
        if manager is None:
            return
        from core.preview.persistence import PreviewStorageBackend

        for tenant_name, config in self._settings.tenants.items():
            with self._runtime_condition:
                runtime = self._tenant_runtimes.get(tenant_name)
            if (
                not config.focus_preview_enabled
                or runtime is None
                or not isinstance(runtime.storage, PreviewStorageBackend)
            ):
                continue
            with self._claim_tenant(tenant_name) as claimed:
                if not claimed:
                    continue
                try:
                    with self._acquire_runtime(tenant_name, config) as leased_runtime:
                        if not isinstance(leased_runtime.storage, PreviewStorageBackend):
                            continue
                        manager.cleanup_retention(
                            tenant_name=tenant_name,
                            tenant_config=config,
                            backend=leased_runtime.storage,
                            now=now,
                        )
                except Exception as exc:
                    logger.error(
                        "Tenant %s: FOCUS Mapping Preview revision retention failed error_type=%s",
                        tenant_name,
                        type(exc).__name__,
                    )

    def run_loop(self, shutdown_event: threading.Event) -> None:
        """Run orchestrator loop until shutdown_event is set."""
        # GAP-005: honor enable_periodic_refresh flag
        if not self._settings.features.enable_periodic_refresh:
            logger.info("Periodic refresh disabled — running single cycle")
            self._log_results(self.run_once())
            return

        interval = self._settings.features.refresh_interval
        while not shutdown_event.is_set():
            with self._periodic_cycle_lock:
                if shutdown_event.is_set():
                    break
                try:
                    cycle_now = datetime.now(UTC)
                    results = self.run_once()
                    self._log_results(results)
                    scheduled_completions = self._publish_scheduled_revisions(results, now=cycle_now)
                    for completion in scheduled_completions:
                        completion.wait()
                    self._cleanup_retention(now=cycle_now)  # TD-016: Retention cleanup after each cycle
                    self._cleanup_preview_revision_retention(now=cycle_now)

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
