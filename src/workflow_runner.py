from __future__ import annotations

import hashlib
import json
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, wait
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

from core.engine.orchestrator import ChargebackOrchestrator, GatherFailureThresholdError, PipelineRunResult

if TYPE_CHECKING:
    from core.config.models import AppSettings, StorageConfig, TenantConfig
    from core.plugin.registry import PluginRegistry
    from core.storage.interface import StorageBackend

logger = logging.getLogger(__name__)


@dataclass
class TenantRuntime:
    """Persistent runtime objects for a single tenant."""

    tenant_name: str
    plugin: object  # EcosystemPlugin protocol — avoid circular import
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
        if hasattr(self.plugin, "close"):
            self.plugin.close()
        metrics = self.plugin.get_metrics_source() if hasattr(self.plugin, "get_metrics_source") else None
        if metrics is not None and hasattr(metrics, "close"):
            metrics.close()


def _config_hash(config: TenantConfig) -> str:
    """Stable hash of tenant config for change detection."""
    try:
        raw = json.dumps(config.model_dump(), sort_keys=True, default=str)
    except Exception:
        logger.debug("Failed to JSON-serialize config for hashing; falling back to repr()", exc_info=True)
        raw = repr(config)
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def _create_storage_backend(config: StorageConfig) -> StorageBackend:
    if config.backend == "sqlmodel":
        from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend

        return SQLModelBackend(config.connection_string)
    raise ValueError(f"Unknown storage backend: {config.backend!r}")


class WorkflowRunner:
    """Periodic execution loop. Runs orchestrator for all tenants concurrently."""

    def __init__(self, settings: AppSettings, plugin_registry: PluginRegistry) -> None:
        self._settings = settings
        self._plugin_registry = plugin_registry
        self._bootstrapped = False
        self._tenant_runtimes: dict[str, TenantRuntime] = {}

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
        plugin.initialize(config.plugin_settings)
        storage = _create_storage_backend(config.storage)
        metrics = plugin.get_metrics_source()
        orchestrator = ChargebackOrchestrator(tenant_name, config, plugin, storage, metrics)

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
        """Create tables for all tenant storage backends. Call once at startup."""
        if self._bootstrapped:
            return
        for config in self._settings.tenants.values():
            storage = _create_storage_backend(config.storage)
            try:
                storage.create_tables()
            finally:
                storage.dispose()
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

        return self._run_tenant(tenant_name, config)

    def run_once(self) -> dict[str, PipelineRunResult]:
        if not self._bootstrapped:
            self.bootstrap_storage()

        results: dict[str, PipelineRunResult] = {}
        tenants = self._settings.tenants
        if not tenants:
            return results

        # GAP-010: bounded concurrency
        max_workers = min(
            len(tenants),
            self._settings.features.max_parallel_tenants,
        )
        # GAP-002: global timeout = max of all tenant timeouts
        max_timeout = max(
            (c.tenant_execution_timeout_seconds for c in tenants.values()),
            default=3600,
        )
        effective_timeout = max_timeout if max_timeout > 0 else None  # 0 means no timeout

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(self._run_tenant, name, config): (name, config) for name, config in tenants.items()
            }
            done, not_done = wait(futures, timeout=effective_timeout)

            # Collect completed results
            for future in done:
                name, config = futures[future]
                try:
                    results[name] = future.result()
                except Exception as exc:
                    logger.error("Tenant %s failed: %s", name, exc)
                    results[name] = PipelineRunResult(
                        tenant_name=name,
                        tenant_id=config.tenant_id,
                        dates_gathered=0,
                        dates_calculated=0,
                        chargeback_rows_written=0,
                        errors=[str(exc)],
                    )

            # Mark timed-out tenants
            for future in not_done:
                name, config = futures[future]
                future.cancel()
                timeout = config.tenant_execution_timeout_seconds
                logger.error("Tenant %s timed out after %ds", name, timeout)
                results[name] = PipelineRunResult(
                    tenant_name=name,
                    tenant_id=config.tenant_id,
                    dates_gathered=0,
                    dates_calculated=0,
                    chargeback_rows_written=0,
                    errors=[f"Execution timed out after {timeout}s"],
                )
        return results

    def _run_tenant(self, name: str, config: TenantConfig) -> PipelineRunResult:
        runtime = self._get_or_create_runtime(name, config)
        try:
            result = runtime.orchestrator.run()
        except GatherFailureThresholdError as exc:
            logger.critical("FATAL: %s", exc)
            import sys

            sys.exit(1)
        runtime.last_run_at = datetime.now(UTC)
        return result

    def _log_results(self, results: dict[str, PipelineRunResult]) -> None:
        for name, result in results.items():
            if result.errors:
                logger.warning("Tenant %s completed with errors: %s", name, result.errors)
            else:
                logger.info(
                    "Tenant %s: gathered=%d, calculated=%d, rows=%d",
                    name,
                    result.dates_gathered,
                    result.dates_calculated,
                    result.chargeback_rows_written,
                )

    def _cleanup_retention(self) -> None:
        """Delete data older than retention_days for each tenant.

        TD-016: Wire up delete_before() calls for retention cleanup.
        Called after each run cycle to prevent unbounded storage growth.
        """
        for name, config in self._settings.tenants.items():
            if config.retention_days <= 0:
                continue  # 0 = disabled

            cutoff = datetime.now(UTC) - timedelta(days=config.retention_days)
            storage = _create_storage_backend(config.storage)
            try:
                with storage.create_unit_of_work() as uow:
                    deleted_billing = uow.billing.delete_before(config.ecosystem, config.tenant_id, cutoff)
                    deleted_resources = uow.resources.delete_before(config.ecosystem, config.tenant_id, cutoff)
                    deleted_identities = uow.identities.delete_before(config.ecosystem, config.tenant_id, cutoff)
                    deleted_chargebacks = uow.chargebacks.delete_before(config.ecosystem, config.tenant_id, cutoff)
                    uow.commit()

                total_deleted = deleted_billing + deleted_resources + deleted_identities + deleted_chargebacks
                if total_deleted > 0:
                    logger.info(
                        "Tenant %s: retention cleanup deleted %d records (before %s)",
                        name,
                        total_deleted,
                        cutoff.date(),
                    )
            except Exception:
                logger.exception("Tenant %s: retention cleanup failed", name)
            finally:
                storage.dispose()

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
            except Exception:
                logger.exception("Unexpected error in run_loop")

            # Sleep in small increments to check shutdown_event
            for _ in range(interval):
                if shutdown_event.is_set():
                    break
                time.sleep(1)
