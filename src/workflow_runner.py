from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, wait
from typing import TYPE_CHECKING

from core.engine.orchestrator import ChargebackOrchestrator, PipelineRunResult

if TYPE_CHECKING:
    from core.config.models import AppSettings, StorageConfig, TenantConfig
    from core.plugin.registry import PluginRegistry
    from core.storage.interface import StorageBackend

logger = logging.getLogger(__name__)


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

    def run_once(self) -> dict[str, PipelineRunResult]:
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
        plugin = self._plugin_registry.create(config.ecosystem)
        storage = _create_storage_backend(config.storage)
        storage.create_tables()  # GAP-003: ensure schema exists
        metrics = plugin.get_metrics_source()  # GAP-015+017: plugin owns metrics
        orchestrator = ChargebackOrchestrator(name, config, plugin, storage, metrics)
        try:
            return orchestrator.run()
        finally:
            storage.dispose()

    def run_loop(self, shutdown_event: threading.Event) -> None:
        """Run orchestrator loop until shutdown_event is set."""
        # GAP-005: honor enable_periodic_refresh flag
        if not self._settings.features.enable_periodic_refresh:
            logger.info("Periodic refresh disabled — running single cycle")
            results = self.run_once()
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
            return

        interval = self._settings.features.refresh_interval
        while not shutdown_event.is_set():
            try:
                results = self.run_once()
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
            except Exception:
                logger.exception("Unexpected error in run_loop")

            # Sleep in small increments to check shutdown_event
            for _ in range(interval):
                if shutdown_event.is_set():
                    break
                time.sleep(1)
