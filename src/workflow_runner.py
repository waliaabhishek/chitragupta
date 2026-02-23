from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import TYPE_CHECKING

from core.engine.orchestrator import ChargebackOrchestrator, PipelineRunResult

if TYPE_CHECKING:
    from core.config.models import AppSettings, StorageConfig, TenantConfig
    from core.metrics.protocol import MetricsSource
    from core.plugin.registry import PluginRegistry
    from core.storage.interface import StorageBackend

logger = logging.getLogger(__name__)


def _create_storage_backend(config: StorageConfig) -> StorageBackend:
    if config.backend == "sqlmodel":
        from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend

        return SQLModelBackend(config.connection_string)
    raise ValueError(f"Unknown storage backend: {config.backend!r}")


def _create_metrics_source(plugin_settings: dict[str, object]) -> MetricsSource | None:
    metrics_cfg = plugin_settings.get("metrics")
    if not metrics_cfg:
        return None
    if not isinstance(metrics_cfg, dict):
        raise TypeError(f"metrics config must be a dict, got {type(metrics_cfg).__name__}")
    mtype = metrics_cfg.get("type", "prometheus")
    if mtype == "prometheus":
        from core.metrics.prometheus import PrometheusConfig, PrometheusMetricsSource

        prom_cfg = {k: v for k, v in metrics_cfg.items() if k != "type"}
        return PrometheusMetricsSource(PrometheusConfig(**prom_cfg))
    raise ValueError(f"Unknown metrics type: {mtype!r}")


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

        with ThreadPoolExecutor(max_workers=len(tenants)) as executor:
            futures = {executor.submit(self._run_tenant, name, config): name for name, config in tenants.items()}
            for future in as_completed(futures):
                tenant_name = futures[future]
                config = tenants[tenant_name]
                timeout = config.tenant_execution_timeout_seconds
                try:
                    results[tenant_name] = future.result(timeout=timeout if timeout > 0 else None)
                except TimeoutError:
                    logger.error("Tenant %s timed out after %ds", tenant_name, timeout)
                    results[tenant_name] = PipelineRunResult(
                        tenant_name=tenant_name,
                        tenant_id=config.tenant_id,
                        dates_gathered=0,
                        dates_calculated=0,
                        chargeback_rows_written=0,
                        errors=[f"Execution timed out after {timeout}s"],
                    )
                except Exception as exc:
                    logger.error("Tenant %s failed: %s", tenant_name, exc)
                    results[tenant_name] = PipelineRunResult(
                        tenant_name=tenant_name,
                        tenant_id=config.tenant_id,
                        dates_gathered=0,
                        dates_calculated=0,
                        chargeback_rows_written=0,
                        errors=[str(exc)],
                    )
        return results

    def _run_tenant(self, name: str, config: TenantConfig) -> PipelineRunResult:
        plugin = self._plugin_registry.create(config.ecosystem)
        storage = _create_storage_backend(config.storage)
        metrics = _create_metrics_source(config.plugin_settings)
        orchestrator = ChargebackOrchestrator(name, config, plugin, storage, metrics)
        try:
            return orchestrator.run()
        finally:
            storage.dispose()

    def run_loop(self, shutdown_event: threading.Event) -> None:
        """Run orchestrator loop until shutdown_event is set."""
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
