from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from core.metrics.config import create_metrics_source
from core.models import CoreResource
from plugins.generic_metrics_only.config import GenericMetricsOnlyConfig
from plugins.generic_metrics_only.cost_input import GenericConstructedCostInput
from plugins.generic_metrics_only.handler import GenericMetricsOnlyHandler
from plugins.generic_metrics_only.shared_context import GenericSharedContext

if TYPE_CHECKING:
    from core.metrics.protocol import MetricsSource
    from core.plugin.protocols import CostAllocator, CostInput, ServiceHandler
    from plugins.generic_metrics_only.storage.module import GenericMetricsOnlyStorageModule
logger = logging.getLogger(__name__)


class GenericMetricsOnlyPlugin:
    """EcosystemPlugin for any metrics-only ecosystem configured via YAML."""

    def __init__(self) -> None:
        self._config: GenericMetricsOnlyConfig | None = None
        self._metrics_source: MetricsSource | None = None
        self._handler: GenericMetricsOnlyHandler | None = None

    @property
    def ecosystem(self) -> str:
        # Returns ecosystem_name from config so billing data is labeled correctly.
        # Returns sentinel only before initialize() -- should never appear in data.
        if self._config is None:
            return "generic_metrics_only"
        return self._config.ecosystem_name

    def initialize(self, config: dict[str, Any]) -> None:
        logger.info("Initializing GenericMetricsOnlyPlugin")
        self._config = GenericMetricsOnlyConfig.from_plugin_settings(config)
        self._metrics_source = create_metrics_source(self._config.metrics)
        self._handler = GenericMetricsOnlyHandler(
            config=self._config,
            metrics_source=self._metrics_source,
        )
        logger.info(
            "GenericMetricsOnlyPlugin initialized ecosystem=%s cluster=%s",
            self._config.ecosystem_name,
            self._config.cluster_id,
        )

    def get_service_handlers(self) -> dict[str, ServiceHandler]:
        if self._handler is None:
            raise RuntimeError("Plugin not initialized.")
        logger.debug("get_service_handlers -> ['generic']")
        return {"generic": self._handler}

    def get_cost_input(self) -> CostInput:
        if self._config is None or self._metrics_source is None:
            raise RuntimeError("Plugin not initialized.")
        logger.debug("get_cost_input building GenericConstructedCostInput")
        return GenericConstructedCostInput(self._config, self._metrics_source)

    def get_metrics_source(self) -> MetricsSource | None:
        return self._metrics_source

    def get_fallback_allocator(self) -> CostAllocator | None:
        return None

    def build_shared_context(self, tenant_id: str) -> GenericSharedContext:
        if self._config is None:
            raise RuntimeError("Plugin not initialized.")
        cluster = CoreResource(
            ecosystem=self._config.ecosystem_name,
            tenant_id=tenant_id,
            resource_id=self._config.cluster_id,
            resource_type="cluster",
            display_name=self._config.display_name or self._config.cluster_id,
            parent_id=None,
            created_at=None,
            deleted_at=None,
            last_seen_at=datetime.now(UTC),
            metadata={},
        )
        return GenericSharedContext(cluster_resource=cluster)

    def get_storage_module(self) -> GenericMetricsOnlyStorageModule:
        from plugins.generic_metrics_only.storage.module import GenericMetricsOnlyStorageModule

        return GenericMetricsOnlyStorageModule()

    def close(self) -> None:
        if self._metrics_source is not None:
            self._metrics_source.close()
            self._metrics_source = None
