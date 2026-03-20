"""Self-managed Kafka ecosystem plugin."""

from __future__ import annotations

import contextlib
import logging
from datetime import timedelta
from typing import TYPE_CHECKING, Any

from core.metrics.config import create_metrics_source
from core.metrics.protocol import MetricsQueryError
from plugins.self_managed_kafka.config import SelfManagedKafkaConfig
from plugins.self_managed_kafka.cost_input import ConstructedCostInput
from plugins.self_managed_kafka.handlers.kafka import SelfManagedKafkaHandler

if TYPE_CHECKING:
    from core.metrics.protocol import MetricsSource
    from core.plugin.protocols import CostAllocator, CostInput, ServiceHandler
    from plugins.self_managed_kafka.shared_context import SMKSharedContext
    from plugins.self_managed_kafka.storage.module import SelfManagedKafkaStorageModule

logger = logging.getLogger(__name__)


class SelfManagedKafkaPlugin:
    """Self-managed Kafka ecosystem plugin.

    Creates and owns:
    - MetricsSource (Prometheus client) — shared by CostInput and Handler
    - KafkaAdminClient (if resource_source="admin_api") — owned by plugin, passed to handler
    - ConstructedCostInput — receives metrics_source
    - SelfManagedKafkaHandler — receives metrics_source + admin_client
    """

    def __init__(self) -> None:
        self._config: SelfManagedKafkaConfig | None = None
        self._metrics_source: MetricsSource | None = None
        self._admin_client: Any = None
        self._handler: SelfManagedKafkaHandler | None = None
        self._prometheus_principals_available: bool = True
        self._cached_discovery: tuple[frozenset[str], frozenset[str], frozenset[str]] | None = None

    @property
    def ecosystem(self) -> str:
        return "self_managed_kafka"

    def initialize(self, config: dict[str, Any]) -> None:
        """Initialize plugin with validated config.

        Creates:
        1. MetricsSource from config.metrics (always required)
        2. KafkaAdminClient if resource_source.source="admin_api"
        3. Validates principal label availability when identity_source uses Prometheus
        4. Handler with clients and principal availability flag
        """
        self._config = SelfManagedKafkaConfig.from_plugin_settings(config)
        self._prometheus_principals_available = True

        # Always create MetricsSource (required for cost construction)
        self._metrics_source = create_metrics_source(self._config.metrics)

        # Create AdminClient if using admin_api for resource discovery
        if self._config.resource_source.source == "admin_api":
            from plugins.self_managed_kafka.gathering.admin_api import create_admin_client

            self._admin_client = create_admin_client(self._config.resource_source)

        # Validate principal label availability before handler creation
        if self._config.identity_source.source in ("prometheus", "both"):
            self._validate_principal_label()

        # Create handler with both clients and principal availability flag
        self._handler = SelfManagedKafkaHandler(
            config=self._config,
            metrics_source=self._metrics_source,
            admin_client=self._admin_client,
            prometheus_principals_available=self._prometheus_principals_available,
        )

    def _validate_principal_label(self) -> None:
        """Warn if Prometheus metrics lack 'principal' label.

        Uses run_combined_discovery() to check for principal label availability.
        Sets self._prometheus_principals_available = False on missing label or
        Prometheus unreachability. Plugin continues either way (lenient).
        """
        from plugins.self_managed_kafka.gathering.prometheus import run_combined_discovery

        step = timedelta(seconds=self._config.metrics_step_seconds)  # type: ignore[union-attr]  # set in initialize()
        try:
            brokers, topics, principals = run_combined_discovery(
                self._metrics_source,  # type: ignore[arg-type]  # set in initialize()
                step,
                discovery_window_hours=self._config.discovery_window_hours,  # type: ignore[union-attr]  # set in initialize()
            )
            self._cached_discovery = (brokers, topics, principals)  # cache for first gather cycle
            if not principals:
                logger.warning(
                    "self_managed_kafka: No 'principal' label found in Prometheus metrics. "
                    "Per-principal identity discovery will be unavailable. "
                    "Costs will be allocated to UNALLOCATED unless static_identities are configured."
                )
                self._prometheus_principals_available = False
        except MetricsQueryError:
            logger.warning(
                "self_managed_kafka: Could not reach Prometheus during principal label validation. "
                "Proceeding with principal discovery disabled."
            )
            self._prometheus_principals_available = False

    def get_service_handlers(self) -> dict[str, ServiceHandler]:
        """Return service handlers keyed by service type."""
        if self._handler is None:
            raise RuntimeError("Plugin not initialized. Call initialize() first.")
        return {"kafka": self._handler}

    def get_cost_input(self) -> CostInput:
        """Return ConstructedCostInput backed by Prometheus metrics."""
        if self._config is None or self._metrics_source is None:
            raise RuntimeError("Plugin not initialized. Call initialize() first.")
        return ConstructedCostInput(self._config, self._metrics_source)

    def get_metrics_source(self) -> MetricsSource | None:
        """Return metrics source (always set after initialize)."""
        return self._metrics_source

    def get_fallback_allocator(self) -> CostAllocator | None:
        return None

    def build_shared_context(self, tenant_id: str) -> SMKSharedContext:
        """Build the cluster resource once for the gather cycle.

        When prometheus is the resource or identity source, also runs the combined
        discovery query to populate broker/topic/principal sets in one round-trip.
        """
        if self._config is None:
            raise RuntimeError("Plugin not initialized. Call initialize() first.")

        from plugins.self_managed_kafka.gathering.prometheus import gather_cluster_resource, run_combined_discovery
        from plugins.self_managed_kafka.shared_context import SMKSharedContext

        cluster = gather_cluster_resource(
            ecosystem=self.ecosystem,
            tenant_id=tenant_id,
            cluster_id=self._config.cluster_id,
            broker_count=self._config.broker_count,
            region=self._config.region,
        )

        needs_prom_resources = self._config.resource_source.source == "prometheus"
        needs_prom_identities = (
            self._config.identity_source.source in ("prometheus", "both") and self._prometheus_principals_available
        )
        needs_prometheus = needs_prom_resources or needs_prom_identities

        if needs_prometheus and self._metrics_source is not None:
            step = timedelta(seconds=self._config.metrics_step_seconds)
            try:
                # Use cached result from _validate_principal_label (first call only)
                if self._cached_discovery is not None:
                    brokers, topics, principals = self._cached_discovery
                    self._cached_discovery = None  # consume once, free memory
                else:
                    brokers, topics, principals = run_combined_discovery(
                        self._metrics_source,
                        step,
                        discovery_window_hours=self._config.discovery_window_hours,
                    )
                return SMKSharedContext(
                    cluster_resource=cluster,
                    discovered_brokers=brokers,
                    discovered_topics=topics,
                    discovered_principals=principals,
                )
            except MetricsQueryError:
                logger.warning("self_managed_kafka: Combined discovery query failed. Discovery sets will be None.")

        return SMKSharedContext(cluster_resource=cluster)

    def get_storage_module(self) -> SelfManagedKafkaStorageModule:
        from plugins.self_managed_kafka.storage.module import SelfManagedKafkaStorageModule

        return SelfManagedKafkaStorageModule()

    def validate_plugin_settings(self, config: dict[str, Any]) -> None:
        """Validate plugin-specific config without creating live connections."""
        SelfManagedKafkaConfig.from_plugin_settings(config)

    def close(self) -> None:
        """Clean up resources (AdminClient connection, metrics source)."""
        if self._admin_client is not None:
            # Best-effort cleanup: suppress all exceptions since we're tearing down.
            with contextlib.suppress(Exception):
                self._admin_client.close()
            self._admin_client = None
        if self._metrics_source is not None:
            self._metrics_source.close()
            self._metrics_source = None
