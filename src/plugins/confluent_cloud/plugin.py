from __future__ import annotations

import logging
from collections.abc import Iterable
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from core.metrics.config import create_metrics_source
from plugins.confluent_cloud.config import CCloudPluginConfig
from plugins.confluent_cloud.connections import CCloudConnection
from plugins.confluent_cloud.cost_input import CCloudBillingCostInput
from plugins.confluent_cloud.handlers.connectors import ConnectorHandler
from plugins.confluent_cloud.handlers.default import DefaultHandler
from plugins.confluent_cloud.handlers.flink import FlinkHandler
from plugins.confluent_cloud.handlers.kafka import KafkaHandler
from plugins.confluent_cloud.handlers.ksqldb import KsqldbHandler
from plugins.confluent_cloud.handlers.org_wide import OrgWideCostHandler
from plugins.confluent_cloud.handlers.schema_registry import SchemaRegistryHandler

if TYPE_CHECKING:
    from core.metrics.protocol import MetricsSource
    from core.models.resource import Resource
    from core.plugin.protocols import CostAllocator, CostInput, ServiceHandler
    from plugins.confluent_cloud.shared_context import CCloudSharedContext
    from plugins.confluent_cloud.storage.module import CCloudStorageModule

logger = logging.getLogger(__name__)


class ConfluentCloudPlugin:
    """Confluent Cloud ecosystem plugin."""

    def __init__(self) -> None:
        self._config: CCloudPluginConfig | None = None
        self._connection: CCloudConnection | None = None
        self._handlers: dict[str, ServiceHandler] | None = None
        self._metrics_source: MetricsSource | None = None

    @property
    def ecosystem(self) -> str:
        return "confluent_cloud"

    def initialize(self, config: dict[str, Any]) -> None:
        """Initialize plugin with validated config."""
        logger.info("Initializing ConfluentCloudPlugin")
        self._config = CCloudPluginConfig.from_plugin_settings(config)
        self._connection = CCloudConnection(
            api_key=self._config.ccloud_api.key,
            api_secret=self._config.ccloud_api.secret,
        )

        # Initialize handlers (ordering no longer load-bearing — shared context
        # eliminates handler-to-handler UoW dependencies)
        self._handlers = {
            "kafka": KafkaHandler(self._connection, self._config, self.ecosystem),
            "schema_registry": SchemaRegistryHandler(self._connection, self._config, self.ecosystem),
            "connector": ConnectorHandler(self._connection, self._config, self.ecosystem),
            "ksqldb": KsqldbHandler(self._connection, self._config, self.ecosystem),
            "flink": FlinkHandler(self._connection, self._config, self.ecosystem),
            "org_wide": OrgWideCostHandler(self.ecosystem),
            "default": DefaultHandler(self.ecosystem),
        }

        # Initialize metrics source if configured
        if self._config.metrics:
            self._metrics_source = create_metrics_source(self._config.metrics)

        logger.info(
            "ConfluentCloudPlugin initialized handlers=%s metrics_enabled=%s",
            list(self._handlers),
            self._metrics_source is not None,
        )

    def get_service_handlers(self) -> dict[str, ServiceHandler]:
        """Return service handlers keyed by service type."""
        if self._handlers is None:
            raise RuntimeError("Plugin not initialized. Call initialize() first.")
        logger.debug("get_service_handlers -> %s", list(self._handlers))
        return self._handlers

    def get_cost_input(self) -> CostInput:
        """Return cost input backed by CCloud Billing API."""
        if self._config is None or self._connection is None:
            raise RuntimeError("Plugin not initialized. Call initialize() first.")
        logger.debug("get_cost_input building CCloudBillingCostInput")
        return CCloudBillingCostInput(self._connection, self._config)

    def get_metrics_source(self) -> MetricsSource | None:
        """Return metrics source if configured, None otherwise."""
        return self._metrics_source

    def get_fallback_allocator(self) -> CostAllocator | None:
        """Return unknown_allocator for unrecognized product types."""
        from plugins.confluent_cloud.allocators import unknown_allocator

        return unknown_allocator

    def build_shared_context(self, tenant_id: str) -> CCloudSharedContext | None:
        """Gather environments and Kafka clusters once for the entire gather cycle.

        Called by the orchestrator before iterating handlers. Returns a frozen
        context object passed to every handler's gather_resources() call.
        Returns None if no connection is available (e.g., test/offline scenario).

        Resolves TD-028: deduplication of environment API calls across handlers
        no longer requires cross-handler state sharing — shared context is built
        once here and passed explicitly.
        """
        logger.debug("Building shared context for tenant=%s", tenant_id)
        from plugins.confluent_cloud.gathering import gather_environments, gather_kafka_clusters
        from plugins.confluent_cloud.shared_context import CCloudSharedContext

        if self._connection is None:
            return None

        env_resources = list(gather_environments(self._connection, self.ecosystem, tenant_id))
        env_ids = [r.resource_id for r in env_resources]
        cluster_resources = list(gather_kafka_clusters(self._connection, self.ecosystem, tenant_id, env_ids))
        logger.info(
            "Shared context built tenant=%s envs=%d clusters=%d",
            tenant_id,
            len(env_resources),
            len(cluster_resources),
        )
        return CCloudSharedContext(
            environment_resources=tuple(env_resources),
            kafka_cluster_resources=tuple(cluster_resources),
        )

    def gather_topic_resources(
        self,
        tenant_id: str,
        cluster_ids: list[str],
    ) -> Iterable[Resource]:
        """Discover topics from Prometheus for CCloud clusters.

        Queries received_bytes, sent_bytes, retained_bytes and takes the union of
        topic names — a topic in ANY metric is considered active.

        Uses a 1-hour lookback window ending at call time for discovery.
        """
        if not self._metrics_source or not self._config or not self._config.topic_attribution.enabled:
            return

        from core.models.resource import CoreResource
        from plugins.confluent_cloud.overlays.topic_attribution import _DISCOVERY_QUERIES

        end = datetime.now(UTC)
        start = end - timedelta(hours=1)

        for cluster_id in cluster_ids:
            try:
                raw = self._metrics_source.query(
                    queries=_DISCOVERY_QUERIES,
                    start=start,
                    end=end,
                    resource_id_filter=cluster_id,
                )
            except Exception:
                logger.warning(
                    "Topic discovery failed for cluster=%s — skipping",
                    cluster_id,
                    exc_info=True,
                )
                continue

            topic_names: set[str] = set()
            for rows in raw.values():
                for row in rows:
                    topic = row.labels.get("topic")
                    if topic:
                        topic_names.add(topic)

            for topic_name in topic_names:
                yield CoreResource(
                    ecosystem=self.ecosystem,
                    tenant_id=tenant_id,
                    resource_type="topic",
                    resource_id=f"{cluster_id}:topic:{topic_name}",
                    display_name=topic_name,
                    parent_id=cluster_id,
                )

    def get_storage_module(self) -> CCloudStorageModule:
        from plugins.confluent_cloud.storage.module import CCloudStorageModule

        return CCloudStorageModule()

    def validate_plugin_settings(self, config: dict[str, Any]) -> None:
        """Validate plugin-specific config without creating live connections."""
        CCloudPluginConfig.from_plugin_settings(config)

    def close(self) -> None:
        """Release plugin resources (HTTP connections).

        TD-018/TD-024: Explicit cleanup instead of relying on GC.
        Called by WorkflowRunner after each tenant run completes.
        """
        if self._connection is not None:
            self._connection.close()
            self._connection = None
        if self._metrics_source is not None:
            self._metrics_source.close()
            self._metrics_source = None
