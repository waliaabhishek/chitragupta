"""Self-managed Kafka ecosystem plugin."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from plugins.self_managed_kafka.config import SelfManagedKafkaConfig
from plugins.self_managed_kafka.cost_input import ConstructedCostInput
from plugins.self_managed_kafka.handlers.kafka import SelfManagedKafkaHandler

if TYPE_CHECKING:
    from core.metrics.protocol import MetricsSource
    from core.plugin.protocols import CostInput, ServiceHandler


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

    @property
    def ecosystem(self) -> str:
        return "self_managed_kafka"

    def initialize(self, config: dict[str, Any]) -> None:
        """Initialize plugin with validated config.

        Creates:
        1. MetricsSource from config.metrics (always required)
        2. KafkaAdminClient if resource_source.source="admin_api"
        3. Handler with both clients
        """
        self._config = SelfManagedKafkaConfig.from_plugin_settings(config)

        # Always create MetricsSource (required for cost construction)
        self._metrics_source = self._create_metrics_source(self._config)

        # Create AdminClient if using admin_api for resource discovery
        if self._config.resource_source.source == "admin_api":
            from plugins.self_managed_kafka.gathering.admin_api import create_admin_client

            self._admin_client = create_admin_client(self._config.resource_source)

        # Create handler with both clients
        self._handler = SelfManagedKafkaHandler(
            config=self._config,
            metrics_source=self._metrics_source,
            admin_client=self._admin_client,
        )

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

    def close(self) -> None:
        """Clean up resources (AdminClient connection)."""
        if self._admin_client is not None:
            import contextlib

            # Best-effort cleanup: suppress all exceptions since we're tearing down.
            # kafka-python raises various errors from close() (network, state, etc.)
            # and none of them should prevent cleanup from completing.
            with contextlib.suppress(Exception):
                self._admin_client.close()
            self._admin_client = None

    def _create_metrics_source(self, config: SelfManagedKafkaConfig) -> MetricsSource:
        """Create PrometheusMetricsSource from config."""
        from core.metrics.prometheus import AuthConfig, PrometheusConfig, PrometheusMetricsSource

        metrics_config = config.metrics
        auth: AuthConfig | None = None
        if metrics_config.auth_type != "none":
            auth = AuthConfig(
                type=metrics_config.auth_type,
                username=metrics_config.username,
                password=metrics_config.password.get_secret_value() if metrics_config.password else None,
                token=metrics_config.bearer_token.get_secret_value() if metrics_config.bearer_token else None,
            )

        return PrometheusMetricsSource(PrometheusConfig(url=metrics_config.url, auth=auth))
