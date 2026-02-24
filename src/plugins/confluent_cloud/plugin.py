from __future__ import annotations

from typing import TYPE_CHECKING, Any

from plugins.confluent_cloud.config import CCloudPluginConfig
from plugins.confluent_cloud.connections import CCloudConnection
from plugins.confluent_cloud.cost_input import CCloudBillingCostInput
from plugins.confluent_cloud.handlers.connectors import ConnectorHandler
from plugins.confluent_cloud.handlers.flink import FlinkHandler
from plugins.confluent_cloud.handlers.kafka import KafkaHandler
from plugins.confluent_cloud.handlers.ksqldb import KsqldbHandler
from plugins.confluent_cloud.handlers.schema_registry import SchemaRegistryHandler

if TYPE_CHECKING:
    from core.metrics.protocol import MetricsSource
    from core.plugin.protocols import CostInput, ServiceHandler


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
        self._config = CCloudPluginConfig.from_plugin_settings(config)
        self._connection = CCloudConnection(
            api_key=self._config.ccloud_api.key,
            api_secret=self._config.ccloud_api.secret,
        )

        # Initialize handlers (order matters: Kafka first for environment gathering)
        self._handlers = {
            "kafka": KafkaHandler(self._connection, self._config, self.ecosystem),
            "schema_registry": SchemaRegistryHandler(self._connection, self._config, self.ecosystem),
            "connector": ConnectorHandler(self._connection, self._config, self.ecosystem),
            "ksqldb": KsqldbHandler(self._connection, self._config, self.ecosystem),
            "flink": FlinkHandler(self._connection, self._config, self.ecosystem),
        }

        # Initialize metrics source if configured
        if self._config.metrics:
            from core.metrics.prometheus import (
                AuthConfig,
                PrometheusConfig,
                PrometheusMetricsSource,
            )

            # Build auth config if authentication is needed
            auth: AuthConfig | None = None
            if self._config.metrics.auth_type != "none":
                auth = AuthConfig(
                    type=self._config.metrics.auth_type,
                    username=self._config.metrics.username,
                    password=(
                        self._config.metrics.password.get_secret_value() if self._config.metrics.password else None
                    ),
                    token=(
                        self._config.metrics.bearer_token.get_secret_value()
                        if self._config.metrics.bearer_token
                        else None
                    ),
                )

            prom_config = PrometheusConfig(
                url=self._config.metrics.url,
                auth=auth,
            )
            self._metrics_source = PrometheusMetricsSource(prom_config)

    def get_service_handlers(self) -> dict[str, ServiceHandler]:
        """Return service handlers keyed by service type."""
        if self._handlers is None:
            raise RuntimeError("Plugin not initialized. Call initialize() first.")
        return self._handlers

    def get_cost_input(self) -> CostInput:
        """Return cost input backed by CCloud Billing API."""
        if self._config is None or self._connection is None:
            raise RuntimeError("Plugin not initialized. Call initialize() first.")
        return CCloudBillingCostInput(self._connection, self._config)

    def get_metrics_source(self) -> MetricsSource | None:
        """Return metrics source if configured, None otherwise."""
        return self._metrics_source
