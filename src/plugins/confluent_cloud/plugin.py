from __future__ import annotations

from typing import TYPE_CHECKING, Any

from plugins.confluent_cloud.config import CCloudPluginConfig
from plugins.confluent_cloud.connections import CCloudConnection
from plugins.confluent_cloud.cost_input import CCloudBillingCostInput

if TYPE_CHECKING:
    from core.metrics.protocol import MetricsSource
    from core.plugin.protocols import CostInput, ServiceHandler


class ConfluentCloudPlugin:
    """Confluent Cloud ecosystem plugin."""

    def __init__(self) -> None:
        self._config: CCloudPluginConfig | None = None
        self._connection: CCloudConnection | None = None

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

    def get_service_handlers(self) -> dict[str, ServiceHandler]:
        """Return service handlers. Stub returns empty dict."""
        return {}

    def get_cost_input(self) -> CostInput:
        """Return cost input backed by CCloud Billing API."""
        if self._config is None or self._connection is None:
            raise RuntimeError("Plugin not initialized. Call initialize() first.")
        return CCloudBillingCostInput(self._connection, self._config)

    def get_metrics_source(self) -> MetricsSource | None:
        """Return metrics source. None until handlers need metrics (chunk 2.3+)."""
        return None
