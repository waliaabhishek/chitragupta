"""Self-managed Kafka ecosystem plugin."""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from core.metrics.protocol import MetricsQueryError
from plugins.self_managed_kafka.config import SelfManagedKafkaConfig
from plugins.self_managed_kafka.cost_input import ConstructedCostInput
from plugins.self_managed_kafka.handlers.kafka import SelfManagedKafkaHandler

if TYPE_CHECKING:
    from core.metrics.protocol import MetricsSource
    from core.plugin.protocols import CostInput, ServiceHandler

LOGGER = logging.getLogger(__name__)


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
        self._metrics_source = self._create_metrics_source(self._config)

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

        Reuses PRINCIPALS_QUERY from gathering/prometheus.py.
        Sets self._prometheus_principals_available = False on missing label or
        Prometheus unreachability. Plugin continues either way (lenient).
        """
        from plugins.self_managed_kafka.gathering.prometheus import PRINCIPALS_QUERY

        now = datetime.now(UTC)
        try:
            results = self._metrics_source.query(  # type: ignore[union-attr]  # set on line above in initialize()
                queries=[PRINCIPALS_QUERY],
                start=now - timedelta(hours=1),
                end=now,
                step=timedelta(hours=1),
            )
            rows = results.get("distinct_principals", [])
            has_principal_label = any(row.labels.get("principal") for row in rows)
            if not has_principal_label:
                LOGGER.warning(
                    "self_managed_kafka: No 'principal' label found in Prometheus metrics. "
                    "Per-principal identity discovery will be unavailable. "
                    "Costs will be allocated to UNALLOCATED unless static_identities are configured."
                )
                self._prometheus_principals_available = False
        except MetricsQueryError:
            LOGGER.warning(
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
