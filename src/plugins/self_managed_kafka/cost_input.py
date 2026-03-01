"""ConstructedCostInput: generates BillingLineItems from YAML cost model + Prometheus metrics.

This is the core innovation of the self-managed Kafka plugin — the "metrics-only" billing
paradigm where no external billing API exists and costs are calculated from infrastructure
pricing × usage metrics.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING

from core.metrics.protocol import MetricsQueryError
from core.models import BillingLineItem, MetricQuery
from core.plugin.protocols import CostInput

if TYPE_CHECKING:
    from core.metrics.protocol import MetricsSource
    from core.models import MetricRow
    from core.storage.interface import UnitOfWork
    from plugins.self_managed_kafka.config import CostModelConfig, SelfManagedKafkaConfig

LOGGER = logging.getLogger(__name__)
ECOSYSTEM = "self_managed_kafka"

# Bytes per GiB (2^30)
_BYTES_PER_GIB = Decimal("1073741824")

# PromQL queries for cluster-wide cost construction.
# No {} placeholder needed since we want cluster-wide totals (no resource filter).
_BYTES_IN_QUERY = MetricQuery(
    key="cluster_bytes_in",
    query_expression="sum(increase(kafka_server_brokertopicmetrics_bytesin_total[1h]))",
    label_keys=(),
    resource_label="",
)

_BYTES_OUT_QUERY = MetricQuery(
    key="cluster_bytes_out",
    query_expression="sum(increase(kafka_server_brokertopicmetrics_bytesout_total[1h]))",
    label_keys=(),
    resource_label="",
)

_STORAGE_QUERY = MetricQuery(
    key="cluster_storage_bytes",
    query_expression="sum(kafka_log_log_size)",
    label_keys=(),
    resource_label="",
)

_COST_QUERIES: list[MetricQuery] = [_BYTES_IN_QUERY, _BYTES_OUT_QUERY, _STORAGE_QUERY]


class ConstructedCostInput(CostInput):
    """Constructs BillingLineItems from YAML cost model + Prometheus metrics.

    All generated billing lines use resource_id = cluster_id, since the cluster
    is the billable unit for self-managed infrastructure.

    Product types generated per day:
    - SELF_KAFKA_COMPUTE: fixed broker compute costs
    - SELF_KAFKA_STORAGE: storage costs from avg bytes
    - SELF_KAFKA_NETWORK_INGRESS: ingress costs from bytes_in total
    - SELF_KAFKA_NETWORK_EGRESS: egress costs from bytes_out total
    """

    def __init__(
        self,
        config: SelfManagedKafkaConfig,
        metrics_source: MetricsSource,
    ) -> None:
        self._config = config
        self._metrics_source = metrics_source

    def gather(
        self,
        tenant_id: str,
        start: datetime,
        end: datetime,
        uow: UnitOfWork,
    ) -> Iterable[BillingLineItem]:
        """Query Prometheus for usage, calculate costs, yield billing lines.

        Generates one set of billing lines per day in the [start, end) range.
        Skips periods where Prometheus data is unavailable.
        """
        # Iterate over each day in the range
        current = start
        one_day = timedelta(days=1)
        while current < end:
            day_end = min(current + one_day, end)
            yield from self._gather_day(tenant_id, current, day_end)
            current = day_end

    def _gather_day(
        self,
        tenant_id: str,
        day_start: datetime,
        day_end: datetime,
    ) -> Iterable[BillingLineItem]:
        """Generate billing lines for a single day."""
        try:
            metrics = self._metrics_source.query(
                queries=_COST_QUERIES,
                start=day_start,
                end=day_end,
                step=timedelta(hours=1),
            )
        except MetricsQueryError as exc:
            LOGGER.warning(
                "Prometheus query failed for tenant=%s date=%s — skipping billing period: %s",
                tenant_id,
                day_start.date(),
                exc,
            )
            return

        # Check if we got any data at all
        has_data = any(rows for rows in metrics.values())
        if not has_data:
            LOGGER.warning(
                "No Prometheus data for tenant=%s date=%s — skipping billing period",
                tenant_id,
                day_start.date(),
            )
            return

        cost_model = self._config.get_effective_cost_model()
        hours = Decimal(str((day_end - day_start).total_seconds() / 3600))
        cluster_id = self._config.cluster_id
        # Use midnight UTC of the day as billing timestamp
        timestamp = day_start.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=UTC)

        yield from _make_compute_line(tenant_id, cluster_id, timestamp, self._config.broker_count, hours, cost_model)
        yield from _make_storage_line(
            tenant_id, cluster_id, timestamp, metrics.get("cluster_storage_bytes", []), hours, cost_model
        )
        yield from _make_network_lines(
            tenant_id,
            cluster_id,
            timestamp,
            metrics.get("cluster_bytes_in", []),
            metrics.get("cluster_bytes_out", []),
            cost_model,
        )


def _make_compute_line(
    tenant_id: str,
    cluster_id: str,
    timestamp: datetime,
    broker_count: int,
    hours: Decimal,
    cost_model: CostModelConfig,
) -> Iterable[BillingLineItem]:
    """Generate SELF_KAFKA_COMPUTE billing line."""
    quantity = Decimal(str(broker_count)) * hours
    unit_price = cost_model.compute_hourly_rate
    yield BillingLineItem(
        ecosystem=ECOSYSTEM,
        tenant_id=tenant_id,
        timestamp=timestamp,
        resource_id=cluster_id,
        product_category="kafka",
        product_type="SELF_KAFKA_COMPUTE",
        quantity=quantity,
        unit_price=unit_price,
        total_cost=quantity * unit_price,
        granularity="daily",
        currency="USD",
    )


def _make_storage_line(
    tenant_id: str,
    cluster_id: str,
    timestamp: datetime,
    storage_rows: list[MetricRow],
    hours: Decimal,
    cost_model: CostModelConfig,
) -> Iterable[BillingLineItem]:
    """Generate SELF_KAFKA_STORAGE billing line from average storage bytes."""
    if storage_rows:
        avg_bytes = sum(row.value for row in storage_rows) / len(storage_rows)
        avg_gib = Decimal(str(avg_bytes)) / _BYTES_PER_GIB
    else:
        avg_gib = Decimal("0")

    quantity = avg_gib * hours
    unit_price = cost_model.storage_per_gib_hourly
    yield BillingLineItem(
        ecosystem=ECOSYSTEM,
        tenant_id=tenant_id,
        timestamp=timestamp,
        resource_id=cluster_id,
        product_category="kafka",
        product_type="SELF_KAFKA_STORAGE",
        quantity=quantity,
        unit_price=unit_price,
        total_cost=quantity * unit_price,
        granularity="daily",
        currency="USD",
    )


def _make_network_lines(
    tenant_id: str,
    cluster_id: str,
    timestamp: datetime,
    bytes_in_rows: list[MetricRow],
    bytes_out_rows: list[MetricRow],
    cost_model: CostModelConfig,
) -> Iterable[BillingLineItem]:
    """Generate SELF_KAFKA_NETWORK_INGRESS and SELF_KAFKA_NETWORK_EGRESS billing lines."""
    total_bytes_in = sum(row.value for row in bytes_in_rows)
    total_bytes_out = sum(row.value for row in bytes_out_rows)

    ingress_gib = Decimal(str(total_bytes_in)) / _BYTES_PER_GIB
    egress_gib = Decimal(str(total_bytes_out)) / _BYTES_PER_GIB

    ingress_price = cost_model.network_ingress_per_gib
    egress_price = cost_model.network_egress_per_gib

    yield BillingLineItem(
        ecosystem=ECOSYSTEM,
        tenant_id=tenant_id,
        timestamp=timestamp,
        resource_id=cluster_id,
        product_category="kafka",
        product_type="SELF_KAFKA_NETWORK_INGRESS",
        quantity=ingress_gib,
        unit_price=ingress_price,
        total_cost=ingress_gib * ingress_price,
        granularity="daily",
        currency="USD",
    )

    yield BillingLineItem(
        ecosystem=ECOSYSTEM,
        tenant_id=tenant_id,
        timestamp=timestamp,
        resource_id=cluster_id,
        product_category="kafka",
        product_type="SELF_KAFKA_NETWORK_EGRESS",
        quantity=egress_gib,
        unit_price=egress_price,
        total_cost=egress_gib * egress_price,
        granularity="daily",
        currency="USD",
    )
