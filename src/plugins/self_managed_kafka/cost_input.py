"""ConstructedCostInput: generates BillingLineItems from YAML cost model + Prometheus metrics.

This is the core innovation of the self-managed Kafka plugin — the "metrics-only" billing
paradigm where no external billing API exists and costs are calculated from infrastructure
pricing × usage metrics.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterable
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING

from core.metrics.protocol import MetricsQueryError
from core.models import BillingLineItem, CoreBillingLineItem, MetricQuery
from core.plugin.protocols import CostInput
from plugins.self_managed_kafka.historical_metrics import iter_daily_evidence

if TYPE_CHECKING:
    from core.metrics.protocol import MetricsSource
    from core.models import MetricRow
    from core.storage.interface import UnitOfWork
    from plugins.self_managed_kafka.config import CostModelConfig, SelfManagedKafkaConfig

logger = logging.getLogger(__name__)
ECOSYSTEM = "self_managed_kafka"

# Bytes per GiB (2^30)
_BYTES_PER_GIB = Decimal("1073741824")


class _MissingStorageEvidenceError(MetricsQueryError):
    """A retryable daily storage-evidence gap that must not abort other days."""


def _cost_queries(metrics_identifier_label: str) -> list[MetricQuery]:
    """Build cost queries bound to this configured Prometheus target scope."""
    return [
        MetricQuery(
            key="cluster_bytes_in",
            query_expression="sum(increase(kafka_server_brokertopicmetrics_alltopics_bytesin_total{}[86400s]))",
            label_keys=(),
            resource_label=metrics_identifier_label,
            query_mode="instant",
        ),
        MetricQuery(
            key="cluster_bytes_out",
            query_expression="sum(increase(kafka_server_brokertopicmetrics_alltopics_bytesout_total{}[86400s]))",
            label_keys=(),
            resource_label=metrics_identifier_label,
            query_mode="instant",
        ),
        MetricQuery(
            key="cluster_storage_bytes",
            query_expression="sum(kafka_log_log_size{})",
            label_keys=(),
            resource_label=metrics_identifier_label,
        ),
    ]


def _day_starts(start: datetime, end: datetime) -> Iterable[tuple[datetime, datetime]]:
    """Yield (day_start, day_end) pairs covering [start, end) one day at a time."""
    current = start
    one_day = timedelta(days=1)
    while current < end:
        day_end = min(current + one_day, end)
        yield current, day_end
        current = day_end


def _utc_day_floor(value: datetime) -> datetime:
    """Return the UTC calendar-day start containing an aware timestamp."""
    if value.tzinfo is None:
        raise ValueError(f"Naive datetime not allowed: {value!r}")
    utc_value = value.astimezone(UTC)
    return utc_value.replace(hour=0, minute=0, second=0, microsecond=0)


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
        inventory_is_partitionless: Callable[[], bool] | None = None,
    ) -> None:
        self._config = config
        self._metrics_source = metrics_source
        self._inventory_is_partitionless = inventory_is_partitionless or (lambda: False)

    def gather(
        self,
        tenant_id: str,
        start: datetime,
        end: datetime,
        uow: UnitOfWork,
    ) -> Iterable[BillingLineItem]:
        """Construct daily pools using identical closed UTC calendar windows."""
        normalized_start = _utc_day_floor(start)
        normalized_end = _utc_day_floor(end)
        if normalized_start >= normalized_end:
            return

        queries = _cost_queries(self._config.metrics_identifier_label)
        windows = tuple(_day_starts(normalized_start, normalized_end))
        if len(windows) > 1:
            for chunk, daily_metrics in iter_daily_evidence(
                self._metrics_source,
                queries,
                windows,
                step=timedelta(seconds=self._config.metrics_step_seconds),
                chunk_days=self._config.historical_acquisition_chunk_days,
                resource_id_filter=self._config.metrics_identifier,
            ):
                for day_start, day_end in chunk:
                    try:
                        yield from self._process_day(
                            tenant_id,
                            day_start,
                            day_end,
                            daily_metrics[(day_start, day_end)],
                        )
                    except _MissingStorageEvidenceError:
                        continue
                    except MetricsQueryError:
                        logger.warning(
                            "Bounded historical evidence incomplete for tenant=%s date=%s — skipping",
                            tenant_id,
                            day_start.date(),
                        )
                        continue
                del daily_metrics
            return

        for day_start, day_end in windows:
            try:
                metrics = self._metrics_source.query(
                    queries=queries,
                    start=day_start,
                    end=day_end,
                    step=timedelta(seconds=self._config.metrics_step_seconds),
                    resource_id_filter=self._config.metrics_identifier,
                )
            except MetricsQueryError as exc:
                logger.warning(
                    "Daily Prometheus query failed for tenant=%s date=%s — skipping: %s",
                    tenant_id,
                    day_start.date(),
                    exc,
                )
                continue
            try:
                yield from self._process_day(tenant_id, day_start, day_end, metrics)
            except _MissingStorageEvidenceError:
                continue

    def _process_day(
        self,
        tenant_id: str,
        day_start: datetime,
        day_end: datetime,
        metrics: dict[str, list[MetricRow]],
    ) -> Iterable[BillingLineItem]:
        """Generate billing lines for a single day from pre-fetched metrics."""
        bytes_in_rows = metrics.get("cluster_bytes_in", [])
        bytes_out_rows = metrics.get("cluster_bytes_out", [])
        storage_rows = [row for row in metrics.get("cluster_storage_bytes", []) if day_start <= row.timestamp < day_end]
        if not bytes_in_rows or not bytes_out_rows:
            missing_metric = (
                "kafka_server_brokertopicmetrics_alltopics_bytesin_total"
                if not bytes_in_rows
                else "kafka_server_brokertopicmetrics_alltopics_bytesout_total"
            )
            logger.error(
                "Missing broker-wide client counter tenant=%s cluster=%s selector=%s date=%s metric=%s",
                tenant_id,
                self._config.cluster_id,
                self._config.metrics_identifier,
                day_start.date(),
                missing_metric,
            )
            raise MetricsQueryError(f"Missing required broker-wide metric family: {missing_metric}")

        if not storage_rows and not self._inventory_is_partitionless():
            logger.warning(
                "Missing storage metric family tenant=%s cluster=%s selector=%s date=%s metric=kafka_log_log_size",
                tenant_id,
                self._config.cluster_id,
                self._config.metrics_identifier,
                day_start.date(),
            )
            raise _MissingStorageEvidenceError("Missing required storage metric family: kafka_log_log_size")

        cost_model = self._config.get_effective_cost_model()
        hours = Decimal(str((day_end - day_start).total_seconds() / 3600))
        cluster_id = self._config.cluster_id
        timestamp = day_start.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=UTC)

        yield from _make_compute_line(tenant_id, cluster_id, timestamp, self._config.broker_count, hours, cost_model)
        yield from _make_storage_line(tenant_id, cluster_id, timestamp, storage_rows, hours, cost_model)
        yield from _make_network_lines(
            tenant_id,
            cluster_id,
            timestamp,
            bytes_in_rows,
            bytes_out_rows,
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
    yield CoreBillingLineItem(
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
    yield CoreBillingLineItem(
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

    yield CoreBillingLineItem(
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

    yield CoreBillingLineItem(
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
