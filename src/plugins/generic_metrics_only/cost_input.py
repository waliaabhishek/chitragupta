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
    from plugins.generic_metrics_only.config import (
        CostTypeConfig,
        GenericMetricsOnlyConfig,
    )

LOGGER = logging.getLogger(__name__)
_BYTES_PER_GIB = Decimal("1073741824")


class GenericConstructedCostInput(CostInput):
    """Constructs BillingLineItems from YAML cost model + Prometheus metrics.

    Query key convention: cost_{ct.name} for quantity queries (avoids collision
    with allocation query keys alloc_{ct.name} used by the handler).
    """

    def __init__(
        self,
        config: GenericMetricsOnlyConfig,
        metrics_source: MetricsSource,
    ) -> None:
        self._config = config
        self._metrics_source = metrics_source
        # Pre-build MetricQuery list (fixed types emit no query)
        self._cost_queries: list[MetricQuery] = []
        for ct in config.cost_types:
            q = ct.cost_quantity
            if q.type != "fixed":
                self._cost_queries.append(
                    MetricQuery(
                        key=f"cost_{ct.name}",
                        query_expression=q.query,  # type: ignore[union-attr]  # q.type != "fixed" checked above; storage_gib and network_gib both have .query
                        label_keys=(),
                        resource_label="",
                    )
                )

    def gather(
        self,
        tenant_id: str,
        start: datetime,
        end: datetime,
        uow: UnitOfWork,
    ) -> Iterable[BillingLineItem]:
        current = start
        one_day = timedelta(days=1)
        while current < end:
            day_end = min(current + one_day, end)
            yield from self._gather_day(tenant_id, current, day_end)
            current = day_end

    def _gather_day(self, tenant_id: str, day_start: datetime, day_end: datetime) -> Iterable[BillingLineItem]:
        metrics: dict[str, list[MetricRow]] = {}
        if self._cost_queries:
            try:
                metrics = self._metrics_source.query(
                    queries=self._cost_queries,
                    start=day_start,
                    end=day_end,
                    step=timedelta(seconds=self._config.metrics_step_seconds),
                )
            except MetricsQueryError as exc:
                LOGGER.warning(
                    "Prometheus query failed for tenant=%s date=%s -- skipping: %s",
                    tenant_id,
                    day_start.date(),
                    exc,
                )
                return

            has_data = any(rows for rows in metrics.values())
            if not has_data:
                LOGGER.warning(
                    "No Prometheus data for tenant=%s date=%s -- skipping",
                    tenant_id,
                    day_start.date(),
                )
                return

        hours = Decimal(str((day_end - day_start).total_seconds() / 3600))
        timestamp = day_start.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=UTC)

        for ct in self._config.cost_types:
            yield from self._make_line(tenant_id, timestamp, hours, ct, metrics)

    def _make_line(
        self,
        tenant_id: str,
        timestamp: datetime,
        hours: Decimal,
        ct: CostTypeConfig,
        metrics: dict[str, list[MetricRow]],
    ) -> Iterable[BillingLineItem]:
        q = ct.cost_quantity

        if q.type == "fixed":
            quantity = Decimal(str(q.count)) * hours  # type: ignore[union-attr]  # q.type == "fixed" checked; CostQuantityFixed has .count
        elif q.type == "storage_gib":
            rows = metrics.get(f"cost_{ct.name}", [])
            avg_bytes = sum(r.value for r in rows) / len(rows) if rows else 0.0
            quantity = Decimal(str(avg_bytes)) / _BYTES_PER_GIB * hours
        else:  # network_gib
            rows = metrics.get(f"cost_{ct.name}", [])
            total_bytes = sum(r.value for r in rows)
            quantity = Decimal(str(total_bytes)) / _BYTES_PER_GIB

        yield BillingLineItem(
            ecosystem=self._config.ecosystem_name,
            tenant_id=tenant_id,
            timestamp=timestamp,
            resource_id=self._config.cluster_id,
            product_category=ct.product_category,
            product_type=ct.name,
            quantity=quantity,
            unit_price=ct.rate,
            total_cost=quantity * ct.rate,
            granularity="daily",
            currency="USD",
        )
