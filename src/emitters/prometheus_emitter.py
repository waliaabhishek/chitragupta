from __future__ import annotations

import logging
import threading
from collections.abc import Iterator, Sequence
from datetime import UTC, datetime
from datetime import date as date_type
from typing import TYPE_CHECKING

from prometheus_client import REGISTRY, start_http_server
from prometheus_client.core import CollectorRegistry, GaugeMetricFamily

if TYPE_CHECKING:
    from core.models.chargeback import ChargebackRow
    from core.models.topic_attribution import TopicAttributionRow
    from core.storage.interface import StorageBackend

logger = logging.getLogger(__name__)

type _Sample = tuple[list[str], float, float]


class _TimestampedGaugeCollector:
    """Custom prometheus_client collector.

    Holds samples keyed by tenant_id. Each emit replaces only the calling
    tenant's slice — other tenants' data is preserved.
    """

    def __init__(
        self,
        name: str,
        documentation: str,
        label_names: list[str],
        registry: CollectorRegistry | None = None,
    ) -> None:
        self._name = name
        self._documentation = documentation
        self._label_names = label_names
        self._samples_by_tenant: dict[str, list[_Sample]] = {}
        self._lock = threading.Lock()
        (registry or REGISTRY).register(self)

    def set_samples_for_tenant(self, tenant_id: str, samples: list[_Sample]) -> None:
        with self._lock:
            self._samples_by_tenant[tenant_id] = samples

    def collect(self) -> Iterator[GaugeMetricFamily]:
        family = GaugeMetricFamily(self._name, self._documentation, labels=self._label_names)
        with self._lock:
            for tenant_samples in self._samples_by_tenant.values():
                for label_values, value, ts in tenant_samples:
                    family.add_metric(label_values, value, timestamp=ts)
        yield family


class PrometheusEmitter:
    """Exposes chargeback, billing, resource, and identity data as Prometheus metrics."""

    _chargeback_col: _TimestampedGaugeCollector | None = None
    _billing_col: _TimestampedGaugeCollector | None = None
    _resource_col: _TimestampedGaugeCollector | None = None
    _identity_col: _TimestampedGaugeCollector | None = None
    _topic_attribution_col: _TimestampedGaugeCollector | None = None
    _col_lock: threading.Lock = threading.Lock()

    _server_started: bool = False
    _server_lock: threading.Lock = threading.Lock()

    def __init__(self, port: int, storage_backend: StorageBackend) -> None:
        self._port = port
        self._storage_backend = storage_backend
        self._start_server_once()
        self._init_collectors_once()

    def _start_server_once(self) -> None:
        with PrometheusEmitter._server_lock:
            if not PrometheusEmitter._server_started:
                start_http_server(self._port)
                logger.info("Prometheus metrics server started on :%d", self._port)
                PrometheusEmitter._server_started = True

    def _init_collectors_once(self) -> None:
        with PrometheusEmitter._col_lock:
            if PrometheusEmitter._chargeback_col is None:
                PrometheusEmitter._chargeback_col = _TimestampedGaugeCollector(
                    "chitragupta_chargeback_amount",
                    "Chargeback cost amount per identity/resource/product combination",
                    [
                        "tenant_id",
                        "ecosystem",
                        "identity_id",
                        "resource_id",
                        "product_type",
                        "cost_type",
                        "allocation_method",
                    ],
                )
                PrometheusEmitter._billing_col = _TimestampedGaugeCollector(
                    "chitragupta_billing_amount",
                    "Billing total cost per resource/product combination",
                    ["tenant_id", "ecosystem", "resource_id", "product_type", "product_category"],
                )
                PrometheusEmitter._resource_col = _TimestampedGaugeCollector(
                    "chitragupta_resource_active",
                    "Active resources at billing date (1 = active)",
                    ["tenant_id", "ecosystem", "resource_id", "resource_type"],
                )
                PrometheusEmitter._identity_col = _TimestampedGaugeCollector(
                    "chitragupta_identity_active",
                    "Active identities at billing date (1 = active)",
                    ["tenant_id", "ecosystem", "identity_id", "identity_type"],
                )
            if PrometheusEmitter._topic_attribution_col is None:
                PrometheusEmitter._topic_attribution_col = _TimestampedGaugeCollector(
                    "chitragupta_topic_attribution_amount",
                    "Topic attribution cost amount per topic/cluster/product combination",
                    [
                        "tenant_id",
                        "ecosystem",
                        "env_id",
                        "cluster_resource_id",
                        "topic_name",
                        "product_category",
                        "product_type",
                    ],
                )

    def __call__(
        self,
        tenant_id: str,
        date: date_type,
        rows: Sequence[ChargebackRow],
    ) -> None:
        if not rows:
            return

        billing_ts = datetime(date.year, date.month, date.day, 0, 0, 0, tzinfo=UTC)
        ts_float = billing_ts.timestamp()
        ecosystem = rows[0].ecosystem

        if PrometheusEmitter._chargeback_col is None:
            raise RuntimeError("Collectors not initialized — call _init_collectors_once first")
        chargeback_samples: list[_Sample] = [
            (
                [
                    tenant_id,
                    row.ecosystem,
                    row.identity_id,
                    row.resource_id or "",
                    row.product_type,
                    str(row.cost_type),
                    row.allocation_method or "",
                ],
                float(row.amount),
                ts_float,
            )
            for row in rows
        ]
        PrometheusEmitter._chargeback_col.set_samples_for_tenant(tenant_id, chargeback_samples)

        if (
            PrometheusEmitter._billing_col is None
            or PrometheusEmitter._resource_col is None
            or PrometheusEmitter._identity_col is None
        ):
            raise RuntimeError("Collectors not initialized — call _init_collectors_once first")
        with self._storage_backend.create_unit_of_work() as uow:
            billing_lines = uow.billing.find_by_date(ecosystem, tenant_id, date)
            billing_samples: list[_Sample] = [
                (
                    [tenant_id, line.ecosystem, line.resource_id, line.product_type, line.product_category],
                    float(line.total_cost),
                    ts_float,
                )
                for line in billing_lines
            ]
            PrometheusEmitter._billing_col.set_samples_for_tenant(tenant_id, billing_samples)

            resources, _ = uow.resources.find_active_at(ecosystem, tenant_id, billing_ts, count=False)
            resource_samples: list[_Sample] = [
                ([tenant_id, r.ecosystem, r.resource_id, r.resource_type], 1.0, ts_float) for r in resources
            ]
            PrometheusEmitter._resource_col.set_samples_for_tenant(tenant_id, resource_samples)

            identities, _ = uow.identities.find_active_at(ecosystem, tenant_id, billing_ts, count=False)
            identity_samples: list[_Sample] = [
                ([tenant_id, i.ecosystem, i.identity_id, i.identity_type], 1.0, ts_float) for i in identities
            ]
            PrometheusEmitter._identity_col.set_samples_for_tenant(tenant_id, identity_samples)

    def emit_topic_attributions(
        self,
        tenant_id: str,
        date: date_type,
        rows: Sequence[TopicAttributionRow],
    ) -> None:
        """Expose topic attribution rows as chitragupta_topic_attribution_amount gauge."""
        if not rows:
            return
        if PrometheusEmitter._topic_attribution_col is None:
            raise RuntimeError("Collectors not initialized — call _init_collectors_once first")

        billing_ts = datetime(date.year, date.month, date.day, 0, 0, 0, tzinfo=UTC)
        ts_float = billing_ts.timestamp()

        samples: list[_Sample] = [
            (
                [
                    tenant_id,
                    row.ecosystem,
                    row.env_id,
                    row.cluster_resource_id,
                    row.topic_name,
                    row.product_category,
                    row.product_type,
                ],
                float(row.amount),
                ts_float,
            )
            for row in rows
        ]
        PrometheusEmitter._topic_attribution_col.set_samples_for_tenant(tenant_id, samples)


def make_prometheus_emitter(
    port: int = 8000,
    storage_backend: StorageBackend | None = None,
) -> PrometheusEmitter:
    """Factory registered in the emitter registry."""
    if storage_backend is None:
        raise ValueError("PrometheusEmitter requires storage_backend — ensure _load_emitters injects it")
    return PrometheusEmitter(port=port, storage_backend=storage_backend)


make_prometheus_emitter.needs_storage_backend = True  # type: ignore[attr-defined]
