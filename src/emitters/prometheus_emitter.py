from __future__ import annotations

import logging
import threading
from collections.abc import Iterator, Sequence
from datetime import UTC, datetime
from datetime import date as date_type
from enum import StrEnum
from typing import Any

from prometheus_client import REGISTRY, start_http_server
from prometheus_client.core import CollectorRegistry, GaugeMetricFamily

from core.models.emit_descriptors import MetricDescriptor  # from models layer, not emitters layer  # noqa: TC001

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


def _serialize_label(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, StrEnum):
        return str(value)
    return str(value)


class PrometheusEmitter:
    """Generic Prometheus emitter — reads __prometheus_metrics__ from row type at emit time.

    No storage_backend dependency. No hardcoded metric names or label sets.
    Collectors are lazily created and cached in a class-level dict keyed by metric name.
    Works for any row type that declares __prometheus_metrics__: ClassVar[tuple[MetricDescriptor, ...]].
    """

    _collectors: dict[str, _TimestampedGaugeCollector] = {}
    _collectors_lock: threading.Lock = threading.Lock()

    _server_started: bool = False
    _server_lock: threading.Lock = threading.Lock()

    def __init__(self, port: int = 8000) -> None:
        self._port = port
        self._start_server_once()

    def _start_server_once(self) -> None:
        with PrometheusEmitter._server_lock:
            if not PrometheusEmitter._server_started:
                start_http_server(self._port)
                logger.info("Prometheus metrics server started on :%d", self._port)
                PrometheusEmitter._server_started = True

    def _get_or_create_collector(self, descriptor: MetricDescriptor) -> _TimestampedGaugeCollector:
        with PrometheusEmitter._collectors_lock:
            if descriptor.name not in PrometheusEmitter._collectors:
                PrometheusEmitter._collectors[descriptor.name] = _TimestampedGaugeCollector(
                    descriptor.name,
                    descriptor.documentation,
                    list(descriptor.label_fields),
                )
            return PrometheusEmitter._collectors[descriptor.name]

    def __call__(self, tenant_id: str, date: date_type, rows: Sequence[Any]) -> None:
        if not rows:
            return
        descriptors: tuple[MetricDescriptor, ...] = type(rows[0]).__prometheus_metrics__
        if not descriptors:
            return

        billing_ts = datetime(date.year, date.month, date.day, 0, 0, 0, tzinfo=UTC)
        ts_float = billing_ts.timestamp()

        for descriptor in descriptors:
            collector = self._get_or_create_collector(descriptor)
            samples: list[_Sample] = []
            for row in rows:
                label_values = [_serialize_label(getattr(row, f)) for f in descriptor.label_fields]
                value = float(getattr(row, descriptor.value_field))
                samples.append((label_values, value, ts_float))
            collector.set_samples_for_tenant(tenant_id, samples)


def make_prometheus_emitter(port: int = 8000) -> PrometheusEmitter:
    """Factory registered as ``"prometheus"`` in EmitterRegistry."""
    return PrometheusEmitter(port=port)
