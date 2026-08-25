"""Canonical self-managed Kafka Prometheus telemetry names and aliases."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, Literal

from core.models import MetricQuery, MetricRow

if TYPE_CHECKING:
    from core.metrics.protocol import MetricsSource
    from plugins.self_managed_kafka.config import SelfManagedKafkaConfig


_CANONICAL_LABEL_METADATA = "_self_managed_kafka_canonical_labels"
_PROMETHEUS_METRIC_IDENTIFIER = re.compile(r"[A-Za-z_:][A-Za-z0-9_:]*$")
_PROMETHEUS_LABEL_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*$")


@dataclass(frozen=True)
class MetricFamilySpec:
    """Immutable description of one supported canonical metric family."""

    canonical_name: str
    metric_type: Literal["counter", "gauge"]
    canonical_labels: tuple[str, ...]
    affected_feature: tuple[str, ...] = ()


@dataclass(frozen=True)
class ResolvedMetricFamily:
    """One canonical metric family resolved to physical Prometheus names."""

    canonical_name: str
    physical_name: str
    canonical_to_physical_labels: Mapping[str, str]
    metric_type: Literal["counter", "gauge"] = "gauge"
    affected_feature: tuple[str, ...] = ()


TELEMETRY_FAMILY_SPECS: tuple[MetricFamilySpec, ...] = (
    MetricFamilySpec("up", "gauge", (), ("target_scope",)),
    MetricFamilySpec(
        "kafka_server_brokertopicmetrics_alltopics_bytesin_total",
        "counter",
        ("broker",),
        ("cluster_ingress",),
    ),
    MetricFamilySpec(
        "kafka_server_brokertopicmetrics_alltopics_bytesout_total",
        "counter",
        ("broker",),
        ("cluster_egress",),
    ),
    MetricFamilySpec(
        "kafka_log_log_size",
        "gauge",
        ("broker", "topic", "partition"),
        ("cluster_storage", "prometheus_discovery", "topic_storage"),
    ),
    MetricFamilySpec(
        "kafka_server_brokertopicmetrics_bytesin_total",
        "counter",
        ("broker", "topic"),
        ("prometheus_discovery", "topic_ingress"),
    ),
    MetricFamilySpec(
        "kafka_server_brokertopicmetrics_bytesout_total",
        "counter",
        ("broker", "topic"),
        ("prometheus_discovery", "topic_egress"),
    ),
    MetricFamilySpec(
        "kafka_server_quota_byte_rate",
        "gauge",
        ("broker", "quota_type", "quota_scope", "user", "client_id"),
        ("principal_readiness", "principal_attribution"),
    ),
    MetricFamilySpec(
        "kafka_server_quota_throttle_time_ms",
        "gauge",
        ("broker", "quota_type", "quota_scope", "user", "client_id"),
        ("principal_readiness",),
    ),
)
TELEMETRY_FAMILY_BY_NAME: dict[str, MetricFamilySpec] = {spec.canonical_name: spec for spec in TELEMETRY_FAMILY_SPECS}


def _require_mapping(value: object, message: str) -> Mapping[object, object]:
    if not isinstance(value, Mapping):
        raise ValueError(message)
    return value


def validate_metric_name_overrides(value: object) -> Mapping[object, object]:
    """Validate raw metric override shape and return the mapping."""
    overrides = _require_mapping(
        value,
        "metric_name_overrides must be a mapping of canonical metric names to one physical metric name",
    )
    for family, physical_name in overrides.items():
        if not isinstance(family, str) or family not in TELEMETRY_FAMILY_BY_NAME:
            raise ValueError(f"metric_name_overrides contains unknown canonical metric family {family}")
        if not isinstance(physical_name, str):
            raise ValueError(f"metric_name_overrides[{family}] must resolve to exactly one physical metric name")
        if _PROMETHEUS_METRIC_IDENTIFIER.fullmatch(physical_name) is None:
            raise ValueError(f"metric_name_overrides[{family}] is not a valid Prometheus metric identifier")
    return overrides


def validate_label_name_overrides(value: object) -> Mapping[object, object]:
    """Validate raw per-family label override shape and return the mapping."""
    overrides = _require_mapping(
        value,
        "label_name_overrides must be a mapping of canonical metric families to label mappings",
    )
    for family, labels_value in overrides.items():
        if not isinstance(family, str) or family not in TELEMETRY_FAMILY_BY_NAME:
            raise ValueError(f"label_name_overrides contains unknown canonical metric family {family}")
        labels = _require_mapping(
            labels_value,
            f"label_name_overrides[{family}] must be a mapping of canonical labels to physical labels",
        )
        allowed_labels = set(TELEMETRY_FAMILY_BY_NAME[family].canonical_labels)
        for canonical_label, physical_label in labels.items():
            if not isinstance(canonical_label, str) or canonical_label not in allowed_labels:
                raise ValueError(f"label_name_overrides[{family}] contains unknown canonical label {canonical_label}")
            if not isinstance(physical_label, str):
                raise ValueError(
                    f"label_name_overrides[{family}][{canonical_label}] must resolve to exactly one physical label name"
                )
            if _PROMETHEUS_LABEL_IDENTIFIER.fullmatch(physical_label) is None:
                raise ValueError(
                    f"label_name_overrides[{family}][{canonical_label}] is not a valid Prometheus label identifier"
                )
    return overrides


def validate_resolved_aliases(
    metric_overrides: Mapping[str, str],
    label_overrides: Mapping[str, Mapping[str, str]],
    metrics_identifier_label: str,
) -> None:
    """Validate duplicate and selector-collision rules on fully resolved aliases."""
    resolved_metrics = {
        spec.canonical_name: metric_overrides.get(spec.canonical_name, spec.canonical_name)
        for spec in TELEMETRY_FAMILY_SPECS
    }
    metrics_to_families: dict[str, list[str]] = {}
    for family, physical_name in resolved_metrics.items():
        metrics_to_families.setdefault(physical_name, []).append(family)
    for physical_name, families in metrics_to_families.items():
        if len(families) > 1:
            raise ValueError(
                f"physical metric {physical_name} is assigned to multiple canonical metric families: "
                + ", ".join(sorted(families))
            )

    for spec in TELEMETRY_FAMILY_SPECS:
        resolved_labels = {
            canonical: label_overrides.get(spec.canonical_name, {}).get(canonical, canonical)
            for canonical in spec.canonical_labels
        }
        labels_to_canonical: dict[str, list[str]] = {}
        for canonical, physical_label in resolved_labels.items():
            labels_to_canonical.setdefault(physical_label, []).append(canonical)
            if physical_label == metrics_identifier_label:
                raise ValueError(
                    f"physical label {physical_label} in {spec.canonical_name} conflicts with metrics_identifier_label"
                )
        for physical_label, canonical_labels in labels_to_canonical.items():
            if len(canonical_labels) > 1:
                raise ValueError(
                    f"physical label {physical_label} is assigned to multiple canonical labels in "
                    f"{spec.canonical_name}: {', '.join(sorted(canonical_labels))}"
                )


class ResolvedTelemetryCatalog:
    """Resolve one tenant's canonical telemetry catalog exactly once."""

    def __init__(self, config: SelfManagedKafkaConfig) -> None:
        metric_overrides = config.metric_name_overrides
        label_overrides = config.label_name_overrides
        self._families: dict[str, ResolvedMetricFamily] = {}
        for spec in TELEMETRY_FAMILY_SPECS:
            labels = {
                canonical: label_overrides.get(spec.canonical_name, {}).get(canonical, canonical)
                for canonical in spec.canonical_labels
            }
            self._families[spec.canonical_name] = ResolvedMetricFamily(
                canonical_name=spec.canonical_name,
                physical_name=metric_overrides.get(spec.canonical_name, spec.canonical_name),
                canonical_to_physical_labels=labels,
                metric_type=spec.metric_type,
                affected_feature=spec.affected_feature,
            )

    def family(self, canonical_name: str) -> ResolvedMetricFamily:
        """Return a resolved family or raise for an unsupported canonical name."""
        try:
            return self._families[canonical_name]
        except KeyError as exc:
            raise KeyError(f"Unknown self-managed Kafka telemetry family: {canonical_name}") from exc

    def metric_name(self, canonical_name: str) -> str:
        """Return the physical metric name for a canonical family."""
        return self.family(canonical_name).physical_name

    def label_name(self, canonical_family: str, canonical_label: str) -> str:
        """Return the physical label name for a canonical family label."""
        family = self.family(canonical_family)
        try:
            return family.canonical_to_physical_labels[canonical_label]
        except KeyError as exc:
            raise KeyError(f"Unknown canonical label {canonical_label} for family {canonical_family}") from exc

    def bind_query(
        self,
        *,
        canonical_family: str,
        key: str,
        query_expression: str,
        canonical_label_keys: tuple[str, ...],
        passthrough_label_keys: tuple[str, ...] = (),
        resource_label: str | None,
        query_mode: Literal["instant", "range"] = "range",
        metadata: Mapping[str, object] | None = None,
    ) -> MetricQuery:
        """Bind canonical dimensions to their physical query labels."""
        self.family(canonical_family)
        physical_labels: list[str] = []
        physical_to_canonical: dict[str, str] = {}
        for canonical_label in canonical_label_keys:
            physical_label = self.label_name(canonical_family, canonical_label)
            if physical_label not in physical_labels:
                physical_labels.append(physical_label)
            physical_to_canonical[physical_label] = canonical_label
        for passthrough_label in passthrough_label_keys:
            if passthrough_label not in physical_labels:
                physical_labels.append(passthrough_label)
        query_metadata: dict[str, Any] = dict(metadata or {})
        query_metadata[_CANONICAL_LABEL_METADATA] = dict(physical_to_canonical)
        return MetricQuery(
            key=key,
            query_expression=query_expression,
            label_keys=tuple(physical_labels),
            resource_label=resource_label,
            query_mode=query_mode,
            metadata=query_metadata,
        )


class CanonicalizingMetricsSource:
    """Normalize bound physical Prometheus labels at the plugin boundary."""

    def __init__(self, source: MetricsSource) -> None:
        self._source = source

    def query(
        self,
        queries: Sequence[MetricQuery],
        start: datetime,
        end: datetime,
        step: timedelta = timedelta(hours=1),
        resource_id_filter: str | None = None,
    ) -> dict[str, list[MetricRow]]:
        """Delegate the request and restore canonical labels on returned rows."""
        results = self._source.query(
            queries=queries,
            start=start,
            end=end,
            step=step,
            resource_id_filter=resource_id_filter,
        )
        query_by_key = {query.key: query for query in queries}
        normalized: dict[str, list[MetricRow]] = {}
        for key, rows in results.items():
            query = query_by_key.get(key)
            mapping = query.metadata.get(_CANONICAL_LABEL_METADATA) if query is not None else None
            if not isinstance(mapping, Mapping) or not mapping:
                normalized[key] = rows
                continue
            normalized[key] = [
                replace(
                    row,
                    labels={str(mapping.get(label, label)): value for label, value in row.labels.items()},
                )
                for row in rows
            ]
        return normalized

    def close(self) -> None:
        """Close the owned metrics source."""
        self._source.close()


__all__ = [
    "CanonicalizingMetricsSource",
    "MetricFamilySpec",
    "ResolvedMetricFamily",
    "ResolvedTelemetryCatalog",
    "TELEMETRY_FAMILY_BY_NAME",
    "TELEMETRY_FAMILY_SPECS",
]
