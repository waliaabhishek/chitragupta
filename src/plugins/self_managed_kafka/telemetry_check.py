"""Explicit operator-invoked self-managed Kafka telemetry checker."""

from __future__ import annotations

import json
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum
from math import isfinite
from typing import TYPE_CHECKING

from core.metrics.config import create_metrics_source
from core.metrics.protocol import MetricsQueryError
from plugins.self_managed_kafka.telemetry_aliases import (
    TELEMETRY_FAMILY_SPECS,
    CanonicalizingMetricsSource,
    MetricFamilySpec,
    ResolvedTelemetryCatalog,
)

if TYPE_CHECKING:
    from core.metrics.protocol import MetricsSource
    from core.models import MetricRow
    from plugins.self_managed_kafka.config import SelfManagedKafkaConfig


class TelemetryCheckState(StrEnum):
    """Possible outcomes for one configured telemetry family."""

    VALID = "valid"
    INVALID = "invalid"
    NOT_OBSERVED = "not_observed"
    INCONCLUSIVE = "inconclusive"
    SKIPPED = "skipped"


@dataclass(frozen=True)
class TelemetryFamilyCheck:
    """Deterministic, value-free report for one tenant/family check."""

    tenant: str
    canonical_metric: str
    state: TelemetryCheckState
    resolved_metric: str
    selector: str
    expected_labels: Mapping[str, str]
    observed_labels: tuple[str, ...]
    affected_feature: tuple[str, ...]
    corrective_override: Mapping[str, object] | None
    warning: str | None


def _selector_value(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def _family_enabled(spec: MetricFamilySpec, config: SelfManagedKafkaConfig) -> bool:
    if spec.canonical_name in {
        "up",
        "kafka_server_brokertopicmetrics_alltopics_bytesin_total",
        "kafka_server_brokertopicmetrics_alltopics_bytesout_total",
        "kafka_log_log_size",
    }:
        return True
    if spec.canonical_name == "kafka_server_brokertopicmetrics_bytesin_total":
        return config.resource_source.source == "prometheus" or config.topic_attribution.enabled
    if spec.canonical_name == "kafka_server_brokertopicmetrics_bytesout_total":
        return config.topic_attribution.enabled
    if spec.canonical_name == "kafka_server_quota_byte_rate":
        return config.identity_source.source in {"prometheus", "both"}
    if spec.canonical_name == "kafka_server_quota_throttle_time_ms":
        return config.identity_source.source in {"prometheus", "both"} and not config.principal_attribution.enabled
    return False


def _base_record(
    *,
    tenant_name: str,
    spec: MetricFamilySpec,
    catalog: ResolvedTelemetryCatalog,
    config: SelfManagedKafkaConfig,
    state: TelemetryCheckState,
    observed_labels: tuple[str, ...] = (),
    corrective_override: Mapping[str, object] | None = None,
    warning: str | None = None,
) -> TelemetryFamilyCheck:
    return TelemetryFamilyCheck(
        tenant=tenant_name,
        canonical_metric=spec.canonical_name,
        state=state,
        resolved_metric=catalog.metric_name(spec.canonical_name),
        selector=(f'{config.metrics_identifier_label}="{_selector_value(config.metrics_identifier)}"'),
        expected_labels=dict(catalog.family(spec.canonical_name).canonical_to_physical_labels),
        observed_labels=observed_labels,
        affected_feature=spec.affected_feature,
        corrective_override=corrective_override,
        warning=warning,
    )


class _TelemetryChecker:
    """Run independent raw-family checks against one canonicalizing source."""

    def __init__(
        self,
        *,
        tenant_name: str,
        config: SelfManagedKafkaConfig,
        catalog: ResolvedTelemetryCatalog,
        source: MetricsSource,
        window_start: datetime,
        window_end: datetime,
    ) -> None:
        self._tenant_name = tenant_name
        self._config = config
        self._catalog = catalog
        self._source = source
        self._window_start = window_start
        self._window_end = window_end

    def run(self) -> tuple[TelemetryFamilyCheck, ...]:
        records: list[TelemetryFamilyCheck] = []
        for spec in TELEMETRY_FAMILY_SPECS:
            if not _family_enabled(spec, self._config):
                records.append(
                    _base_record(
                        tenant_name=self._tenant_name,
                        spec=spec,
                        catalog=self._catalog,
                        config=self._config,
                        state=TelemetryCheckState.SKIPPED,
                    )
                )
                continue
            records.append(self._check_family(spec))
        return tuple(records)

    def _check_family(self, spec: MetricFamilySpec) -> TelemetryFamilyCheck:
        query = self._catalog.bind_query(
            canonical_family=spec.canonical_name,
            key=f"telemetry_check_{spec.canonical_name}",
            query_expression=f"{self._catalog.metric_name(spec.canonical_name)}{{}}",
            canonical_label_keys=spec.canonical_labels,
            passthrough_label_keys=(self._config.metrics_identifier_label,),
            resource_label=self._config.metrics_identifier_label,
            query_mode="range",
        )
        try:
            results = self._source.query(
                queries=[query],
                start=self._window_start,
                end=self._window_end,
                step=timedelta(seconds=self._config.metrics_step_seconds),
                resource_id_filter=self._config.metrics_identifier,
            )
        except MetricsQueryError as exc:
            return _base_record(
                tenant_name=self._tenant_name,
                spec=spec,
                catalog=self._catalog,
                config=self._config,
                state=TelemetryCheckState.INCONCLUSIVE,
                warning=f"Prometheus family query failed: {type(exc).__name__}.",
            )

        if query.key not in results:
            return _base_record(
                tenant_name=self._tenant_name,
                spec=spec,
                catalog=self._catalog,
                config=self._config,
                state=TelemetryCheckState.INCONCLUSIVE,
                warning=f"Prometheus response omitted result key {query.key}.",
            )
        rows = results[query.key]
        if not rows:
            return _base_record(
                tenant_name=self._tenant_name,
                spec=spec,
                catalog=self._catalog,
                config=self._config,
                state=TelemetryCheckState.NOT_OBSERVED,
                corrective_override={"metric_name_overrides": {spec.canonical_name: "<physical-metric-name>"}},
                warning=(
                    "No selected series was observed during the check window; "
                    "historical coverage remains a warning only."
                ),
            )

        series = self._physical_series(rows)
        observed_labels = tuple(sorted({label for labels in series for label in labels if label != "__name__"}))
        selector_label = self._config.metrics_identifier_label
        selector_missing = any(not labels.get(selector_label, "").strip() for labels in series)
        if selector_missing:
            return _base_record(
                tenant_name=self._tenant_name,
                spec=spec,
                catalog=self._catalog,
                config=self._config,
                state=TelemetryCheckState.INVALID,
                observed_labels=observed_labels,
                warning=(f"Selected series is missing or has a blank global selector label {selector_label}."),
            )

        family = self._catalog.family(spec.canonical_name)
        missing_labels = {
            canonical_label
            for labels in series
            for canonical_label, physical_label in family.canonical_to_physical_labels.items()
            if not labels.get(physical_label, "").strip()
        }
        if missing_labels:
            missing = tuple(sorted(missing_labels))
            correction = {
                "label_name_overrides": {spec.canonical_name: {label: "<physical-label>" for label in missing}}
            }
            return _base_record(
                tenant_name=self._tenant_name,
                spec=spec,
                catalog=self._catalog,
                config=self._config,
                state=TelemetryCheckState.INVALID,
                observed_labels=observed_labels,
                corrective_override=correction,
                warning=(
                    "Observed series are missing or have blank resolved labels for canonical labels: "
                    f"{', '.join(missing)}."
                ),
            )

        if spec.canonical_name == "up" and any(not isfinite(row.value) or row.value != 1.0 for row in rows):
            return _base_record(
                tenant_name=self._tenant_name,
                spec=spec,
                catalog=self._catalog,
                config=self._config,
                state=TelemetryCheckState.INVALID,
                observed_labels=observed_labels,
                warning="At least one selected up sample is non-finite or not equal to 1.",
            )
        return _base_record(
            tenant_name=self._tenant_name,
            spec=spec,
            catalog=self._catalog,
            config=self._config,
            state=TelemetryCheckState.VALID,
            observed_labels=observed_labels,
        )

    @staticmethod
    def _physical_series(rows: Sequence[MetricRow]) -> tuple[dict[str, str], ...]:
        unique: dict[tuple[tuple[str, str], ...], dict[str, str]] = {}
        for row in rows:
            source_series = tuple(row.source_series or ())
            labels = dict(source_series)
            unique.setdefault(tuple(sorted(labels.items())), labels)
        return tuple(unique.values())


def check_self_managed_telemetry(
    *,
    tenant_name: str,
    config: SelfManagedKafkaConfig,
    window_end: datetime,
) -> tuple[TelemetryFamilyCheck, ...]:
    """Check one validated tenant without initializing normal pipeline state."""
    if window_end.tzinfo is None or window_end.utcoffset() is None:
        raise ValueError(f"Naive datetime not allowed: {window_end!r}")
    catalog = ResolvedTelemetryCatalog(config)
    window_start = window_end - timedelta(hours=config.discovery_window_hours)
    try:
        raw_source = create_metrics_source(config.metrics)
    except Exception as exc:
        warning = f"Prometheus source construction failed before observation: {type(exc).__name__}."
        return tuple(
            _base_record(
                tenant_name=tenant_name,
                spec=spec,
                catalog=catalog,
                config=config,
                state=(
                    TelemetryCheckState.INCONCLUSIVE if _family_enabled(spec, config) else TelemetryCheckState.SKIPPED
                ),
                warning=warning if _family_enabled(spec, config) else None,
            )
            for spec in TELEMETRY_FAMILY_SPECS
        )

    source = CanonicalizingMetricsSource(raw_source)
    try:
        return _TelemetryChecker(
            tenant_name=tenant_name,
            config=config,
            catalog=catalog,
            source=source,
            window_start=window_start,
            window_end=window_end,
        ).run()
    finally:
        source.close()


def _record_dict(record: TelemetryFamilyCheck) -> dict[str, object]:
    return {
        "tenant": record.tenant,
        "canonical_metric": record.canonical_metric,
        "state": record.state.value,
        "resolved_metric": record.resolved_metric,
        "selector": record.selector,
        "expected_labels": dict(record.expected_labels),
        "observed_labels": list(record.observed_labels),
        "affected_feature": list(record.affected_feature),
        "corrective_override": record.corrective_override,
        "warning": record.warning,
    }


def render_telemetry_check_jsonl(
    records: Sequence[TelemetryFamilyCheck],
    *,
    tenant_count: int,
) -> str:
    """Render ordered family records and one deterministic summary line."""
    lines = [json.dumps(_record_dict(record), sort_keys=True, separators=(",", ":")) for record in records]
    counts = Counter(record.state.value for record in records)
    summary = {
        "summary": {state.value: counts.get(state.value, 0) for state in TelemetryCheckState},
        "tenants": tenant_count,
    }
    lines.append(json.dumps(summary, sort_keys=True, separators=(",", ":")))
    return "\n".join(lines) + "\n"


__all__ = [
    "TelemetryCheckState",
    "TelemetryFamilyCheck",
    "check_self_managed_telemetry",
    "render_telemetry_check_jsonl",
]
