"""Self-managed Kafka telemetry evidence and plugin-local allocation details."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from plugins.self_managed_kafka.principal_attribution import PrincipalDirectionEvaluation


class MetricsScopeStatus(StrEnum):
    VALID = "valid"
    NOT_OBSERVED = "not_observed"
    TARGET_DOWN = "target_down"
    MISMATCH = "mismatch"
    TRANSIENT_FAILURE = "transient_failure"
    RETENTION_GAP = "retention_gap"


class PrincipalTelemetryStatus(StrEnum):
    OBSERVED = "observed"
    NOT_OBSERVED = "not_observed"
    INVALID = "invalid"
    TRANSIENT_FAILURE = "transient_failure"
    POLICY_ONLY_CONFIGURED = "policy_only_configured"
    UNAVAILABLE = "unavailable"


SMK_DETAIL_PRINCIPAL_TELEMETRY_NOT_OBSERVED = "principal_telemetry_not_observed"
SMK_DETAIL_PRINCIPAL_TELEMETRY_INVALID = "principal_telemetry_invalid"
SMK_DETAIL_NO_FINITE_POSITIVE_THROTTLE = "no_finite_positive_throttle_observed"


@dataclass(frozen=True)
class MetricsScopeEvidence:
    label: str
    identifier: str
    window_start: datetime
    window_end: datetime
    status: MetricsScopeStatus
    detail: str
    observed_target_count: int = 0


@dataclass(frozen=True)
class MetricsScopeRequest:
    """Exact key for one run-local scope request or bounded physical query."""

    tenant_id: str
    metrics_identifier: str
    metrics_identifier_label: str
    step: timedelta
    start: datetime
    end: datetime


@dataclass(frozen=True)
class PrincipalTelemetryEvidence:
    window_start: datetime
    window_end: datetime
    status: PrincipalTelemetryStatus
    detail: str
    quota_scopes: frozenset[str] = frozenset()
    ingress: PrincipalDirectionEvaluation | None = None
    egress: PrincipalDirectionEvaluation | None = None
