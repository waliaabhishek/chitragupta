from __future__ import annotations

import json
import logging
import math
import re
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from typing import Any, Protocol, runtime_checkable

logger = logging.getLogger(__name__)


class PreviewEvidenceBootstrapStatus(StrEnum):
    ALREADY_CURRENT = "already_current"
    BOOTSTRAPPED = "bootstrapped"
    UNAVAILABLE = "unavailable"


class PreviewEvidenceBootstrapReason(StrEnum):
    NO_LEGACY_EVIDENCE = "no_legacy_evidence"
    INVALID_LEGACY_EVIDENCE = "invalid_legacy_evidence"
    CONCURRENT_CHANGE = "concurrent_change"


class SourceAttemptStatus(StrEnum):
    PENDING = "pending"
    COMPLETE = "complete"
    FAILED = "failed"
    ABORTED = "aborted"


class SourceAttemptFinalStatus(StrEnum):
    COMPLETE = "complete"
    FAILED = "failed"
    ABORTED = "aborted"


class SourceAttemptFailureReason(StrEnum):
    ATTEMPT_BEGIN_FAILED = "attempt_begin_failed"
    CONSTRUCTION_FAILED = "construction_failed"
    PERSISTENCE_FAILED = "persistence_failed"
    CAPABILITY_UNAVAILABLE = "capability_unavailable"
    BOOTSTRAP_INVALID = "bootstrap_invalid"
    BOOTSTRAP_CONCURRENT_CHANGE = "bootstrap_concurrent_change"
    GENERIC_GATHER_FAILED = "generic_gather_failed"
    GENERIC_COMMIT_FAILED = "generic_commit_failed"


class PreviewSourceAttemptOrigin(StrEnum):
    ORDINARY = "ordinary"
    REPAIR = "repair"


_REPAIR_ATTEMPT_TOKEN = re.compile(r"repair:[^:]+:\d{4}-\d{2}-\d{2}\Z")


def source_attempt_origin(refresh_token: str) -> PreviewSourceAttemptOrigin:
    if _REPAIR_ATTEMPT_TOKEN.fullmatch(refresh_token) is not None:
        return PreviewSourceAttemptOrigin.REPAIR
    return PreviewSourceAttemptOrigin.ORDINARY


@dataclass(frozen=True)
class PreviewSourceAttempt:
    attempt_sequence: int
    ecosystem: str
    tenant_id: str
    refresh_token: str
    refresh_start: datetime
    refresh_end: datetime
    status: SourceAttemptStatus
    started_at: datetime
    completed_at: datetime | None
    failure_reason: SourceAttemptFailureReason | None

    def __post_init__(self) -> None:
        if not isinstance(self.status, SourceAttemptStatus):
            raise ValueError("invalid source attempt status")
        if self.failure_reason is not None and not isinstance(self.failure_reason, SourceAttemptFailureReason):
            raise ValueError("invalid source attempt failure reason")
        if self.attempt_sequence <= 0 or not all(
            value.strip() for value in (self.ecosystem, self.tenant_id, self.refresh_token)
        ):
            raise ValueError("invalid source attempt identity")
        if (
            not _aware(self.refresh_start)
            or not _aware(self.refresh_end)
            or self.refresh_start >= self.refresh_end
            or not _aware(self.started_at)
        ):
            raise ValueError("invalid source attempt times")
        if self.status is SourceAttemptStatus.PENDING:
            if self.completed_at is not None or self.failure_reason is not None:
                raise ValueError("pending source attempt has completion fields")
            return
        if self.completed_at is None or not _aware(self.completed_at) or self.completed_at < self.started_at:
            raise ValueError("terminal source attempt requires completion time")
        if self.status is SourceAttemptStatus.COMPLETE:
            if self.failure_reason is not None:
                raise ValueError("complete source attempt cannot have a reason")
            return
        failed_reasons = {
            SourceAttemptFailureReason.ATTEMPT_BEGIN_FAILED,
            SourceAttemptFailureReason.CONSTRUCTION_FAILED,
            SourceAttemptFailureReason.PERSISTENCE_FAILED,
            SourceAttemptFailureReason.CAPABILITY_UNAVAILABLE,
            SourceAttemptFailureReason.BOOTSTRAP_INVALID,
            SourceAttemptFailureReason.BOOTSTRAP_CONCURRENT_CHANGE,
        }
        aborted_reasons = {
            SourceAttemptFailureReason.GENERIC_GATHER_FAILED,
            SourceAttemptFailureReason.GENERIC_COMMIT_FAILED,
        }
        expected = failed_reasons if self.status is SourceAttemptStatus.FAILED else aborted_reasons
        if self.failure_reason not in expected:
            raise ValueError("source attempt status and reason do not match")


@dataclass(frozen=True)
class PreviewSourceAuthoritySlice:
    start: datetime
    end: datetime
    attempt: PreviewSourceAttempt | None

    def __post_init__(self) -> None:
        if not _aware(self.start) or not _aware(self.end) or self.start > self.end:
            raise ValueError("invalid source authority slice")


@dataclass(frozen=True)
class PreviewEvidenceBootstrapResult:
    status: PreviewEvidenceBootstrapStatus
    bootstrapped_windows: int
    bootstrapped_rows: int
    reason: PreviewEvidenceBootstrapReason | None

    def __post_init__(self) -> None:
        if not isinstance(self.status, PreviewEvidenceBootstrapStatus):
            raise ValueError("invalid Preview evidence bootstrap status")
        if self.reason is not None and not isinstance(self.reason, PreviewEvidenceBootstrapReason):
            raise ValueError("invalid Preview evidence bootstrap reason")
        if self.bootstrapped_windows < 0 or self.bootstrapped_rows < 0:
            raise ValueError("bootstrap counts must be nonnegative")
        valid = (
            (
                self.status is PreviewEvidenceBootstrapStatus.ALREADY_CURRENT
                and self.bootstrapped_windows == self.bootstrapped_rows == 0
                and self.reason is None
            )
            or (
                self.status is PreviewEvidenceBootstrapStatus.BOOTSTRAPPED
                and self.bootstrapped_windows > 0
                and self.bootstrapped_rows > 0
                and self.reason is None
            )
            or (
                self.status is PreviewEvidenceBootstrapStatus.UNAVAILABLE
                and self.bootstrapped_windows == self.bootstrapped_rows == 0
                and self.reason is not None
            )
        )
        if not valid:
            raise ValueError("invalid Preview evidence bootstrap result")


class AllocationLineageRunStatus(StrEnum):
    COMPLETE = "complete"
    UNAVAILABLE = "unavailable"


class AllocationLineageUnavailableReason(StrEnum):
    CAPTURE_FAILED = "capture_failed"
    PERSISTENCE_FAILED = "persistence_failed"


@dataclass(frozen=True)
class AllocationLineageRun:
    ecosystem: str
    tenant_id: str
    tracking_date: date
    calculation_id: str
    calculation_completed_at: datetime
    status: AllocationLineageRunStatus
    portion_count: int
    preview_portion_count: int | None = None

    def __post_init__(self) -> None:
        if not self.ecosystem.strip() or not self.tenant_id.strip() or not self.calculation_id.strip():
            raise ValueError("lineage identity must not be blank")
        if (
            not _aware(self.calculation_completed_at)
            or self.portion_count < 0
            or (self.preview_portion_count is not None and self.preview_portion_count < 0)
        ):
            raise ValueError("invalid lineage completion")
        if self.status is not AllocationLineageRunStatus.COMPLETE:
            raise ValueError("complete lineage run requires complete status")


@dataclass(frozen=True)
class AllocationLineageUnavailableRun:
    ecosystem: str
    tenant_id: str
    tracking_date: date
    calculation_id: str
    calculation_completed_at: datetime
    status: AllocationLineageRunStatus
    reason: AllocationLineageUnavailableReason
    portion_count: int = 0

    def __post_init__(self) -> None:
        if not self.ecosystem.strip() or not self.tenant_id.strip() or not self.calculation_id.strip():
            raise ValueError("lineage identity must not be blank")
        if not _aware(self.calculation_completed_at) or self.portion_count != 0:
            raise ValueError("invalid unavailable lineage completion")
        if self.status is not AllocationLineageRunStatus.UNAVAILABLE:
            raise ValueError("unavailable lineage run requires unavailable status")
        if not isinstance(self.reason, AllocationLineageUnavailableReason):
            raise ValueError("unavailable lineage run requires a closed reason")


@dataclass(frozen=True)
class PreviewSourceReadiness:
    ecosystem: str
    tenant_id: str
    window_start: datetime
    window_end: datetime
    capture_id: str
    captured_at: datetime
    source_count: int
    attempt_sequence: int

    def __post_init__(self) -> None:
        if not self.ecosystem.strip() or not self.tenant_id.strip() or not self.capture_id.strip():
            raise ValueError("source readiness identity must not be blank")
        if not _aware(self.window_start) or not _aware(self.window_end) or self.window_start >= self.window_end:
            raise ValueError("source readiness bounds must be aware and ordered")
        if not _aware(self.captured_at) or self.source_count < 0 or self.attempt_sequence <= 0:
            raise ValueError("invalid source readiness metadata")


class PreviewAllocationEvidenceDecodeError(ValueError):
    """Persisted lineage evidence does not satisfy the closed storage codec."""


def decode_lineage_decimal(value: str) -> Decimal:
    if not value or value != value.strip():
        raise PreviewAllocationEvidenceDecodeError("invalid lineage decimal")
    try:
        decoded = Decimal(value)
    except InvalidOperation as exc:
        raise PreviewAllocationEvidenceDecodeError("invalid lineage decimal") from exc
    if not decoded.is_finite() or str(decoded) != value:
        raise PreviewAllocationEvidenceDecodeError("invalid lineage decimal")
    return decoded


def _decode_lineage_metadata(value: Any) -> Any:
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise PreviewAllocationEvidenceDecodeError("invalid lineage metadata")
        return value
    if isinstance(value, list):
        return [_decode_lineage_metadata(item) for item in value]
    if isinstance(value, dict):
        if set(value) == {"decimal"}:
            decimal_value = value["decimal"]
            if not isinstance(decimal_value, str):
                raise PreviewAllocationEvidenceDecodeError("invalid lineage decimal tag")
            decode_lineage_decimal(decimal_value)
            return value
        if not all(isinstance(key, str) for key in value):
            raise PreviewAllocationEvidenceDecodeError("invalid lineage metadata")
        return {key: _decode_lineage_metadata(item) for key, item in value.items()}
    raise PreviewAllocationEvidenceDecodeError("invalid lineage metadata")


def decode_lineage_method_details(value: str, *, target_kind: str) -> dict[str, Any]:
    try:
        decoded = json.loads(value)
    except (TypeError, ValueError) as exc:
        raise PreviewAllocationEvidenceDecodeError("invalid lineage method details") from exc
    if (
        not isinstance(decoded, dict)
        or set(decoded) != {"allocation_detail", "metadata", "target_kind"}
        or not isinstance(decoded.get("metadata"), dict)
        or decoded.get("target_kind") != target_kind
    ):
        raise PreviewAllocationEvidenceDecodeError("invalid lineage method details")
    _decode_lineage_metadata(decoded)
    if json.dumps(decoded, sort_keys=True, separators=(",", ":"), ensure_ascii=False) != value:
        raise PreviewAllocationEvidenceDecodeError("noncanonical lineage method details")
    return decoded


def normalize_preview_source_economics(
    *,
    line_type: str | None,
    amount: Decimal | None,
    original_amount: Decimal | None,
    discount_amount: Decimal | None,
    price: Decimal | None,
    quantity: Decimal | None,
) -> tuple[Decimal, Decimal, Decimal, Decimal, Decimal]:
    """Return finite exact source economics using the Preview PROMO_CREDIT convention."""

    if line_type == "PROMO_CREDIT":
        price = Decimal(0) if price is None else price
        quantity = Decimal(0) if quantity is None else quantity
    values = (amount, original_amount, discount_amount, price, quantity)
    if any(value is None or not value.is_finite() for value in values):
        raise ValueError("source economics are incomplete or nonfinite")
    normalized_amount, normalized_original, normalized_discount, normalized_price, normalized_quantity = values
    assert normalized_amount is not None
    assert normalized_original is not None
    assert normalized_discount is not None
    assert normalized_price is not None
    assert normalized_quantity is not None
    if (
        line_type != "PROMO_CREDIT"
        and normalized_price * normalized_quantity != normalized_original
        or normalized_original - normalized_discount != normalized_amount
    ):
        raise ValueError("source economics do not reconcile")
    return (
        normalized_amount,
        normalized_original,
        normalized_discount,
        normalized_price,
        normalized_quantity,
    )


def _aware(value: datetime) -> bool:
    return value.tzinfo is not None and value.utcoffset() is not None


@dataclass(frozen=True)
class PreviewEvidenceScope:
    ecosystem: str
    tenant_id: str
    start: datetime
    end: datetime

    def __post_init__(self) -> None:
        if not _aware(self.start) or not _aware(self.end):
            raise ValueError("preview evidence bounds must be timezone-aware")
        if self.start >= self.end:
            raise ValueError("preview evidence start must be before end")


@dataclass(frozen=True)
class PreviewSourceEvidence:
    source_record_id: str
    identity_scheme: str
    provider_cost_id: str | None
    source_period_start: datetime | None
    source_period_end: datetime | None
    collection_window_start: datetime
    collection_window_end: datetime
    evidence_scope_start: datetime
    evidence_scope_end: datetime
    allocation_timestamp: datetime
    granularity: str | None
    native_product: str | None
    native_line_type: str | None
    amount: Decimal | None
    original_amount: Decimal | None
    discount_amount: Decimal | None
    price: Decimal | None
    quantity: Decimal | None
    unit: str | None
    native_description: str | None
    native_network_access_type: str | None
    resource_id: str | None
    resource_name: str | None
    environment_id: str | None
    native_tier_dimensions: tuple[tuple[str, str], ...]
    malformed: bool
    diagnostics: tuple[str, ...]
    billing_timestamp: datetime | None = None
    billing_env_id: str | None = None
    billing_resource_id: str | None = None
    billing_product_type: str | None = None
    billing_product_category: str | None = None
    capture_id: str | None = None
    ecosystem: str | None = None
    tenant_id: str | None = None
    retention_timestamp: datetime | None = None
    raw_payload_json: str | None = None


@dataclass(frozen=True)
class PreviewAggregateEvidence:
    timestamp: datetime
    environment_id: str
    resource_id: str
    native_product: str
    native_line_type: str
    quantity: Decimal
    unit_price: Decimal
    total_cost: Decimal
    compatibility_currency: str
    granularity: str
    source_record_id: str = ""
    evidence_scope_start: datetime | None = None
    evidence_scope_end: datetime | None = None
    compatibility_total_cost: Decimal | None = None
    compatibility_quantity: Decimal | None = None


@dataclass(frozen=True)
class PreviewAllocationEvidence:
    timestamp: datetime
    environment_id: str
    resource_id: str
    native_product: str
    native_line_type: str
    allocation_target_id: str
    allocation_method: str
    amount: Decimal
    calculation_id: str = ""
    portion_ordinal: int = 0
    target_kind: str = "identity"
    target_id: str | None = None
    allocated_cost: Decimal = Decimal(0)
    allocated_quantity: Decimal = Decimal(0)
    allocation_ratio: Decimal = Decimal(0)
    method_id: str = ""
    method_version: str = ""
    method_details_json: str = ""
    origin_total_cost: Decimal = Decimal(0)
    origin_quantity: Decimal = Decimal(0)
    origin_unit_price: Decimal = Decimal(0)
    origin_currency: str = ""
    origin_granularity: str = ""
    source_record_id: str = ""
    evidence_scope_start: datetime | None = None
    evidence_scope_end: datetime | None = None
    allocated_original_cost: Decimal = Decimal(0)
    origin_original_cost: Decimal = Decimal(0)
    compatibility_allocated_cost: Decimal | None = None
    compatibility_allocated_quantity: Decimal | None = None


@dataclass(frozen=True)
class PreviewAllocationRunEvidence:
    ecosystem: str
    tenant_id: str
    tracking_date: date
    calculation_id: str
    calculation_completed_at: datetime
    capture_status: AllocationLineageRunStatus
    capture_reason: AllocationLineageUnavailableReason | None
    portion_count: int
    preview_portion_count: int | None = None

    def __post_init__(self) -> None:
        if (
            not self.ecosystem.strip()
            or not self.tenant_id.strip()
            or not self.calculation_id.strip()
            or not _aware(self.calculation_completed_at)
            or self.portion_count < 0
            or (self.preview_portion_count is not None and self.preview_portion_count < 0)
        ):
            raise ValueError("invalid allocation lineage run evidence")
        if not isinstance(self.capture_status, AllocationLineageRunStatus):
            raise ValueError("invalid allocation lineage capture status")
        if self.capture_reason is not None and not isinstance(self.capture_reason, AllocationLineageUnavailableReason):
            raise ValueError("invalid allocation lineage capture reason")
        if self.capture_status is AllocationLineageRunStatus.COMPLETE:
            if self.capture_reason is not None:
                raise ValueError("complete allocation lineage cannot have a reason")
            return
        if self.capture_reason is None or self.portion_count != 0:
            raise ValueError("unavailable allocation lineage requires a reason and no portions")


@runtime_checkable
class PreviewCostEvidenceReader(Protocol):
    def iter_preview_sources(self, scope: PreviewEvidenceScope) -> Iterator[PreviewSourceEvidence]: ...

    def iter_preview_aggregates(self, scope: PreviewEvidenceScope) -> Iterator[PreviewAggregateEvidence]: ...

    def find_preview_source_candidates(self, scope: PreviewEvidenceScope) -> tuple[PreviewSourceEvidence, ...]: ...

    def find_preview_aggregate_candidates(
        self, scope: PreviewEvidenceScope, source: PreviewSourceEvidence
    ) -> tuple[PreviewAggregateEvidence, ...]: ...


@runtime_checkable
class PreviewAllocationEvidenceReader(Protocol):
    def find_preview_allocation_candidates(
        self, scope: PreviewEvidenceScope, source: PreviewSourceEvidence
    ) -> tuple[PreviewAllocationEvidence, ...]: ...

    def iter_preview_allocations(
        self,
        scope: PreviewEvidenceScope,
        calculation_ids: tuple[str, ...],
    ) -> Iterator[PreviewAllocationEvidence]: ...

    def iter_preview_allocation_runs(
        self,
        scope: PreviewEvidenceScope,
        calculation_ids: tuple[str, ...],
    ) -> Iterator[PreviewAllocationRunEvidence]: ...
