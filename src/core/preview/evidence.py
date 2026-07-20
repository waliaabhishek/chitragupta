from __future__ import annotations

import json
import logging
import math
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Protocol, runtime_checkable

logger = logging.getLogger(__name__)


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


@dataclass(frozen=True)
class PreviewAllocationRunEvidence:
    ecosystem: str
    tenant_id: str
    tracking_date: date
    calculation_id: str
    calculation_completed_at: datetime
    capture_status: str
    capture_reason: str | None
    portion_count: int


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
