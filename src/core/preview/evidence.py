from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Protocol, runtime_checkable

logger = logging.getLogger(__name__)


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


@runtime_checkable
class PreviewCostEvidenceReader(Protocol):
    def find_preview_source_candidates(self, scope: PreviewEvidenceScope) -> tuple[PreviewSourceEvidence, ...]: ...

    def find_preview_aggregate_candidates(
        self, scope: PreviewEvidenceScope, source: PreviewSourceEvidence
    ) -> tuple[PreviewAggregateEvidence, ...]: ...


@runtime_checkable
class PreviewAllocationEvidenceReader(Protocol):
    def find_preview_allocation_candidates(
        self, scope: PreviewEvidenceScope, source: PreviewSourceEvidence
    ) -> tuple[PreviewAllocationEvidence, ...]: ...
