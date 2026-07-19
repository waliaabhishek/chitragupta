from __future__ import annotations

import csv
import hashlib
import io
import json
import logging
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from core.models.identity import Identity  # noqa: TC001  # resolved by get_type_hints contract test
from core.models.resource import Resource  # noqa: TC001  # resolved by get_type_hints contract test
from core.preview.evidence import (  # noqa: TC001  # resolved by get_type_hints contract test
    PreviewAggregateEvidence,
    PreviewAllocationEvidence,
    PreviewSourceEvidence,
)
from core.preview.models import PreviewArtifactPayload, PreviewPackagePayload, PreviewRequest, PreviewSourceSnapshot

logger = logging.getLogger(__name__)

MAPPING_PROFILE_VERSION = "focus-1.4-daily-full-tracer-v1"


class PreviewMappingError(ValueError):
    """Base error raised by the Daily Full mapping boundary."""


class PreviewTracerScopeError(PreviewMappingError):
    """Persisted evidence is outside the narrow positive tracer."""


class PreviewSourceSnapshotError(PreviewMappingError):
    """Persisted source authority is incomplete or invalid."""


class PreviewReconciliationError(PreviewMappingError):
    """Persisted source, aggregate, and allocation evidence disagree."""


FOCUS_1_4_FULL_COLUMNS = (
    "AllocatedMethodId",
    "AllocatedMethodDetails",
    "AllocatedResourceId",
    "AllocatedResourceName",
    "AllocatedTags",
    "AvailabilityZone",
    "BilledCost",
    "BillingAccountId",
    "BillingAccountName",
    "BillingAccountType",
    "BillingCurrency",
    "BillingPeriodEnd",
    "BillingPeriodStart",
    "CapacityReservationId",
    "CapacityReservationStatus",
    "ChargeCategory",
    "ChargeClass",
    "ChargeDescription",
    "ChargeFrequency",
    "ChargePeriodEnd",
    "ChargePeriodStart",
    "CommitmentDiscountCategory",
    "CommitmentDiscountId",
    "CommitmentDiscountName",
    "CommitmentDiscountQuantity",
    "CommitmentDiscountStatus",
    "CommitmentDiscountType",
    "CommitmentDiscountUnit",
    "CommitmentProgramEligibilityDetails",
    "ConsumedQuantity",
    "ConsumedUnit",
    "ContractApplied",
    "ContractedCost",
    "ContractedUnitPrice",
    "EffectiveCost",
    "HostProviderName",
    "InvoiceDetailId",
    "InvoiceId",
    "InvoiceIssuerName",
    "ListCost",
    "ListUnitPrice",
    "PricingCategory",
    "PricingCurrency",
    "PricingCurrencyContractedUnitPrice",
    "PricingCurrencyEffectiveCost",
    "PricingCurrencyListUnitPrice",
    "PricingQuantity",
    "PricingUnit",
    "RegionId",
    "RegionName",
    "ResourceId",
    "ResourceName",
    "ResourceType",
    "ServiceProviderName",
    "ServiceCategory",
    "ServiceName",
    "ServiceSubcategory",
    "SkuId",
    "SkuMeter",
    "SkuPriceDetails",
    "SkuPriceId",
    "SubAccountId",
    "SubAccountName",
    "SubAccountType",
    "Tags",
)

CUSTOM_EVIDENCE_COLUMNS = (
    "x_ChitraguptaSourceCostId",
    "x_ChitraguptaBillingScopeId",
    "x_ChitraguptaAllocationRatio",
    "x_ChitraguptaAllocationMethodVersion",
    "x_ChitraguptaMappingProfileVersion",
    "x_ChitraguptaSkuComponents",
    "x_ConfluentProduct",
    "x_ConfluentLineType",
    "x_ConfluentDescription",
    "x_ConfluentDiscountAmount",
    "x_ConfluentNetworkAccessType",
    "x_ConfluentTierDimensions",
)

ORDINARY_METERED_LINE_TYPES = frozenset(
    {
        "KAFKA_STORAGE",
        "KAFKA_PARTITION",
        "KAFKA_NETWORK_READ",
        "KAFKA_NETWORK_WRITE",
        "KAFKA_BASE",
        "KAFKA_NUM_CKUS",
        "KAFKA_REST_PRODUCE",
        "KSQL_NUM_CSUS",
        "CONNECT_CAPACITY",
        "CONNECT_NUM_TASKS",
        "CONNECT_THROUGHPUT",
        "CONNECT_NUM_RECORDS",
        "CLUSTER_LINKING_PER_LINK",
        "CLUSTER_LINKING_WRITE",
        "CLUSTER_LINKING_READ",
        "AUDIT_LOG_READ",
        "GOVERNANCE_BASE",
        "SCHEMA_REGISTRY",
        "CUSTOM_CONNECT_NUM_TASKS",
        "CUSTOM_CONNECT_THROUGHPUT",
        "NUM_RULES",
        "FLINK_NUM_CFUS",
        "TABLEFLOW_DATA_PROCESSED",
        "TABLEFLOW_NUM_TOPICS",
        "TABLEFLOW_STORAGE",
        "USM_CONNECTED_NODE",
    }
)


@dataclass(frozen=True)
class KnownGap:
    code: str
    description: str
    owner_task: str
    columns: tuple[str, ...]


KNOWN_GAPS = (
    KnownGap(
        "billing_account_and_issuer_mapping_pending",
        "Billing account and issuer mapping is pending.",
        "TASK-254.04",
        (
            "BillingAccountId",
            "BillingAccountName",
            "BillingAccountType",
            "InvoiceIssuerName",
            "x_ChitraguptaBillingScopeId",
        ),
    ),
    KnownGap(
        "billing_period_authority_pending",
        "Authoritative provider billing-period mapping is pending.",
        "TASK-254.04",
        ("BillingPeriodEnd", "BillingPeriodStart"),
    ),
    KnownGap(
        "commercial_arrangement_and_billing_currency_authority_pending",
        "Commercial arrangement and authoritative billing currency are unavailable.",
        "TASK-254.03",
        ("BillingCurrency",),
    ),
    KnownGap(
        "provider_authoritative_sku_identity_unavailable",
        "Provider-authoritative SKU identity is unavailable.",
        "TASK-254.04",
        ("SkuId", "SkuMeter", "SkuPriceDetails", "SkuPriceId", "x_ChitraguptaSkuComponents"),
    ),
    KnownGap(
        "invoice_identity_unavailable",
        "Post-issuance invoice identity is unavailable.",
        "TASK-254.04",
        ("InvoiceDetailId", "InvoiceId"),
    ),
    KnownGap(
        "allocation_lineage_and_tag_projection_pending",
        "Allocation lineage and tag projection are pending.",
        "TASK-254.05",
        (
            "AllocatedMethodDetails",
            "AllocatedTags",
            "Tags",
            "x_ChitraguptaAllocationRatio",
            "x_ChitraguptaAllocationMethodVersion",
        ),
    ),
    KnownGap(
        "task_254_04_applicability_and_provider_mapping_pending",
        "Provider applicability and mapping are pending.",
        "TASK-254.04",
        (
            "AvailabilityZone",
            "CapacityReservationId",
            "CapacityReservationStatus",
            "ConsumedQuantity",
            "ConsumedUnit",
            "ContractApplied",
            "ContractedUnitPrice",
            "HostProviderName",
            "ListUnitPrice",
            "PricingCategory",
            "PricingCurrency",
            "PricingCurrencyContractedUnitPrice",
            "PricingCurrencyEffectiveCost",
            "PricingCurrencyListUnitPrice",
            "PricingQuantity",
            "PricingUnit",
            "RegionId",
            "RegionName",
            "ServiceCategory",
            "ServiceSubcategory",
            "SubAccountType",
        ),
    ),
)

PROFILE_NOT_APPLICABLE_COLUMNS = (
    "ChargeClass",
    "CommitmentDiscountCategory",
    "CommitmentDiscountId",
    "CommitmentDiscountName",
    "CommitmentDiscountQuantity",
    "CommitmentDiscountStatus",
    "CommitmentDiscountType",
    "CommitmentDiscountUnit",
    "CommitmentProgramEligibilityDetails",
)

_GAP_COLUMNS = frozenset(column for gap in KNOWN_GAPS for column in gap.columns)
MAPPED_COLUMNS = tuple(
    column
    for column in (*FOCUS_1_4_FULL_COLUMNS, *CUSTOM_EVIDENCE_COLUMNS)
    if column not in _GAP_COLUMNS and column not in PROFILE_NOT_APPLICABLE_COLUMNS
)


def _utc(value: datetime) -> str:
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _decimal(value: Decimal) -> str:
    result = format(value, "f")
    if "." in result:
        result = result.rstrip("0").rstrip(".")
    return result or "0"


_SEMANTIC_PREFIXES = ("support", "promo", "credit", "refund", "adjust", "correct", "revers", "rebate")


def _has_rejected_semantics(*values: str) -> bool:
    for value in values:
        tokens = re.findall(r"[a-z0-9]+", value.casefold())
        if any(token.startswith(_SEMANTIC_PREFIXES) or token == "trueup" for token in tokens):
            return True
        if any(pair in {("prior", "period"), ("true", "up")} for pair in zip(tokens, tokens[1:], strict=False)):
            return True
    return False


def _finite(value: Decimal | None) -> bool:
    return value is not None and value.is_finite()


def source_through(source: PreviewSourceEvidence) -> datetime:
    start = source.collection_window_start
    end = source.collection_window_end
    if (
        start.tzinfo is None
        or start.utcoffset() is None
        or end.tzinfo is None
        or end.utcoffset() is None
        or start >= end
    ):
        raise PreviewSourceSnapshotError("persisted collection window is invalid")
    return end.astimezone(UTC)


def validate_daily_full_mapping(
    *,
    request_start: datetime,
    request_end: datetime,
    source: PreviewSourceEvidence,
    aggregate: PreviewAggregateEvidence,
    allocation: PreviewAllocationEvidence,
) -> None:
    validate_daily_full_source(request_start=request_start, request_end=request_end, source=source)

    aggregate_values = (aggregate.total_cost, aggregate.quantity, aggregate.unit_price, allocation.amount)
    if not all(_finite(value) for value in aggregate_values) or not (
        aggregate.total_cost > 0 and aggregate.quantity > 0 and aggregate.unit_price > 0 and allocation.amount > 0
    ):
        raise PreviewTracerScopeError("aggregate or allocation economics are outside the positive tracer")

    source_origin = (
        source.allocation_timestamp,
        source.environment_id,
        source.resource_id,
        source.native_product,
        source.native_line_type,
    )
    aggregate_origin = (
        aggregate.timestamp,
        aggregate.environment_id,
        aggregate.resource_id,
        aggregate.native_product,
        aggregate.native_line_type,
    )
    allocation_origin = (
        allocation.timestamp,
        allocation.environment_id,
        allocation.resource_id,
        allocation.native_product,
        allocation.native_line_type,
    )
    if source_origin != aggregate_origin or source_origin != allocation_origin:
        raise PreviewReconciliationError("persisted evidence origins do not match")
    if (
        aggregate.compatibility_currency != "USD"
        or aggregate.total_cost != source.amount
        or aggregate.quantity != source.quantity
        or aggregate.unit_price != source.price
        or allocation.amount != source.amount
    ):
        raise PreviewReconciliationError("persisted evidence arithmetic does not reconcile")
    if allocation.allocation_target_id == "UNALLOCATED":
        raise PreviewTracerScopeError("unallocated output is outside the Daily Full tracer")


def validate_daily_full_source(
    *, request_start: datetime, request_end: datetime, source: PreviewSourceEvidence
) -> None:
    if source.malformed or source.diagnostics or source.source_period_start is None or source.source_period_end is None:
        raise PreviewSourceSnapshotError("persisted source evidence is incomplete")
    if not (request_start <= source.source_period_start < source.source_period_end <= request_end):
        raise PreviewTracerScopeError("source period is outside the requested Daily range")

    required = (
        source.provider_cost_id,
        source.native_product,
        source.native_line_type,
        source.unit,
        source.native_description,
        source.resource_id,
        source.environment_id,
    )
    if (
        any(not value for value in required)
        or source.native_line_type not in ORDINARY_METERED_LINE_TYPES
        or _has_rejected_semantics(source.native_product or "", source.native_description or "")
    ):
        raise PreviewTracerScopeError("source semantics are outside the positive tracer")

    source_values = (
        source.amount,
        source.original_amount,
        source.discount_amount,
        source.price,
        source.quantity,
    )
    if not all(_finite(value) for value in source_values):
        raise PreviewTracerScopeError("source economics must be finite")
    assert source.amount is not None
    assert source.original_amount is not None
    assert source.discount_amount is not None
    assert source.price is not None
    assert source.quantity is not None
    if not (
        source.amount > 0
        and source.original_amount > 0
        and source.price > 0
        and source.quantity > 0
        and source.discount_amount >= 0
    ):
        raise PreviewTracerScopeError("source economics are outside the positive tracer")
    if (
        source.original_amount - source.discount_amount != source.amount
        or source.price * source.quantity != source.original_amount
    ):
        raise PreviewReconciliationError("source arithmetic does not reconcile")

    source_through(source)


def build_daily_full_package(
    *,
    request: PreviewRequest,
    snapshot: PreviewSourceSnapshot,
    source: PreviewSourceEvidence,
    aggregate: PreviewAggregateEvidence,
    allocation: PreviewAllocationEvidence,
    resource: Resource,
    identity: Identity,
    environment: Resource | None,
    generated_at: datetime,
) -> PreviewPackagePayload:
    request_start = datetime.combine(request.start_date, datetime.min.time(), tzinfo=UTC)
    request_end = datetime.combine(request.end_date, datetime.min.time(), tzinfo=UTC)
    validate_daily_full_mapping(
        request_start=request_start,
        request_end=request_end,
        source=source,
        aggregate=aggregate,
        allocation=allocation,
    )
    row: dict[str, Any] = {column: None for column in (*FOCUS_1_4_FULL_COLUMNS, *CUSTOM_EVIDENCE_COLUMNS)}
    row.update(
        {
            "AllocatedMethodId": allocation.allocation_method,
            "AllocatedResourceId": allocation.allocation_target_id,
            "AllocatedResourceName": identity.display_name,
            "BilledCost": source.amount,
            "ChargeCategory": "Usage",
            "ChargeDescription": source.native_description,
            "ChargeFrequency": "Usage-Based",
            "ChargePeriodEnd": source.source_period_end,
            "ChargePeriodStart": source.source_period_start,
            "ContractedCost": source.original_amount,
            "EffectiveCost": allocation.amount,
            "ListCost": source.original_amount,
            "ResourceId": source.resource_id,
            "ResourceName": resource.display_name or source.resource_name,
            "ResourceType": resource.resource_type,
            "ServiceProviderName": "Confluent Cloud",
            "ServiceName": source.native_product,
            "SubAccountId": source.environment_id,
            "SubAccountName": environment.display_name if environment is not None else None,
            "x_ChitraguptaSourceCostId": source.provider_cost_id or source.source_record_id,
            "x_ChitraguptaMappingProfileVersion": MAPPING_PROFILE_VERSION,
            "x_ConfluentProduct": source.native_product,
            "x_ConfluentLineType": source.native_line_type,
            "x_ConfluentDescription": source.native_description,
            "x_ConfluentDiscountAmount": source.discount_amount,
            "x_ConfluentNetworkAccessType": source.native_network_access_type,
            "x_ConfluentTierDimensions": json.dumps(
                dict(source.native_tier_dimensions), sort_keys=True, separators=(",", ":")
            ),
        }
    )
    buffer = io.StringIO(newline="")
    writer = csv.writer(buffer, lineterminator="\n")
    columns = (*FOCUS_1_4_FULL_COLUMNS, *CUSTOM_EVIDENCE_COLUMNS)
    writer.writerow(columns)
    writer.writerow(
        [
            ""
            if row[column] is None
            else _utc(row[column])
            if isinstance(row[column], datetime)
            else _decimal(row[column])
            if isinstance(row[column], Decimal)
            else str(row[column])
            for column in columns
        ]
    )
    csv_body = buffer.getvalue().encode("utf-8")
    file_metadata = {
        "name": "cost-and-usage.csv",
        "media_type": "text/csv",
        "size_bytes": len(csv_body),
        "sha256": hashlib.sha256(csv_body).hexdigest(),
        "order": 1,
    }
    source_snapshot = {
        "calculation_timestamp": _utc(snapshot.calculation_timestamp),
        "calculation_coverage": [
            {
                "tracking_date": entry.tracking_date.isoformat(),
                "calculation_id": entry.calculation_id,
                "calculation_completed_at": _utc(entry.calculation_completed_at),
                "calculation_run_id": entry.calculation_run_id,
            }
            for entry in snapshot.calculation_coverage
        ],
        "source_through": _utc(snapshot.source_through),
    }
    manifest = {
        "schema_version": "chitragupta.preview-manifest.v1",
        "package_type": "requested_preview_package",
        "request_id": request.request_id,
        "tenant_name": request.tenant_name,
        "grain": request.grain,
        "start_date": request.start_date.isoformat(),
        "end_date": request.end_date.isoformat(),
        "column_profile": request.column_profile,
        "target_focus_version": "1.4",
        "conformance_status": "non_conforming",
        "mapping_profile_version": MAPPING_PROFILE_VERSION,
        "known_gaps": [
            {
                "code": gap.code,
                "description": gap.description,
                "owner_task": gap.owner_task,
                "columns": list(gap.columns),
            }
            for gap in KNOWN_GAPS
        ],
        "profile_not_applicable_columns": list(PROFILE_NOT_APPLICABLE_COLUMNS),
        "source_snapshot": source_snapshot,
        "validation": {"status": "passed", "source_records": 1, "rows": 1},
        "reconciliation": {
            "source_cost": _decimal(source.amount or Decimal(0)),
            "allocated_cost": _decimal(allocation.amount),
            "difference": _decimal((source.amount or Decimal(0)) - allocation.amount),
        },
        "generated_at": _utc(generated_at),
        "files": [file_metadata],
    }
    manifest_body = (json.dumps(manifest, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
    return PreviewPackagePayload(
        manifest_body=manifest_body,
        data_files=(PreviewArtifactPayload("cost-and-usage.csv", "text/csv", 1, csv_body),),
    )
