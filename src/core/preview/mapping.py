from __future__ import annotations

import csv
import hashlib
import io
import json
import logging
import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from decimal import Context, Decimal, localcontext
from enum import StrEnum
from itertools import zip_longest
from pathlib import Path
from types import MappingProxyType
from typing import Literal, cast

import core.preview.models as preview_models
from core.models.identity import Identity  # noqa: TC001 - resolved by contract tests
from core.models.resource import Resource  # noqa: TC001 - resolved by contract tests
from core.preview.evidence import (  # noqa: TC001 - resolved by contract tests
    AllocationLineageRunStatus,
    PreviewAggregateEvidence,
    PreviewAllocationEvidence,
    PreviewAllocationRunEvidence,
    PreviewSourceEvidence,
    decode_lineage_method_details,
    normalize_preview_source_economics,
)
from core.preview.models import (
    PreviewArtifactMetadata,
    PreviewArtifactPayload,
    PreviewColumnProfile,
    PreviewMonthlyStatus,
    PreviewRequest,
    PreviewRequestStatus,
    PreviewSourceSnapshot,
    preview_month,
    validate_preview_request_snapshot,
)
from core.preview.spooling import (
    SQLITE_BATCH_SIZE,
    PreviewGenerationWorkspace,
    PreviewSpooledArtifactCollection,
    PreviewSpooledBody,
    spooled_body_metadata,
)
from core.storage.interface import ResourceRepository  # noqa: TC001 - resolved by contract tests

MAPPING_PROFILE_VERSION = "focus-1.4-preview-v1"
PREVIEW_MANIFEST_SCHEMA_VERSION = "chitragupta.preview-manifest.v1"
PREVIEW_DECIMAL_CONTEXT = Context(prec=38)
_DECIMAL_CONTEXT = PREVIEW_DECIMAL_CONTEXT
logger = logging.getLogger(__name__)


class PreviewMappingError(ValueError):
    """Base error raised by the Daily Full mapping boundary."""


class PreviewCsvFileSizeError(PreviewMappingError):
    """A complete canonical CSV record cannot fit in the configured part size."""


class PreviewEffectiveColumnsError(PreviewMappingError):
    """The persisted or requested profile/column combination is invalid."""


class PreviewSourceEvidenceError(PreviewMappingError):
    """Persisted source authority is incomplete or invalid."""


class PreviewFinancialUnsupportedError(PreviewMappingError):
    """Source economics are not supported by this profile."""


class PreviewFinancialReconciliationError(PreviewMappingError):
    """Persisted source, aggregate, or allocation evidence does not reconcile."""


class PreviewMappingScopeError(PreviewMappingError):
    """Evidence exceeds the current single-allocation mapping scope."""


class PreviewBillingAccountUnavailableError(PreviewMappingError):
    """Provider-issued billing-account evidence is unavailable."""


class PreviewBillingAccountConflictError(PreviewMappingError):
    """Provider-issued billing-account evidence conflicts."""


class PreviewProviderContextIncompleteError(PreviewMappingError):
    """Provider resource context is missing or incompatible."""


class PreviewAllocationLineageError(PreviewMappingError):
    """Persisted allocation lineage is incomplete or structurally invalid."""


class PreviewSourceCoverageError(PreviewMappingError):
    """Accepted source evidence does not cover the persisted billing origins."""


class PreviewBillingCurrencyUnknownError(PreviewMappingError):
    def __init__(self, source_record_ids: tuple[str, ...]) -> None:
        self.source_record_ids = source_record_ids
        super().__init__("persisted billing currency is unknown")


class PreviewBillingCurrencyUnsupportedError(PreviewMappingError):
    def __init__(self, source_record_ids: tuple[str, ...]) -> None:
        self.source_record_ids = source_record_ids
        super().__init__("persisted billing currency is unsupported")


class PreviewSourceAggregateReconciliationError(PreviewFinancialReconciliationError):
    def __init__(self, source_record_ids: tuple[str, ...]) -> None:
        self.source_record_ids = source_record_ids
        super().__init__("persisted source and aggregate evidence do not reconcile")


class PreviewProfileDefinitionError(RuntimeError):
    """The executable mapping profile is internally inconsistent."""


class PreviewRowRuleId(StrEnum):
    COLUMN_COUNT = "column_count"
    NULLABILITY = "nullability"
    APPLICABILITY = "applicability"
    TYPE = "type"
    ALLOWED_VALUE = "allowed_value"
    DEPENDENT_FIELDS = "dependent_fields"
    FINANCIAL_PROJECTION = "financial_projection"
    PERIOD_CONTAINMENT = "period_containment"
    DERIVED_KEY = "derived_key"
    INVOICE_SEPARATION = "invoice_separation"
    GAP_COVERAGE = "gap_coverage"


class PreviewRowValidationError(PreviewMappingError):
    def __init__(self, rule_id: PreviewRowRuleId, *, column: str | None = None) -> None:
        self.rule_id = rule_id
        self.column = column
        super().__init__(rule_id.value if column is None else f"{rule_id.value}:{column}")


# Stable exception names used across Preview mapping callers.
PreviewTracerScopeError = PreviewMappingScopeError
PreviewSourceSnapshotError = PreviewSourceEvidenceError
PreviewReconciliationError = PreviewFinancialReconciliationError


class PreviewSourceIssue(StrEnum):
    RECORD_MALFORMED = "preview_source_record_malformed"
    SCOPE_UNSUPPORTED = "preview_source_scope_unsupported"
    CHARGE_CLASSIFICATION_AMBIGUOUS = "preview_charge_classification_ambiguous"
    LINE_TYPE_UNKNOWN = "preview_source_line_type_unknown"
    LINE_TYPE_UNSUPPORTED = "preview_source_line_type_unsupported"
    MAPPING_UNAVAILABLE = "preview_source_mapping_unavailable"
    RECORD_INCOMPLETE = "preview_source_record_incomplete"
    ECONOMICS_UNSUPPORTED = "preview_source_economics_unsupported"
    RECONCILIATION_FAILED = "preview_source_reconciliation_failed"


class FocusFeatureLevel(StrEnum):
    MANDATORY = "mandatory"
    CONDITIONAL = "conditional"
    RECOMMENDED = "recommended"


class PreviewApplicability(StrEnum):
    APPLICABLE = "applicable"
    NOT_APPLICABLE = "not_applicable"
    DEFERRED = "deferred"
    DECLARED_GAP = "declared_gap"


class PreviewValidatorKind(StrEnum):
    DECIMAL = "decimal"
    DATETIME = "datetime"
    ENUM = "enum"
    IDENTIFIER = "identifier"
    JSON = "json"
    TEXT = "text"


@dataclass(frozen=True)
class FocusColumnRule:
    column: str
    feature_level: FocusFeatureLevel
    allows_null: bool
    applicability: PreviewApplicability
    source: str
    transformation: str
    allowed_values: tuple[str, ...] | None
    validator: PreviewValidatorKind
    gap_code: str | None = None
    owner_task: str | None = None


@dataclass(frozen=True)
class CustomEvidenceRule:
    column: str
    allows_null: bool
    applicability: PreviewApplicability
    source: str
    transformation: str
    validator: PreviewValidatorKind
    allowed_values: tuple[str, ...] | None = None
    gap_code: str | None = None
    owner_task: str | None = None


_COLUMN_SPECS = (
    ("AllocatedMethodId", "C", True),
    ("AllocatedMethodDetails", "R", True),
    ("AllocatedResourceId", "C", True),
    ("AllocatedResourceName", "C", True),
    ("AllocatedTags", "C", True),
    ("AvailabilityZone", "R", True),
    ("BilledCost", "M", False),
    ("BillingAccountId", "M", False),
    ("BillingAccountName", "M", True),
    ("BillingAccountType", "C", False),
    ("BillingCurrency", "M", False),
    ("BillingPeriodEnd", "M", False),
    ("BillingPeriodStart", "M", False),
    ("CapacityReservationId", "C", True),
    ("CapacityReservationStatus", "C", True),
    ("ChargeCategory", "M", False),
    ("ChargeClass", "M", True),
    ("ChargeDescription", "M", True),
    ("ChargeFrequency", "R", False),
    ("ChargePeriodEnd", "M", False),
    ("ChargePeriodStart", "M", False),
    ("CommitmentDiscountCategory", "C", True),
    ("CommitmentDiscountId", "C", True),
    ("CommitmentDiscountName", "C", True),
    ("CommitmentDiscountQuantity", "C", True),
    ("CommitmentDiscountStatus", "C", True),
    ("CommitmentDiscountType", "C", True),
    ("CommitmentDiscountUnit", "C", True),
    ("CommitmentProgramEligibilityDetails", "C", True),
    ("ConsumedQuantity", "C", True),
    ("ConsumedUnit", "C", True),
    ("ContractApplied", "C", True),
    ("ContractedCost", "M", False),
    ("ContractedUnitPrice", "C", True),
    ("EffectiveCost", "M", False),
    ("HostProviderName", "M", True),
    ("InvoiceDetailId", "C", True),
    ("InvoiceId", "C", True),
    ("InvoiceIssuerName", "M", False),
    ("ListCost", "M", False),
    ("ListUnitPrice", "C", True),
    ("PricingCategory", "C", True),
    ("PricingCurrency", "C", False),
    ("PricingCurrencyContractedUnitPrice", "C", True),
    ("PricingCurrencyEffectiveCost", "C", False),
    ("PricingCurrencyListUnitPrice", "C", True),
    ("PricingQuantity", "M", True),
    ("PricingUnit", "M", True),
    ("RegionId", "C", True),
    ("RegionName", "C", True),
    ("ResourceId", "C", True),
    ("ResourceName", "C", True),
    ("ResourceType", "C", True),
    ("ServiceProviderName", "M", False),
    ("ServiceCategory", "M", False),
    ("ServiceName", "M", False),
    ("ServiceSubcategory", "R", False),
    ("SkuId", "C", True),
    ("SkuMeter", "C", True),
    ("SkuPriceDetails", "C", True),
    ("SkuPriceId", "C", True),
    ("SubAccountId", "C", True),
    ("SubAccountName", "C", True),
    ("SubAccountType", "C", True),
    ("Tags", "C", True),
)

_TARGET_RULE_AUTHORITIES = {
    "AllocatedMethodId": ("allocation.allocation_method", "copy the current allocation method"),
    "AllocatedMethodDetails": ("persisted allocation lineage", "copy canonical method details"),
    "AllocatedResourceId": ("allocation.allocation_target_id", "copy the allocation target identifier"),
    "AllocatedResourceName": ("allocation target identity", "copy its display name"),
    "AllocatedTags": ("allocation target tags", "serialize separately as canonical JSON"),
    "AvailabilityZone": ("none", "not applicable to retained Direct PAYG evidence"),
    "BilledCost": ("source.amount and allocation.amount", "copy the exactly reconciled allocated share"),
    "BillingAccountId": ("bound provider organization resource", "copy the provider organization identifier"),
    "BillingAccountName": ("bound provider organization resource", "copy its optional display name"),
    "BillingAccountType": ("mapping profile", "emit Organization"),
    "BillingCurrency": ("none", "remain null under the TASK-254.03 provider-field gap"),
    "BillingPeriodEnd": ("source.source_period_start", "derive the exclusive next UTC month boundary"),
    "BillingPeriodStart": ("source.source_period_start", "derive the inclusive UTC month boundary"),
    "CapacityReservationId": ("none", "not applicable to Direct PAYG"),
    "CapacityReservationStatus": ("none", "not applicable to Direct PAYG"),
    "ChargeCategory": ("typed charge semantics", "copy the closed category"),
    "ChargeClass": ("none", "remain null because corrections are ineligible"),
    "ChargeDescription": ("source.native_description", "copy losslessly"),
    "ChargeFrequency": ("typed charge semantics", "copy the closed frequency"),
    "ChargePeriodEnd": ("source.source_period_end", "copy the native exclusive boundary"),
    "ChargePeriodStart": ("source.source_period_start", "copy the native inclusive boundary"),
    "CommitmentDiscountCategory": ("none", "not applicable to Direct PAYG"),
    "CommitmentDiscountId": ("none", "not applicable to Direct PAYG"),
    "CommitmentDiscountName": ("none", "not applicable to Direct PAYG"),
    "CommitmentDiscountQuantity": ("none", "not applicable to Direct PAYG"),
    "CommitmentDiscountStatus": ("none", "not applicable to Direct PAYG"),
    "CommitmentDiscountType": ("none", "not applicable to Direct PAYG"),
    "CommitmentDiscountUnit": ("none", "not applicable to Direct PAYG"),
    "CommitmentProgramEligibilityDetails": ("none", "not applicable to Direct PAYG"),
    "ConsumedQuantity": ("financial projection", "copy metered native quantity when consumption is emitted"),
    "ConsumedUnit": ("financial projection", "copy the matching normalized native unit"),
    "ContractApplied": ("none", "not applicable to Direct PAYG"),
    "ContractedCost": ("financial projection", "copy allocated original_amount"),
    "ContractedUnitPrice": ("none", "not applicable without negotiated pricing"),
    "EffectiveCost": ("financial projection", "copy the reconciled allocation amount"),
    "HostProviderName": ("resource.metadata.provider_cloud", "copy the raw provider cloud code unchanged"),
    "InvoiceDetailId": ("none", "remain null under the invoice identity gap"),
    "InvoiceId": ("none", "remain null under the invoice identity gap"),
    "InvoiceIssuerName": ("none", "remain null under the invoice issuer gap"),
    "ListCost": ("financial projection", "copy allocated original_amount"),
    "ListUnitPrice": ("financial projection", "copy native price after exact arithmetic"),
    "PricingCategory": ("mapping profile", "emit Standard when SKU pricing is emitted"),
    "PricingCurrency": ("configured pricing contract", "emit USD without conversion"),
    "PricingCurrencyContractedUnitPrice": ("none", "not applicable without negotiated pricing"),
    "PricingCurrencyEffectiveCost": ("financial projection", "copy EffectiveCost in USD"),
    "PricingCurrencyListUnitPrice": ("financial projection", "copy ListUnitPrice in USD"),
    "PricingQuantity": ("financial projection", "copy native quantity when SKU pricing is emitted"),
    "PricingUnit": ("financial projection", "copy the matching normalized native unit"),
    "RegionId": ("resource.metadata.provider_region", "copy the raw provider region unchanged"),
    "RegionName": ("none", "remain null under the provider region display-name gap"),
    "ResourceId": ("origin provider resource", "copy its provider identifier"),
    "ResourceName": ("origin provider resource then source", "copy inventory display name with native fallback"),
    "ResourceType": ("origin provider resource", "copy its concrete resource type"),
    "ServiceProviderName": ("mapping profile", "emit Confluent Cloud"),
    "ServiceCategory": ("versioned service rule", "copy its FOCUS category"),
    "ServiceName": ("versioned service rule", "copy its service name"),
    "ServiceSubcategory": ("versioned service rule", "copy its FOCUS subcategory"),
    "SkuId": ("canonical native product and line type", "derive the namespaced v1 SHA-256 key"),
    "SkuMeter": ("financial projection", "copy PricingUnit when SKU pricing is emitted"),
    "SkuPriceDetails": ("canonical SKU price components", "serialize as canonical JSON"),
    "SkuPriceId": ("canonical SKU price components", "derive the namespaced v1 SHA-256 key"),
    "SubAccountId": ("source.environment_id", "copy the native Confluent environment identifier"),
    "SubAccountName": ("environment inventory", "copy its display name"),
    "SubAccountType": ("mapping profile", "emit Environment when SubAccountId is present"),
    "Tags": ("origin resource tags", "serialize separately as canonical JSON"),
}

_FEATURE_LEVEL = {
    "M": FocusFeatureLevel.MANDATORY,
    "C": FocusFeatureLevel.CONDITIONAL,
    "R": FocusFeatureLevel.RECOMMENDED,
}
_DECIMAL_COLUMNS = frozenset(
    {
        "BilledCost",
        "CommitmentDiscountQuantity",
        "ConsumedQuantity",
        "ContractedCost",
        "ContractedUnitPrice",
        "EffectiveCost",
        "ListCost",
        "ListUnitPrice",
        "PricingCurrencyContractedUnitPrice",
        "PricingCurrencyEffectiveCost",
        "PricingCurrencyListUnitPrice",
        "PricingQuantity",
    }
)
_DATETIME_COLUMNS = frozenset({"BillingPeriodEnd", "BillingPeriodStart", "ChargePeriodEnd", "ChargePeriodStart"})
_ENUM_VALUES = {
    "BillingAccountType": ("Organization",),
    "ChargeCategory": ("Usage", "Purchase", "Credit"),
    "ChargeClass": ("Correction",),
    "ChargeFrequency": ("Usage-Based", "Recurring", "One-Time"),
    "PricingCategory": ("Standard",),
    "PricingCurrency": ("USD",),
    "SubAccountType": ("Environment",),
}
_NOT_APPLICABLE = frozenset(
    {
        "AvailabilityZone",
        "CapacityReservationId",
        "CapacityReservationStatus",
        "ChargeClass",
        "CommitmentDiscountCategory",
        "CommitmentDiscountId",
        "CommitmentDiscountName",
        "CommitmentDiscountQuantity",
        "CommitmentDiscountStatus",
        "CommitmentDiscountType",
        "CommitmentDiscountUnit",
        "CommitmentProgramEligibilityDetails",
        "ContractApplied",
        "ContractedUnitPrice",
        "PricingCurrencyContractedUnitPrice",
    }
)
_DEFERRED: frozenset[str] = frozenset()
_DECLARED_GAPS = {
    "BillingCurrency": ("provider_billing_currency_field_unavailable", "TASK-254.03"),
    "HostProviderName": ("provider_host_display_name_unavailable", "TASK-254.04"),
    "InvoiceDetailId": ("invoice_identity_unavailable", "TASK-254.04"),
    "InvoiceId": ("invoice_identity_unavailable", "TASK-254.04"),
    "InvoiceIssuerName": ("invoice_issuer_name_unavailable", "TASK-254.04"),
    "RegionName": ("provider_region_display_name_unavailable", "TASK-254.04"),
    "SkuId": ("derived_sku_identity_not_provider_authoritative", "TASK-254.04"),
    "SkuMeter": ("derived_sku_identity_not_provider_authoritative", "TASK-254.04"),
    "SkuPriceDetails": ("derived_sku_identity_not_provider_authoritative", "TASK-254.04"),
    "SkuPriceId": ("derived_sku_identity_not_provider_authoritative", "TASK-254.04"),
}


def _target_rule(column: str, level: str, allows_null: bool) -> FocusColumnRule:
    applicability = PreviewApplicability.APPLICABLE
    gap_code = owner = None
    if column in _NOT_APPLICABLE:
        applicability = PreviewApplicability.NOT_APPLICABLE
    elif column in _DEFERRED:
        applicability = PreviewApplicability.DEFERRED
        gap_code, owner = "allocation_lineage_and_tag_projection_pending", "TASK-254.05"
    elif column in _DECLARED_GAPS:
        applicability = PreviewApplicability.DECLARED_GAP
        gap_code, owner = _DECLARED_GAPS[column]
    validator = (
        PreviewValidatorKind.DECIMAL
        if column in _DECIMAL_COLUMNS
        else PreviewValidatorKind.DATETIME
        if column in _DATETIME_COLUMNS
        else PreviewValidatorKind.ENUM
        if column in _ENUM_VALUES
        else PreviewValidatorKind.JSON
        if column in {"AllocatedMethodDetails", "AllocatedTags", "SkuPriceDetails", "Tags"}
        else PreviewValidatorKind.IDENTIFIER
        if column.endswith("Id")
        else PreviewValidatorKind.TEXT
    )
    source, transformation = _TARGET_RULE_AUTHORITIES[column]
    return FocusColumnRule(
        column=column,
        feature_level=_FEATURE_LEVEL[level],
        allows_null=allows_null,
        applicability=applicability,
        source=source,
        transformation=transformation,
        allowed_values=_ENUM_VALUES.get(column),
        validator=validator,
        gap_code=gap_code,
        owner_task=owner,
    )


FOCUS_1_4_COLUMN_RULES = tuple(_target_rule(*spec) for spec in _COLUMN_SPECS)
FOCUS_1_4_FULL_COLUMNS = tuple(rule.column for rule in FOCUS_1_4_COLUMN_RULES)

_ACCEPTED_PROVIDER_LINE_TYPES = (
    "KAFKA_STORAGE",
    "KAFKA_PARTITION",
    "KAFKA_NETWORK_READ",
    "KAFKA_NETWORK_WRITE",
    "KAFKA_BASE",
    "KAFKA_NUM_CKUS",
    "KAFKA_REST_PRODUCE",
    "KAFKA_STREAMS",
    "CLUSTER_LINKING_PER_LINK",
    "CLUSTER_LINKING_WRITE",
    "CLUSTER_LINKING_READ",
    "KSQL_NUM_CSUS",
    "FLINK_NUM_CFUS",
    "CONNECT_CAPACITY",
    "CONNECT_NUM_TASKS",
    "CONNECT_THROUGHPUT",
    "CONNECT_NUM_RECORDS",
    "CUSTOM_CONNECT_NUM_TASKS",
    "CUSTOM_CONNECT_THROUGHPUT",
    "GOVERNANCE_BASE",
    "SCHEMA_REGISTRY",
    "NUM_RULES",
    "AUDIT_LOG_READ",
    "TABLEFLOW_DATA_PROCESSED",
    "TABLEFLOW_NUM_TOPICS",
    "TABLEFLOW_STORAGE",
    "USM_CONNECTED_NODE",
    "SUPPORT",
    "PROMO_CREDIT",
)

_CUSTOM_SPECS = (
    ("x_ChitraguptaSourceCostId", False, PreviewApplicability.APPLICABLE, PreviewValidatorKind.IDENTIFIER, None, None),
    (
        "x_ChitraguptaBillingScopeId",
        False,
        PreviewApplicability.APPLICABLE,
        PreviewValidatorKind.IDENTIFIER,
        None,
        None,
    ),
    (
        "x_ChitraguptaAllocationRatio",
        True,
        PreviewApplicability.APPLICABLE,
        PreviewValidatorKind.DECIMAL,
        None,
        None,
    ),
    (
        "x_ChitraguptaAllocationMethodVersion",
        True,
        PreviewApplicability.APPLICABLE,
        PreviewValidatorKind.TEXT,
        None,
        None,
    ),
    (
        "x_ChitraguptaMappingProfileVersion",
        False,
        PreviewApplicability.APPLICABLE,
        PreviewValidatorKind.TEXT,
        None,
        None,
    ),
    (
        "x_ChitraguptaSkuComponents",
        True,
        PreviewApplicability.DECLARED_GAP,
        PreviewValidatorKind.JSON,
        "derived_sku_identity_not_provider_authoritative",
        "TASK-254.04",
    ),
    ("x_ConfluentProduct", True, PreviewApplicability.APPLICABLE, PreviewValidatorKind.TEXT, None, None),
    ("x_ConfluentLineType", False, PreviewApplicability.APPLICABLE, PreviewValidatorKind.ENUM, None, None),
    ("x_ConfluentDescription", False, PreviewApplicability.APPLICABLE, PreviewValidatorKind.TEXT, None, None),
    ("x_ConfluentDiscountAmount", False, PreviewApplicability.APPLICABLE, PreviewValidatorKind.DECIMAL, None, None),
    ("x_ConfluentNetworkAccessType", True, PreviewApplicability.APPLICABLE, PreviewValidatorKind.TEXT, None, None),
    ("x_ConfluentTierDimensions", False, PreviewApplicability.APPLICABLE, PreviewValidatorKind.JSON, None, None),
)
_CUSTOM_RULE_AUTHORITIES = {
    "x_ChitraguptaSourceCostId": (
        "source provider identity",
        "use provider Cost ID with stable source-record fallback",
    ),
    "x_ChitraguptaBillingScopeId": (
        "provider organization and UTC billing month",
        "derive the namespaced v1 SHA-256 key",
    ),
    "x_ChitraguptaAllocationRatio": ("persisted allocation lineage", "copy exact realized ratio"),
    "x_ChitraguptaAllocationMethodVersion": ("persisted allocation lineage", "copy lineage method version"),
    "x_ChitraguptaMappingProfileVersion": ("mapping profile", "emit focus-1.4-preview-v1"),
    "x_ChitraguptaSkuComponents": (
        "canonical SKU and SKU price components",
        "serialize as canonical JSON",
    ),
    "x_ConfluentProduct": ("source.native_product", "copy losslessly"),
    "x_ConfluentLineType": ("source.native_line_type", "copy losslessly"),
    "x_ConfluentDescription": ("source.native_description", "copy losslessly"),
    "x_ConfluentDiscountAmount": ("source.discount_amount", "copy the exact Decimal"),
    "x_ConfluentNetworkAccessType": ("source.native_network_access_type", "copy losslessly when supplied"),
    "x_ConfluentTierDimensions": (
        "source.native_tier_dimensions",
        "serialize the sorted retained values as canonical JSON",
    ),
}
CUSTOM_EVIDENCE_RULES = tuple(
    CustomEvidenceRule(
        column=column,
        allows_null=allows_null,
        applicability=applicability,
        source=_CUSTOM_RULE_AUTHORITIES[column][0],
        transformation=_CUSTOM_RULE_AUTHORITIES[column][1],
        validator=validator,
        allowed_values=(_ACCEPTED_PROVIDER_LINE_TYPES if column == "x_ConfluentLineType" else None),
        gap_code=gap,
        owner_task=owner,
    )
    for column, allows_null, applicability, validator, gap, owner in _CUSTOM_SPECS
)
CUSTOM_EVIDENCE_COLUMNS = tuple(rule.column for rule in CUSTOM_EVIDENCE_RULES)

FOCUS_1_4_FULL_PROFILE_COLUMNS = (
    *FOCUS_1_4_FULL_COLUMNS,
    *CUSTOM_EVIDENCE_COLUMNS,
)
FOCUS_1_4_SUMMARY_COLUMNS = (
    "AllocatedResourceId",
    "AllocatedResourceName",
    "AllocatedTags",
    "BilledCost",
    "BillingAccountId",
    "BillingAccountName",
    "BillingCurrency",
    "BillingPeriodEnd",
    "BillingPeriodStart",
    "ChargeCategory",
    "ChargePeriodEnd",
    "ChargePeriodStart",
    "EffectiveCost",
    "ResourceId",
    "ResourceName",
    "ServiceCategory",
    "ServiceName",
    "SubAccountId",
    "SubAccountName",
    "Tags",
)


def validate_preview_effective_columns(
    profile: PreviewColumnProfile,
    effective_columns: tuple[str, ...],
) -> None:
    if profile == "full":
        valid = effective_columns == FOCUS_1_4_FULL_PROFILE_COLUMNS
    elif profile == "summary":
        valid = effective_columns == FOCUS_1_4_SUMMARY_COLUMNS
    elif profile == "custom":
        valid = (
            bool(effective_columns)
            and len(effective_columns) == len(set(effective_columns))
            and all(column in FOCUS_1_4_FULL_PROFILE_COLUMNS for column in effective_columns)
        )
    else:
        valid = False
    if not valid:
        raise PreviewEffectiveColumnsError("preview effective columns do not match the selected profile")


@dataclass(frozen=True)
class KnownGap:
    code: str
    description: str
    owner_task: str
    columns: tuple[str, ...]


KNOWN_GAPS = (
    KnownGap(
        "provider_billing_currency_field_unavailable",
        "Confluent Costs records do not carry a per-record billing currency.",
        "TASK-254.03",
        ("BillingCurrency",),
    ),
    KnownGap(
        "invoice_identity_unavailable",
        "Post-issuance invoice identity is unavailable.",
        "TASK-254.04",
        ("InvoiceDetailId", "InvoiceId"),
    ),
    KnownGap(
        "invoice_issuer_name_unavailable",
        "Provider legal invoice-issuer evidence is unavailable.",
        "TASK-254.04",
        ("InvoiceIssuerName",),
    ),
    KnownGap(
        "provider_host_display_name_unavailable",
        "HostProviderName contains the raw provider cloud code, not a provider display name.",
        "TASK-254.04",
        ("HostProviderName",),
    ),
    KnownGap(
        "provider_region_display_name_unavailable",
        "Confluent inventory does not provide a distinct region display name.",
        "TASK-254.04",
        ("RegionName",),
    ),
    KnownGap(
        "derived_sku_identity_not_provider_authoritative",
        "SKU values are deterministic Chitragupta-derived evidence, not provider-issued identifiers.",
        "TASK-254.04",
        ("SkuId", "SkuMeter", "SkuPriceDetails", "SkuPriceId", "x_ChitraguptaSkuComponents"),
    ),
)


def preview_manifest_known_gaps() -> list[dict[str, object]]:
    return [
        {
            "code": gap.code,
            "description": gap.description,
            "columns": list(gap.columns),
        }
        for gap in KNOWN_GAPS
    ]


PROFILE_NOT_APPLICABLE_COLUMNS = tuple(
    rule.column for rule in FOCUS_1_4_COLUMN_RULES if rule.applicability is PreviewApplicability.NOT_APPLICABLE
)
_GAP_COLUMNS = frozenset(column for gap in KNOWN_GAPS for column in gap.columns)
MAPPED_COLUMNS = tuple(
    column
    for column in (*FOCUS_1_4_FULL_COLUMNS, *CUSTOM_EVIDENCE_COLUMNS)
    if column not in _GAP_COLUMNS and column not in PROFILE_NOT_APPLICABLE_COLUMNS
)


class PreviewChargeKind(StrEnum):
    METERED_USAGE = "metered_usage"
    RECURRING_SUPPORT = "recurring_support"
    PROMOTIONAL_ALLOWANCE = "promotional_allowance"
    USAGE_REFUND = "usage_refund"
    SUPPORT_REFUND = "support_refund"


class PreviewResourceShape(StrEnum):
    RESOURCE_SPECIFIC = "resource_specific"
    ORGANIZATION_WIDE = "organization_wide"


class PreviewServiceRuleKey(StrEnum):
    KAFKA = "kafka"
    CLUSTER_LINK = "cluster_link"
    KSQLDB = "ksqldb"
    FLINK = "flink"
    CONNECT = "connect"
    DATA_GOVERNANCE = "data_governance"
    AUDIT_LOG = "audit_log"
    TABLEFLOW = "tableflow"
    USM = "usm"
    SUPPORT = "support"
    PROMOTIONAL_CREDIT = "promotional_credit"


class PreviewLineageReadiness(StrEnum):
    READY = "ready"
    TASK_254_05 = "task_254_05"


@dataclass(frozen=True)
class FocusServiceRule:
    key: PreviewServiceRuleKey
    native_line_types: tuple[str, ...]
    service_category: str
    service_name: str
    service_subcategory: str
    resource_shape: PreviewResourceShape
    context_strategy: PreviewContextStrategy
    allowed_origin_resource_types: tuple[str, ...]


type PreviewContextStrategy = Literal[
    "self",
    "connector_parent_kafka",
    "ksqldb_kafka_reference",
    "flink_pool_or_reference",
    "organization_wide",
    "unsupported_provider_context",
]


_KAFKA_TYPES = (
    "KAFKA_STORAGE",
    "KAFKA_PARTITION",
    "KAFKA_NETWORK_READ",
    "KAFKA_NETWORK_WRITE",
    "KAFKA_BASE",
    "KAFKA_NUM_CKUS",
    "KAFKA_REST_PRODUCE",
    "KAFKA_STREAMS",
)
_CONNECT_TYPES = (
    "CONNECT_CAPACITY",
    "CONNECT_NUM_TASKS",
    "CONNECT_THROUGHPUT",
    "CONNECT_NUM_RECORDS",
    "CUSTOM_CONNECT_NUM_TASKS",
    "CUSTOM_CONNECT_THROUGHPUT",
)
_CLUSTER_LINK_TYPES = ("CLUSTER_LINKING_PER_LINK", "CLUSTER_LINKING_WRITE", "CLUSTER_LINKING_READ")
_DATA_GOVERNANCE_TYPES = ("GOVERNANCE_BASE", "SCHEMA_REGISTRY", "NUM_RULES")
_TABLEFLOW_TYPES = ("TABLEFLOW_DATA_PROCESSED", "TABLEFLOW_NUM_TOPICS", "TABLEFLOW_STORAGE")


def _service_rule(
    key: PreviewServiceRuleKey,
    line_types: tuple[str, ...],
    category: str,
    name: str,
    subcategory: str,
    shape: PreviewResourceShape,
    strategy: PreviewContextStrategy,
    types: tuple[str, ...],
) -> FocusServiceRule:
    return FocusServiceRule(key, line_types, category, name, subcategory, shape, strategy, types)


_FOCUS_1_4_SERVICE_RULE_DEFINITIONS_V1 = (
    _service_rule(
        PreviewServiceRuleKey.KAFKA,
        _KAFKA_TYPES,
        "Integration",
        "Confluent Cloud Apache Kafka",
        "Messaging",
        PreviewResourceShape.RESOURCE_SPECIFIC,
        "self",
        ("kafka_cluster",),
    ),
    _service_rule(
        PreviewServiceRuleKey.CLUSTER_LINK,
        _CLUSTER_LINK_TYPES,
        "Integration",
        "Confluent Cloud Cluster Linking",
        "Messaging",
        PreviewResourceShape.RESOURCE_SPECIFIC,
        "self",
        ("kafka_cluster",),
    ),
    _service_rule(
        PreviewServiceRuleKey.KSQLDB,
        ("KSQL_NUM_CSUS",),
        "Analytics",
        "Confluent Cloud ksqlDB",
        "Streaming Analytics",
        PreviewResourceShape.RESOURCE_SPECIFIC,
        "ksqldb_kafka_reference",
        ("ksqldb_cluster",),
    ),
    _service_rule(
        PreviewServiceRuleKey.FLINK,
        ("FLINK_NUM_CFUS",),
        "Analytics",
        "Confluent Cloud Flink",
        "Streaming Analytics",
        PreviewResourceShape.RESOURCE_SPECIFIC,
        "flink_pool_or_reference",
        ("flink_compute_pool", "flink_statement"),
    ),
    _service_rule(
        PreviewServiceRuleKey.CONNECT,
        _CONNECT_TYPES,
        "Integration",
        "Confluent Cloud Connect",
        "Messaging",
        PreviewResourceShape.RESOURCE_SPECIFIC,
        "connector_parent_kafka",
        ("connector",),
    ),
    _service_rule(
        PreviewServiceRuleKey.DATA_GOVERNANCE,
        _DATA_GOVERNANCE_TYPES,
        "Management and Governance",
        "Confluent Cloud Data Governance",
        "Data Governance",
        PreviewResourceShape.RESOURCE_SPECIFIC,
        "self",
        ("schema_registry",),
    ),
    _service_rule(
        PreviewServiceRuleKey.AUDIT_LOG,
        ("AUDIT_LOG_READ",),
        "Management and Governance",
        "Confluent Cloud Audit Logs",
        "Observability",
        PreviewResourceShape.ORGANIZATION_WIDE,
        "organization_wide",
        (),
    ),
    _service_rule(
        PreviewServiceRuleKey.TABLEFLOW,
        _TABLEFLOW_TYPES,
        "Storage",
        "Confluent Cloud Tableflow",
        "Object Storage",
        PreviewResourceShape.RESOURCE_SPECIFIC,
        "unsupported_provider_context",
        (),
    ),
    _service_rule(
        PreviewServiceRuleKey.USM,
        ("USM_CONNECTED_NODE",),
        "Management and Governance",
        "Confluent Cloud Unified Stream Manager",
        "Observability",
        PreviewResourceShape.RESOURCE_SPECIFIC,
        "self",
        ("kafka_cluster",),
    ),
    _service_rule(
        PreviewServiceRuleKey.SUPPORT,
        ("SUPPORT",),
        "Management and Governance",
        "Confluent Cloud Support",
        "Support",
        PreviewResourceShape.ORGANIZATION_WIDE,
        "organization_wide",
        (),
    ),
    _service_rule(
        PreviewServiceRuleKey.PROMOTIONAL_CREDIT,
        ("PROMO_CREDIT",),
        "Other",
        "Confluent Cloud Promotional Credits",
        "Other (Other)",
        PreviewResourceShape.ORGANIZATION_WIDE,
        "organization_wide",
        (),
    ),
)
FOCUS_1_4_SERVICE_RULES_V1 = MappingProxyType({rule.key: rule for rule in _FOCUS_1_4_SERVICE_RULE_DEFINITIONS_V1})

_READY_NATIVE_LINE_TYPES_V1 = (
    "KAFKA_STORAGE",
    "KAFKA_PARTITION",
    "KAFKA_NETWORK_READ",
    "KAFKA_NETWORK_WRITE",
    "KAFKA_BASE",
    "KAFKA_NUM_CKUS",
    "CONNECT_CAPACITY",
    "CONNECT_NUM_TASKS",
    "CONNECT_THROUGHPUT",
    "CUSTOM_CONNECT_NUM_TASKS",
    "CUSTOM_CONNECT_THROUGHPUT",
    "KSQL_NUM_CSUS",
    "FLINK_NUM_CFUS",
    "GOVERNANCE_BASE",
    "SCHEMA_REGISTRY",
    "NUM_RULES",
)
_TASK_254_05_NATIVE_LINE_TYPES_V1 = (
    "AUDIT_LOG_READ",
    "SUPPORT",
    "PROMO_CREDIT",
    "KAFKA_REST_PRODUCE",
    "KAFKA_STREAMS",
    "CONNECT_NUM_RECORDS",
    "CLUSTER_LINKING_PER_LINK",
    "CLUSTER_LINKING_READ",
    "CLUSTER_LINKING_WRITE",
    "USM_CONNECTED_NODE",
    "TABLEFLOW_DATA_PROCESSED",
    "TABLEFLOW_NUM_TOPICS",
    "TABLEFLOW_STORAGE",
)
FOCUS_1_4_NATIVE_LINE_READINESS_V1 = MappingProxyType(
    {
        **{line_type: PreviewLineageReadiness.READY for line_type in _READY_NATIVE_LINE_TYPES_V1},
        **{line_type: PreviewLineageReadiness.READY for line_type in _TASK_254_05_NATIVE_LINE_TYPES_V1},
    }
)


@dataclass(frozen=True)
class NativeProductServiceRule:
    native_product: str
    service_rule_key: PreviewServiceRuleKey
    original_category: Literal["Usage", "Purchase"]
    original_frequency: Literal["Usage-Based", "Recurring"]


_PRODUCT_KEYS = {
    "KAFKA": PreviewServiceRuleKey.KAFKA,
    "CONNECT": PreviewServiceRuleKey.CONNECT,
    "KSQL": PreviewServiceRuleKey.KSQLDB,
    "AUDIT_LOG": PreviewServiceRuleKey.AUDIT_LOG,
    "STREAM_GOVERNANCE": PreviewServiceRuleKey.DATA_GOVERNANCE,
    "CLUSTER_LINK": PreviewServiceRuleKey.CLUSTER_LINK,
    "CUSTOM_CONNECT": PreviewServiceRuleKey.CONNECT,
    "FLINK": PreviewServiceRuleKey.FLINK,
    "TABLEFLOW": PreviewServiceRuleKey.TABLEFLOW,
    "USM": PreviewServiceRuleKey.USM,
    "SUPPORT_CLOUD_BASIC": PreviewServiceRuleKey.SUPPORT,
    "SUPPORT_CLOUD_DEVELOPER": PreviewServiceRuleKey.SUPPORT,
    "SUPPORT_CLOUD_BUSINESS": PreviewServiceRuleKey.SUPPORT,
    "SUPPORT_CLOUD_PREMIER": PreviewServiceRuleKey.SUPPORT,
}
NATIVE_PRODUCT_SERVICE_RULES_V1 = MappingProxyType(
    {
        product: NativeProductServiceRule(
            product,
            key,
            "Purchase" if key is PreviewServiceRuleKey.SUPPORT else "Usage",
            "Recurring" if key is PreviewServiceRuleKey.SUPPORT else "Usage-Based",
        )
        for product, key in _PRODUCT_KEYS.items()
    }
)


@dataclass(frozen=True)
class PreviewChargeSemantics:
    kind: PreviewChargeKind
    charge_category: Literal["Usage", "Purchase", "Credit"]
    charge_frequency: Literal["Usage-Based", "Recurring", "One-Time"]
    service_rule_key: PreviewServiceRuleKey
    emits_pricing: bool
    emits_consumption: bool


@dataclass(frozen=True)
class AcceptedPreviewSource:
    semantics: PreviewChargeSemantics


@dataclass(frozen=True)
class RejectedPreviewSource:
    issue: PreviewSourceIssue


type PreviewSourceClassification = AcceptedPreviewSource | RejectedPreviewSource


@dataclass(frozen=True)
class PreviewFinancialProjection:
    billed_cost: Decimal
    contracted_cost: Decimal
    effective_cost: Decimal
    list_cost: Decimal
    list_unit_price: Decimal | None
    pricing_currency_effective_cost: Decimal
    pricing_currency_list_unit_price: Decimal | None
    pricing_quantity: Decimal | None
    pricing_unit: str | None
    consumed_quantity: Decimal | None
    consumed_unit: str | None


type PreviewCell = str | Decimal | datetime | None
type CompatibilityPreviewOriginKey = tuple[datetime, str, str, str, str]
type ExactPreviewOriginKey = tuple[datetime, str, str, str, str, str, datetime, datetime]
type PreviewOriginKey = CompatibilityPreviewOriginKey | ExactPreviewOriginKey
type LegacyAggregateCandidateKey = tuple[
    datetime,
    str,
    str | None,
    str,
    str,
    Decimal | None,
    Decimal | None,
    Decimal | None,
]


@dataclass
class _CompatibilityTotals:
    source_costs: list[Decimal]
    source_quantities: list[Decimal]
    compatibility_cost: Decimal
    compatibility_quantity: Decimal
    source_ids: list[str] = field(default_factory=list)
    expected_mismatch: bool = False


@dataclass
class _CompatibilityColumnTotals:
    allocated_costs: list[Decimal]
    allocated_quantities: list[Decimal]
    compatibility_cost: Decimal
    compatibility_quantity: Decimal
    expected_mismatch: bool = False


@dataclass(frozen=True)
class PreviewLineageMember:
    source_cost_id: str
    calculation_id: str
    origin_timestamp: datetime
    origin_environment_id: str
    origin_resource_id: str
    origin_product_type: str
    origin_product_category: str
    portion_ordinal: int


@dataclass(frozen=True)
class PreviewFullRow:
    target_values: tuple[PreviewCell, ...]
    custom_values: tuple[PreviewCell, ...]
    financials: PreviewFinancialProjection
    lineage_members: tuple[PreviewLineageMember, ...]


@dataclass(frozen=True)
class SelectedSourceProjection:
    source: PreviewSourceEvidence
    semantics: PreviewChargeSemantics
    financials: PreviewFinancialProjection


@dataclass(frozen=True)
class SelectedPreviewEvidence:
    selected: SelectedSourceProjection
    aggregate: PreviewAggregateEvidence
    allocation: PreviewAllocationEvidence


@dataclass(frozen=True)
class PreviewProviderContext:
    billing_account_id: str
    billing_account_name: str | None


@dataclass(frozen=True)
class PreviewResourceContext:
    resource_id: str | None
    resource_name: str | None
    resource_type: str | None
    host_provider_code: str | None
    region_id: str | None


@dataclass(frozen=True)
class PreparedPreviewPackageRow:
    evidence: SelectedPreviewEvidence
    resource_context: PreviewResourceContext
    allocated_entity: Identity | Resource | None
    environment: Resource | None
    origin_tags_json: str
    allocated_tags_json: str | None


def preview_utc_text(value: datetime) -> str:
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def preview_decimal_text(value: Decimal) -> str:
    result = format(value, "f")
    if "." in result:
        result = result.rstrip("0").rstrip(".")
    return result or "0"


def preview_sum_decimals(values: Iterable[Decimal]) -> Decimal:
    """Sum Preview decimals deterministically under the mapping-owned context."""
    with localcontext(PREVIEW_DECIMAL_CONTEXT):
        return sum(sorted(values), Decimal(0))


def preview_subtract_decimals(minuend: Decimal, subtrahend: Decimal) -> Decimal:
    """Subtract Preview decimals under the mapping-owned context."""
    with localcontext(PREVIEW_DECIMAL_CONTEXT):
        return minuend - subtrahend


def preview_serialize_cell(value: PreviewCell) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return preview_utc_text(value)
    if isinstance(value, Decimal):
        return preview_decimal_text(value)
    return value


def preview_canonical_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


# Private spellings used by mapping-local call sites.
_utc = preview_utc_text
_decimal = preview_decimal_text
_serialize_cell = preview_serialize_cell
_canonical_json = preview_canonical_json


def _semantic_flags(*values: str) -> tuple[bool, bool, bool, bool]:
    token_groups = tuple(tuple(re.findall(r"[a-z0-9]+", value.casefold())) for value in values)
    tokens = tuple(token for group in token_groups for token in group)
    pairs = {pair for group in token_groups for pair in zip(group, group[1:], strict=False)}
    refund = any(token.startswith("refund") for token in tokens)
    promotion = any(token.startswith(("promo", "credit")) for token in tokens)
    support = any(token.startswith("support") for token in tokens)
    ambiguous = any(
        token.startswith(("adjust", "correct", "revers", "rebate")) or token == "trueup" for token in tokens
    ) or bool(pairs & {("prior", "period"), ("true", "up")})
    return refund, promotion, support, ambiguous


def _service_key_for_line_type(line_type: str) -> PreviewServiceRuleKey | None:
    return next((key for key, rule in FOCUS_1_4_SERVICE_RULES_V1.items() if line_type in rule.native_line_types), None)


def classify_daily_full_source(
    *, request_start: datetime, request_end: datetime, source: PreviewSourceEvidence
) -> PreviewSourceClassification:
    if source.malformed or source.diagnostics:
        return RejectedPreviewSource(PreviewSourceIssue.RECORD_MALFORMED)
    if source.source_period_start is None or source.source_period_end is None:
        return RejectedPreviewSource(PreviewSourceIssue.SCOPE_UNSUPPORTED)
    bounds = (
        source.source_period_start,
        source.source_period_end,
        source.collection_window_start,
        source.collection_window_end,
        source.evidence_scope_start,
        source.evidence_scope_end,
    )
    if any(value.tzinfo is None or value.utcoffset() is None for value in bounds) or not (
        request_start <= source.source_period_start < source.source_period_end <= request_end
        and source.collection_window_start < source.collection_window_end
        and source.evidence_scope_start < source.evidence_scope_end
    ):
        return RejectedPreviewSource(PreviewSourceIssue.SCOPE_UNSUPPORTED)
    if not source.native_line_type:
        return RejectedPreviewSource(PreviewSourceIssue.LINE_TYPE_UNKNOWN)
    if not source.provider_cost_id or not source.native_description:
        return RejectedPreviewSource(PreviewSourceIssue.RECORD_INCOMPLETE)
    if not all(
        isinstance(value, Decimal) for value in (source.amount, source.original_amount, source.discount_amount)
    ) or any(value is not None and not isinstance(value, Decimal) for value in (source.price, source.quantity)):
        return RejectedPreviewSource(PreviewSourceIssue.RECORD_MALFORMED)
    refund, promotion, support, ambiguous = _semantic_flags(source.native_product or "", source.native_description)
    if ambiguous:
        return RejectedPreviewSource(PreviewSourceIssue.CHARGE_CLASSIFICATION_AMBIGUOUS)
    line_type = source.native_line_type
    if line_type == "KAFKA_STREAMS" and not source.unit:
        return RejectedPreviewSource(PreviewSourceIssue.MAPPING_UNAVAILABLE)
    if line_type not in {"SUPPORT", "PROMO_CREDIT"} and _service_key_for_line_type(line_type) is None:
        return RejectedPreviewSource(PreviewSourceIssue.LINE_TYPE_UNSUPPORTED)

    if line_type == "PROMO_CREDIT" and not refund:
        return AcceptedPreviewSource(
            PreviewChargeSemantics(
                PreviewChargeKind.PROMOTIONAL_ALLOWANCE,
                "Credit",
                "One-Time",
                PreviewServiceRuleKey.PROMOTIONAL_CREDIT,
                False,
                False,
            )
        )

    if not source.native_product:
        return RejectedPreviewSource(PreviewSourceIssue.RECORD_INCOMPLETE)
    product_rule = NATIVE_PRODUCT_SERVICE_RULES_V1.get(source.native_product)
    if product_rule is None:
        return RejectedPreviewSource(PreviewSourceIssue.CHARGE_CLASSIFICATION_AMBIGUOUS)
    if line_type == "PROMO_CREDIT":
        if not refund or promotion or support != (product_rule.original_category == "Purchase"):
            return RejectedPreviewSource(PreviewSourceIssue.CHARGE_CLASSIFICATION_AMBIGUOUS)
        refund_rule = FOCUS_1_4_SERVICE_RULES_V1[product_rule.service_rule_key]
        if refund_rule.resource_shape is PreviewResourceShape.RESOURCE_SPECIFIC and (
            not source.resource_id or not source.environment_id
        ):
            return RejectedPreviewSource(PreviewSourceIssue.RECORD_INCOMPLETE)
        kind = (
            PreviewChargeKind.SUPPORT_REFUND
            if product_rule.original_category == "Purchase"
            else PreviewChargeKind.USAGE_REFUND
        )
        return AcceptedPreviewSource(
            PreviewChargeSemantics(
                kind,
                product_rule.original_category,
                product_rule.original_frequency,
                product_rule.service_rule_key,
                True,
                kind is PreviewChargeKind.USAGE_REFUND,
            )
        )

    inferred_key = _service_key_for_line_type(line_type)
    if line_type == "SUPPORT":
        inferred_key = PreviewServiceRuleKey.SUPPORT
        if not support or product_rule.service_rule_key is not inferred_key:
            return RejectedPreviewSource(PreviewSourceIssue.CHARGE_CLASSIFICATION_AMBIGUOUS)
    elif product_rule.service_rule_key is not inferred_key:
        return RejectedPreviewSource(PreviewSourceIssue.CHARGE_CLASSIFICATION_AMBIGUOUS)
    assert inferred_key is not None
    rule = FOCUS_1_4_SERVICE_RULES_V1[inferred_key]
    if rule.resource_shape is PreviewResourceShape.RESOURCE_SPECIFIC and (
        not source.resource_id or not source.environment_id
    ):
        return RejectedPreviewSource(PreviewSourceIssue.RECORD_INCOMPLETE)
    if refund:
        if promotion or (support and line_type != "SUPPORT"):
            return RejectedPreviewSource(PreviewSourceIssue.CHARGE_CLASSIFICATION_AMBIGUOUS)
        category = product_rule.original_category
        frequency = product_rule.original_frequency
        kind = PreviewChargeKind.SUPPORT_REFUND if category == "Purchase" else PreviewChargeKind.USAGE_REFUND
        return AcceptedPreviewSource(
            PreviewChargeSemantics(
                kind,
                category,
                frequency,
                inferred_key,
                True,
                kind is PreviewChargeKind.USAGE_REFUND,
            )
        )
    if promotion or (support and line_type != "SUPPORT"):
        return RejectedPreviewSource(PreviewSourceIssue.CHARGE_CLASSIFICATION_AMBIGUOUS)
    if line_type == "SUPPORT":
        return AcceptedPreviewSource(
            PreviewChargeSemantics(
                PreviewChargeKind.RECURRING_SUPPORT, "Purchase", "Recurring", inferred_key, True, False
            )
        )
    return AcceptedPreviewSource(
        PreviewChargeSemantics(PreviewChargeKind.METERED_USAGE, "Usage", "Usage-Based", inferred_key, True, True)
    )


def project_financials(
    *, source: PreviewSourceEvidence, semantics: PreviewChargeSemantics, billed_share: Decimal
) -> PreviewFinancialProjection:
    amount, original, discount = source.amount, source.original_amount, source.discount_amount
    if not all(
        isinstance(value, Decimal) and value.is_finite() for value in (amount, original, discount, billed_share)
    ):
        raise PreviewFinancialUnsupportedError("source economics are non-finite")
    assert isinstance(amount, Decimal) and isinstance(original, Decimal) and isinstance(discount, Decimal)
    if billed_share != amount:
        raise PreviewFinancialReconciliationError("billed share does not equal source amount")
    price, quantity, unit = source.price, source.quantity, source.unit
    if semantics.kind is PreviewChargeKind.PROMOTIONAL_ALLOWANCE:
        if any(value is not None and not value.is_finite() for value in (price, quantity)):
            raise PreviewFinancialUnsupportedError("promotional allowance pricing is non-finite")
        if not (
            amount == original < 0 and discount == 0 and price in {None, Decimal(0)} and quantity in {None, Decimal(0)}
        ):
            if amount >= 0 or not amount.is_finite():
                raise PreviewFinancialUnsupportedError("promotional allowance sign is unsupported")
            raise PreviewFinancialReconciliationError("promotional allowance arithmetic does not reconcile")
        return PreviewFinancialProjection(amount, amount, amount, amount, None, amount, None, None, None, None, None)
    is_refund_economics = semantics.kind in {PreviewChargeKind.USAGE_REFUND, PreviewChargeKind.SUPPORT_REFUND}
    if is_refund_economics:
        if not (amount < 0 and original < 0 and discount <= 0):
            raise PreviewFinancialUnsupportedError("refund sign is unsupported")
    elif not (amount > 0 and original > 0 and discount >= 0):
        raise PreviewFinancialUnsupportedError("positive charge sign is unsupported")
    if (
        not isinstance(price, Decimal)
        or not isinstance(quantity, Decimal)
        or not price.is_finite()
        or not quantity.is_finite()
        or price == 0
        or quantity == 0
        or not unit
    ):
        raise PreviewFinancialUnsupportedError("priced charge lacks supported price evidence")
    if not is_refund_economics and (price < 0 or quantity < 0):
        raise PreviewFinancialUnsupportedError("positive charge price or quantity sign is unsupported")
    with localcontext(PREVIEW_DECIMAL_CONTEXT):
        source_arithmetic_reconciles = original - discount == amount and price * quantity == original
    if not source_arithmetic_reconciles:
        raise PreviewFinancialReconciliationError("source arithmetic does not reconcile")
    consumed_quantity = quantity if semantics.emits_consumption else None
    consumed_unit = unit if semantics.emits_consumption else None
    return PreviewFinancialProjection(
        amount,
        original,
        billed_share,
        original,
        price,
        billed_share,
        price,
        quantity,
        unit,
        consumed_quantity,
        consumed_unit,
    )


def reconcile_selected_evidence(
    *, selected: SelectedSourceProjection, aggregate: PreviewAggregateEvidence, allocation: PreviewAllocationEvidence
) -> SelectedPreviewEvidence:
    reconcile_source_aggregate_evidence(selected=selected, aggregate=aggregate)
    validate_allocation_lineage_evidence(aggregate=aggregate, allocations=(allocation,))
    return SelectedPreviewEvidence(selected, aggregate, allocation)


def project_allocated_financials(
    *,
    selected: SelectedSourceProjection,
    allocation: PreviewAllocationEvidence,
) -> PreviewFinancialProjection:
    """Project one persisted actual portion without changing allocation economics."""
    source = selected.source
    if source.amount is None or source.amount == 0:
        raise PreviewFinancialReconciliationError("source amount cannot support allocation projection")
    if source.original_amount is None:
        raise PreviewSourceEvidenceError("source original amount is unavailable")
    if (
        allocation.source_record_id
        and allocation.evidence_scope_start is not None
        and allocation.evidence_scope_end is not None
    ):
        list_cost = allocation.allocated_original_cost
    else:
        with localcontext(_DECIMAL_CONTEXT):
            list_cost = source.original_amount * allocation.allocation_ratio
    emits_pricing = selected.semantics.emits_pricing
    emits_consumption = selected.semantics.emits_consumption
    return PreviewFinancialProjection(
        billed_cost=allocation.allocated_cost,
        contracted_cost=list_cost,
        effective_cost=allocation.allocated_cost,
        list_cost=list_cost,
        list_unit_price=source.price if emits_pricing else None,
        pricing_currency_effective_cost=allocation.allocated_cost,
        pricing_currency_list_unit_price=source.price if emits_pricing else None,
        pricing_quantity=allocation.allocated_quantity if emits_pricing else None,
        pricing_unit=source.unit if emits_pricing else None,
        consumed_quantity=allocation.allocated_quantity if emits_consumption else None,
        consumed_unit=source.unit if emits_consumption else None,
    )


def reconcile_source_aggregate_evidence(
    *,
    selected: SelectedSourceProjection,
    aggregate: PreviewAggregateEvidence,
) -> None:
    source = selected.source
    source_amount, source_quantity, source_price = _source_reconciliation_financials(source)
    if (
        aggregate.total_cost != source_amount
        or aggregate.quantity != source_quantity
        or aggregate.unit_price != source_price
    ):
        raise PreviewFinancialReconciliationError("persisted source and aggregate evidence do not reconcile")


def _source_reconciliation_financials(
    source: PreviewSourceEvidence,
) -> tuple[Decimal | None, Decimal | None, Decimal | None]:
    try:
        amount, _original, _discount, price, quantity = normalize_preview_source_economics(
            line_type=source.native_line_type,
            amount=source.amount,
            original_amount=source.original_amount,
            discount_amount=source.discount_amount,
            price=source.price,
            quantity=source.quantity,
        )
    except ValueError:
        return source.amount, source.quantity, source.price
    return amount, quantity, price


def _legacy_source_candidate_key(source: PreviewSourceEvidence) -> LegacyAggregateCandidateKey:
    return (
        source.allocation_timestamp,
        source.environment_id or "",
        source.resource_id,
        source.native_product or "",
        source.native_line_type or "",
        *_source_reconciliation_financials(source),
    )


def _legacy_aggregate_candidate_keys(
    aggregate: PreviewAggregateEvidence,
) -> tuple[LegacyAggregateCandidateKey, LegacyAggregateCandidateKey]:
    common = (
        aggregate.timestamp,
        aggregate.environment_id,
        aggregate.native_product,
        aggregate.native_line_type,
        aggregate.total_cost,
        aggregate.quantity,
        aggregate.unit_price,
    )
    return (
        (common[0], common[1], aggregate.resource_id, *common[2:]),
        (common[0], common[1], None, *common[2:]),
    )


def _source_billing_origin(
    source: PreviewSourceEvidence,
    *,
    exact: bool = True,
) -> PreviewOriginKey | None:
    values = (
        source.billing_timestamp,
        source.billing_env_id,
        source.billing_resource_id,
        source.billing_product_category,
        source.billing_product_type,
    )
    if any(value is None for value in values):
        return None
    timestamp, environment_id, resource_id, product, line_type = values
    assert isinstance(timestamp, datetime)
    assert isinstance(environment_id, str)
    assert isinstance(resource_id, str)
    assert isinstance(product, str)
    assert isinstance(line_type, str)
    compatibility: CompatibilityPreviewOriginKey = timestamp, environment_id, resource_id, product, line_type
    if not exact:
        return compatibility
    return (
        *compatibility,
        source.source_record_id,
        source.evidence_scope_start,
        source.evidence_scope_end,
    )


def _aggregate_billing_origin(aggregate: PreviewAggregateEvidence) -> PreviewOriginKey:
    compatibility: CompatibilityPreviewOriginKey = (
        aggregate.timestamp,
        aggregate.environment_id,
        aggregate.resource_id,
        aggregate.native_product,
        aggregate.native_line_type,
    )
    if (
        aggregate.source_record_id
        and aggregate.evidence_scope_start is not None
        and aggregate.evidence_scope_end is not None
    ):
        return (
            *compatibility,
            aggregate.source_record_id,
            aggregate.evidence_scope_start,
            aggregate.evidence_scope_end,
        )
    return compatibility


def _allocation_billing_origin(allocation: PreviewAllocationEvidence) -> PreviewOriginKey:
    compatibility: CompatibilityPreviewOriginKey = (
        allocation.timestamp,
        allocation.environment_id,
        allocation.resource_id,
        allocation.native_product,
        allocation.native_line_type,
    )
    if (
        allocation.source_record_id
        and allocation.evidence_scope_start is not None
        and allocation.evidence_scope_end is not None
    ):
        return (
            *compatibility,
            allocation.source_record_id,
            allocation.evidence_scope_start,
            allocation.evidence_scope_end,
        )
    return compatibility


def reconcile_source_aggregate_stream(
    *,
    selected_sources: Iterable[SelectedSourceProjection],
    aggregates: Iterable[PreviewAggregateEvidence],
) -> tuple[
    dict[PreviewOriginKey, SelectedSourceProjection],
    dict[PreviewOriginKey, PreviewAggregateEvidence],
]:
    """Validate the complete source/aggregate stream in closed stage order."""
    aggregate_rows = tuple(aggregates)
    exact_stream = bool(aggregate_rows) and all(
        aggregate.source_record_id
        and aggregate.evidence_scope_start is not None
        and aggregate.evidence_scope_end is not None
        for aggregate in aggregate_rows
    )
    sources_by_origin: dict[PreviewOriginKey, list[SelectedSourceProjection]] = {}
    legacy_unassociated: list[SelectedSourceProjection] = []
    missing_association = False
    for selected in selected_sources:
        origin = _source_billing_origin(selected.source, exact=exact_stream)
        if origin is None:
            if (selected.source.capture_id or "").startswith("legacy:v1:"):
                legacy_unassociated.append(selected)
            else:
                missing_association = True
            continue
        sources_by_origin.setdefault(origin, []).append(selected)

    aggregates_by_origin: dict[PreviewOriginKey, PreviewAggregateEvidence] = {}
    duplicate_aggregate = False
    for aggregate in aggregate_rows:
        origin = _aggregate_billing_origin(aggregate)
        duplicate_aggregate = duplicate_aggregate or origin in aggregates_by_origin
        aggregates_by_origin[origin] = aggregate

    legacy_candidates_by_key: dict[LegacyAggregateCandidateKey, list[PreviewOriginKey]] = {}
    if legacy_unassociated:
        for origin, aggregate in aggregates_by_origin.items():
            for candidate_key in _legacy_aggregate_candidate_keys(aggregate):
                legacy_candidates_by_key.setdefault(candidate_key, []).append(origin)

    for selected in legacy_unassociated:
        candidates = legacy_candidates_by_key.get(_legacy_source_candidate_key(selected.source), ())
        if len(candidates) != 1:
            missing_association = True
            continue
        sources_by_origin.setdefault(candidates[0], []).append(selected)

    if not sources_by_origin and not aggregates_by_origin and not missing_association and not duplicate_aggregate:
        return {}, {}

    if (
        missing_association
        or not sources_by_origin
        or duplicate_aggregate
        or set(sources_by_origin) != set(aggregates_by_origin)
    ):
        raise PreviewSourceCoverageError("source and aggregate coverage is incomplete")
    if any(len(sources) != 1 for sources in sources_by_origin.values()):
        raise PreviewMappingScopeError("billing origin has multiple accepted sources")

    selected_by_origin = {origin: sources[0] for origin, sources in sources_by_origin.items()}
    unknown_currency = tuple(
        selected_by_origin[origin].source.source_record_id
        for origin, aggregate in aggregates_by_origin.items()
        if not aggregate.compatibility_currency
    )
    if unknown_currency:
        raise PreviewBillingCurrencyUnknownError(unknown_currency)
    unsupported_currency = tuple(
        selected_by_origin[origin].source.source_record_id
        for origin, aggregate in aggregates_by_origin.items()
        if aggregate.compatibility_currency != "USD"
    )
    if unsupported_currency:
        raise PreviewBillingCurrencyUnsupportedError(unsupported_currency)

    mismatched_sources: list[str] = []
    compatibility_totals: dict[CompatibilityPreviewOriginKey, _CompatibilityTotals] = {}
    for origin, selected in selected_by_origin.items():
        aggregate = aggregates_by_origin[origin]
        try:
            reconcile_source_aggregate_evidence(selected=selected, aggregate=aggregate)
        except PreviewFinancialReconciliationError:
            mismatched_sources.append(selected.source.source_record_id)
        if aggregate.compatibility_total_cost is None or aggregate.compatibility_quantity is None:
            continue
        compatibility_origin: CompatibilityPreviewOriginKey = (
            aggregate.timestamp,
            aggregate.environment_id,
            aggregate.resource_id,
            aggregate.native_product,
            aggregate.native_line_type,
        )
        current = compatibility_totals.get(compatibility_origin)
        if current is None:
            compatibility_totals[compatibility_origin] = _CompatibilityTotals(
                source_costs=[aggregate.total_cost],
                source_quantities=[aggregate.quantity],
                compatibility_cost=aggregate.compatibility_total_cost,
                compatibility_quantity=aggregate.compatibility_quantity,
                source_ids=[selected.source.source_record_id],
            )
        else:
            current.source_costs.append(aggregate.total_cost)
            current.source_quantities.append(aggregate.quantity)
            current.source_ids.append(selected.source.source_record_id)
            if (
                aggregate.compatibility_total_cost != current.compatibility_cost
                or aggregate.compatibility_quantity != current.compatibility_quantity
            ):
                current.expected_mismatch = True
    for totals in compatibility_totals.values():
        if (
            totals.expected_mismatch
            or preview_sum_decimals(totals.source_costs) != totals.compatibility_cost
            or preview_sum_decimals(totals.source_quantities) != totals.compatibility_quantity
        ):
            mismatched_sources.extend(totals.source_ids)
    if mismatched_sources:
        raise PreviewSourceAggregateReconciliationError(tuple(dict.fromkeys(mismatched_sources)))
    return selected_by_origin, aggregates_by_origin


def validate_allocation_lineage_structure(
    *,
    aggregate: PreviewAggregateEvidence,
    allocations: Sequence[PreviewAllocationEvidence],
) -> None:
    if not allocations:
        raise PreviewAllocationLineageError("allocation lineage has no portions")
    ordinals = {item.portion_ordinal for item in allocations}
    if len(ordinals) != len(allocations) or ordinals != set(range(len(allocations))):
        raise PreviewAllocationLineageError("allocation lineage ordinals are invalid")
    for allocation in allocations:
        validate_allocation_lineage_portion(aggregate=aggregate, allocation=allocation)


def validate_allocation_lineage_portion(
    *,
    aggregate: PreviewAggregateEvidence,
    allocation: PreviewAllocationEvidence,
) -> None:
    """Validate one allocation portion without retaining its sibling portions."""

    origin = _aggregate_billing_origin(aggregate)
    allocation_origin = _allocation_billing_origin(allocation)
    target_valid = (allocation.target_kind == "unallocated" and allocation.target_id is None) or (
        allocation.target_kind in {"identity", "resource"}
        and isinstance(allocation.target_id, str)
        and bool(allocation.target_id.strip())
        and (allocation.target_kind != "resource" or allocation.target_id == aggregate.resource_id)
    )
    quantity_sign_valid = (
        aggregate.quantity == 0
        and allocation.allocated_quantity == 0
        or aggregate.quantity > 0
        and allocation.allocated_quantity >= 0
        or aggregate.quantity < 0
        and allocation.allocated_quantity <= 0
    )
    try:
        with localcontext(_DECIMAL_CONTEXT):
            realized_ratio = allocation.allocated_cost / allocation.origin_total_cost
        decode_lineage_method_details(
            allocation.method_details_json,
            target_kind=allocation.target_kind,
        )
    except (ArithmeticError, ValueError) as exc:
        raise PreviewAllocationLineageError("allocation lineage encoding is invalid") from exc
    if not (
        allocation_origin == origin
        and allocation.origin_total_cost == aggregate.total_cost
        and allocation.origin_quantity == aggregate.quantity
        and allocation.origin_unit_price == aggregate.unit_price
        and allocation.origin_currency == aggregate.compatibility_currency
        and allocation.origin_granularity == aggregate.granularity
        and allocation.method_id.strip()
        and allocation.method_version == "v1"
        and allocation.allocation_ratio.is_finite()
        and allocation.allocation_ratio >= 0
        and allocation.allocation_ratio == realized_ratio
        and allocation.allocated_cost.is_finite()
        and allocation.allocated_quantity.is_finite()
        and allocation.allocated_original_cost.is_finite()
        and allocation.origin_original_cost.is_finite()
        and quantity_sign_valid
        and target_valid
    ):
        raise PreviewAllocationLineageError("allocation lineage structure is invalid")


def reconcile_allocation_lineage_totals(
    *,
    aggregate: PreviewAggregateEvidence,
    allocations: Sequence[PreviewAllocationEvidence],
) -> None:
    with localcontext(_DECIMAL_CONTEXT):
        allocated_cost = sum((item.allocated_cost for item in allocations), Decimal(0))
        allocated_quantity = sum((item.allocated_quantity for item in allocations), Decimal(0))
        allocated_original_cost = sum((item.allocated_original_cost for item in allocations), Decimal(0))
    original_totals = {item.origin_original_cost for item in allocations}
    if (
        allocated_cost != aggregate.total_cost
        or allocated_quantity != aggregate.quantity
        or len(original_totals) != 1
        or allocated_original_cost != next(iter(original_totals))
    ):
        raise PreviewFinancialReconciliationError("allocation lineage totals do not reconcile")


def reconcile_compatibility_allocation_columns(
    allocations: Iterable[PreviewAllocationEvidence],
) -> None:
    totals_by_column: dict[
        tuple[CompatibilityPreviewOriginKey, str, int],
        _CompatibilityColumnTotals,
    ] = {}
    for allocation in allocations:
        compatibility_cost = allocation.compatibility_allocated_cost
        compatibility_quantity = allocation.compatibility_allocated_quantity
        if compatibility_cost is None or compatibility_quantity is None:
            continue
        compatibility_origin: CompatibilityPreviewOriginKey = (
            allocation.timestamp,
            allocation.environment_id,
            allocation.resource_id,
            allocation.native_product,
            allocation.native_line_type,
        )
        key = compatibility_origin, allocation.calculation_id, allocation.portion_ordinal
        current = totals_by_column.get(key)
        if current is None:
            totals_by_column[key] = _CompatibilityColumnTotals(
                allocated_costs=[allocation.allocated_cost],
                allocated_quantities=[allocation.allocated_quantity],
                compatibility_cost=compatibility_cost,
                compatibility_quantity=compatibility_quantity,
            )
        else:
            current.allocated_costs.append(allocation.allocated_cost)
            current.allocated_quantities.append(allocation.allocated_quantity)
            if (
                compatibility_cost != current.compatibility_cost
                or compatibility_quantity != current.compatibility_quantity
            ):
                current.expected_mismatch = True
    if any(
        totals.expected_mismatch
        or preview_sum_decimals(totals.allocated_costs) != totals.compatibility_cost
        or preview_sum_decimals(totals.allocated_quantities) != totals.compatibility_quantity
        for totals in totals_by_column.values()
    ):
        raise PreviewFinancialReconciliationError("exact source columns do not reconcile with compatibility")


def reconcile_allocation_lineage_stream(
    *,
    aggregates_by_origin: Mapping[PreviewOriginKey, PreviewAggregateEvidence],
    expected_completion_by_run: Mapping[tuple[date, str], datetime],
    runs: Iterable[PreviewAllocationRunEvidence],
    allocations: Iterable[PreviewAllocationEvidence],
) -> dict[PreviewOriginKey, list[PreviewAllocationEvidence]]:
    """Validate the complete run/portion stream before reconciling any totals."""
    runs_by_identity: dict[tuple[date, str], PreviewAllocationRunEvidence] = {}
    duplicate_run = False
    for run in runs:
        identity = run.tracking_date, run.calculation_id
        duplicate_run = duplicate_run or identity in runs_by_identity
        runs_by_identity[identity] = run

    expected_calculation_by_date = {
        tracking_date: calculation_id for tracking_date, calculation_id in expected_completion_by_run
    }
    allocations_by_origin: dict[PreviewOriginKey, list[PreviewAllocationEvidence]] = {}
    allocation_count_by_run: dict[tuple[date, str], int] = {}
    invalid_allocation_scope = False
    for allocation in allocations:
        origin = _allocation_billing_origin(allocation)
        allocations_by_origin.setdefault(origin, []).append(allocation)
        run_identity = allocation.timestamp.date(), allocation.calculation_id
        allocation_count_by_run[run_identity] = allocation_count_by_run.get(run_identity, 0) + 1
        invalid_allocation_scope = invalid_allocation_scope or not (
            origin in aggregates_by_origin
            and allocation.calculation_id == expected_calculation_by_date.get(allocation.timestamp.date())
        )

    invalid_runs = (
        duplicate_run
        or set(runs_by_identity) != set(expected_completion_by_run)
        or any(
            run.capture_status is not AllocationLineageRunStatus.COMPLETE
            or run.capture_reason is not None
            or run.calculation_completed_at != expected_completion_by_run.get(identity)
            or (run.preview_portion_count if run.preview_portion_count is not None else run.portion_count)
            != allocation_count_by_run.get(identity, 0)
            for identity, run in runs_by_identity.items()
        )
    )
    if invalid_runs or invalid_allocation_scope or set(allocations_by_origin) != set(aggregates_by_origin):
        raise PreviewAllocationLineageError("allocation lineage stream is incomplete")

    reconcile_compatibility_allocation_columns(
        allocation for portions in allocations_by_origin.values() for allocation in portions
    )
    for origin, portions in allocations_by_origin.items():
        validate_allocation_lineage_structure(
            aggregate=aggregates_by_origin[origin],
            allocations=portions,
        )
    for origin, portions in allocations_by_origin.items():
        reconcile_allocation_lineage_totals(
            aggregate=aggregates_by_origin[origin],
            allocations=portions,
        )
        portions.sort(key=lambda item: item.portion_ordinal)
    return allocations_by_origin


def validate_allocation_lineage_evidence(
    *,
    aggregate: PreviewAggregateEvidence,
    allocations: tuple[PreviewAllocationEvidence, ...],
) -> tuple[PreviewAllocationEvidence, ...]:
    validate_allocation_lineage_structure(aggregate=aggregate, allocations=allocations)
    reconcile_allocation_lineage_totals(aggregate=aggregate, allocations=allocations)
    ordered = tuple(sorted(allocations, key=lambda item: item.portion_ordinal))
    return ordered


def _provider_metadata(resource: Resource) -> tuple[str, str]:
    cloud = resource.metadata.get("provider_cloud")
    region = resource.metadata.get("provider_region")
    if not isinstance(cloud, str) or not cloud.strip() or not isinstance(region, str) or not region.strip():
        raise PreviewProviderContextIncompleteError("raw provider cloud or region is unavailable")
    return cloud, region


def resolve_provider_resource_context(
    *,
    source: PreviewSourceEvidence,
    semantics: PreviewChargeSemantics,
    origin_resource: Resource | None,
    resources: ResourceRepository,
) -> PreviewResourceContext:
    return _resolve_provider_resource_context(
        source=source,
        semantics=semantics,
        origin_resource=origin_resource,
        resources=resources,
        resource_by_id=None,
    )


def resolve_provider_resource_context_from_mapping(
    *,
    source: PreviewSourceEvidence,
    semantics: PreviewChargeSemantics,
    origin_resource: Resource | None,
    resources: ResourceRepository,
    resource_by_id: Mapping[str, Resource],
) -> PreviewResourceContext:
    return _resolve_provider_resource_context(
        source=source,
        semantics=semantics,
        origin_resource=origin_resource,
        resources=resources,
        resource_by_id=resource_by_id,
    )


def _resolve_provider_resource_context(
    *,
    source: PreviewSourceEvidence,
    semantics: PreviewChargeSemantics,
    origin_resource: Resource | None,
    resources: ResourceRepository,
    resource_by_id: Mapping[str, Resource] | None,
) -> PreviewResourceContext:
    rule = FOCUS_1_4_SERVICE_RULES_V1[semantics.service_rule_key]
    if rule.context_strategy == "unsupported_provider_context":
        raise PreviewProviderContextIncompleteError("provider relationship is unsupported")
    if rule.resource_shape is PreviewResourceShape.ORGANIZATION_WIDE:
        return PreviewResourceContext(None, None, None, None, None)
    if origin_resource is None or origin_resource.resource_type not in rule.allowed_origin_resource_types:
        raise PreviewProviderContextIncompleteError("origin resource type is incompatible")
    if origin_resource.resource_id != source.resource_id or not source.environment_id:
        raise PreviewProviderContextIncompleteError("origin resource identity is incompatible")
    authority: Resource | None = origin_resource
    ecosystem = origin_resource.ecosystem
    tenant_id = origin_resource.tenant_id

    def find_resource(resource_id: str) -> Resource | None:
        if resource_by_id is not None:
            return resource_by_id.get(resource_id)
        return resources.get(ecosystem, tenant_id, resource_id)

    if rule.context_strategy == "connector_parent_kafka":
        if origin_resource.metadata.get("env_id") != source.environment_id:
            raise PreviewProviderContextIncompleteError("connector environment is incompatible")
        authority = find_resource(origin_resource.parent_id or "")
        if (
            authority is None
            or authority.resource_type != "kafka_cluster"
            or authority.tenant_id != tenant_id
            or authority.parent_id != source.environment_id
        ):
            raise PreviewProviderContextIncompleteError("connector parent is not Kafka")
    elif rule.context_strategy == "ksqldb_kafka_reference":
        if origin_resource.parent_id != source.environment_id:
            raise PreviewProviderContextIncompleteError("ksqlDB environment is incompatible")
        reference = origin_resource.metadata.get("kafka_cluster_id")
        authority = find_resource(reference if isinstance(reference, str) else "")
        if (
            authority is None
            or authority.resource_type != "kafka_cluster"
            or authority.tenant_id != tenant_id
            or authority.parent_id != source.environment_id
        ):
            raise PreviewProviderContextIncompleteError("ksqlDB Kafka reference is incompatible")
    elif rule.context_strategy == "flink_pool_or_reference" and origin_resource.resource_type == "flink_statement":
        if origin_resource.parent_id != source.environment_id:
            raise PreviewProviderContextIncompleteError("Flink statement environment is incompatible")
        reference = origin_resource.metadata.get("compute_pool_id")
        authority = find_resource(reference if isinstance(reference, str) else "")
        if (
            authority is None
            or authority.resource_type != "flink_compute_pool"
            or authority.tenant_id != tenant_id
            or authority.parent_id != source.environment_id
        ):
            raise PreviewProviderContextIncompleteError("Flink pool reference is incompatible")
    elif origin_resource.parent_id != source.environment_id:
        raise PreviewProviderContextIncompleteError("origin resource environment is incompatible")
    assert authority is not None
    cloud, region = _provider_metadata(authority)
    return PreviewResourceContext(
        origin_resource.resource_id,
        origin_resource.display_name or source.resource_name,
        origin_resource.resource_type,
        cloud,
        region,
    )


def source_through(source: PreviewSourceEvidence) -> datetime:
    start, end = source.collection_window_start, source.collection_window_end
    if (
        start.tzinfo is None
        or start.utcoffset() is None
        or end.tzinfo is None
        or end.utcoffset() is None
        or start >= end
    ):
        raise PreviewSourceEvidenceError("persisted collection window is invalid")
    return end.astimezone(UTC)


def validate_daily_full_source(
    *, request_start: datetime, request_end: datetime, source: PreviewSourceEvidence
) -> None:
    result = classify_daily_full_source(request_start=request_start, request_end=request_end, source=source)
    if isinstance(result, RejectedPreviewSource):
        if result.issue in {
            PreviewSourceIssue.RECORD_MALFORMED,
            PreviewSourceIssue.RECORD_INCOMPLETE,
            PreviewSourceIssue.SCOPE_UNSUPPORTED,
        }:
            raise PreviewSourceEvidenceError(result.issue.value)
        raise PreviewMappingScopeError(result.issue.value)
    assert source.amount is not None
    project_financials(source=source, semantics=result.semantics, billed_share=source.amount)


def validate_daily_full_mapping(
    *,
    request_start: datetime,
    request_end: datetime,
    source: PreviewSourceEvidence,
    aggregate: PreviewAggregateEvidence,
    allocation: PreviewAllocationEvidence,
) -> None:
    result = classify_daily_full_source(request_start=request_start, request_end=request_end, source=source)
    if isinstance(result, RejectedPreviewSource):
        raise PreviewSourceEvidenceError(result.issue.value)
    assert source.amount is not None
    projection = project_financials(source=source, semantics=result.semantics, billed_share=source.amount)
    selected = SelectedSourceProjection(source, result.semantics, projection)
    reconcile_source_aggregate_evidence(selected=selected, aggregate=aggregate)
    validate_allocation_lineage_evidence(
        aggregate=aggregate,
        allocations=(allocation,),
    )


def _validate_cell(value: PreviewCell, rule: FocusColumnRule | CustomEvidenceRule) -> None:
    if value is None:
        if (
            rule.applicability
            in {PreviewApplicability.NOT_APPLICABLE, PreviewApplicability.DEFERRED, PreviewApplicability.DECLARED_GAP}
            or rule.allows_null
        ):
            return
        raise PreviewRowValidationError(PreviewRowRuleId.NULLABILITY, column=rule.column)
    if rule.applicability in {PreviewApplicability.NOT_APPLICABLE, PreviewApplicability.DEFERRED}:
        raise PreviewRowValidationError(PreviewRowRuleId.APPLICABILITY, column=rule.column)
    match rule.validator:
        case PreviewValidatorKind.DECIMAL:
            valid = isinstance(value, Decimal) and value.is_finite()
        case PreviewValidatorKind.DATETIME:
            valid = isinstance(value, datetime) and value.tzinfo is not None and value.utcoffset() is not None
        case PreviewValidatorKind.ENUM:
            valid = isinstance(value, str) and bool(value)
        case PreviewValidatorKind.IDENTIFIER | PreviewValidatorKind.TEXT:
            valid = isinstance(value, str) and bool(value.strip())
        case PreviewValidatorKind.JSON:
            valid = isinstance(value, str)
    if not valid:
        raise PreviewRowValidationError(PreviewRowRuleId.TYPE, column=rule.column)
    if rule.validator is PreviewValidatorKind.JSON:
        if not isinstance(value, str):
            raise PreviewRowValidationError(PreviewRowRuleId.TYPE, column=rule.column)
        try:
            parsed = json.loads(value)
        except (TypeError, ValueError) as exc:
            raise PreviewRowValidationError(PreviewRowRuleId.TYPE, column=rule.column) from exc
        if not isinstance(parsed, dict) or _canonical_json(parsed) != value:
            raise PreviewRowValidationError(PreviewRowRuleId.TYPE, column=rule.column)
    if rule.allowed_values is not None and value not in rule.allowed_values:
        raise PreviewRowValidationError(PreviewRowRuleId.ALLOWED_VALUE, column=rule.column)


def _rule_gap_ownership(
    target_rules: tuple[FocusColumnRule, ...],
    custom_rules: tuple[CustomEvidenceRule, ...],
) -> dict[tuple[str, str], list[str]]:
    ownership: dict[tuple[str, str], list[str]] = {}
    all_rules: tuple[FocusColumnRule | CustomEvidenceRule, ...] = (*target_rules, *custom_rules)
    for rule in all_rules:
        if rule.applicability not in {
            PreviewApplicability.DEFERRED,
            PreviewApplicability.DECLARED_GAP,
        }:
            if rule.gap_code is not None or rule.owner_task is not None:
                raise PreviewProfileDefinitionError(f"non-gap rule has gap ownership: {rule.column}")
            continue
        if not rule.gap_code or not rule.owner_task:
            raise PreviewProfileDefinitionError(f"gap rule is incomplete: {rule.column}")
        key = (rule.gap_code, rule.owner_task)
        columns = ownership.get(key)
        if columns is None:
            columns = []
            ownership[key] = columns
        columns.append(rule.column)
    return ownership


def _manifest_gap_ownership() -> dict[tuple[str, str], list[str]]:
    ownership: dict[tuple[str, str], list[str]] = {}
    seen_columns: set[str] = set()
    for gap in KNOWN_GAPS:
        key = (gap.code, gap.owner_task)
        if key in ownership or not gap.code or not gap.owner_task or not gap.columns:
            raise PreviewProfileDefinitionError("manifest gap ownership is invalid")
        if seen_columns.intersection(gap.columns):
            raise PreviewProfileDefinitionError("manifest gap columns overlap")
        ownership[key] = list(gap.columns)
        seen_columns.update(gap.columns)
    return ownership


def validate_preview_row(
    *,
    row: PreviewFullRow,
    target_rules: tuple[FocusColumnRule, ...],
    custom_rules: tuple[CustomEvidenceRule, ...],
) -> None:
    if len(row.target_values) != len(target_rules) or len(row.custom_values) != len(custom_rules):
        raise PreviewRowValidationError(PreviewRowRuleId.COLUMN_COUNT)
    for value, rule in zip(row.target_values, target_rules, strict=True):
        _validate_cell(value, rule)
    for value, custom_rule in zip(row.custom_values, custom_rules, strict=True):
        _validate_cell(value, custom_rule)
    values = dict(zip(FOCUS_1_4_FULL_COLUMNS, row.target_values, strict=True))
    custom_values = dict(zip(CUSTOM_EVIDENCE_COLUMNS, row.custom_values, strict=True))
    all_rules: tuple[FocusColumnRule | CustomEvidenceRule, ...] = (*target_rules, *custom_rules)

    try:
        rule_gap_ownership = _rule_gap_ownership(target_rules, custom_rules)
        manifest_gap_ownership = _manifest_gap_ownership()
    except PreviewProfileDefinitionError as exc:
        column = next(
            (
                rule.column
                for rule in all_rules
                if rule.applicability in {PreviewApplicability.DEFERRED, PreviewApplicability.DECLARED_GAP}
                and (not rule.gap_code or not rule.owner_task)
            ),
            None,
        )
        raise PreviewRowValidationError(PreviewRowRuleId.GAP_COVERAGE, column=column) from exc
    if rule_gap_ownership != manifest_gap_ownership:
        authoritative = {column: key for key, columns in manifest_gap_ownership.items() for column in columns}
        column = next(
            (
                rule.column
                for rule in all_rules
                if authoritative.get(rule.column)
                != (
                    (rule.gap_code, rule.owner_task)
                    if rule.applicability in {PreviewApplicability.DEFERRED, PreviewApplicability.DECLARED_GAP}
                    else None
                )
            ),
            None,
        )
        raise PreviewRowValidationError(PreviewRowRuleId.GAP_COVERAGE, column=column)
    for column in ("BillingCurrency", "RegionName"):
        if values[column] is not None:
            raise PreviewRowValidationError(PreviewRowRuleId.GAP_COVERAGE, column=column)

    dependent_groups = (
        ("ConsumedQuantity", "ConsumedUnit"),
        ("PricingQuantity", "PricingUnit"),
    )
    for left, right in dependent_groups:
        if (values[left] is None) != (values[right] is None):
            missing = left if values[left] is None else right
            raise PreviewRowValidationError(PreviewRowRuleId.DEPENDENT_FIELDS, column=missing)
    consumption_is_emitted = values["ConsumedQuantity"] is not None
    if values["ChargeCategory"] == "Usage" and values["SkuPriceId"] is not None and not consumption_is_emitted:
        raise PreviewRowValidationError(PreviewRowRuleId.DEPENDENT_FIELDS, column="ConsumedQuantity")
    if values["ChargeCategory"] != "Usage" and consumption_is_emitted:
        raise PreviewRowValidationError(PreviewRowRuleId.DEPENDENT_FIELDS, column="ConsumedQuantity")
    if values["SubAccountId"] is not None:
        for column in ("SubAccountName", "SubAccountType"):
            if values[column] is None:
                raise PreviewRowValidationError(PreviewRowRuleId.DEPENDENT_FIELDS, column=column)
    elif values["SubAccountName"] is not None or values["SubAccountType"] is not None:
        raise PreviewRowValidationError(PreviewRowRuleId.DEPENDENT_FIELDS, column="SubAccountId")
    if values["ResourceId"] is not None and values["ResourceType"] is None:
        raise PreviewRowValidationError(PreviewRowRuleId.DEPENDENT_FIELDS, column="ResourceType")
    if values["ResourceId"] is None and any(values[column] is not None for column in ("ResourceName", "ResourceType")):
        raise PreviewRowValidationError(PreviewRowRuleId.DEPENDENT_FIELDS, column="ResourceId")
    sku_columns = ("SkuId", "SkuMeter", "SkuPriceDetails", "SkuPriceId")
    pricing_is_emitted = any(
        values[column] is not None
        for column in (
            "ListUnitPrice",
            "PricingCategory",
            "PricingCurrencyListUnitPrice",
            "PricingQuantity",
            "PricingUnit",
        )
    )
    if pricing_is_emitted and values["SkuId"] is None:
        raise PreviewRowValidationError(PreviewRowRuleId.DEPENDENT_FIELDS, column="SkuId")
    if any(values[column] is not None for column in sku_columns):
        for column in sku_columns:
            if values[column] is None:
                raise PreviewRowValidationError(PreviewRowRuleId.DEPENDENT_FIELDS, column=column)
        if custom_values["x_ChitraguptaSkuComponents"] is None:
            raise PreviewRowValidationError(
                PreviewRowRuleId.DEPENDENT_FIELDS,
                column="x_ChitraguptaSkuComponents",
            )

    financial = row.financials
    expected = {
        "BilledCost": financial.billed_cost,
        "ContractedCost": financial.contracted_cost,
        "EffectiveCost": financial.effective_cost,
        "ListCost": financial.list_cost,
        "ListUnitPrice": financial.list_unit_price,
        "PricingCurrencyEffectiveCost": financial.pricing_currency_effective_cost,
        "PricingCurrencyListUnitPrice": financial.pricing_currency_list_unit_price,
        "PricingQuantity": financial.pricing_quantity,
        "PricingUnit": financial.pricing_unit,
        "ConsumedQuantity": financial.consumed_quantity,
        "ConsumedUnit": financial.consumed_unit,
    }
    for column, expected_value in expected.items():
        if values[column] != expected_value:
            raise PreviewRowValidationError(PreviewRowRuleId.FINANCIAL_PROJECTION, column=column)

    billing_start = values["BillingPeriodStart"]
    billing_end = values["BillingPeriodEnd"]
    charge_start = values["ChargePeriodStart"]
    charge_end = values["ChargePeriodEnd"]
    assert isinstance(billing_start, datetime)
    assert isinstance(billing_end, datetime)
    assert isinstance(charge_start, datetime)
    assert isinstance(charge_end, datetime)
    expected_billing_start, expected_billing_end = _billing_bounds(billing_start)
    if billing_start != expected_billing_start:
        raise PreviewRowValidationError(PreviewRowRuleId.PERIOD_CONTAINMENT, column="BillingPeriodStart")
    if billing_end != expected_billing_end:
        raise PreviewRowValidationError(PreviewRowRuleId.PERIOD_CONTAINMENT, column="BillingPeriodEnd")
    if charge_start < billing_start:
        raise PreviewRowValidationError(PreviewRowRuleId.PERIOD_CONTAINMENT, column="ChargePeriodStart")
    if charge_end <= charge_start or charge_end > billing_end:
        raise PreviewRowValidationError(PreviewRowRuleId.PERIOD_CONTAINMENT, column="ChargePeriodEnd")

    if values["SkuId"] is not None:
        components_value = custom_values["x_ChitraguptaSkuComponents"]
        assert isinstance(components_value, str)
        components = json.loads(components_value)
        sku = components.get("sku")
        sku_price = components.get("sku_price")
        if (
            set(components) != {"schema_version", "sku", "sku_price"}
            or components.get("schema_version") != "v1"
            or not isinstance(sku, dict)
            or not isinstance(sku_price, dict)
            or set(sku) != {"line_type", "product"}
            or set(sku_price)
            != {
                "cloud",
                "line_type",
                "network_access_type",
                "product",
                "region",
                "resource_type",
                "tier_dimensions",
            }
        ):
            raise PreviewRowValidationError(
                PreviewRowRuleId.DERIVED_KEY,
                column="x_ChitraguptaSkuComponents",
            )
        authoritative_sku = {
            "line_type": custom_values["x_ConfluentLineType"],
            "product": custom_values["x_ConfluentProduct"],
        }
        authoritative_price_components = {
            "cloud": values["HostProviderName"],
            "line_type": custom_values["x_ConfluentLineType"],
            "network_access_type": custom_values["x_ConfluentNetworkAccessType"],
            "product": custom_values["x_ConfluentProduct"],
            "region": values["RegionId"],
            "resource_type": values["ResourceType"],
        }
        if (
            sku != authoritative_sku
            or {component: sku_price[component] for component in authoritative_price_components}
            != authoritative_price_components
        ):
            raise PreviewRowValidationError(
                PreviewRowRuleId.DERIVED_KEY,
                column="x_ChitraguptaSkuComponents",
            )

        tier_value = custom_values["x_ConfluentTierDimensions"]
        assert isinstance(tier_value, str)
        tier_evidence = json.loads(tier_value)
        component_tiers = sku_price["tier_dimensions"]
        expected_component_tiers = [list(item) for item in tier_evidence.items()]
        if component_tiers != expected_component_tiers:
            pricing_quantity = values["PricingQuantity"]

            def contains_pricing_quantity(tiers: object) -> bool:
                if not isinstance(pricing_quantity, Decimal) or not isinstance(tiers, list):
                    return False
                try:
                    bounds = dict(tiers)
                    lower_bound = Decimal(bounds["lower_bound"])
                    upper_bound = Decimal(bounds["upper_bound"])
                except KeyError, TypeError, ValueError:
                    return False
                return lower_bound <= pricing_quantity < upper_bound

            column = (
                "x_ConfluentTierDimensions"
                if contains_pricing_quantity(component_tiers)
                and not contains_pricing_quantity(expected_component_tiers)
                else "x_ChitraguptaSkuComponents"
            )
            raise PreviewRowValidationError(PreviewRowRuleId.DERIVED_KEY, column=column)

        derived_checks = {
            "SkuId": values["SkuId"] == _hash_key("sku", sku),
            "SkuPriceId": values["SkuPriceId"] == _hash_key("sku-price", sku_price),
            "SkuPriceDetails": values["SkuPriceDetails"] == _canonical_json(sku_price),
        }
        for column, valid in derived_checks.items():
            if not valid:
                raise PreviewRowValidationError(PreviewRowRuleId.DERIVED_KEY, column=column)
        if values["SkuMeter"] != values["PricingUnit"]:
            raise PreviewRowValidationError(PreviewRowRuleId.DERIVED_KEY, column="SkuMeter")

    if custom_values["x_ChitraguptaMappingProfileVersion"] != MAPPING_PROFILE_VERSION:
        raise PreviewRowValidationError(
            PreviewRowRuleId.ALLOWED_VALUE,
            column="x_ChitraguptaMappingProfileVersion",
        )

    billing_scope = custom_values["x_ChitraguptaBillingScopeId"]
    billing_account_id = values["BillingAccountId"]
    assert isinstance(billing_account_id, str)
    assert isinstance(billing_start, datetime)
    expected_billing_scope = _hash_key(
        "billing-scope",
        {
            "billing_account_id": billing_account_id,
            "billing_period_start": _utc(billing_start),
        },
    )
    if billing_scope != expected_billing_scope:
        raise PreviewRowValidationError(
            PreviewRowRuleId.DERIVED_KEY,
            column="x_ChitraguptaBillingScopeId",
        )

    for column in ("InvoiceId", "InvoiceDetailId", "InvoiceIssuerName"):
        if values[column] is not None:
            raise PreviewRowValidationError(PreviewRowRuleId.INVOICE_SEPARATION, column=column)


def _hash_key(namespace: str, payload: object) -> str:
    return f"chitragupta:confluent-cloud:{namespace}:v1:{hashlib.sha256(_canonical_json(payload).encode()).hexdigest()}"


def _billing_bounds(value: datetime) -> tuple[datetime, datetime]:
    start = value.astimezone(UTC).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    end = start.replace(year=start.year + 1, month=1) if start.month == 12 else start.replace(month=start.month + 1)
    return start, end


def _project_daily_full_row(
    *,
    evidence: SelectedPreviewEvidence,
    provider_context: PreviewProviderContext,
    resource_context: PreviewResourceContext,
    identity: Identity | Resource | None,
    environment: Resource | None,
    origin_tags_json: str = "{}",
    allocated_tags_json: str | None = None,
) -> dict[str, PreviewCell]:
    source = evidence.selected.source
    semantics = evidence.selected.semantics
    financials = evidence.selected.financials
    allocation = evidence.allocation
    allocated_resource_id = None if allocation.target_kind == "unallocated" else allocation.target_id
    service = FOCUS_1_4_SERVICE_RULES_V1[semantics.service_rule_key]
    assert source.source_period_start is not None and source.source_period_end is not None
    assert source.amount is not None
    billing_start, billing_end = _billing_bounds(source.source_period_start)
    sku_payload = {"line_type": source.native_line_type, "product": source.native_product}
    price_payload = {
        "cloud": resource_context.host_provider_code,
        "line_type": source.native_line_type,
        "network_access_type": source.native_network_access_type,
        "product": source.native_product,
        "region": resource_context.region_id,
        "resource_type": resource_context.resource_type,
        "tier_dimensions": [list(item) for item in source.native_tier_dimensions],
    }
    sku_id = _hash_key("sku", sku_payload) if semantics.emits_pricing else None
    sku_price_id = _hash_key("sku-price", price_payload) if semantics.emits_pricing else None
    sku_components = (
        _canonical_json({"schema_version": "v1", "sku": sku_payload, "sku_price": price_payload})
        if semantics.emits_pricing
        else None
    )
    billing_scope = _hash_key(
        "billing-scope",
        {"billing_account_id": provider_context.billing_account_id, "billing_period_start": _utc(billing_start)},
    )
    row: dict[str, PreviewCell] = {column: None for column in (*FOCUS_1_4_FULL_COLUMNS, *CUSTOM_EVIDENCE_COLUMNS)}
    row.update(
        {
            "AllocatedMethodId": allocation.method_id or allocation.allocation_method,
            "AllocatedMethodDetails": allocation.method_details_json or None,
            "AllocatedResourceId": allocated_resource_id,
            "AllocatedResourceName": identity.display_name if identity is not None else None,
            "AllocatedTags": allocated_tags_json,
            "BilledCost": financials.billed_cost,
            "BillingAccountId": provider_context.billing_account_id,
            "BillingAccountName": provider_context.billing_account_name,
            "BillingAccountType": "Organization",
            "BillingPeriodEnd": billing_end,
            "BillingPeriodStart": billing_start,
            "ChargeCategory": semantics.charge_category,
            "ChargeDescription": source.native_description,
            "ChargeFrequency": semantics.charge_frequency,
            "ChargePeriodEnd": source.source_period_end,
            "ChargePeriodStart": source.source_period_start,
            "ConsumedQuantity": financials.consumed_quantity,
            "ConsumedUnit": financials.consumed_unit,
            "ContractedCost": financials.contracted_cost,
            "EffectiveCost": financials.effective_cost,
            "HostProviderName": resource_context.host_provider_code,
            "ListCost": financials.list_cost,
            "ListUnitPrice": financials.list_unit_price,
            "PricingCategory": "Standard" if semantics.emits_pricing else None,
            "PricingCurrency": "USD",
            "PricingCurrencyEffectiveCost": financials.pricing_currency_effective_cost,
            "PricingCurrencyListUnitPrice": financials.pricing_currency_list_unit_price,
            "PricingQuantity": financials.pricing_quantity,
            "PricingUnit": financials.pricing_unit,
            "RegionId": resource_context.region_id,
            "ResourceId": resource_context.resource_id,
            "ResourceName": resource_context.resource_name,
            "ResourceType": resource_context.resource_type,
            "ServiceProviderName": "Confluent Cloud",
            "ServiceCategory": service.service_category,
            "ServiceName": service.service_name,
            "ServiceSubcategory": service.service_subcategory,
            "SkuId": sku_id,
            "SkuMeter": financials.pricing_unit,
            "SkuPriceDetails": _canonical_json(price_payload) if semantics.emits_pricing else None,
            "SkuPriceId": sku_price_id,
            "SubAccountId": source.environment_id,
            "SubAccountName": environment.display_name if environment is not None else None,
            "SubAccountType": "Environment" if source.environment_id is not None else None,
            "Tags": origin_tags_json,
            "x_ChitraguptaSourceCostId": source.provider_cost_id or source.source_record_id,
            "x_ChitraguptaBillingScopeId": billing_scope,
            "x_ChitraguptaAllocationRatio": allocation.allocation_ratio,
            "x_ChitraguptaAllocationMethodVersion": allocation.method_version or None,
            "x_ChitraguptaMappingProfileVersion": MAPPING_PROFILE_VERSION,
            "x_ChitraguptaSkuComponents": sku_components,
            "x_ConfluentProduct": source.native_product,
            "x_ConfluentLineType": source.native_line_type,
            "x_ConfluentDescription": source.native_description,
            "x_ConfluentDiscountAmount": source.discount_amount,
            "x_ConfluentNetworkAccessType": source.native_network_access_type,
            "x_ConfluentTierDimensions": _canonical_json(dict(source.native_tier_dimensions)),
        }
    )
    return row


def project_daily_portion_full_row(
    *,
    prepared: PreparedPreviewPackageRow,
    provider_context: PreviewProviderContext,
) -> PreviewFullRow:
    values = _project_daily_full_row(
        evidence=prepared.evidence,
        provider_context=provider_context,
        resource_context=prepared.resource_context,
        identity=prepared.allocated_entity,
        environment=prepared.environment,
        origin_tags_json=prepared.origin_tags_json,
        allocated_tags_json=prepared.allocated_tags_json,
    )
    source = prepared.evidence.selected.source
    allocation = prepared.evidence.allocation
    source_id = source.provider_cost_id or source.source_record_id
    member = PreviewLineageMember(
        source_cost_id=source_id,
        calculation_id=allocation.calculation_id,
        origin_timestamp=allocation.timestamp,
        origin_environment_id=allocation.environment_id,
        origin_resource_id=allocation.resource_id,
        origin_product_type=allocation.native_product,
        origin_product_category=allocation.native_line_type,
        portion_ordinal=allocation.portion_ordinal,
    )
    row = PreviewFullRow(
        target_values=tuple(values[column] for column in FOCUS_1_4_FULL_COLUMNS),
        custom_values=tuple(values[column] for column in CUSTOM_EVIDENCE_COLUMNS),
        financials=prepared.evidence.selected.financials,
        lineage_members=(member,),
    )
    validate_preview_row(row=row, target_rules=FOCUS_1_4_COLUMN_RULES, custom_rules=CUSTOM_EVIDENCE_RULES)
    return row


@dataclass(frozen=True)
class PreviewPackageReconciliation:
    source_records: int
    source_cost: Decimal
    allocated_cost: Decimal
    source_quantity: Decimal
    allocated_quantity: Decimal


@dataclass(frozen=True)
class PreviewDataPackageDraft:
    data_files: tuple[PreviewArtifactPayload, ...]
    source_records: int
    rows: int
    reconciliation: PreviewPackageReconciliation
    logical_data_sha256: str
    _workspace: PreviewGenerationWorkspace | None = field(default=None, repr=False, compare=False)

    def close(self) -> None:
        if self._workspace is not None:
            self._workspace.close()


def _preview_csv_record(values: Iterable[object]) -> bytes:
    buffer = io.StringIO(newline="")
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(values)
    return buffer.getvalue().encode()


def build_preview_data_package(
    *,
    request: PreviewRequest,
    snapshot: PreviewSourceSnapshot,
    full_rows: Iterable[PreviewFullRow],
    reconciliation: PreviewPackageReconciliation,
    max_csv_file_bytes: int | None,
) -> PreviewDataPackageDraft:
    validate_preview_effective_columns(request.column_profile, request.effective_columns)
    validate_preview_request_snapshot(
        request=request,
        snapshot=snapshot,
        resulting_status=PreviewRequestStatus.READY,
        mode="candidate_ready",
    )
    rows = sorted(
        full_rows,
        key=lambda row: tuple(preview_serialize_cell(value) for value in (*row.target_values, *row.custom_values)),
    )
    header = _preview_csv_record(request.effective_columns)
    logical_hasher = hashlib.sha256(header)
    if max_csv_file_bytes is not None and (max_csv_file_bytes <= 0 or len(header) > max_csv_file_bytes):
        raise PreviewCsvFileSizeError("A Preview CSV header or row exceeds the configured file-size limit.")
    if reconciliation.source_records < 0:
        raise PreviewMappingError("source record count cannot be negative")
    if reconciliation.source_records == 0:
        if (
            reconciliation.source_cost != 0
            or reconciliation.allocated_cost != 0
            or rows
            or snapshot.source_through is not None
        ):
            raise PreviewMappingError("zero-source package state is inconsistent")
    elif not rows or snapshot.source_through is None:
        raise PreviewMappingError("positive-source package state is inconsistent")

    all_columns = FOCUS_1_4_FULL_PROFILE_COLUMNS
    parts: list[bytes] = []
    current = bytearray(header)
    for row in rows:
        values = dict(zip(all_columns, (*row.target_values, *row.custom_values), strict=True))
        record = _preview_csv_record(preview_serialize_cell(values[column]) for column in request.effective_columns)
        logical_hasher.update(record)
        if max_csv_file_bytes is not None and len(header) + len(record) > max_csv_file_bytes:
            raise PreviewCsvFileSizeError("A Preview CSV header or row exceeds the configured file-size limit.")
        if max_csv_file_bytes is not None and len(current) + len(record) > max_csv_file_bytes:
            parts.append(bytes(current))
            current = bytearray(header)
        current.extend(record)
    parts.append(bytes(current))
    part_count = len(parts)
    width = max(5, len(str(part_count)))
    data_files = tuple(
        PreviewArtifactPayload(
            name=(
                "cost-and-usage.csv"
                if part_count == 1
                else f"cost-and-usage-part-{index:0{width}d}-of-{part_count:0{width}d}.csv"
            ),
            media_type="text/csv",
            order=index,
            body=body,
        )
        for index, body in enumerate(parts, start=1)
    )
    return PreviewDataPackageDraft(
        data_files=data_files,
        source_records=reconciliation.source_records,
        rows=len(rows),
        reconciliation=reconciliation,
        logical_data_sha256=logical_hasher.hexdigest(),
    )


def build_bounded_preview_data_package(
    *,
    request: PreviewRequest,
    snapshot: PreviewSourceSnapshot,
    full_rows: Iterable[PreviewFullRow],
    reconciliation: PreviewPackageReconciliation,
    max_csv_file_bytes: int | None,
    max_generation_spool_bytes: int,
    workspace: PreviewGenerationWorkspace | None = None,
) -> PreviewDataPackageDraft:
    """Render a package through an external SQLite order and disk-backed parts."""

    validate_preview_effective_columns(request.column_profile, request.effective_columns)
    validate_preview_request_snapshot(
        request=request,
        snapshot=snapshot,
        resulting_status=PreviewRequestStatus.READY,
        mode="candidate_ready",
    )
    header = _preview_csv_record(request.effective_columns)
    if max_csv_file_bytes is not None and (max_csv_file_bytes <= 0 or len(header) > max_csv_file_bytes):
        raise PreviewCsvFileSizeError("A Preview CSV header or row exceeds the configured file-size limit.")
    if reconciliation.source_records < 0:
        raise PreviewMappingError("source record count cannot be negative")

    workspace = workspace or PreviewGenerationWorkspace(max_generation_spool_bytes)
    database_path = workspace.root / "projection.sqlite"
    all_columns = FOCUS_1_4_FULL_PROFILE_COLUMNS
    sort_column_count = len(all_columns)
    sort_columns = tuple(f"sort_{index}" for index in range(sort_column_count))
    quoted_sort_columns = ", ".join(f'"{column}" TEXT NOT NULL' for column in sort_columns)
    insert_columns = ", ".join((*sort_columns, "encounter_ordinal", "record"))
    placeholders = ", ".join("?" for _ in range(sort_column_count + 2))
    order_columns = ", ".join((*sort_columns, "encounter_ordinal"))
    row_count = 0
    try:
        with workspace.sqlite_connection(database_path) as connection:
            connection.execute(
                f"""
                CREATE TABLE ordered_rows (
                    {quoted_sort_columns},
                    encounter_ordinal INTEGER NOT NULL,
                    record BLOB NOT NULL,
                    PRIMARY KEY ({order_columns})
                ) WITHOUT ROWID
                """
            )
            connection.execute(
                """
                CREATE TABLE artifacts (
                    file_order INTEGER PRIMARY KEY,
                    name TEXT NOT NULL UNIQUE,
                    media_type TEXT NOT NULL,
                    path TEXT NOT NULL,
                    size_bytes INTEGER NOT NULL,
                    sha256 TEXT NOT NULL
                )
                """
            )
            pending_rows = 0
            for row_count, row in enumerate(full_rows, start=1):
                serialized = tuple(preview_serialize_cell(value) for value in (*row.target_values, *row.custom_values))
                values = dict(zip(all_columns, (*row.target_values, *row.custom_values), strict=True))
                record = _preview_csv_record(
                    preview_serialize_cell(values[column]) for column in request.effective_columns
                )
                if max_csv_file_bytes is not None and len(header) + len(record) > max_csv_file_bytes:
                    raise PreviewCsvFileSizeError("A Preview CSV header or row exceeds the configured file-size limit.")
                workspace.preflight_write(len(record) + sum(len(value.encode()) for value in serialized))
                connection.execute(
                    f"INSERT INTO ordered_rows ({insert_columns}) VALUES ({placeholders})",
                    (*serialized, row_count, record),
                )
                pending_rows += 1
                if pending_rows == SQLITE_BATCH_SIZE:
                    connection.commit()
                    workspace.enforce_limit()
                    pending_rows = 0
            if pending_rows:
                connection.commit()
                workspace.enforce_limit()
            if reconciliation.source_records == 0:
                if (
                    reconciliation.source_cost != 0
                    or reconciliation.allocated_cost != 0
                    or row_count
                    or snapshot.source_through is not None
                ):
                    raise PreviewMappingError("zero-source package state is inconsistent")
            elif not row_count or snapshot.source_through is None:
                raise PreviewMappingError("positive-source package state is inconsistent")

            logical_hasher = hashlib.sha256(header)
            part_handle = None
            part_hasher = hashlib.sha256()
            part_size = 0
            part_count = 0
            current_part_path: Path | None = None

            def open_part() -> None:
                nonlocal current_part_path, part_count, part_handle, part_hasher, part_size
                part_count += 1
                current_part_path = workspace.root / f"part-{part_count:020d}.tmp"
                part_handle = current_part_path.open("xb")
                workspace.record_write(len(header))
                part_handle.write(header)
                part_hasher = hashlib.sha256(header)
                part_size = len(header)

            def close_part() -> None:
                nonlocal part_handle
                assert part_handle is not None
                assert current_part_path is not None
                part_handle.flush()
                part_handle.close()
                part_handle = None
                database_size = workspace.constrain_sqlite_growth(connection, database_path)
                connection.execute(
                    """
                    INSERT INTO artifacts (file_order, name, media_type, path, size_bytes, sha256)
                    VALUES (?, ?, 'text/csv', ?, ?, ?)
                    """,
                    (
                        part_count,
                        current_part_path.name,
                        str(current_part_path),
                        part_size,
                        part_hasher.hexdigest(),
                    ),
                )
                connection.commit()
                database_growth = database_path.stat().st_size - database_size
                if database_growth > 0:
                    workspace.record_write(database_growth)

            open_part()
            cursor = connection.execute(f"SELECT record FROM ordered_rows ORDER BY {order_columns}")
            for (record_value,) in cursor:
                record = bytes(record_value)
                logical_hasher.update(record)
                if max_csv_file_bytes is not None and part_size + len(record) > max_csv_file_bytes:
                    close_part()
                    open_part()
                assert part_handle is not None
                workspace.record_write(len(record))
                part_handle.write(record)
                part_hasher.update(record)
                part_size += len(record)
            close_part()

            width = max(5, len(str(part_count)))
            artifacts = connection.execute("SELECT file_order, path FROM artifacts ORDER BY file_order")
            for index, path_text in artifacts:
                name = (
                    "cost-and-usage.csv"
                    if part_count == 1
                    else f"cost-and-usage-part-{index:0{width}d}-of-{part_count:0{width}d}.csv"
                )
                path = Path(str(path_text))
                final_path = workspace.root / name
                path.rename(final_path)
                connection.execute(
                    """
                    UPDATE artifacts
                    SET name = ?, path = ?
                    WHERE file_order = ?
                    """,
                    (name, str(final_path), index),
                )
            workspace.constrain_sqlite_growth(connection, database_path)
            connection.execute("DROP TABLE ordered_rows")
            connection.commit()
            workspace.enforce_limit()

        if row_count == 0:
            body = (workspace.root / "cost-and-usage.csv").read_bytes()
            return PreviewDataPackageDraft(
                data_files=(PreviewArtifactPayload("cost-and-usage.csv", "text/csv", 1, body),),
                source_records=reconciliation.source_records,
                rows=0,
                reconciliation=reconciliation,
                logical_data_sha256=logical_hasher.hexdigest(),
                _workspace=workspace,
            )

        data_files = PreviewSpooledArtifactCollection(database_path)
        return PreviewDataPackageDraft(
            data_files=cast("tuple[PreviewArtifactPayload, ...]", data_files),
            source_records=reconciliation.source_records,
            rows=row_count,
            reconciliation=reconciliation,
            logical_data_sha256=logical_hasher.hexdigest(),
            _workspace=workspace,
        )
    except BaseException:
        workspace.close()
        raise


def preview_revision_content_sha256(
    *,
    logical_data_sha256: str,
    mapping_profile_version: str = MAPPING_PROFILE_VERSION,
    target_focus_version: str = "1.4",
    column_profile: PreviewColumnProfile = "full",
    effective_columns: tuple[str, ...] = FOCUS_1_4_FULL_PROFILE_COLUMNS,
) -> str:
    if re.fullmatch(r"[0-9a-f]{64}", logical_data_sha256) is None:
        raise PreviewMappingError("logical_data_sha256 must be canonical lowercase hexadecimal")
    preimage = {
        "mapping_profile_version": mapping_profile_version,
        "target_focus_version": target_focus_version,
        "column_profile": column_profile,
        "effective_columns": list(effective_columns),
        "logical_data_sha256": logical_data_sha256,
    }
    return hashlib.sha256(preview_canonical_json(preimage).encode()).hexdigest()


def _validate_rendered_file_metadata(
    draft: PreviewDataPackageDraft,
    files: Iterable[PreviewArtifactMetadata],
) -> None:
    missing = object()
    for payload, metadata in zip_longest(draft.data_files, files, fillvalue=missing):
        if payload is missing or metadata is missing:
            raise PreviewMappingError("artifact metadata does not match rendered package bytes")
        assert isinstance(payload, PreviewArtifactPayload)
        assert isinstance(metadata, PreviewArtifactMetadata)
        size_bytes, sha256 = spooled_body_metadata(payload.body)
        if metadata != PreviewArtifactMetadata(
            name=payload.name,
            media_type=payload.media_type,
            size_bytes=size_bytes,
            sha256=sha256,
            order=payload.order,
        ):
            raise PreviewMappingError("artifact metadata does not match rendered package bytes")


def _render_manifest(
    manifest: Mapping[str, object],
    *,
    files: Iterable[PreviewArtifactMetadata],
    workspace: PreviewGenerationWorkspace | None,
) -> bytes:
    if workspace is None:
        return (
            preview_canonical_json(
                {
                    **manifest,
                    "files": [
                        {
                            "name": item.name,
                            "media_type": item.media_type,
                            "size_bytes": item.size_bytes,
                            "sha256": item.sha256,
                            "order": item.order,
                        }
                        for item in files
                    ],
                }
            )
            + "\n"
        ).encode()

    manifest_path = workspace.root / "manifest.json.tmp"
    keys = sorted((*manifest, "files"))
    digest = hashlib.sha256()
    size_bytes = 0
    with manifest_path.open("xb") as handle:

        def write(value: bytes) -> None:
            nonlocal size_bytes
            workspace.record_write(len(value))
            handle.write(value)
            digest.update(value)
            size_bytes += len(value)

        write(b"{")
        for key_index, key in enumerate(keys):
            if key_index:
                write(b",")
            write(json.dumps(key, separators=(",", ":")).encode())
            write(b":")
            if key != "files":
                write(preview_canonical_json(manifest[key]).encode())
                continue
            write(b"[")
            for file_index, item in enumerate(files):
                if file_index:
                    write(b",")
                write(
                    preview_canonical_json(
                        {
                            "name": item.name,
                            "media_type": item.media_type,
                            "size_bytes": item.size_bytes,
                            "sha256": item.sha256,
                            "order": item.order,
                        }
                    ).encode()
                )
            write(b"]")
        write(b"}\n")
        handle.flush()
    workspace.enforce_limit()
    return cast("bytes", PreviewSpooledBody(manifest_path, size_bytes, digest.hexdigest()))


def build_requested_preview_manifest(
    *,
    request: PreviewRequest,
    snapshot: PreviewSourceSnapshot,
    draft: PreviewDataPackageDraft,
    files: tuple[PreviewArtifactMetadata, ...],
    ready_at: datetime,
    expires_at: datetime,
) -> bytes:
    if ready_at.tzinfo is None or ready_at.utcoffset() is None:
        raise PreviewMappingError("ready_at must be timezone-aware")
    if expires_at != ready_at + timedelta(days=7):
        raise PreviewMappingError("expires_at must be exactly seven days after ready_at")
    _validate_rendered_file_metadata(draft, files)
    reconciliation = draft.reconciliation
    evidence_start = snapshot.effective_coverage_start_date
    evidence_end = snapshot.effective_coverage_end_date
    assert evidence_start is not None and evidence_end is not None
    evidence_through = None if evidence_start == evidence_end else evidence_end - date.resolution
    source_snapshot = {
        "calculation_timestamp": (
            None if snapshot.calculation_timestamp is None else preview_utc_text(snapshot.calculation_timestamp)
        ),
        "calculation_coverage": [
            {
                "tracking_date": entry.tracking_date.isoformat(),
                "calculation_id": entry.calculation_id,
                "calculation_completed_at": preview_utc_text(entry.calculation_completed_at),
                "calculation_run_id": entry.calculation_run_id,
            }
            for entry in snapshot.calculation_coverage
        ],
        "source_through": None if snapshot.source_through is None else preview_utc_text(snapshot.source_through),
    }
    manifest = {
        "schema_version": PREVIEW_MANIFEST_SCHEMA_VERSION,
        "package_type": "requested_preview_package",
        "request_id": request.request_id,
        "tenant_name": request.tenant_name,
        "grain": request.grain,
        "start_date": request.start_date.isoformat(),
        "end_date": request.end_date.isoformat(),
        "month": preview_month(grain=request.grain, start_date=request.start_date, end_date=request.end_date),
        "column_profile": request.column_profile,
        "effective_columns": list(request.effective_columns),
        "target_focus_version": "1.4",
        "conformance_status": "non_conforming",
        "mapping_profile_version": MAPPING_PROFILE_VERSION,
        "known_gaps": preview_manifest_known_gaps(),
        "profile_not_applicable_columns": list(PROFILE_NOT_APPLICABLE_COLUMNS),
        "source_snapshot": source_snapshot,
        "evidence_coverage": {
            "start_date": evidence_start.isoformat(),
            "end_date": evidence_end.isoformat(),
            "end_exclusive": True,
            "evidence_through_date": None if evidence_through is None else evidence_through.isoformat(),
            "availability_cutoff_end_date": (
                None
                if snapshot.availability_cutoff_end_date is None
                else snapshot.availability_cutoff_end_date.isoformat()
            ),
        },
        "monthly_status": snapshot.monthly_status,
        "validation": {
            "status": "passed",
            "mapping_profile_version": MAPPING_PROFILE_VERSION,
            "source_records": draft.source_records,
            "rows": draft.rows,
            "mapping_errors": 0,
            "artifact_integrity": "passed",
        },
        "reconciliation": {
            "source_cost": preview_decimal_text(reconciliation.source_cost),
            "allocated_cost": preview_decimal_text(reconciliation.allocated_cost),
            "difference": preview_decimal_text(
                preview_subtract_decimals(reconciliation.source_cost, reconciliation.allocated_cost)
            ),
            "source_quantity": preview_decimal_text(reconciliation.source_quantity),
            "allocated_quantity": preview_decimal_text(reconciliation.allocated_quantity),
            "quantity_difference": preview_decimal_text(
                preview_subtract_decimals(reconciliation.source_quantity, reconciliation.allocated_quantity)
            ),
        },
        "lifecycle": {
            "ready_at": preview_utc_text(ready_at),
            "expires_at": preview_utc_text(expires_at),
            "retention_days": 7,
        },
        "generated_at": preview_utc_text(ready_at),
    }
    return _render_manifest(manifest, files=files, workspace=draft._workspace)


def preview_revision_source_snapshot(snapshot: PreviewSourceSnapshot) -> dict[str, object]:
    return {
        "calculation_timestamp": (
            None if snapshot.calculation_timestamp is None else preview_utc_text(snapshot.calculation_timestamp)
        ),
        "calculation_coverage": [
            {
                "tracking_date": entry.tracking_date.isoformat(),
                "calculation_id": entry.calculation_id,
                "calculation_completed_at": preview_utc_text(entry.calculation_completed_at),
                "calculation_run_id": entry.calculation_run_id,
            }
            for entry in snapshot.calculation_coverage
        ],
        "source_through": None if snapshot.source_through is None else preview_utc_text(snapshot.source_through),
        "effective_coverage_start_date": snapshot.effective_coverage_start_date.isoformat(),
        "effective_coverage_end_date": snapshot.effective_coverage_end_date.isoformat(),
        "availability_cutoff_end_date": (
            None if snapshot.availability_cutoff_end_date is None else snapshot.availability_cutoff_end_date.isoformat()
        ),
        "monthly_status": snapshot.monthly_status,
    }


def build_preview_revision_manifest(
    *,
    revision_id: str,
    tenant_name_at_publication: str,
    month: str,
    start_date: date,
    end_date: date,
    monthly_status: PreviewMonthlyStatus,
    material_sha256: str,
    supersedes_revision_id: str | None,
    snapshot: PreviewSourceSnapshot,
    draft: PreviewDataPackageDraft,
    files: tuple[PreviewArtifactMetadata, ...],
    published_at: datetime,
) -> bytes:
    preview_models.validate_preview_revision_invariant(
        month=month,
        start_date=start_date,
        end_date=end_date,
        monthly_status=monthly_status,
        source_snapshot=snapshot,
    )
    if published_at.tzinfo is None or published_at.utcoffset() is None:
        raise PreviewMappingError("published_at must be timezone-aware")
    _validate_rendered_file_metadata(draft, files)
    mapping_profile_version = MAPPING_PROFILE_VERSION
    target_focus_version = "1.4"
    column_profile: PreviewColumnProfile = "full"
    effective_columns = FOCUS_1_4_FULL_PROFILE_COLUMNS
    recomputed = preview_revision_content_sha256(
        mapping_profile_version=mapping_profile_version,
        target_focus_version=target_focus_version,
        column_profile=column_profile,
        effective_columns=effective_columns,
        logical_data_sha256=draft.logical_data_sha256,
    )
    if material_sha256 != recomputed:
        raise PreviewMappingError("revision material digest does not match canonical preimage")
    reconciliation = draft.reconciliation
    manifest = {
        "schema_version": PREVIEW_MANIFEST_SCHEMA_VERSION,
        "package_type": "published_preview_revision",
        "revision_id": revision_id,
        "tenant_name": tenant_name_at_publication,
        "grain": "monthly",
        "month": month,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "monthly_status": monthly_status,
        "supersedes_revision_id": supersedes_revision_id,
        "published_at": preview_utc_text(published_at),
        "mapping_profile_version": mapping_profile_version,
        "target_focus_version": target_focus_version,
        "column_profile": column_profile,
        "effective_columns": list(effective_columns),
        "logical_data_sha256": draft.logical_data_sha256,
        "material_sha256": material_sha256,
        "conformance_status": "non_conforming",
        "known_gaps": preview_manifest_known_gaps(),
        "source_snapshot": preview_revision_source_snapshot(snapshot),
        "validation": {
            "status": "passed",
            "mapping_profile_version": mapping_profile_version,
            "source_records": draft.source_records,
            "rows": draft.rows,
            "mapping_errors": 0,
            "artifact_integrity": "passed",
        },
        "reconciliation": {
            "source_cost": preview_decimal_text(reconciliation.source_cost),
            "allocated_cost": preview_decimal_text(reconciliation.allocated_cost),
            "difference": preview_decimal_text(
                preview_subtract_decimals(reconciliation.source_cost, reconciliation.allocated_cost)
            ),
            "source_quantity": preview_decimal_text(reconciliation.source_quantity),
            "allocated_quantity": preview_decimal_text(reconciliation.allocated_quantity),
            "quantity_difference": preview_decimal_text(
                preview_subtract_decimals(reconciliation.source_quantity, reconciliation.allocated_quantity)
            ),
        },
    }
    return _render_manifest(manifest, files=files, workspace=draft._workspace)


def _validate_profile_definition() -> None:
    if len(FOCUS_1_4_COLUMN_RULES) != 65 or len(CUSTOM_EVIDENCE_RULES) != 12:
        raise PreviewProfileDefinitionError("profile column count is invalid")
    columns = (*FOCUS_1_4_FULL_COLUMNS, *CUSTOM_EVIDENCE_COLUMNS)
    if len(columns) != len(set(columns)):
        raise PreviewProfileDefinitionError("profile columns overlap")
    if tuple(column for column, _level, _allows_null in _COLUMN_SPECS) != FOCUS_1_4_FULL_COLUMNS:
        raise PreviewProfileDefinitionError("target profile order is invalid")
    if tuple(column for column, *_rest in _CUSTOM_SPECS) != CUSTOM_EVIDENCE_COLUMNS:
        raise PreviewProfileDefinitionError("custom profile order is invalid")
    if set(_TARGET_RULE_AUTHORITIES) != set(FOCUS_1_4_FULL_COLUMNS):
        raise PreviewProfileDefinitionError("target rule authority coverage is invalid")
    if set(_CUSTOM_RULE_AUTHORITIES) != set(CUSTOM_EVIDENCE_COLUMNS):
        raise PreviewProfileDefinitionError("custom rule authority coverage is invalid")
    counts = {level: sum(rule.feature_level is level for rule in FOCUS_1_4_COLUMN_RULES) for level in FocusFeatureLevel}
    if counts != {FocusFeatureLevel.MANDATORY: 21, FocusFeatureLevel.CONDITIONAL: 40, FocusFeatureLevel.RECOMMENDED: 4}:
        raise PreviewProfileDefinitionError("profile feature counts are invalid")
    for rule in FOCUS_1_4_COLUMN_RULES:
        if (
            not rule.source
            or not rule.transformation
            or (
                rule.applicability in {PreviewApplicability.DEFERRED, PreviewApplicability.DECLARED_GAP}
                and (not rule.gap_code or not rule.owner_task)
            )
        ):
            raise PreviewProfileDefinitionError(f"profile rule is incomplete: {rule.column}")
        if rule.validator is PreviewValidatorKind.ENUM and not rule.allowed_values:
            raise PreviewProfileDefinitionError(f"enum validator requires allowed values: {rule.column}")
        if rule.allowed_values is not None and rule.validator is not PreviewValidatorKind.ENUM:
            raise PreviewProfileDefinitionError(f"allowed values require enum validator: {rule.column}")
    for custom_rule in CUSTOM_EVIDENCE_RULES:
        if (
            not custom_rule.source
            or not custom_rule.transformation
            or (
                custom_rule.applicability in {PreviewApplicability.DEFERRED, PreviewApplicability.DECLARED_GAP}
                and (not custom_rule.gap_code or not custom_rule.owner_task)
            )
        ):
            raise PreviewProfileDefinitionError(f"profile rule is incomplete: {custom_rule.column}")
        if custom_rule.validator is PreviewValidatorKind.ENUM and not custom_rule.allowed_values:
            raise PreviewProfileDefinitionError(f"enum validator requires allowed values: {custom_rule.column}")
        if custom_rule.allowed_values is not None and custom_rule.validator is not PreviewValidatorKind.ENUM:
            raise PreviewProfileDefinitionError(f"allowed values require enum validator: {custom_rule.column}")

    if _rule_gap_ownership(FOCUS_1_4_COLUMN_RULES, CUSTOM_EVIDENCE_RULES) != _manifest_gap_ownership():
        raise PreviewProfileDefinitionError("profile rules and manifest gaps disagree")

    if set(FOCUS_1_4_SERVICE_RULES_V1) != set(PreviewServiceRuleKey):
        raise PreviewProfileDefinitionError("service rule key coverage is invalid")
    expected_service_rules = {rule.key: rule for rule in _FOCUS_1_4_SERVICE_RULE_DEFINITIONS_V1}
    if len(expected_service_rules) != len(_FOCUS_1_4_SERVICE_RULE_DEFINITIONS_V1):
        raise PreviewProfileDefinitionError("service rule definitions contain duplicate keys")
    if dict(FOCUS_1_4_SERVICE_RULES_V1) != expected_service_rules:
        raise PreviewProfileDefinitionError("service rule matrix differs from its immutable v1 definition")
    line_type_owners: list[str] = []
    unsupported_context_keys: list[PreviewServiceRuleKey] = []
    for key, service_rule in FOCUS_1_4_SERVICE_RULES_V1.items():
        if key is not service_rule.key:
            raise PreviewProfileDefinitionError("service rule key disagrees with its authority key")
        line_type_owners.extend(service_rule.native_line_types)
        if service_rule.context_strategy == "organization_wide":
            if (
                service_rule.resource_shape is not PreviewResourceShape.ORGANIZATION_WIDE
                or service_rule.allowed_origin_resource_types
            ):
                raise PreviewProfileDefinitionError("organization-wide service context is invalid")
        elif service_rule.context_strategy == "unsupported_provider_context":
            unsupported_context_keys.append(key)
            if (
                service_rule.resource_shape is not PreviewResourceShape.RESOURCE_SPECIFIC
                or service_rule.allowed_origin_resource_types
            ):
                raise PreviewProfileDefinitionError("unsupported provider context rule is invalid")
        elif (
            service_rule.resource_shape is not PreviewResourceShape.RESOURCE_SPECIFIC
            or not service_rule.allowed_origin_resource_types
        ):
            raise PreviewProfileDefinitionError("resource-specific service context is invalid")
        expected_context_types: dict[PreviewContextStrategy, set[tuple[str, ...]]] = {
            "self": {("kafka_cluster",), ("schema_registry",)},
            "connector_parent_kafka": {("connector",)},
            "ksqldb_kafka_reference": {("ksqldb_cluster",)},
            "flink_pool_or_reference": {("flink_compute_pool", "flink_statement")},
            "organization_wide": {()},
            "unsupported_provider_context": {()},
        }
        if service_rule.allowed_origin_resource_types not in expected_context_types[service_rule.context_strategy]:
            raise PreviewProfileDefinitionError("service context strategy and origin types disagree")
        allowed_taxonomy_pairs = {
            ("Integration", "Messaging"),
            ("Analytics", "Streaming Analytics"),
            ("Management and Governance", "Data Governance"),
            ("Management and Governance", "Observability"),
            ("Management and Governance", "Support"),
            ("Storage", "Object Storage"),
            ("Other", "Other (Other)"),
        }
        if (service_rule.service_category, service_rule.service_subcategory) not in allowed_taxonomy_pairs:
            raise PreviewProfileDefinitionError("service category and subcategory disagree")
    if len(line_type_owners) != len(set(line_type_owners)):
        raise PreviewProfileDefinitionError("native line types have multiple service owners")
    if tuple(line_type_owners) != _ACCEPTED_PROVIDER_LINE_TYPES:
        raise PreviewProfileDefinitionError("accepted native line type coverage is invalid")
    expected_readiness = {
        **{line_type: PreviewLineageReadiness.READY for line_type in _READY_NATIVE_LINE_TYPES_V1},
        **{line_type: PreviewLineageReadiness.READY for line_type in _TASK_254_05_NATIVE_LINE_TYPES_V1},
    }
    if dict(FOCUS_1_4_NATIVE_LINE_READINESS_V1) != expected_readiness or set(expected_readiness) != set(
        line_type_owners
    ):
        raise PreviewProfileDefinitionError("native line readiness authority is invalid")
    if unsupported_context_keys != [PreviewServiceRuleKey.TABLEFLOW]:
        raise PreviewProfileDefinitionError("TABLEFLOW must be the sole unsupported provider context")
    if set(NATIVE_PRODUCT_SERVICE_RULES_V1) != set(_PRODUCT_KEYS):
        raise PreviewProfileDefinitionError("native product authority coverage is invalid")
    for product, product_rule in NATIVE_PRODUCT_SERVICE_RULES_V1.items():
        expected_category = "Purchase" if product_rule.service_rule_key is PreviewServiceRuleKey.SUPPORT else "Usage"
        expected_frequency = (
            "Recurring" if product_rule.service_rule_key is PreviewServiceRuleKey.SUPPORT else "Usage-Based"
        )
        if (
            product != product_rule.native_product
            or product_rule.service_rule_key not in FOCUS_1_4_SERVICE_RULES_V1
            or _PRODUCT_KEYS.get(product) is not product_rule.service_rule_key
            or product_rule.original_category != expected_category
            or product_rule.original_frequency != expected_frequency
        ):
            raise PreviewProfileDefinitionError("native product authority is inconsistent")


_validate_profile_definition()
