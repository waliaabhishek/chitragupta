from __future__ import annotations

import csv
import hashlib
import io
import json
import logging
import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Context, Decimal, localcontext
from enum import StrEnum
from types import MappingProxyType
from typing import Literal

from core.models.identity import Identity  # noqa: TC001 - resolved by contract tests
from core.models.resource import Resource  # noqa: TC001 - resolved by contract tests
from core.preview.evidence import (  # noqa: TC001 - resolved by contract tests
    PreviewAggregateEvidence,
    PreviewAllocationEvidence,
    PreviewAllocationRunEvidence,
    PreviewSourceEvidence,
    decode_lineage_method_details,
)
from core.preview.models import PreviewArtifactPayload, PreviewPackagePayload, PreviewRequest, PreviewSourceSnapshot
from core.storage.interface import ResourceRepository  # noqa: TC001 - resolved by contract tests

MAPPING_PROFILE_VERSION = "focus-1.4-daily-full-v4"
_DECIMAL_CONTEXT = Context(prec=38)
logger = logging.getLogger(__name__)


class PreviewMappingError(ValueError):
    """Base error raised by the Daily Full mapping boundary."""


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


# Compatibility aliases retained for callers from the v2 boundary.
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
    "x_ChitraguptaMappingProfileVersion": ("mapping profile", "emit focus-1.4-daily-full-v4"),
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
type PreviewOriginKey = tuple[datetime, str, str, str, str]


@dataclass(frozen=True)
class PreviewRowProjection:
    target_values: tuple[PreviewCell, ...]
    custom_values: tuple[PreviewCell, ...]
    financials: PreviewFinancialProjection


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


def _utc(value: datetime) -> str:
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _decimal(value: Decimal) -> str:
    result = format(value, "f")
    if "." in result:
        result = result.rstrip("0").rstrip(".")
    return result or "0"


def _serialize_cell(value: PreviewCell) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return _utc(value)
    if isinstance(value, Decimal):
        return _decimal(value)
    return value


def _canonical_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


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
                False,
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
        return AcceptedPreviewSource(PreviewChargeSemantics(kind, category, frequency, inferred_key, True, False))
    if promotion or (support and line_type != "SUPPORT"):
        return RejectedPreviewSource(PreviewSourceIssue.CHARGE_CLASSIFICATION_AMBIGUOUS)
    if line_type == "SUPPORT":
        return AcceptedPreviewSource(
            PreviewChargeSemantics(
                PreviewChargeKind.RECURRING_SUPPORT, "Purchase", "Recurring", inferred_key, True, False
            )
        )
    return AcceptedPreviewSource(
        PreviewChargeSemantics(PreviewChargeKind.METERED_USAGE, "Usage", "Usage-Based", inferred_key, True, False)
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
    is_refund = semantics.kind in {PreviewChargeKind.USAGE_REFUND, PreviewChargeKind.SUPPORT_REFUND}
    if is_refund:
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
    if not is_refund and (price < 0 or quantity < 0):
        raise PreviewFinancialUnsupportedError("positive charge price or quantity sign is unsupported")
    if original - discount != amount or price * quantity != original:
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
    original = source.original_amount
    if original is None:
        raise PreviewSourceEvidenceError("source original amount is unavailable")
    with localcontext(_DECIMAL_CONTEXT):
        list_cost = original * allocation.allocation_ratio
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
    source_quantity = source.quantity
    source_price = source.price
    if source.native_line_type == "PROMO_CREDIT":
        source_quantity = Decimal(0) if source_quantity is None else source_quantity
        source_price = Decimal(0) if source_price is None else source_price
    if (
        aggregate.total_cost != source.amount
        or aggregate.quantity != source_quantity
        or aggregate.unit_price != source_price
    ):
        raise PreviewFinancialReconciliationError("persisted source and aggregate evidence do not reconcile")


def _source_billing_origin(source: PreviewSourceEvidence) -> PreviewOriginKey | None:
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
    return timestamp, environment_id, resource_id, product, line_type


def _aggregate_billing_origin(aggregate: PreviewAggregateEvidence) -> PreviewOriginKey:
    return (
        aggregate.timestamp,
        aggregate.environment_id,
        aggregate.resource_id,
        aggregate.native_product,
        aggregate.native_line_type,
    )


def _allocation_billing_origin(allocation: PreviewAllocationEvidence) -> PreviewOriginKey:
    return (
        allocation.timestamp,
        allocation.environment_id,
        allocation.resource_id,
        allocation.native_product,
        allocation.native_line_type,
    )


def reconcile_source_aggregate_stream(
    *,
    selected_sources: Iterable[SelectedSourceProjection],
    aggregates: Iterable[PreviewAggregateEvidence],
) -> tuple[
    dict[PreviewOriginKey, SelectedSourceProjection],
    dict[PreviewOriginKey, PreviewAggregateEvidence],
]:
    """Validate the complete source/aggregate stream in closed stage order."""
    sources_by_origin: dict[PreviewOriginKey, list[SelectedSourceProjection]] = {}
    missing_association = False
    for selected in selected_sources:
        origin = _source_billing_origin(selected.source)
        if origin is None:
            missing_association = True
            continue
        sources_by_origin.setdefault(origin, []).append(selected)

    aggregates_by_origin: dict[PreviewOriginKey, PreviewAggregateEvidence] = {}
    duplicate_aggregate = False
    for aggregate in aggregates:
        origin = _aggregate_billing_origin(aggregate)
        duplicate_aggregate = duplicate_aggregate or origin in aggregates_by_origin
        aggregates_by_origin[origin] = aggregate

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
    for origin, selected in selected_by_origin.items():
        try:
            reconcile_source_aggregate_evidence(selected=selected, aggregate=aggregates_by_origin[origin])
        except PreviewFinancialReconciliationError:
            mismatched_sources.append(selected.source.source_record_id)
    if mismatched_sources:
        raise PreviewSourceAggregateReconciliationError(tuple(mismatched_sources))
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
    origin = (
        aggregate.timestamp,
        aggregate.environment_id,
        aggregate.resource_id,
        aggregate.native_product,
        aggregate.native_line_type,
    )
    for allocation in allocations:
        allocation_origin = (
            allocation.timestamp,
            allocation.environment_id,
            allocation.resource_id,
            allocation.native_product,
            allocation.native_line_type,
        )
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
    if allocated_cost != aggregate.total_cost or allocated_quantity != aggregate.quantity:
        raise PreviewFinancialReconciliationError("allocation lineage totals do not reconcile")


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
            run.capture_status != "complete"
            or run.capture_reason is not None
            or run.calculation_completed_at != expected_completion_by_run.get(identity)
            or run.portion_count != allocation_count_by_run.get(identity, 0)
            for identity, run in runs_by_identity.items()
        )
    )
    if invalid_runs or invalid_allocation_scope or set(allocations_by_origin) != set(aggregates_by_origin):
        raise PreviewAllocationLineageError("allocation lineage stream is incomplete")

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
    row: PreviewRowProjection,
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
    projection = PreviewRowProjection(
        tuple(row[column] for column in FOCUS_1_4_FULL_COLUMNS),
        tuple(row[column] for column in CUSTOM_EVIDENCE_COLUMNS),
        financials,
    )
    validate_preview_row(row=projection, target_rules=FOCUS_1_4_COLUMN_RULES, custom_rules=CUSTOM_EVIDENCE_RULES)
    return row


def _build_daily_full_package_payload(
    *,
    request: PreviewRequest,
    snapshot: PreviewSourceSnapshot,
    csv_body: bytes,
    row_count: int,
    source_records: int,
    source_cost: Decimal,
    allocated_cost: Decimal,
    generated_at: datetime,
) -> PreviewPackagePayload:
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
        "validation": {
            "status": "passed",
            "mapping_profile_version": MAPPING_PROFILE_VERSION,
            "source_records": source_records,
            "rows": row_count,
            "mapping_errors": 0,
        },
        "reconciliation": {
            "source_cost": _decimal(source_cost),
            "allocated_cost": _decimal(allocated_cost),
            "difference": _decimal(source_cost - allocated_cost),
        },
        "generated_at": _utc(generated_at),
        "files": [file_metadata],
    }
    manifest_body = (_canonical_json(manifest) + "\n").encode()
    return PreviewPackagePayload(
        manifest_body=manifest_body, data_files=(PreviewArtifactPayload("cost-and-usage.csv", "text/csv", 1, csv_body),)
    )


def build_daily_full_package(
    *,
    request: PreviewRequest,
    snapshot: PreviewSourceSnapshot,
    evidence: SelectedPreviewEvidence,
    provider_context: PreviewProviderContext,
    resource_context: PreviewResourceContext,
    identity: Identity,
    environment: Resource | None,
    generated_at: datetime,
) -> PreviewPackagePayload:
    """Build the compatibility single-row Daily / Full package."""
    row = _project_daily_full_row(
        evidence=evidence,
        provider_context=provider_context,
        resource_context=resource_context,
        identity=identity,
        environment=environment,
    )
    source_amount = evidence.selected.source.amount
    assert source_amount is not None
    columns = (*FOCUS_1_4_FULL_COLUMNS, *CUSTOM_EVIDENCE_COLUMNS)
    buffer = io.StringIO(newline="")
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(columns)
    writer.writerow([_serialize_cell(row[column]) for column in columns])
    return _build_daily_full_package_payload(
        request=request,
        snapshot=snapshot,
        csv_body=buffer.getvalue().encode(),
        row_count=1,
        source_records=1,
        source_cost=source_amount,
        allocated_cost=evidence.allocation.amount,
        generated_at=generated_at,
    )


def build_daily_full_package_rows(
    *,
    request: PreviewRequest,
    snapshot: PreviewSourceSnapshot,
    rows: Iterable[PreparedPreviewPackageRow],
    provider_context: PreviewProviderContext,
    generated_at: datetime,
) -> PreviewPackagePayload:
    columns = (*FOCUS_1_4_FULL_COLUMNS, *CUSTOM_EVIDENCE_COLUMNS)
    buffer = io.StringIO(newline="")
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(columns)
    source_record_ids: set[str] = set()
    source_cost = Decimal(0)
    allocated_cost = Decimal(0)
    row_count = 0
    for prepared in rows:
        source = prepared.evidence.selected.source
        with localcontext(_DECIMAL_CONTEXT):
            if source.source_record_id not in source_record_ids:
                source_record_ids.add(source.source_record_id)
                source_cost += source.amount or Decimal(0)
            allocated_cost += prepared.evidence.allocation.allocated_cost
        projected = _project_daily_full_row(
            evidence=prepared.evidence,
            provider_context=provider_context,
            resource_context=prepared.resource_context,
            identity=prepared.allocated_entity,
            environment=prepared.environment,
            origin_tags_json=prepared.origin_tags_json,
            allocated_tags_json=prepared.allocated_tags_json,
        )
        writer.writerow([_serialize_cell(projected[column]) for column in columns])
        row_count += 1
    if row_count == 0:
        raise PreviewMappingScopeError("no allocation portions are available")
    return _build_daily_full_package_payload(
        request=request,
        snapshot=snapshot,
        csv_body=buffer.getvalue().encode(),
        row_count=row_count,
        source_records=len(source_record_ids),
        source_cost=source_cost,
        allocated_cost=allocated_cost,
        generated_at=generated_at,
    )


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
