from __future__ import annotations

import ast
import csv
import inspect
import io
import json
from dataclasses import fields, replace
from datetime import UTC, date, datetime
from decimal import Decimal
from types import MappingProxyType
from typing import Any
from unittest.mock import MagicMock

import pytest

from core.models.identity import CoreIdentity
from core.models.resource import CoreResource
from core.storage.interface import ResourceRepository
from tests.unit.core.preview.conftest import preview_module

REQUEST_START = datetime(2026, 7, 1, tzinfo=UTC)
REQUEST_END = datetime(2026, 7, 2, tzinfo=UTC)


TARGET_RULE_AUTHORITIES = (
    ("AllocatedMethodId", "allocation.allocation_method", "copy the current allocation method"),
    ("AllocatedMethodDetails", "none", "remain null until TASK-254.05"),
    ("AllocatedResourceId", "allocation.allocation_target_id", "copy the allocation target identifier"),
    ("AllocatedResourceName", "allocation target identity", "copy its display name"),
    ("AllocatedTags", "none", "remain null until TASK-254.05"),
    ("AvailabilityZone", "none", "not applicable to retained Direct PAYG evidence"),
    ("BilledCost", "source.amount and allocation.amount", "copy the exactly reconciled allocated share"),
    ("BillingAccountId", "bound provider organization resource", "copy the provider organization identifier"),
    ("BillingAccountName", "bound provider organization resource", "copy its optional display name"),
    ("BillingAccountType", "mapping profile", "emit Organization"),
    ("BillingCurrency", "none", "remain null under the TASK-254.03 provider-field gap"),
    ("BillingPeriodEnd", "source.source_period_start", "derive the exclusive next UTC month boundary"),
    ("BillingPeriodStart", "source.source_period_start", "derive the inclusive UTC month boundary"),
    ("CapacityReservationId", "none", "not applicable to Direct PAYG"),
    ("CapacityReservationStatus", "none", "not applicable to Direct PAYG"),
    ("ChargeCategory", "typed charge semantics", "copy the closed category"),
    ("ChargeClass", "none", "remain null because corrections are ineligible"),
    ("ChargeDescription", "source.native_description", "copy losslessly"),
    ("ChargeFrequency", "typed charge semantics", "copy the closed frequency"),
    ("ChargePeriodEnd", "source.source_period_end", "copy the native exclusive boundary"),
    ("ChargePeriodStart", "source.source_period_start", "copy the native inclusive boundary"),
    ("CommitmentDiscountCategory", "none", "not applicable to Direct PAYG"),
    ("CommitmentDiscountId", "none", "not applicable to Direct PAYG"),
    ("CommitmentDiscountName", "none", "not applicable to Direct PAYG"),
    ("CommitmentDiscountQuantity", "none", "not applicable to Direct PAYG"),
    ("CommitmentDiscountStatus", "none", "not applicable to Direct PAYG"),
    ("CommitmentDiscountType", "none", "not applicable to Direct PAYG"),
    ("CommitmentDiscountUnit", "none", "not applicable to Direct PAYG"),
    ("CommitmentProgramEligibilityDetails", "none", "not applicable to Direct PAYG"),
    ("ConsumedQuantity", "financial projection", "copy metered native quantity when consumption is emitted"),
    ("ConsumedUnit", "financial projection", "copy the matching normalized native unit"),
    ("ContractApplied", "none", "not applicable to Direct PAYG"),
    ("ContractedCost", "financial projection", "copy allocated original_amount"),
    ("ContractedUnitPrice", "none", "not applicable without negotiated pricing"),
    ("EffectiveCost", "financial projection", "copy the reconciled allocation amount"),
    ("HostProviderName", "resource.metadata.provider_cloud", "copy the raw provider cloud code unchanged"),
    ("InvoiceDetailId", "none", "remain null under the invoice identity gap"),
    ("InvoiceId", "none", "remain null under the invoice identity gap"),
    ("InvoiceIssuerName", "none", "remain null under the invoice issuer gap"),
    ("ListCost", "financial projection", "copy allocated original_amount"),
    ("ListUnitPrice", "financial projection", "copy native price after exact arithmetic"),
    ("PricingCategory", "mapping profile", "emit Standard when SKU pricing is emitted"),
    ("PricingCurrency", "configured pricing contract", "emit USD without conversion"),
    ("PricingCurrencyContractedUnitPrice", "none", "not applicable without negotiated pricing"),
    ("PricingCurrencyEffectiveCost", "financial projection", "copy EffectiveCost in USD"),
    ("PricingCurrencyListUnitPrice", "financial projection", "copy ListUnitPrice in USD"),
    ("PricingQuantity", "financial projection", "copy native quantity when SKU pricing is emitted"),
    ("PricingUnit", "financial projection", "copy the matching normalized native unit"),
    ("RegionId", "resource.metadata.provider_region", "copy the raw provider region unchanged"),
    ("RegionName", "none", "remain null under the provider region display-name gap"),
    ("ResourceId", "origin provider resource", "copy its provider identifier"),
    ("ResourceName", "origin provider resource then source", "copy inventory display name with native fallback"),
    ("ResourceType", "origin provider resource", "copy its concrete resource type"),
    ("ServiceProviderName", "mapping profile", "emit Confluent Cloud"),
    ("ServiceCategory", "versioned service rule", "copy its FOCUS category"),
    ("ServiceName", "versioned service rule", "copy its service name"),
    ("ServiceSubcategory", "versioned service rule", "copy its FOCUS subcategory"),
    ("SkuId", "canonical native product and line type", "derive the namespaced v1 SHA-256 key"),
    ("SkuMeter", "financial projection", "copy PricingUnit when SKU pricing is emitted"),
    ("SkuPriceDetails", "canonical SKU price components", "serialize as canonical JSON"),
    ("SkuPriceId", "canonical SKU price components", "derive the namespaced v1 SHA-256 key"),
    ("SubAccountId", "source.environment_id", "copy the native Confluent environment identifier"),
    ("SubAccountName", "environment inventory", "copy its display name"),
    ("SubAccountType", "mapping profile", "emit Environment when SubAccountId is present"),
    ("Tags", "none", "remain null until TASK-254.05"),
)

CUSTOM_RULE_AUTHORITIES = (
    (
        "x_ChitraguptaSourceCostId",
        "source provider identity",
        "use provider Cost ID with stable source-record fallback",
    ),
    (
        "x_ChitraguptaBillingScopeId",
        "provider organization and UTC billing month",
        "derive the namespaced v1 SHA-256 key",
    ),
    ("x_ChitraguptaAllocationRatio", "none", "remain null until TASK-254.05"),
    ("x_ChitraguptaAllocationMethodVersion", "none", "remain null until TASK-254.05"),
    ("x_ChitraguptaMappingProfileVersion", "mapping profile", "emit focus-1.4-daily-full-v3"),
    ("x_ChitraguptaSkuComponents", "canonical SKU and SKU price components", "serialize as canonical JSON"),
    ("x_ConfluentProduct", "source.native_product", "copy losslessly"),
    ("x_ConfluentLineType", "source.native_line_type", "copy losslessly"),
    ("x_ConfluentDescription", "source.native_description", "copy losslessly"),
    ("x_ConfluentDiscountAmount", "source.discount_amount", "copy the exact Decimal"),
    ("x_ConfluentNetworkAccessType", "source.native_network_access_type", "copy losslessly when supplied"),
    (
        "x_ConfluentTierDimensions",
        "source.native_tier_dimensions",
        "serialize the sorted retained values as canonical JSON",
    ),
)

READY_NATIVE_LINE_TYPES = frozenset(
    {
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
    }
)

TASK_254_05_NATIVE_LINE_TYPES = frozenset(
    {
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
    }
)

EXPECTED_SERVICE_RULES = {
    "kafka": (
        (
            "KAFKA_STORAGE",
            "KAFKA_PARTITION",
            "KAFKA_NETWORK_READ",
            "KAFKA_NETWORK_WRITE",
            "KAFKA_BASE",
            "KAFKA_NUM_CKUS",
            "KAFKA_REST_PRODUCE",
            "KAFKA_STREAMS",
        ),
        "Integration",
        "Confluent Cloud Apache Kafka",
        "Messaging",
        "resource_specific",
        "self",
        ("kafka_cluster",),
    ),
    "cluster_link": (
        ("CLUSTER_LINKING_PER_LINK", "CLUSTER_LINKING_WRITE", "CLUSTER_LINKING_READ"),
        "Integration",
        "Confluent Cloud Cluster Linking",
        "Messaging",
        "resource_specific",
        "self",
        ("kafka_cluster",),
    ),
    "ksqldb": (
        ("KSQL_NUM_CSUS",),
        "Analytics",
        "Confluent Cloud ksqlDB",
        "Streaming Analytics",
        "resource_specific",
        "ksqldb_kafka_reference",
        ("ksqldb_cluster",),
    ),
    "flink": (
        ("FLINK_NUM_CFUS",),
        "Analytics",
        "Confluent Cloud Flink",
        "Streaming Analytics",
        "resource_specific",
        "flink_pool_or_reference",
        ("flink_compute_pool", "flink_statement"),
    ),
    "connect": (
        (
            "CONNECT_CAPACITY",
            "CONNECT_NUM_TASKS",
            "CONNECT_THROUGHPUT",
            "CONNECT_NUM_RECORDS",
            "CUSTOM_CONNECT_NUM_TASKS",
            "CUSTOM_CONNECT_THROUGHPUT",
        ),
        "Integration",
        "Confluent Cloud Connect",
        "Messaging",
        "resource_specific",
        "connector_parent_kafka",
        ("connector",),
    ),
    "data_governance": (
        ("GOVERNANCE_BASE", "SCHEMA_REGISTRY", "NUM_RULES"),
        "Management and Governance",
        "Confluent Cloud Data Governance",
        "Data Governance",
        "resource_specific",
        "self",
        ("schema_registry",),
    ),
    "audit_log": (
        ("AUDIT_LOG_READ",),
        "Management and Governance",
        "Confluent Cloud Audit Logs",
        "Observability",
        "organization_wide",
        "organization_wide",
        (),
    ),
    "tableflow": (
        ("TABLEFLOW_DATA_PROCESSED", "TABLEFLOW_NUM_TOPICS", "TABLEFLOW_STORAGE"),
        "Storage",
        "Confluent Cloud Tableflow",
        "Object Storage",
        "resource_specific",
        "unsupported_provider_context",
        (),
    ),
    "usm": (
        ("USM_CONNECTED_NODE",),
        "Management and Governance",
        "Confluent Cloud Unified Stream Manager",
        "Observability",
        "resource_specific",
        "self",
        ("kafka_cluster",),
    ),
    "support": (
        ("SUPPORT",),
        "Management and Governance",
        "Confluent Cloud Support",
        "Support",
        "organization_wide",
        "organization_wide",
        (),
    ),
    "promotional_credit": (
        ("PROMO_CREDIT",),
        "Other",
        "Confluent Cloud Promotional Credits",
        "Other (Other)",
        "organization_wide",
        "organization_wide",
        (),
    ),
}

EXPECTED_NATIVE_PRODUCT_RULES = {
    "KAFKA": ("kafka", "Usage", "Usage-Based"),
    "CONNECT": ("connect", "Usage", "Usage-Based"),
    "KSQL": ("ksqldb", "Usage", "Usage-Based"),
    "AUDIT_LOG": ("audit_log", "Usage", "Usage-Based"),
    "STREAM_GOVERNANCE": ("data_governance", "Usage", "Usage-Based"),
    "CLUSTER_LINK": ("cluster_link", "Usage", "Usage-Based"),
    "CUSTOM_CONNECT": ("connect", "Usage", "Usage-Based"),
    "FLINK": ("flink", "Usage", "Usage-Based"),
    "TABLEFLOW": ("tableflow", "Usage", "Usage-Based"),
    "SUPPORT_CLOUD_BASIC": ("support", "Purchase", "Recurring"),
    "SUPPORT_CLOUD_DEVELOPER": ("support", "Purchase", "Recurring"),
    "SUPPORT_CLOUD_BUSINESS": ("support", "Purchase", "Recurring"),
    "SUPPORT_CLOUD_PREMIER": ("support", "Purchase", "Recurring"),
    "USM": ("usm", "Usage", "Usage-Based"),
}

COMPATIBLE_PRODUCT_BY_SERVICE_RULE = {
    "kafka": "KAFKA",
    "cluster_link": "CLUSTER_LINK",
    "ksqldb": "KSQL",
    "flink": "FLINK",
    "connect": "CONNECT",
    "data_governance": "STREAM_GOVERNANCE",
    "audit_log": "AUDIT_LOG",
    "tableflow": "TABLEFLOW",
    "usm": "USM",
    "support": "SUPPORT_CLOUD_BASIC",
    "promotional_credit": "KAFKA",
}

ACCEPTED_LINE_TYPE_CLASSIFICATIONS = tuple(
    (line_type, COMPATIBLE_PRODUCT_BY_SERVICE_RULE[service_rule], service_rule)
    for service_rule, expected in EXPECTED_SERVICE_RULES.items()
    for line_type in expected[0]
)


def _valid_row_projection(mapping: Any) -> Any:
    financials = mapping.PreviewFinancialProjection(
        billed_cost=Decimal("8"),
        contracted_cost=Decimal("10"),
        effective_cost=Decimal("8"),
        list_cost=Decimal("10"),
        list_unit_price=Decimal("2"),
        pricing_currency_effective_cost=Decimal("8"),
        pricing_currency_list_unit_price=Decimal("2"),
        pricing_quantity=Decimal("5"),
        pricing_unit="GB",
        consumed_quantity=Decimal("5"),
        consumed_unit="GB",
    )
    sku = {"line_type": "KAFKA_STORAGE", "product": "KAFKA"}
    sku_price = {
        "cloud": "AWS",
        "line_type": "KAFKA_STORAGE",
        "network_access_type": "PUBLIC_INTERNET",
        "product": "KAFKA",
        "region": "us-east-1",
        "resource_type": "kafka_cluster",
        "tier_dimensions": [["lower_bound", "0"], ["upper_bound", "100"]],
    }
    values = {column: None for column in mapping.FOCUS_1_4_FULL_COLUMNS}
    values.update(
        {
            "AllocatedMethodId": "direct",
            "AllocatedResourceId": "sa-1",
            "AllocatedResourceName": "service-account",
            "BilledCost": Decimal("8"),
            "BillingAccountId": "org-1",
            "BillingAccountName": "Provider organization",
            "BillingAccountType": "Organization",
            "BillingPeriodEnd": datetime(2026, 8, 1, tzinfo=UTC),
            "BillingPeriodStart": datetime(2026, 7, 1, tzinfo=UTC),
            "ChargeCategory": "Usage",
            "ChargeDescription": "Kafka storage usage",
            "ChargeFrequency": "Usage-Based",
            "ChargePeriodEnd": datetime(2026, 7, 2, tzinfo=UTC),
            "ChargePeriodStart": datetime(2026, 7, 1, tzinfo=UTC),
            "ConsumedQuantity": Decimal("5"),
            "ConsumedUnit": "GB",
            "ContractedCost": Decimal("10"),
            "EffectiveCost": Decimal("8"),
            "HostProviderName": "AWS",
            "ListCost": Decimal("10"),
            "ListUnitPrice": Decimal("2"),
            "PricingCategory": "Standard",
            "PricingCurrency": "USD",
            "PricingCurrencyEffectiveCost": Decimal("8"),
            "PricingCurrencyListUnitPrice": Decimal("2"),
            "PricingQuantity": Decimal("5"),
            "PricingUnit": "GB",
            "RegionId": "us-east-1",
            "ResourceId": "lkc-1",
            "ResourceName": "Orders",
            "ResourceType": "kafka_cluster",
            "ServiceProviderName": "Confluent Cloud",
            "ServiceCategory": "Integration",
            "ServiceName": "Confluent Cloud Apache Kafka",
            "ServiceSubcategory": "Messaging",
            "SkuId": mapping._hash_key("sku", sku),
            "SkuMeter": "GB",
            "SkuPriceDetails": mapping._canonical_json(sku_price),
            "SkuPriceId": mapping._hash_key("sku-price", sku_price),
            "SubAccountId": "env-1",
            "SubAccountName": "Production",
            "SubAccountType": "Environment",
        }
    )
    billing_scope = mapping._hash_key(
        "billing-scope",
        {
            "billing_account_id": "org-1",
            "billing_period_start": "2026-07-01T00:00:00Z",
        },
    )
    custom_values = {column: None for column in mapping.CUSTOM_EVIDENCE_COLUMNS}
    custom_values.update(
        {
            "x_ChitraguptaSourceCostId": "cost-1",
            "x_ChitraguptaBillingScopeId": billing_scope,
            "x_ChitraguptaMappingProfileVersion": "focus-1.4-daily-full-v3",
            "x_ChitraguptaSkuComponents": mapping._canonical_json(
                {"schema_version": "v1", "sku": sku, "sku_price": sku_price}
            ),
            "x_ConfluentProduct": "KAFKA",
            "x_ConfluentLineType": "KAFKA_STORAGE",
            "x_ConfluentDescription": "Kafka storage usage",
            "x_ConfluentDiscountAmount": Decimal("2"),
            "x_ConfluentNetworkAccessType": "PUBLIC_INTERNET",
            "x_ConfluentTierDimensions": '{"lower_bound":"0","upper_bound":"100"}',
        }
    )
    return mapping.PreviewRowProjection(
        target_values=tuple(values[column] for column in mapping.FOCUS_1_4_FULL_COLUMNS),
        custom_values=tuple(custom_values[column] for column in mapping.CUSTOM_EVIDENCE_COLUMNS),
        financials=financials,
    )


def _replace_target(mapping: Any, row: Any, column: str, value: object) -> Any:
    values = list(row.target_values)
    values[mapping.FOCUS_1_4_FULL_COLUMNS.index(column)] = value
    return replace(row, target_values=tuple(values))


def _replace_custom(mapping: Any, row: Any, column: str, value: object) -> Any:
    values = list(row.custom_values)
    values[mapping.CUSTOM_EVIDENCE_COLUMNS.index(column)] = value
    return replace(row, custom_values=tuple(values))


def _validate(mapping: Any, row: Any, **overrides: object) -> None:
    mapping.validate_preview_row(
        row=row,
        target_rules=overrides.get("target_rules", mapping.FOCUS_1_4_COLUMN_RULES),
        custom_rules=overrides.get("custom_rules", mapping.CUSTOM_EVIDENCE_RULES),
    )


def _package_row(
    mapping: Any,
    *,
    source: Any,
    aggregate: Any,
    allocation: Any,
    request_id: str = "request-1",
    calculation_id: str = "calculation-1",
    billing_account_id: str = "org-1",
    resource_id: str = "lkc-1",
    resource_type: str = "kafka_cluster",
    cloud: str = "AWS",
    region: str = "us-east-1",
    include_manifest: bool = False,
) -> Any:
    classification = mapping.classify_daily_full_source(
        request_start=REQUEST_START,
        request_end=REQUEST_END,
        source=source,
    )
    assert isinstance(classification, mapping.AcceptedPreviewSource)
    assert source.amount is not None
    financials = mapping.project_financials(
        source=source,
        semantics=classification.semantics,
        billed_share=source.amount,
    )
    evidence = mapping.SelectedPreviewEvidence(
        mapping.SelectedSourceProjection(source, classification.semantics, financials),
        aggregate,
        allocation,
    )
    request = preview_module("models").PreviewRequest(
        request_id=request_id,
        tenant_name="tenant",
        ecosystem="confluent_cloud",
        tenant_id="tenant-1",
        grain="daily",
        start_date=date(2026, 7, 1),
        end_date=date(2026, 7, 1),
        column_profile="full",
        status=preview_module("models").PreviewRequestStatus.RUNNING,
        created_at=datetime(2026, 7, 1, tzinfo=UTC),
        started_at=datetime(2026, 7, 1, tzinfo=UTC),
        completed_at=None,
        source_snapshot=None,
        diagnostic=None,
        storage_key=None,
        package=None,
    )
    models = preview_module("models")
    snapshot = models.PreviewSourceSnapshot(
        calculation_timestamp=datetime(2026, 7, 3, tzinfo=UTC),
        calculation_coverage=(
            models.PreviewCalculationCoverageEntry(
                tracking_date=date(2026, 7, 1),
                calculation_id=calculation_id,
                calculation_completed_at=datetime(2026, 7, 3, tzinfo=UTC),
                calculation_run_id=1,
            ),
        ),
        source_through=datetime(2026, 7, 3, tzinfo=UTC),
    )
    package = mapping.build_daily_full_package(
        request=request,
        snapshot=snapshot,
        evidence=evidence,
        provider_context=mapping.PreviewProviderContext(billing_account_id, "Provider organization"),
        resource_context=mapping.PreviewResourceContext(
            resource_id,
            "Orders",
            resource_type,
            cloud,
            region,
        ),
        identity=CoreIdentity("confluent_cloud", "tenant-1", "sa-1", "service_account", "Owner"),
        environment=CoreResource(
            "confluent_cloud",
            "tenant-1",
            "env-1",
            "environment",
            "Production",
        ),
        generated_at=datetime(2026, 7, 4, tzinfo=UTC),
    )
    row = next(csv.DictReader(io.StringIO(package.data_files[0].body.decode())))
    if include_manifest:
        return row, json.loads(package.manifest_body)
    return row


def test_focus_rule_table_is_the_complete_ordered_65_column_authority() -> None:
    mapping = preview_module("mapping")

    rules = mapping.FOCUS_1_4_COLUMN_RULES
    columns = tuple(rule.column for rule in rules)
    feature_counts = {
        level: sum(rule.feature_level.value == level for rule in rules)
        for level in ("mandatory", "conditional", "recommended")
    }

    assert len(rules) == 65
    assert len(columns) == len(set(columns))
    assert columns == mapping.FOCUS_1_4_FULL_COLUMNS
    assert feature_counts == {"mandatory": 21, "conditional": 40, "recommended": 4}
    assert tuple((rule.column, rule.source, rule.transformation) for rule in rules) == TARGET_RULE_AUTHORITIES
    assert all(rule.validator is not None for rule in rules)
    assert {
        rule.column: (rule.gap_code, rule.owner_task)
        for rule in rules
        if rule.applicability.value in {"deferred", "declared_gap"}
    } == {
        "AllocatedMethodDetails": ("allocation_lineage_and_tag_projection_pending", "TASK-254.05"),
        "AllocatedTags": ("allocation_lineage_and_tag_projection_pending", "TASK-254.05"),
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
        "Tags": ("allocation_lineage_and_tag_projection_pending", "TASK-254.05"),
    }


def test_custom_rule_table_is_the_complete_ordered_evidence_authority() -> None:
    mapping = preview_module("mapping")

    rules = mapping.CUSTOM_EVIDENCE_RULES
    columns = tuple(rule.column for rule in rules)

    assert len(rules) == 12
    assert len(columns) == len(set(columns))
    assert columns == mapping.CUSTOM_EVIDENCE_COLUMNS
    assert set(columns).isdisjoint(mapping.FOCUS_1_4_FULL_COLUMNS)
    assert tuple((rule.column, rule.source, rule.transformation) for rule in rules) == CUSTOM_RULE_AUTHORITIES
    assert all(rule.validator is not None for rule in rules)
    for rule in rules:
        if rule.applicability.value in {"deferred", "declared_gap"}:
            assert rule.gap_code
            assert rule.owner_task


def test_service_and_native_product_rule_tables_are_closed_immutable_authorities() -> None:
    mapping = preview_module("mapping")

    assert not hasattr(mapping.FOCUS_1_4_SERVICE_RULES_V1, "__setitem__")
    assert not hasattr(mapping.NATIVE_PRODUCT_SERVICE_RULES_V1, "__setitem__")
    assert set(mapping.FOCUS_1_4_SERVICE_RULES_V1) == set(mapping.PreviewServiceRuleKey)
    assert all(key is rule.key for key, rule in mapping.FOCUS_1_4_SERVICE_RULES_V1.items())
    assert all(
        product == rule.native_product and rule.service_rule_key in mapping.FOCUS_1_4_SERVICE_RULES_V1
        for product, rule in mapping.NATIVE_PRODUCT_SERVICE_RULES_V1.items()
    )
    line_type_owners = [
        line_type for rule in mapping.FOCUS_1_4_SERVICE_RULES_V1.values() for line_type in rule.native_line_types
    ]
    assert len(line_type_owners) == len(set(line_type_owners))


def test_native_line_readiness_is_the_exact_typed_immutable_v1_authority() -> None:
    mapping = preview_module("mapping")

    assert tuple((member.name, member.value) for member in mapping.PreviewLineageReadiness) == (
        ("READY", "ready"),
        ("TASK_254_05", "task_254_05"),
    )
    assert not hasattr(mapping.FOCUS_1_4_NATIVE_LINE_READINESS_V1, "__setitem__")
    accepted_line_types = {
        line_type for rule in mapping.FOCUS_1_4_SERVICE_RULES_V1.values() for line_type in rule.native_line_types
    }
    assert set(mapping.FOCUS_1_4_NATIVE_LINE_READINESS_V1) == accepted_line_types
    assert {
        line_type
        for line_type, readiness in mapping.FOCUS_1_4_NATIVE_LINE_READINESS_V1.items()
        if readiness is mapping.PreviewLineageReadiness.READY
    } == READY_NATIVE_LINE_TYPES
    assert {
        line_type
        for line_type, readiness in mapping.FOCUS_1_4_NATIVE_LINE_READINESS_V1.items()
        if readiness is mapping.PreviewLineageReadiness.TASK_254_05
    } == TASK_254_05_NATIVE_LINE_TYPES

    with pytest.raises(TypeError):
        mapping.FOCUS_1_4_NATIVE_LINE_READINESS_V1["KAFKA_STORAGE"] = (  # type: ignore[index]
            mapping.PreviewLineageReadiness.TASK_254_05
        )


@pytest.mark.parametrize("drift", ("missing", "extra", "wrong-readiness"))
def test_profile_self_validation_rejects_native_line_readiness_drift(
    monkeypatch: pytest.MonkeyPatch,
    drift: str,
) -> None:
    mapping = preview_module("mapping")
    readiness = dict(mapping.FOCUS_1_4_NATIVE_LINE_READINESS_V1)
    if drift == "missing":
        readiness.pop("KAFKA_STORAGE")
    elif drift == "extra":
        readiness["FUTURE_PROVIDER_LINE"] = mapping.PreviewLineageReadiness.READY
    else:
        readiness["KAFKA_STORAGE"] = mapping.PreviewLineageReadiness.TASK_254_05
    monkeypatch.setattr(
        mapping,
        "FOCUS_1_4_NATIVE_LINE_READINESS_V1",
        MappingProxyType(readiness),
    )

    with pytest.raises(mapping.PreviewProfileDefinitionError):
        mapping._validate_profile_definition()


def test_preview_service_consumes_native_line_readiness_authority_without_a_private_set() -> None:
    mapping = preview_module("mapping")
    service = preview_module("service")
    generate_source = inspect.getsource(service.PreviewRuntime._generate)

    assert service.FOCUS_1_4_NATIVE_LINE_READINESS_V1 is mapping.FOCUS_1_4_NATIVE_LINE_READINESS_V1
    assert "FOCUS_1_4_NATIVE_LINE_READINESS_V1" in generate_source
    assert not hasattr(service, "_TASK_254_05_DEFERRED_NATIVE_LINE_TYPES")
    assert "_TASK_254_05_DEFERRED_NATIVE_LINE_TYPES" not in generate_source


def test_service_rule_table_matches_the_exact_v3_taxonomy_and_context_matrix() -> None:
    mapping = preview_module("mapping")

    actual = {
        key.value: (
            rule.native_line_types,
            rule.service_category,
            rule.service_name,
            rule.service_subcategory,
            rule.resource_shape.value,
            rule.context_strategy,
            rule.allowed_origin_resource_types,
        )
        for key, rule in mapping.FOCUS_1_4_SERVICE_RULES_V1.items()
    }

    assert actual == EXPECTED_SERVICE_RULES


@pytest.mark.parametrize("service_rule_key", tuple(EXPECTED_SERVICE_RULES))
@pytest.mark.parametrize("binding", ("line_types", "taxonomy", "service_name", "context"))
def test_profile_self_validation_rejects_every_exact_service_key_binding_drift(
    monkeypatch: pytest.MonkeyPatch,
    service_rule_key: str,
    binding: str,
) -> None:
    mapping = preview_module("mapping")
    key = mapping.PreviewServiceRuleKey(service_rule_key)
    rules = dict(mapping.FOCUS_1_4_SERVICE_RULES_V1)
    rule = rules[key]
    if binding == "line_types":
        replacement = replace(rule, native_line_types=(*rule.native_line_types, "FUTURE_PROVIDER_LINE"))
    elif binding == "taxonomy":
        category, subcategory = (
            ("Integration", "Messaging") if rule.service_category == "Other" else ("Other", "Other (Other)")
        )
        replacement = replace(
            rule,
            service_category=category,
            service_subcategory=subcategory,
        )
    elif binding == "service_name":
        replacement = replace(rule, service_name=f"{rule.service_name} drift")
    else:
        strategy, origin_types = (
            ("self", ("kafka_cluster",))
            if rule.context_strategy != "self"
            else ("connector_parent_kafka", ("connector",))
        )
        replacement = replace(
            rule,
            resource_shape=mapping.PreviewResourceShape.RESOURCE_SPECIFIC,
            context_strategy=strategy,
            allowed_origin_resource_types=origin_types,
        )
    rules[key] = replacement
    monkeypatch.setattr(mapping, "FOCUS_1_4_SERVICE_RULES_V1", MappingProxyType(rules))

    with pytest.raises(mapping.PreviewProfileDefinitionError):
        mapping._validate_profile_definition()


def test_focus_service_rule_uses_the_single_preview_context_strategy_alias() -> None:
    mapping = preview_module("mapping")
    class_definition = ast.parse(inspect.getsource(mapping.FocusServiceRule)).body[0]
    assert isinstance(class_definition, ast.ClassDef)
    context_field = next(
        statement
        for statement in class_definition.body
        if isinstance(statement, ast.AnnAssign)
        and isinstance(statement.target, ast.Name)
        and statement.target.id == "context_strategy"
    )

    assert isinstance(context_field.annotation, ast.Name)
    assert context_field.annotation.id == "PreviewContextStrategy"


def test_native_product_rule_table_matches_the_exact_provider_product_authority() -> None:
    mapping = preview_module("mapping")

    actual = {
        product: (
            rule.service_rule_key.value,
            rule.original_category,
            rule.original_frequency,
        )
        for product, rule in mapping.NATIVE_PRODUCT_SERVICE_RULES_V1.items()
    }

    assert actual == EXPECTED_NATIVE_PRODUCT_RULES


@pytest.mark.parametrize(
    ("native_line_type", "native_product", "expected_service_rule"),
    ACCEPTED_LINE_TYPE_CLASSIFICATIONS,
)
def test_every_accepted_native_line_type_executes_the_classifier_with_its_exact_service_rule(
    valid_source_evidence: Any,
    native_line_type: str,
    native_product: str,
    expected_service_rule: str,
) -> None:
    mapping = preview_module("mapping")
    description = (
        "Support subscription"
        if expected_service_rule == "support"
        else "Promotional allowance"
        if expected_service_rule == "promotional_credit"
        else f"{native_product} usage"
    )
    source = replace(
        valid_source_evidence,
        native_line_type=native_line_type,
        native_product=native_product,
        native_description=description,
    )

    result = mapping.classify_daily_full_source(
        request_start=REQUEST_START,
        request_end=REQUEST_END,
        source=source,
    )

    assert isinstance(result, mapping.AcceptedPreviewSource)
    assert result.semantics.service_rule_key is mapping.PreviewServiceRuleKey(expected_service_rule)


@pytest.mark.parametrize(
    ("service_rule", "native_line_type", "native_product"),
    tuple(
        (service_rule, expected[0][0], COMPATIBLE_PRODUCT_BY_SERVICE_RULE[service_rule])
        for service_rule, expected in EXPECTED_SERVICE_RULES.items()
    ),
)
def test_current_service_rules_do_not_claim_identical_consumption_grain_without_provider_authority(
    valid_source_evidence: Any,
    service_rule: str,
    native_line_type: str,
    native_product: str,
) -> None:
    mapping = preview_module("mapping")
    description = (
        "Support subscription"
        if service_rule == "support"
        else "Promotional allowance"
        if service_rule == "promotional_credit"
        else f"{native_product} usage"
    )
    source = replace(
        valid_source_evidence,
        native_line_type=native_line_type,
        native_product=native_product,
        native_description=description,
        **(
            {
                "amount": Decimal("-5"),
                "original_amount": Decimal("-5"),
                "discount_amount": Decimal("0"),
                "price": None,
                "quantity": None,
                "unit": None,
            }
            if service_rule == "promotional_credit"
            else {}
        ),
    )
    result = mapping.classify_daily_full_source(
        request_start=REQUEST_START,
        request_end=REQUEST_END,
        source=source,
    )
    assert isinstance(result, mapping.AcceptedPreviewSource)

    assert result.semantics.emits_consumption is False
    financials = mapping.project_financials(
        source=source,
        semantics=result.semantics,
        billed_share=source.amount,
    )
    assert financials.consumed_quantity is None
    assert financials.consumed_unit is None


def test_line_type_enum_covers_every_accepted_provider_line_type_exactly_once() -> None:
    mapping = preview_module("mapping")
    expected_line_types = tuple(line_type for rule in EXPECTED_SERVICE_RULES.values() for line_type in rule[0])
    line_type_rule = next(rule for rule in mapping.CUSTOM_EVIDENCE_RULES if rule.column == "x_ConfluentLineType")

    assert line_type_rule.validator is mapping.PreviewValidatorKind.ENUM
    assert line_type_rule.allowed_values == expected_line_types


@pytest.mark.parametrize(
    ("field", "replacement"),
    [
        ("native_line_types", ("KAFKA_STORAGE",)),
        ("service_subcategory", "Support"),
        ("context_strategy", "connector_parent_kafka"),
        ("allowed_origin_resource_types", ("connector",)),
    ],
)
def test_profile_self_validation_rejects_service_rule_matrix_drift(
    monkeypatch: pytest.MonkeyPatch,
    field: str,
    replacement: object,
) -> None:
    mapping = preview_module("mapping")
    rules = dict(mapping.FOCUS_1_4_SERVICE_RULES_V1)
    rules[mapping.PreviewServiceRuleKey.KAFKA] = replace(
        rules[mapping.PreviewServiceRuleKey.KAFKA],
        **{field: replacement},
    )
    monkeypatch.setattr(mapping, "FOCUS_1_4_SERVICE_RULES_V1", MappingProxyType(rules))

    with pytest.raises(mapping.PreviewProfileDefinitionError):
        mapping._validate_profile_definition()


def test_profile_self_validation_rejects_enum_without_closed_allowed_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mapping = preview_module("mapping")
    rules = tuple(
        replace(rule, allowed_values=None) if rule.column == "ChargeCategory" else rule
        for rule in mapping.FOCUS_1_4_COLUMN_RULES
    )
    monkeypatch.setattr(mapping, "FOCUS_1_4_COLUMN_RULES", rules)

    with pytest.raises(mapping.PreviewProfileDefinitionError):
        mapping._validate_profile_definition()


@pytest.mark.parametrize(
    ("field", "replacement"),
    [("original_category", "Purchase"), ("original_frequency", "Recurring")],
)
def test_profile_self_validation_rejects_native_product_semantic_drift(
    monkeypatch: pytest.MonkeyPatch,
    field: str,
    replacement: str,
) -> None:
    mapping = preview_module("mapping")
    rules = dict(mapping.NATIVE_PRODUCT_SERVICE_RULES_V1)
    rules["KAFKA"] = replace(rules["KAFKA"], **{field: replacement})
    monkeypatch.setattr(mapping, "NATIVE_PRODUCT_SERVICE_RULES_V1", MappingProxyType(rules))

    with pytest.raises(mapping.PreviewProfileDefinitionError):
        mapping._validate_profile_definition()


def test_profile_self_validation_rejects_rule_to_manifest_gap_drift(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mapping = preview_module("mapping")
    gaps = tuple(
        replace(gap, columns=gap.columns[:-1]) if gap.code == "derived_sku_identity_not_provider_authoritative" else gap
        for gap in mapping.KNOWN_GAPS
    )
    monkeypatch.setattr(mapping, "KNOWN_GAPS", gaps)

    with pytest.raises(mapping.PreviewProfileDefinitionError):
        mapping._validate_profile_definition()


def test_profile_rules_and_known_gaps_have_exact_bidirectional_ownership() -> None:
    mapping = preview_module("mapping")
    rule_columns: dict[tuple[str, str], list[str]] = {}
    for rule in (*mapping.FOCUS_1_4_COLUMN_RULES, *mapping.CUSTOM_EVIDENCE_RULES):
        if rule.applicability in {
            mapping.PreviewApplicability.DEFERRED,
            mapping.PreviewApplicability.DECLARED_GAP,
        }:
            assert rule.gap_code is not None
            assert rule.owner_task is not None
            rule_columns.setdefault((rule.gap_code, rule.owner_task), []).append(rule.column)
    manifest_columns = {(gap.code, gap.owner_task): list(gap.columns) for gap in mapping.KNOWN_GAPS}

    assert rule_columns == manifest_columns


@pytest.mark.parametrize("authority", ["service", "native_product"])
def test_profile_self_validation_rejects_inconsistent_mapping_authority(
    monkeypatch: pytest.MonkeyPatch,
    authority: str,
) -> None:
    mapping = preview_module("mapping")

    if authority == "service":
        rules = dict(mapping.FOCUS_1_4_SERVICE_RULES_V1)
        rules[mapping.PreviewServiceRuleKey.KAFKA] = replace(
            rules[mapping.PreviewServiceRuleKey.KAFKA],
            key=mapping.PreviewServiceRuleKey.CONNECT,
        )
        monkeypatch.setattr(mapping, "FOCUS_1_4_SERVICE_RULES_V1", MappingProxyType(rules))
    else:
        rules = dict(mapping.NATIVE_PRODUCT_SERVICE_RULES_V1)
        rules["KAFKA"] = replace(rules["KAFKA"], native_product="CONNECT")
        monkeypatch.setattr(mapping, "NATIVE_PRODUCT_SERVICE_RULES_V1", MappingProxyType(rules))

    with pytest.raises(mapping.PreviewProfileDefinitionError):
        mapping._validate_profile_definition()


def test_validate_preview_row_accepts_the_complete_valid_projection() -> None:
    mapping = preview_module("mapping")

    assert _validate(mapping, _valid_row_projection(mapping)) is None


@pytest.mark.parametrize(
    ("column", "value", "rule_id"),
    [
        ("BillingAccountId", None, "nullability"),
        ("AvailabilityZone", "us-east-1a", "applicability"),
        ("BilledCost", "8", "type"),
        ("ChargeCategory", "Adjustment", "allowed_value"),
    ],
)
def test_validate_preview_row_reports_exact_cell_rule_and_column(
    column: str,
    value: object,
    rule_id: str,
) -> None:
    mapping = preview_module("mapping")
    row = _replace_target(mapping, _valid_row_projection(mapping), column, value)

    with pytest.raises(mapping.PreviewRowValidationError) as caught:
        _validate(mapping, row)

    assert caught.value.rule_id is mapping.PreviewRowRuleId(rule_id)
    assert caught.value.column == column
    assert str(caught.value) == f"{rule_id}:{column}"


@pytest.mark.parametrize(
    ("column", "value"),
    [
        ("BilledCost", "8"),
        ("BillingPeriodStart", "2026-07-01T00:00:00Z"),
        ("BillingAccountId", " "),
        ("ChargeDescription", Decimal("1")),
        ("ChargeCategory", Decimal("1")),
        ("SkuPriceDetails", Decimal("1")),
    ],
)
def test_validate_preview_row_dispatches_every_typed_validator(column: str, value: object) -> None:
    mapping = preview_module("mapping")
    row = _replace_target(mapping, _valid_row_projection(mapping), column, value)

    with pytest.raises(mapping.PreviewRowValidationError) as caught:
        _validate(mapping, row)

    assert caught.value.rule_id is mapping.PreviewRowRuleId.TYPE
    assert caught.value.column == column


def test_cell_validator_evaluates_only_the_selected_validator_predicate() -> None:
    mapping = preview_module("mapping")
    calls: list[str] = []

    class ObservedDecimal(Decimal):
        def is_finite(self) -> bool:
            calls.append("decimal")
            return True

    text_rule = next(rule for rule in mapping.FOCUS_1_4_COLUMN_RULES if rule.column == "ChargeDescription")

    with pytest.raises(mapping.PreviewRowValidationError) as caught:
        mapping._validate_cell(ObservedDecimal("1"), text_rule)

    assert caught.value.rule_id is mapping.PreviewRowRuleId.TYPE
    assert caught.value.column == "ChargeDescription"
    assert calls == []


def test_validate_preview_row_reports_column_count_without_a_column() -> None:
    mapping = preview_module("mapping")
    row = _valid_row_projection(mapping)
    row = replace(row, target_values=row.target_values[:-1])

    with pytest.raises(mapping.PreviewRowValidationError) as caught:
        _validate(mapping, row)

    assert caught.value.rule_id is mapping.PreviewRowRuleId.COLUMN_COUNT
    assert caught.value.column is None
    assert str(caught.value) == "column_count"


@pytest.mark.parametrize(
    ("column", "value"),
    [
        ("SkuPriceDetails", '{"product": "KAFKA"}'),
        ("x_ChitraguptaSkuComponents", '[ "sku" ]'),
        ("x_ConfluentTierDimensions", "not-json"),
    ],
)
def test_validate_preview_row_rejects_noncanonical_or_wrong_shape_json(
    column: str,
    value: str,
) -> None:
    mapping = preview_module("mapping")
    row = _valid_row_projection(mapping)
    row = (
        _replace_target(mapping, row, column, value)
        if column in mapping.FOCUS_1_4_FULL_COLUMNS
        else _replace_custom(mapping, row, column, value)
    )

    with pytest.raises(mapping.PreviewRowValidationError) as caught:
        _validate(mapping, row)

    assert caught.value.rule_id is mapping.PreviewRowRuleId.TYPE
    assert caught.value.column == column


@pytest.mark.parametrize(
    ("column", "value", "expected_column"),
    [
        ("ConsumedUnit", None, "ConsumedUnit"),
        ("PricingQuantity", None, "PricingQuantity"),
        ("SubAccountName", None, "SubAccountName"),
        ("ResourceType", None, "ResourceType"),
        ("SkuMeter", None, "SkuMeter"),
    ],
)
def test_validate_preview_row_enforces_dependent_field_pairs(
    column: str,
    value: object,
    expected_column: str,
) -> None:
    mapping = preview_module("mapping")
    row = _replace_target(mapping, _valid_row_projection(mapping), column, value)

    with pytest.raises(mapping.PreviewRowValidationError) as caught:
        _validate(mapping, row)

    assert caught.value.rule_id is mapping.PreviewRowRuleId.DEPENDENT_FIELDS
    assert caught.value.column == expected_column


def test_validate_preview_row_enforces_frozen_financial_projection() -> None:
    mapping = preview_module("mapping")
    row = _replace_target(mapping, _valid_row_projection(mapping), "BilledCost", Decimal("7"))

    with pytest.raises(mapping.PreviewRowValidationError) as caught:
        _validate(mapping, row)

    assert caught.value.rule_id is mapping.PreviewRowRuleId.FINANCIAL_PROJECTION
    assert caught.value.column == "BilledCost"


@pytest.mark.parametrize(
    ("column", "value"),
    [
        ("BillingPeriodStart", datetime(2026, 7, 2, tzinfo=UTC)),
        ("BillingPeriodEnd", datetime(2026, 7, 1, tzinfo=UTC)),
        ("ChargePeriodStart", datetime(2026, 6, 30, tzinfo=UTC)),
        ("ChargePeriodEnd", datetime(2026, 8, 2, tzinfo=UTC)),
    ],
)
def test_validate_preview_row_enforces_period_order_and_containment(column: str, value: datetime) -> None:
    mapping = preview_module("mapping")
    row = _replace_target(mapping, _valid_row_projection(mapping), column, value)

    with pytest.raises(mapping.PreviewRowValidationError) as caught:
        _validate(mapping, row)

    assert caught.value.rule_id is mapping.PreviewRowRuleId.PERIOD_CONTAINMENT
    assert caught.value.column == column


@pytest.mark.parametrize("column", ["SkuId", "SkuPriceId", "SkuPriceDetails", "x_ChitraguptaSkuComponents"])
def test_validate_preview_row_recomputes_derived_sku_contract(column: str) -> None:
    mapping = preview_module("mapping")
    row = _valid_row_projection(mapping)
    replacement = "{}" if column.endswith(("Details", "Components")) else f"wrong-{column}"
    row = (
        _replace_target(mapping, row, column, replacement)
        if column in mapping.FOCUS_1_4_FULL_COLUMNS
        else _replace_custom(mapping, row, column, replacement)
    )

    with pytest.raises(mapping.PreviewRowValidationError) as caught:
        _validate(mapping, row)

    assert caught.value.rule_id is mapping.PreviewRowRuleId.DERIVED_KEY
    assert caught.value.column == column


@pytest.mark.parametrize("column", ["InvoiceId", "InvoiceDetailId", "InvoiceIssuerName"])
def test_validate_preview_row_keeps_invoice_fields_separate_from_custom_correlations(column: str) -> None:
    mapping = preview_module("mapping")
    row = _replace_target(mapping, _valid_row_projection(mapping), column, "billing-scope-1")

    with pytest.raises(mapping.PreviewRowValidationError) as caught:
        _validate(mapping, row)

    assert caught.value.rule_id is mapping.PreviewRowRuleId.INVOICE_SEPARATION
    assert caught.value.column == column


def test_validate_preview_row_requires_declared_gap_coverage_for_gap_null() -> None:
    mapping = preview_module("mapping")
    rules = tuple(
        replace(rule, gap_code=None, owner_task=None) if rule.column == "BillingCurrency" else rule
        for rule in mapping.FOCUS_1_4_COLUMN_RULES
    )

    with pytest.raises(mapping.PreviewRowValidationError) as caught:
        _validate(mapping, _valid_row_projection(mapping), target_rules=rules)

    assert caught.value.rule_id is mapping.PreviewRowRuleId.GAP_COVERAGE
    assert caught.value.column == "BillingCurrency"


@pytest.mark.parametrize(
    ("column", "value"),
    [("BillingCurrency", "USD"), ("RegionName", "US East")],
)
def test_validate_preview_row_enforces_declared_gap_required_nulls(
    column: str,
    value: str,
) -> None:
    mapping = preview_module("mapping")
    row = _replace_target(mapping, _valid_row_projection(mapping), column, value)

    with pytest.raises(mapping.PreviewRowValidationError) as caught:
        _validate(mapping, row)

    assert caught.value.rule_id is mapping.PreviewRowRuleId.GAP_COVERAGE
    assert caught.value.column == column


def test_validate_preview_row_enforces_exact_declared_gap_ownership() -> None:
    mapping = preview_module("mapping")
    rules = tuple(
        replace(rule, gap_code="invoice_identity_unavailable") if rule.column == "BillingCurrency" else rule
        for rule in mapping.FOCUS_1_4_COLUMN_RULES
    )

    with pytest.raises(mapping.PreviewRowValidationError) as caught:
        _validate(mapping, _valid_row_projection(mapping), target_rules=rules)

    assert caught.value.rule_id is mapping.PreviewRowRuleId.GAP_COVERAGE
    assert caught.value.column == "BillingCurrency"


def test_validate_preview_row_requires_sku_evidence_when_pricing_is_emitted() -> None:
    mapping = preview_module("mapping")
    row = _valid_row_projection(mapping)
    for column in ("SkuId", "SkuMeter", "SkuPriceDetails", "SkuPriceId"):
        row = _replace_target(mapping, row, column, None)
    row = _replace_custom(mapping, row, "x_ChitraguptaSkuComponents", None)

    with pytest.raises(mapping.PreviewRowValidationError) as caught:
        _validate(mapping, row)

    assert caught.value.rule_id is mapping.PreviewRowRuleId.DEPENDENT_FIELDS
    assert caught.value.column == "SkuId"


@pytest.mark.parametrize(
    ("column", "value", "rule_id"),
    [
        ("x_ChitraguptaBillingScopeId", "wrong-scope", "derived_key"),
        ("x_ChitraguptaMappingProfileVersion", "focus-1.4-daily-full-v2", "allowed_value"),
        ("x_ConfluentLineType", "FUTURE_PROVIDER_LINE", "allowed_value"),
    ],
)
def test_validate_preview_row_recomputes_exact_custom_profile_authorities(
    column: str,
    value: str,
    rule_id: str,
) -> None:
    mapping = preview_module("mapping")
    row = _replace_custom(mapping, _valid_row_projection(mapping), column, value)

    with pytest.raises(mapping.PreviewRowValidationError) as caught:
        _validate(mapping, row)

    assert caught.value.rule_id is mapping.PreviewRowRuleId(rule_id)
    assert caught.value.column == column


def test_billing_scope_is_always_the_canonical_account_month_digest() -> None:
    mapping = preview_module("mapping")
    row = _valid_row_projection(mapping)
    billing_scope = row.custom_values[mapping.CUSTOM_EVIDENCE_COLUMNS.index("x_ChitraguptaBillingScopeId")]

    assert billing_scope == mapping._hash_key(
        "billing-scope",
        {
            "billing_account_id": "org-1",
            "billing_period_start": "2026-07-01T00:00:00Z",
        },
    )
    assert _validate(mapping, row) is None


def test_validate_preview_row_rejects_noncanonical_billing_scope_without_trusting_auto_seeded_authority() -> None:
    mapping = preview_module("mapping")
    row = _valid_row_projection(mapping)
    custom_values = list(row.custom_values)
    custom_values[mapping.CUSTOM_EVIDENCE_COLUMNS.index("x_ChitraguptaBillingScopeId")] = "billing-scope-1"
    untrusted = mapping.PreviewRowProjection(
        target_values=row.target_values,
        custom_values=tuple(custom_values),
        financials=row.financials,
    )

    with pytest.raises(mapping.PreviewRowValidationError) as caught:
        _validate(mapping, untrusted)

    assert caught.value.rule_id is mapping.PreviewRowRuleId.DERIVED_KEY
    assert caught.value.column == "x_ChitraguptaBillingScopeId"


def test_row_projection_has_no_private_derived_authority_state() -> None:
    mapping = preview_module("mapping")

    assert tuple(field.name for field in fields(mapping.PreviewRowProjection)) == (
        "target_values",
        "custom_values",
        "financials",
    )


@pytest.mark.parametrize(
    ("component_path", "extra_key"),
    [
        ((), "request_id"),
        ((), "price"),
        (("sku",), "request_id"),
        (("sku",), "price"),
        (("sku_price",), "request_id"),
        (("sku_price",), "price"),
    ],
)
def test_validate_preview_row_rejects_extra_sku_schema_keys(
    component_path: tuple[str, ...],
    extra_key: str,
) -> None:
    mapping = preview_module("mapping")
    row = _valid_row_projection(mapping)
    components = json.loads(row.custom_values[mapping.CUSTOM_EVIDENCE_COLUMNS.index("x_ChitraguptaSkuComponents")])
    target = components if not component_path else components[component_path[0]]
    target[extra_key] = "forbidden"
    row = _replace_custom(
        mapping,
        row,
        "x_ChitraguptaSkuComponents",
        mapping._canonical_json(components),
    )
    row = _replace_target(mapping, row, "SkuId", mapping._hash_key("sku", components["sku"]))
    row = _replace_target(
        mapping,
        row,
        "SkuPriceDetails",
        mapping._canonical_json(components["sku_price"]),
    )
    row = _replace_target(
        mapping,
        row,
        "SkuPriceId",
        mapping._hash_key("sku-price", components["sku_price"]),
    )

    with pytest.raises(mapping.PreviewRowValidationError) as caught:
        _validate(mapping, row)

    assert caught.value.rule_id is mapping.PreviewRowRuleId.DERIVED_KEY
    assert caught.value.column == "x_ChitraguptaSkuComponents"


def test_validate_preview_row_derives_closed_sku_schemas_from_row_authorities() -> None:
    mapping = preview_module("mapping")
    row = _valid_row_projection(mapping)
    values = dict(zip(mapping.FOCUS_1_4_FULL_COLUMNS, row.target_values, strict=True))
    custom_values = dict(zip(mapping.CUSTOM_EVIDENCE_COLUMNS, row.custom_values, strict=True))
    components = json.loads(custom_values["x_ChitraguptaSkuComponents"])
    tier_dimensions = json.loads(custom_values["x_ConfluentTierDimensions"])

    assert components == {
        "schema_version": "v1",
        "sku": {
            "line_type": custom_values["x_ConfluentLineType"],
            "product": custom_values["x_ConfluentProduct"],
        },
        "sku_price": {
            "cloud": values["HostProviderName"],
            "line_type": custom_values["x_ConfluentLineType"],
            "network_access_type": custom_values["x_ConfluentNetworkAccessType"],
            "product": custom_values["x_ConfluentProduct"],
            "region": values["RegionId"],
            "resource_type": values["ResourceType"],
            "tier_dimensions": [list(item) for item in tier_dimensions.items()],
        },
    }
    assert _validate(mapping, row) is None


def test_validate_preview_row_matches_tier_evidence_to_sku_price_components() -> None:
    mapping = preview_module("mapping")
    row = _replace_custom(
        mapping,
        _valid_row_projection(mapping),
        "x_ConfluentTierDimensions",
        '{"lower_bound":"100","upper_bound":"200"}',
    )

    with pytest.raises(mapping.PreviewRowValidationError) as caught:
        _validate(mapping, row)

    assert caught.value.rule_id is mapping.PreviewRowRuleId.DERIVED_KEY
    assert caught.value.column == "x_ConfluentTierDimensions"


def test_validate_preview_row_rejects_self_consistent_sku_with_wrong_tier_evidence() -> None:
    mapping = preview_module("mapping")
    row = _valid_row_projection(mapping)
    components = json.loads(row.custom_values[mapping.CUSTOM_EVIDENCE_COLUMNS.index("x_ChitraguptaSkuComponents")])
    components["sku_price"]["tier_dimensions"] = [["lower_bound", "100"], ["upper_bound", "200"]]
    row = _replace_custom(
        mapping,
        row,
        "x_ChitraguptaSkuComponents",
        mapping._canonical_json(components),
    )
    row = _replace_target(
        mapping,
        row,
        "SkuPriceDetails",
        mapping._canonical_json(components["sku_price"]),
    )
    row = _replace_target(
        mapping,
        row,
        "SkuPriceId",
        mapping._hash_key("sku-price", components["sku_price"]),
    )

    with pytest.raises(mapping.PreviewRowValidationError) as caught:
        _validate(mapping, row)

    assert caught.value.rule_id is mapping.PreviewRowRuleId.DERIVED_KEY
    assert caught.value.column == "x_ChitraguptaSkuComponents"


@pytest.mark.parametrize(
    ("component", "replacement"),
    [
        ("product", "CONNECT"),
        ("line_type", "CONNECT_CAPACITY"),
        ("cloud", "GCP"),
        ("region", "us-west1"),
        ("network_access_type", "PRIVATE"),
        ("resource_type", "connector"),
    ],
)
def test_validate_preview_row_binds_every_sku_component_to_retained_row_authority(
    component: str,
    replacement: str,
) -> None:
    mapping = preview_module("mapping")
    row = _valid_row_projection(mapping)
    components = json.loads(row.custom_values[mapping.CUSTOM_EVIDENCE_COLUMNS.index("x_ChitraguptaSkuComponents")])
    if component in {"product", "line_type"}:
        components["sku"][component] = replacement
    components["sku_price"][component] = replacement
    row = _replace_custom(
        mapping,
        row,
        "x_ChitraguptaSkuComponents",
        mapping._canonical_json(components),
    )
    row = _replace_target(mapping, row, "SkuId", mapping._hash_key("sku", components["sku"]))
    row = _replace_target(
        mapping,
        row,
        "SkuPriceDetails",
        mapping._canonical_json(components["sku_price"]),
    )
    row = _replace_target(
        mapping,
        row,
        "SkuPriceId",
        mapping._hash_key("sku-price", components["sku_price"]),
    )

    with pytest.raises(mapping.PreviewRowValidationError) as caught:
        _validate(mapping, row)

    assert caught.value.rule_id is mapping.PreviewRowRuleId.DERIVED_KEY
    assert caught.value.column == "x_ChitraguptaSkuComponents"


def test_tableflow_rule_truthfully_declares_provider_context_unsupported() -> None:
    mapping = preview_module("mapping")

    rule = mapping.FOCUS_1_4_SERVICE_RULES_V1[mapping.PreviewServiceRuleKey.TABLEFLOW]

    assert rule.resource_shape is mapping.PreviewResourceShape.RESOURCE_SPECIFIC
    assert rule.context_strategy == "unsupported_provider_context"
    assert rule.allowed_origin_resource_types == ()


def test_tableflow_fails_provider_context_before_any_resource_lookup(
    valid_source_evidence: object,
) -> None:
    mapping = preview_module("mapping")
    source = replace(
        valid_source_evidence,
        native_product="TABLEFLOW",
        native_line_type="TABLEFLOW_DATA_PROCESSED",
        native_description="Tableflow data processed",
        resource_id="lkc-1:topic:orders",
    )
    classification = mapping.classify_daily_full_source(
        request_start=REQUEST_START,
        request_end=REQUEST_END,
        source=source,
    )
    assert isinstance(classification, mapping.AcceptedPreviewSource)
    resources = MagicMock(spec=ResourceRepository)
    synthetic_topic = CoreResource(
        ecosystem="confluent_cloud",
        tenant_id="tenant-1",
        resource_id="lkc-1:topic:orders",
        resource_type="topic",
        parent_id="lkc-1",
        metadata={"provider_cloud": "AWS", "provider_region": "us-east-1"},
    )

    with pytest.raises(mapping.PreviewProviderContextIncompleteError):
        mapping.resolve_provider_resource_context(
            source=source,
            semantics=classification.semantics,
            origin_resource=synthetic_topic,
            resources=resources,
        )

    resources.get.assert_not_called()


def test_concrete_resource_type_mismatch_is_provider_context_incomplete(
    valid_source_evidence: object,
) -> None:
    mapping = preview_module("mapping")
    classification = mapping.classify_daily_full_source(
        request_start=REQUEST_START,
        request_end=REQUEST_END,
        source=valid_source_evidence,
    )
    assert isinstance(classification, mapping.AcceptedPreviewSource)
    wrong_type = CoreResource(
        ecosystem="confluent_cloud",
        tenant_id="tenant-1",
        resource_id="lkc-1",
        resource_type="connector",
        parent_id="lkc-parent",
        metadata={"provider_cloud": "AWS", "provider_region": "us-east-1"},
    )

    with pytest.raises(mapping.PreviewProviderContextIncompleteError):
        mapping.resolve_provider_resource_context(
            source=valid_source_evidence,
            semantics=classification.semantics,
            origin_resource=wrong_type,
            resources=MagicMock(spec=ResourceRepository),
        )


def test_native_product_and_line_type_disagreement_is_classification_ambiguous(
    valid_source_evidence: object,
) -> None:
    mapping = preview_module("mapping")

    result = mapping.classify_daily_full_source(
        request_start=REQUEST_START,
        request_end=REQUEST_END,
        source=replace(valid_source_evidence, native_product="CONNECT"),
    )

    assert isinstance(result, mapping.RejectedPreviewSource)
    assert result.issue is mapping.PreviewSourceIssue.CHARGE_CLASSIFICATION_AMBIGUOUS


def test_native_product_rules_cover_current_provider_products_once() -> None:
    mapping = preview_module("mapping")
    expected_products = {
        "KAFKA",
        "CONNECT",
        "KSQL",
        "AUDIT_LOG",
        "STREAM_GOVERNANCE",
        "CLUSTER_LINK",
        "CUSTOM_CONNECT",
        "FLINK",
        "TABLEFLOW",
        "SUPPORT_CLOUD_BASIC",
        "SUPPORT_CLOUD_DEVELOPER",
        "SUPPORT_CLOUD_BUSINESS",
        "SUPPORT_CLOUD_PREMIER",
        "USM",
    }

    rules = mapping.NATIVE_PRODUCT_SERVICE_RULES_V1

    assert set(rules) == expected_products
    assert len(rules) == len(expected_products)
    assert all(rule.service_rule_key in mapping.FOCUS_1_4_SERVICE_RULES_V1 for rule in rules.values())


@pytest.mark.parametrize(
    ("component", "row_kwargs", "source_changes", "expected_changed"),
    [
        ("product", {}, {"native_product": "CUSTOM_CONNECT"}, ("SkuId", "SkuPriceId")),
        ("line_type", {}, {"native_line_type": "CONNECT_NUM_TASKS"}, ("SkuId", "SkuPriceId")),
        ("cloud", {"cloud": "GCP"}, {}, ("SkuPriceId",)),
        ("network_access_type", {}, {"native_network_access_type": "PRIVATE"}, ("SkuPriceId",)),
        ("region", {"region": "us-west1"}, {}, ("SkuPriceId",)),
        ("resource_type", {"resource_type": "managed_connector"}, {}, ("SkuPriceId",)),
        (
            "tier_dimensions",
            {},
            {"native_tier_dimensions": (("lower_bound", "100"), ("upper_bound", "200"))},
            ("SkuPriceId",),
        ),
    ],
)
def test_derived_sku_keys_change_for_every_included_component(
    valid_source_evidence: Any,
    valid_aggregate_evidence: Any,
    valid_allocation_evidence: Any,
    component: str,
    row_kwargs: dict[str, str],
    source_changes: dict[str, object],
    expected_changed: tuple[str, ...],
) -> None:
    mapping = preview_module("mapping")
    connect_source = replace(
        valid_source_evidence,
        native_product="CONNECT",
        native_line_type="CONNECT_CAPACITY",
        native_description="Connect capacity",
        resource_id="lcc-1",
    )
    baseline = _package_row(
        mapping,
        source=connect_source,
        aggregate=valid_aggregate_evidence,
        allocation=valid_allocation_evidence,
        resource_id="lcc-1",
        resource_type="connector",
    )
    changed_row_kwargs = {"resource_id": "lcc-1", "resource_type": "connector", **row_kwargs}
    changed = _package_row(
        mapping,
        source=replace(connect_source, **source_changes),
        aggregate=valid_aggregate_evidence,
        allocation=valid_allocation_evidence,
        **changed_row_kwargs,
    )

    for key in ("SkuId", "SkuPriceId"):
        assert (changed[key] != baseline[key]) is (key in expected_changed), f"{component} -> {key}"


@pytest.mark.parametrize(
    ("excluded", "row_kwargs", "source_changes"),
    [
        ("price and quantity", {}, {"price": Decimal("5"), "quantity": Decimal("2")}),
        (
            "charge dates",
            {},
            {
                "source_period_start": datetime(2026, 7, 1, 1, tzinfo=UTC),
                "source_period_end": datetime(2026, 7, 1, 23, tzinfo=UTC),
            },
        ),
        ("billing account", {"billing_account_id": "org-2"}, {}),
        ("tenant and request", {"request_id": "request-2"}, {}),
        ("resource identifier", {"resource_id": "lkc-2"}, {"resource_id": "lkc-2"}),
        ("provider Cost identifier", {}, {"provider_cost_id": "cost-2", "source_record_id": "provider:cost-2"}),
        ("calculation identifier", {"calculation_id": "calculation-2"}, {}),
    ],
)
def test_derived_sku_keys_ignore_every_forbidden_volatile_component(
    valid_source_evidence: Any,
    valid_aggregate_evidence: Any,
    valid_allocation_evidence: Any,
    excluded: str,
    row_kwargs: dict[str, str],
    source_changes: dict[str, object],
) -> None:
    mapping = preview_module("mapping")
    baseline = _package_row(
        mapping,
        source=valid_source_evidence,
        aggregate=valid_aggregate_evidence,
        allocation=valid_allocation_evidence,
    )
    changed = _package_row(
        mapping,
        source=replace(valid_source_evidence, **source_changes),
        aggregate=valid_aggregate_evidence,
        allocation=valid_allocation_evidence,
        **row_kwargs,
    )

    assert (changed["SkuId"], changed["SkuPriceId"]) == (
        baseline["SkuId"],
        baseline["SkuPriceId"],
    ), excluded


def test_sku_component_evidence_is_canonical_and_recomputes_both_parented_keys(
    valid_source_evidence: Any,
    valid_aggregate_evidence: Any,
    valid_allocation_evidence: Any,
) -> None:
    mapping = preview_module("mapping")
    row = _package_row(
        mapping,
        source=valid_source_evidence,
        aggregate=valid_aggregate_evidence,
        allocation=valid_allocation_evidence,
    )
    components = json.loads(row["x_ChitraguptaSkuComponents"])

    assert row["x_ChitraguptaSkuComponents"] == mapping._canonical_json(components)
    assert row["SkuPriceDetails"] == mapping._canonical_json(components["sku_price"])
    assert row["SkuId"] == mapping._hash_key("sku", components["sku"])
    assert row["SkuPriceId"] == mapping._hash_key("sku-price", components["sku_price"])
    assert components["sku"]["product"] == components["sku_price"]["product"]
    assert components["sku"]["line_type"] == components["sku_price"]["line_type"]


def test_billing_scope_is_stable_within_account_month_and_changes_with_account(
    valid_source_evidence: Any,
    valid_aggregate_evidence: Any,
    valid_allocation_evidence: Any,
) -> None:
    mapping = preview_module("mapping")
    baseline = _package_row(
        mapping,
        source=valid_source_evidence,
        aggregate=valid_aggregate_evidence,
        allocation=valid_allocation_evidence,
    )
    unrelated_changes = _package_row(
        mapping,
        source=replace(
            valid_source_evidence,
            provider_cost_id="cost-2",
            source_record_id="provider:cost-2",
            resource_id="lkc-2",
        ),
        aggregate=valid_aggregate_evidence,
        allocation=valid_allocation_evidence,
        request_id="request-2",
        resource_id="lkc-2",
    )
    other_account = _package_row(
        mapping,
        source=valid_source_evidence,
        aggregate=valid_aggregate_evidence,
        allocation=valid_allocation_evidence,
        billing_account_id="org-2",
    )

    assert unrelated_changes["x_ChitraguptaBillingScopeId"] == baseline["x_ChitraguptaBillingScopeId"]
    assert other_account["x_ChitraguptaBillingScopeId"] != baseline["x_ChitraguptaBillingScopeId"]


def test_priced_row_always_emits_repeatable_sku_identity(
    valid_source_evidence: Any,
    valid_aggregate_evidence: Any,
    valid_allocation_evidence: Any,
) -> None:
    mapping = preview_module("mapping")
    first = _package_row(
        mapping,
        source=valid_source_evidence,
        aggregate=valid_aggregate_evidence,
        allocation=valid_allocation_evidence,
    )
    second = _package_row(
        mapping,
        source=valid_source_evidence,
        aggregate=valid_aggregate_evidence,
        allocation=valid_allocation_evidence,
    )

    assert first["PricingQuantity"]
    assert first["SkuId"]
    assert first["SkuPriceId"]
    assert (first["SkuId"], first["SkuPriceId"]) == (second["SkuId"], second["SkuPriceId"])


def test_sku_identity_is_deterministic_when_optional_price_components_are_missing(
    valid_source_evidence: Any,
    valid_aggregate_evidence: Any,
    valid_allocation_evidence: Any,
) -> None:
    mapping = preview_module("mapping")
    source = replace(
        valid_source_evidence,
        native_network_access_type=None,
        native_tier_dimensions=(),
    )

    first = _package_row(
        mapping,
        source=source,
        aggregate=valid_aggregate_evidence,
        allocation=valid_allocation_evidence,
    )
    second = _package_row(
        mapping,
        source=source,
        aggregate=valid_aggregate_evidence,
        allocation=valid_allocation_evidence,
    )

    assert first["x_ConfluentNetworkAccessType"] == ""
    assert first["x_ConfluentTierDimensions"] == "{}"
    assert first["SkuPriceId"] == second["SkuPriceId"]


def test_package_manifest_reports_the_complete_validated_v3_profile(
    valid_source_evidence: Any,
    valid_aggregate_evidence: Any,
    valid_allocation_evidence: Any,
) -> None:
    mapping = preview_module("mapping")
    _row, manifest = _package_row(
        mapping,
        source=valid_source_evidence,
        aggregate=valid_aggregate_evidence,
        allocation=valid_allocation_evidence,
        include_manifest=True,
    )

    assert manifest["mapping_profile_version"] == "focus-1.4-daily-full-v3"
    assert manifest["conformance_status"] == "non_conforming"
    assert manifest["validation"] == {
        "mapping_errors": 0,
        "mapping_profile_version": "focus-1.4-daily-full-v3",
        "rows": 1,
        "source_records": 1,
        "status": "passed",
    }
    assert manifest["profile_not_applicable_columns"] == list(mapping.PROFILE_NOT_APPLICABLE_COLUMNS)
    assert manifest["known_gaps"] == [
        {
            "code": gap.code,
            "columns": list(gap.columns),
            "description": gap.description,
            "owner_task": gap.owner_task,
        }
        for gap in mapping.KNOWN_GAPS
    ]


@pytest.mark.parametrize(
    ("error_name", "code", "message"),
    [
        (
            "PreviewSourceEvidenceError",
            "preview_source_record_incomplete",
            "One or more source records lack required Preview evidence.",
        ),
        (
            "PreviewFinancialUnsupportedError",
            "preview_source_economics_unsupported",
            "One or more source records have unsupported monetary or quantity values.",
        ),
        (
            "PreviewFinancialReconciliationError",
            "preview_source_reconciliation_failed",
            "Persisted source, aggregate, or allocation evidence does not reconcile.",
        ),
        (
            "PreviewMappingScopeError",
            "preview_mapping_scope_unsupported",
            "The complete source set exceeds the current Daily Full mapping scope.",
        ),
        (
            "PreviewBillingAccountUnavailableError",
            "preview_billing_account_unavailable",
            "Authoritative Confluent Cloud organization evidence is unavailable for this tenant.",
        ),
        (
            "PreviewBillingAccountConflictError",
            "preview_billing_account_conflicting",
            "Persisted Confluent Cloud organization evidence conflicts for this tenant.",
        ),
        (
            "PreviewProviderContextIncompleteError",
            "preview_provider_context_incomplete",
            "Authoritative provider resource context is unavailable for one or more source records.",
        ),
        (
            "PreviewRowValidationError",
            "preview_mapping_validation_failed",
            "The generated row does not satisfy the Daily Full mapping profile.",
        ),
    ],
)
def test_mapping_failure_exhaustively_transports_each_typed_mapping_error(
    error_name: str,
    code: str,
    message: str,
) -> None:
    mapping = preview_module("mapping")
    service = preview_module("service")
    error_type = getattr(mapping, error_name)
    error = (
        error_type(mapping.PreviewRowRuleId.TYPE, column="BilledCost")
        if error_name == "PreviewRowValidationError"
        else error_type("private detail")
    )

    failure = service._mapping_failure(error, ("corr-1", "corr-2"))

    assert failure.diagnostic.code == code
    assert failure.diagnostic.message == message
    assert failure.diagnostic.retryable is False
    assert failure.diagnostic.source_correlation_ids == ("corr-1", "corr-2")


def test_mapping_failure_has_no_assertion_based_catch_all() -> None:
    service = preview_module("service")
    function = ast.parse(inspect.getsource(service._mapping_failure))

    assert not any(isinstance(node, ast.Assert) for node in ast.walk(function))


def test_gap_ownership_bookkeeping_does_not_allocate_throwaway_setdefault_lists() -> None:
    mapping = preview_module("mapping")
    function = ast.parse(inspect.getsource(mapping._rule_gap_ownership))

    assert not any(
        isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "setdefault"
        for node in ast.walk(function)
    )
