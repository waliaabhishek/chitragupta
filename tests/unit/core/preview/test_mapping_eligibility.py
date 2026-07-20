from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from tests.unit.core.preview.conftest import preview_module

REQUEST_START = datetime(2026, 7, 1, tzinfo=UTC)
REQUEST_END = datetime(2026, 7, 2, tzinfo=UTC)


@pytest.mark.parametrize(
    ("changes", "expected_issue"),
    [
        ({"malformed": True}, "preview_source_record_malformed"),
        ({"diagnostics": ("invalid_date:start_date",)}, "preview_source_record_malformed"),
        ({"source_period_start": None}, "preview_source_scope_unsupported"),
        (
            {"source_period_start": datetime(2026, 6, 30, tzinfo=UTC)},
            "preview_source_scope_unsupported",
        ),
        ({"native_description": "Prior period correction"}, "preview_charge_classification_ambiguous"),
        ({"native_line_type": None}, "preview_source_line_type_unknown"),
        ({"native_line_type": "KAFKA_STREAMS", "unit": None}, "preview_source_mapping_unavailable"),
        ({"native_line_type": "FUTURE_LINE_TYPE"}, "preview_source_line_type_unsupported"),
        ({"resource_id": None}, "preview_source_record_incomplete"),
        ({"native_product": "CONNECT"}, "preview_charge_classification_ambiguous"),
    ],
)
def test_authoritative_source_classifier_rejects_structural_or_semantic_conflicts(
    valid_source_evidence: object,
    changes: dict[str, object],
    expected_issue: str,
) -> None:
    mapping = preview_module("mapping")
    source = replace(valid_source_evidence, **changes)

    result = mapping.classify_daily_full_source(
        request_start=REQUEST_START,
        request_end=REQUEST_END,
        source=source,
    )

    assert isinstance(result, mapping.RejectedPreviewSource)
    assert result.issue.value == expected_issue


@pytest.mark.parametrize(
    ("changes", "expected_kind", "expected_category", "expected_frequency", "expected_rule"),
    [
        ({}, "metered_usage", "Usage", "Usage-Based", "kafka"),
        ({"native_line_type": "KAFKA_STREAMS"}, "metered_usage", "Usage", "Usage-Based", "kafka"),
        (
            {
                "native_product": "SUPPORT_CLOUD_BUSINESS",
                "native_line_type": "SUPPORT",
                "native_description": "Support subscription",
                "resource_id": None,
                "environment_id": None,
            },
            "recurring_support",
            "Purchase",
            "Recurring",
            "support",
        ),
        (
            {
                "native_line_type": "PROMO_CREDIT",
                "native_description": "Promotional allowance",
                "amount": Decimal("-5"),
                "original_amount": Decimal("-5"),
                "discount_amount": Decimal("0"),
                "price": None,
                "quantity": None,
                "unit": None,
                "resource_id": None,
                "environment_id": None,
            },
            "promotional_allowance",
            "Credit",
            "One-Time",
            "promotional_credit",
        ),
        (
            {
                "native_description": "Refund Kafka storage",
                "amount": Decimal("-8"),
                "original_amount": Decimal("-10"),
                "discount_amount": Decimal("-2"),
                "price": Decimal("-2"),
            },
            "usage_refund",
            "Usage",
            "Usage-Based",
            "kafka",
        ),
    ],
)
def test_authoritative_source_classifier_returns_typed_charge_semantics(
    valid_source_evidence: object,
    changes: dict[str, object],
    expected_kind: str,
    expected_category: str,
    expected_frequency: str,
    expected_rule: str,
) -> None:
    mapping = preview_module("mapping")

    result = mapping.classify_daily_full_source(
        request_start=REQUEST_START,
        request_end=REQUEST_END,
        source=replace(valid_source_evidence, **changes),
    )

    assert isinstance(result, mapping.AcceptedPreviewSource)
    assert result.semantics.kind.value == expected_kind
    assert result.semantics.charge_category == expected_category
    assert result.semantics.charge_frequency == expected_frequency
    assert result.semantics.service_rule_key.value == expected_rule


@pytest.mark.parametrize(
    ("native_product", "native_description"),
    [
        ("SUPPORT_CLOUD_BASIC", "Promotional allowance"),
        ("KAFKA", "Support service promotional credit"),
        ("SUPPORT_CLOUD_PREMIER", "Support service promotional credit"),
    ],
)
def test_non_refund_native_promo_credit_remains_one_time_credit_despite_support_context(
    valid_source_evidence: object,
    native_product: str,
    native_description: str,
) -> None:
    mapping = preview_module("mapping")
    source = replace(
        valid_source_evidence,
        native_product=native_product,
        native_line_type="PROMO_CREDIT",
        native_description=native_description,
        amount=Decimal("-5"),
        original_amount=Decimal("-5"),
        discount_amount=Decimal("0"),
        price=None,
        quantity=None,
        unit=None,
        resource_id=None,
        environment_id=None,
    )

    result = mapping.classify_daily_full_source(
        request_start=REQUEST_START,
        request_end=REQUEST_END,
        source=source,
    )

    assert isinstance(result, mapping.AcceptedPreviewSource)
    assert result.semantics.kind is mapping.PreviewChargeKind.PROMOTIONAL_ALLOWANCE
    assert result.semantics.charge_category == "Credit"
    assert result.semantics.charge_frequency == "One-Time"
    assert result.semantics.service_rule_key is mapping.PreviewServiceRuleKey.PROMOTIONAL_CREDIT


@pytest.mark.parametrize(
    ("native_product", "description", "kind", "category", "frequency", "rule_key"),
    [
        ("KAFKA", "Refund Kafka usage", "usage_refund", "Usage", "Usage-Based", "kafka"),
        ("CONNECT", "Refund Connect usage", "usage_refund", "Usage", "Usage-Based", "connect"),
        ("CUSTOM_CONNECT", "Refund custom Connect usage", "usage_refund", "Usage", "Usage-Based", "connect"),
        ("KSQL", "Refund ksqlDB usage", "usage_refund", "Usage", "Usage-Based", "ksqldb"),
        ("FLINK", "Refund Flink usage", "usage_refund", "Usage", "Usage-Based", "flink"),
        (
            "STREAM_GOVERNANCE",
            "Refund governance usage",
            "usage_refund",
            "Usage",
            "Usage-Based",
            "data_governance",
        ),
        (
            "CLUSTER_LINK",
            "Refund cluster linking usage",
            "usage_refund",
            "Usage",
            "Usage-Based",
            "cluster_link",
        ),
        ("AUDIT_LOG", "Refund audit log usage", "usage_refund", "Usage", "Usage-Based", "audit_log"),
        ("TABLEFLOW", "Refund Tableflow usage", "usage_refund", "Usage", "Usage-Based", "tableflow"),
        ("USM", "Refund USM usage", "usage_refund", "Usage", "Usage-Based", "usm"),
        (
            "SUPPORT_CLOUD_BASIC",
            "Refund support subscription",
            "support_refund",
            "Purchase",
            "Recurring",
            "support",
        ),
        (
            "SUPPORT_CLOUD_DEVELOPER",
            "Refund support subscription",
            "support_refund",
            "Purchase",
            "Recurring",
            "support",
        ),
        (
            "SUPPORT_CLOUD_BUSINESS",
            "Refund support subscription",
            "support_refund",
            "Purchase",
            "Recurring",
            "support",
        ),
        (
            "SUPPORT_CLOUD_PREMIER",
            "Refund support subscription",
            "support_refund",
            "Purchase",
            "Recurring",
            "support",
        ),
    ],
)
def test_promo_credit_refund_uses_every_native_product_authority_once(
    valid_source_evidence: object,
    native_product: str,
    description: str,
    kind: str,
    category: str,
    frequency: str,
    rule_key: str,
) -> None:
    mapping = preview_module("mapping")
    source = replace(
        valid_source_evidence,
        native_product=native_product,
        native_line_type="PROMO_CREDIT",
        native_description=description,
        amount=Decimal("-8"),
        original_amount=Decimal("-10"),
        discount_amount=Decimal("-2"),
        price=Decimal("-2"),
    )

    result = mapping.classify_daily_full_source(
        request_start=REQUEST_START,
        request_end=REQUEST_END,
        source=source,
    )

    assert isinstance(result, mapping.AcceptedPreviewSource)
    assert result.semantics.kind.value == kind
    assert result.semantics.charge_category == category
    assert result.semantics.charge_frequency == frequency
    assert result.semantics.service_rule_key.value == rule_key


def test_promotional_allowance_financial_projection_keeps_signed_costs_and_omits_pricing(
    valid_source_evidence: object,
) -> None:
    mapping = preview_module("mapping")
    source = replace(
        valid_source_evidence,
        native_line_type="PROMO_CREDIT",
        native_description="Promotional allowance",
        amount=Decimal("-5"),
        original_amount=Decimal("-5"),
        discount_amount=Decimal("0"),
        price=None,
        quantity=None,
        unit=None,
        resource_id=None,
        environment_id=None,
    )
    classification = mapping.classify_daily_full_source(
        request_start=REQUEST_START,
        request_end=REQUEST_END,
        source=source,
    )
    assert isinstance(classification, mapping.AcceptedPreviewSource)

    projection = mapping.project_financials(
        source=source,
        semantics=classification.semantics,
        billed_share=Decimal("-5"),
    )

    assert projection.billed_cost == Decimal("-5")
    assert projection.effective_cost == Decimal("-5")
    assert projection.list_cost == Decimal("-5")
    assert projection.contracted_cost == Decimal("-5")
    assert projection.list_unit_price is None
    assert projection.pricing_quantity is None
    assert projection.pricing_unit is None
    assert projection.consumed_quantity is None
    assert projection.consumed_unit is None


@pytest.mark.parametrize(
    ("changes", "error_name"),
    [
        ({"amount": Decimal("0")}, "PreviewFinancialUnsupportedError"),
        ({"amount": Decimal("NaN")}, "PreviewFinancialUnsupportedError"),
        ({"amount": Decimal("7")}, "PreviewFinancialReconciliationError"),
    ],
)
def test_financial_projection_owns_numeric_acceptability_and_arithmetic(
    valid_source_evidence: object,
    changes: dict[str, object],
    error_name: str,
) -> None:
    mapping = preview_module("mapping")
    source = replace(valid_source_evidence, **changes)
    classification = mapping.classify_daily_full_source(
        request_start=REQUEST_START,
        request_end=REQUEST_END,
        source=source,
    )
    assert isinstance(classification, mapping.AcceptedPreviewSource)

    with pytest.raises(getattr(mapping, error_name)):
        mapping.project_financials(
            source=source,
            semantics=classification.semantics,
            billed_share=source.amount,
        )


def test_source_issue_is_str_enum_with_stable_values() -> None:
    mapping = preview_module("mapping")

    assert str(mapping.PreviewSourceIssue.MAPPING_UNAVAILABLE) == "preview_source_mapping_unavailable"


def test_mapping_profile_and_currency_gap_advance_without_mapping_billing_currency() -> None:
    mapping = preview_module("mapping")
    gaps = {gap.code: gap for gap in mapping.KNOWN_GAPS}

    assert mapping.MAPPING_PROFILE_VERSION == "focus-1.4-preview-v5"
    assert "commercial_arrangement_and_billing_currency_authority_pending" not in gaps
    assert gaps["provider_billing_currency_field_unavailable"].columns == ("BillingCurrency",)
    assert gaps["provider_billing_currency_field_unavailable"].owner_task == "TASK-254.03"
