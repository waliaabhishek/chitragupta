from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from core.preview.mapping import (
    PreviewReconciliationError,
    PreviewSourceSnapshotError,
    PreviewTracerScopeError,
)
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
        ({"native_line_type": "SUPPORT"}, "preview_source_mapping_unavailable"),
        ({"native_line_type": "KAFKA_STREAMS"}, "preview_source_mapping_unavailable"),
        ({"native_line_type": "PROMO_CREDIT"}, "preview_charge_classification_ambiguous"),
        ({"native_line_type": "FUTURE_LINE_TYPE"}, "preview_source_line_type_unsupported"),
        ({"resource_id": None}, "preview_source_record_incomplete"),
        ({"amount": Decimal("0")}, "preview_source_economics_unsupported"),
        ({"amount": Decimal("NaN")}, "preview_source_economics_unsupported"),
        ({"amount": Decimal("7")}, "preview_source_reconciliation_failed"),
    ],
)
def test_authoritative_source_classifier_matrix(
    valid_source_evidence: object,
    changes: dict[str, object],
    expected_issue: str,
) -> None:
    mapping = preview_module("mapping")
    source = replace(valid_source_evidence, **changes)

    issue = mapping.classify_daily_full_source(
        request_start=REQUEST_START,
        request_end=REQUEST_END,
        source=source,
    )

    assert issue.value == expected_issue


def test_authoritative_source_classifier_accepts_current_ordinary_metered_source(
    valid_source_evidence: object,
) -> None:
    mapping = preview_module("mapping")

    assert (
        mapping.classify_daily_full_source(
            request_start=REQUEST_START,
            request_end=REQUEST_END,
            source=valid_source_evidence,
        )
        is None
    )


@pytest.mark.parametrize(
    ("changes", "error_type"),
    [
        ({"malformed": True}, PreviewSourceSnapshotError),
        ({"native_line_type": "SUPPORT"}, PreviewTracerScopeError),
        ({"native_line_type": "KAFKA_STREAMS"}, PreviewTracerScopeError),
        ({"native_line_type": "PROMO_CREDIT"}, PreviewTracerScopeError),
        ({"amount": Decimal("7")}, PreviewReconciliationError),
    ],
)
def test_validate_daily_full_source_consumes_classifier_result(
    valid_source_evidence: object,
    changes: dict[str, object],
    error_type: type[Exception],
) -> None:
    mapping = preview_module("mapping")
    source = replace(valid_source_evidence, **changes)

    with pytest.raises(error_type):
        mapping.validate_daily_full_source(
            request_start=REQUEST_START,
            request_end=REQUEST_END,
            source=source,
        )


@pytest.mark.parametrize(
    "description",
    [
        "credit for Kafka storage",
        "customer refund",
        "billing adjustment",
        "pricing correction",
        "charge reversal",
        "volume rebate",
        "prior period true-up",
    ],
)
def test_credit_like_text_is_never_positively_classified(
    valid_source_evidence: object,
    description: str,
) -> None:
    mapping = preview_module("mapping")
    issue = mapping.classify_daily_full_source(
        request_start=REQUEST_START,
        request_end=REQUEST_END,
        source=replace(valid_source_evidence, native_description=description),
    )

    assert issue is mapping.PreviewSourceIssue.CHARGE_CLASSIFICATION_AMBIGUOUS


def test_source_issue_is_str_enum_with_stable_values() -> None:
    mapping = preview_module("mapping")

    assert str(mapping.PreviewSourceIssue.MAPPING_UNAVAILABLE) == "preview_source_mapping_unavailable"


def test_mapping_profile_and_currency_gap_advance_without_mapping_billing_currency() -> None:
    mapping = preview_module("mapping")
    gaps = {gap.code: gap for gap in mapping.KNOWN_GAPS}

    assert mapping.MAPPING_PROFILE_VERSION == "focus-1.4-daily-full-tracer-v2"
    assert "commercial_arrangement_and_billing_currency_authority_pending" not in gaps
    assert gaps["provider_billing_currency_field_unavailable"].columns == ("BillingCurrency",)
    assert gaps["provider_billing_currency_field_unavailable"].owner_task == "TASK-254.03"
