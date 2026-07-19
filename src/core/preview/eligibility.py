from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import TYPE_CHECKING, Literal

from core.preview.mapping import PreviewSourceIssue
from core.preview.models import PreviewDiagnostic, PreviewRequest

if TYPE_CHECKING:
    from core.config.models import TenantConfig

logger = logging.getLogger(__name__)

MAX_DIAGNOSTIC_SOURCE_CORRELATIONS = 20


@dataclass(frozen=True)
class PreviewEligibilityPolicy:
    commercial_profile: Literal["direct_payg"] | None
    billing_currency: str | None
    effective_start_date: date | None
    effective_end_date: date | None
    acquisition_start_date: date
    acquisition_end_date: date


def policy_from_tenant_config(
    tenant_config: TenantConfig,
    *,
    created_at: datetime,
) -> PreviewEligibilityPolicy:
    if created_at.tzinfo is None or created_at.utcoffset() is None:
        raise ValueError("created_at must be timezone-aware")
    anchor_date = created_at.astimezone(UTC).date()
    configured = tenant_config.focus_preview
    return PreviewEligibilityPolicy(
        commercial_profile=configured.commercial_profile if configured else None,
        billing_currency=configured.billing_currency if configured else None,
        effective_start_date=configured.effective_start_date if configured else None,
        effective_end_date=configured.effective_end_date if configured else None,
        acquisition_start_date=anchor_date - timedelta(days=tenant_config.lookback_days),
        acquisition_end_date=anchor_date - timedelta(days=tenant_config.cutoff_days),
    )


def request_eligibility_diagnostic(
    *,
    request: PreviewRequest,
    policy: PreviewEligibilityPolicy,
) -> PreviewDiagnostic | None:
    if (
        policy.commercial_profile != "direct_payg"
        or policy.billing_currency is None
        or policy.effective_start_date is None
        or policy.effective_end_date is None
        or request.start_date < policy.effective_start_date
        or request.end_date > policy.effective_end_date
    ):
        return PreviewDiagnostic(
            "preview_commercial_profile_unavailable",
            "An explicit Direct-billed PAYG profile does not cover the requested interval.",
            False,
        )
    if policy.billing_currency != "USD":
        return PreviewDiagnostic(
            "preview_billing_currency_unsupported",
            "FOCUS Mapping Preview currently supports only USD billing currency.",
            False,
        )
    return None


def public_source_correlation_id(*, ecosystem: str, tenant_id: str, source_record_id: str) -> str:
    digest = hashlib.sha256(f"{ecosystem}\0{tenant_id}\0{source_record_id}".encode()).hexdigest()
    return f"src:v1:{digest}"


def capped_correlations(values: tuple[str, ...] | list[str]) -> tuple[str, ...]:
    return tuple(sorted(set(values))[:MAX_DIAGNOSTIC_SOURCE_CORRELATIONS])


_SOURCE_DIAGNOSTICS = {
    PreviewSourceIssue.RECORD_MALFORMED: "One or more persisted Confluent Costs API records are malformed.",
    PreviewSourceIssue.SCOPE_UNSUPPORTED: (
        "One or more source records are not fully contained in the requested Daily scope."
    ),
    PreviewSourceIssue.CHARGE_CLASSIFICATION_AMBIGUOUS: (
        "One or more credit, refund, adjustment, or correction-like records cannot be classified authoritatively."
    ),
    PreviewSourceIssue.LINE_TYPE_UNKNOWN: "One or more source records have no supported line type.",
    PreviewSourceIssue.LINE_TYPE_UNSUPPORTED: "One or more Confluent Costs API line types are unsupported.",
    PreviewSourceIssue.MAPPING_UNAVAILABLE: (
        "One or more known source line types do not yet have a complete Preview mapping."
    ),
    PreviewSourceIssue.RECORD_INCOMPLETE: "One or more source records lack required Preview evidence.",
    PreviewSourceIssue.ECONOMICS_UNSUPPORTED: (
        "One or more source records have unsupported monetary or quantity values."
    ),
    PreviewSourceIssue.RECONCILIATION_FAILED: (
        "Persisted source, aggregate, or allocation evidence does not reconcile."
    ),
}


def source_issue_diagnostic(
    issue: PreviewSourceIssue,
    correlations: tuple[str, ...],
) -> PreviewDiagnostic:
    return PreviewDiagnostic(issue.value, _SOURCE_DIAGNOSTICS[issue], False, capped_correlations(correlations))
