from __future__ import annotations

from datetime import UTC, datetime
from importlib import import_module

import pytest


def _module() -> object:
    return import_module("core.preview.retention")


def test_success_outcome_requires_whole_second_utc_and_no_diagnostic() -> None:
    module = _module()
    success_status = module.PreviewRetentionOutcomeStatus.SUCCESS
    ordinary_kind = module.PreviewRetentionCleanupKind.ORDINARY

    with pytest.raises(ValueError, match="diagnostic"):
        module.PreviewRetentionOutcome(
            owner="production",
            cleanup_kind=ordinary_kind,
            attempted_at=datetime(2026, 7, 30, 23, 25, 1, tzinfo=UTC),
            status=success_status,
            diagnostic=module.PreviewRetentionDiagnostic(
                code="focus_preview_ordinary_retention_failed",
                message="should not persist on success",
                error_type="RuntimeError",
            ),
        )

    with pytest.raises(ValueError, match="whole-second|UTC|aware"):
        module.PreviewRetentionOutcome(
            owner="production",
            cleanup_kind=ordinary_kind,
            attempted_at=datetime(2026, 7, 30, 23, 25, 1, 123456, tzinfo=UTC),
            status=success_status,
            diagnostic=None,
        )


def test_failure_outcome_requires_bounded_redaction_safe_diagnostic() -> None:
    module = _module()
    failure_status = module.PreviewRetentionOutcomeStatus.FAILURE
    evidence_kind = module.PreviewRetentionCleanupKind.PREVIEW_EVIDENCE

    with pytest.raises(ValueError, match="diagnostic"):
        module.PreviewRetentionOutcome(
            owner="production",
            cleanup_kind=evidence_kind,
            attempted_at=datetime(2026, 7, 30, 23, 25, 1, tzinfo=UTC),
            status=failure_status,
            diagnostic=None,
        )

    with pytest.raises(ValueError, match="error_type|bounded|80"):
        module.PreviewRetentionDiagnostic(
            code="focus_preview_evidence_retention_failed",
            message=(
                "FOCUS Preview evidence retention cleanup failed. Review worker logs "
                "and restore Preview evidence storage; existing valid Preview data remains available."
            ),
            error_type="X" * 81,
        )


def test_outcome_set_keeps_ordinary_and_evidence_latest_results_distinct() -> None:
    module = _module()
    outcomes = module.PreviewRetentionOutcomeSet(
        ordinary=module.PreviewRetentionOutcome(
            owner="production",
            cleanup_kind=module.PreviewRetentionCleanupKind.ORDINARY,
            attempted_at=datetime(2026, 7, 30, 23, 25, 1, tzinfo=UTC),
            status=module.PreviewRetentionOutcomeStatus.FAILURE,
            diagnostic=module.PreviewRetentionDiagnostic(
                code="focus_preview_ordinary_retention_failed",
                message=(
                    "Ordinary tenant retention cleanup failed. Review worker logs and "
                    "restore tenant storage; existing valid Preview data remains available."
                ),
                error_type="OperationalError",
            ),
        ),
        preview_evidence=module.PreviewRetentionOutcome(
            owner="production",
            cleanup_kind=module.PreviewRetentionCleanupKind.PREVIEW_EVIDENCE,
            attempted_at=datetime(2026, 7, 30, 23, 40, 1, tzinfo=UTC),
            status=module.PreviewRetentionOutcomeStatus.SUCCESS,
            diagnostic=None,
        ),
    )

    assert outcomes.ordinary is not None
    assert outcomes.preview_evidence is not None
    assert outcomes.ordinary.cleanup_kind is module.PreviewRetentionCleanupKind.ORDINARY
    assert outcomes.preview_evidence.cleanup_kind is module.PreviewRetentionCleanupKind.PREVIEW_EVIDENCE
    assert outcomes.ordinary.status is module.PreviewRetentionOutcomeStatus.FAILURE
    assert outcomes.preview_evidence.status is module.PreviewRetentionOutcomeStatus.SUCCESS
