from __future__ import annotations

import logging
import re
from collections.abc import Sequence
from datetime import date
from typing import Protocol

from core.preview.mapping import (
    FOCUS_1_4_FULL_PROFILE_COLUMNS,
    FOCUS_1_4_SUMMARY_COLUMNS,
    validate_preview_effective_columns,
)
from core.preview.models import (
    PreviewColumnProfile,
    PreviewColumnSelection,
    PreviewEvidenceInterval,
    PreviewInterval,
    PreviewRequest,
    canonical_next_month_boundary,
    resolve_monthly_evidence,
)

_MONTH_PATTERN = re.compile(r"^[0-9]{4}-(0[1-9]|1[0-2])$")
logger = logging.getLogger(__name__)


class PreviewRequestValidationError(ValueError):
    def __init__(self, detail: str) -> None:
        self.detail = detail
        super().__init__(detail)


class PreviewColumnSelectionEmptyError(PreviewRequestValidationError):
    def __init__(
        self,
        *,
        ignored_unknown: tuple[str, ...],
        ignored_duplicates: tuple[str, ...],
    ) -> None:
        self.ignored_unknown = ignored_unknown
        self.ignored_duplicates = ignored_duplicates
        super().__init__("Custom column selection must contain at least one supported Full-profile column")


class PreviewEvidencePendingError(RuntimeError):
    """The immutable submission state has no eligible evidence yet."""


class _EligibilityPolicy(Protocol):
    @property
    def acquisition_end_date(self) -> date: ...


def canonicalize_monthly_interval(*, month: str) -> PreviewInterval:
    if _MONTH_PATTERN.fullmatch(month) is None:
        raise PreviewRequestValidationError("month must use YYYY-MM")
    year = int(month[:4])
    if year == 0:
        raise PreviewRequestValidationError("month must use YYYY-MM")
    try:
        start = date(year, int(month[5:]), 1)
        end = canonical_next_month_boundary(start)
    except ValueError:
        raise PreviewRequestValidationError("month must use YYYY-MM") from None
    return PreviewInterval("monthly", start, end)


def canonicalize_daily_interval(*, start_date: date, end_date: date) -> PreviewInterval:
    if start_date >= end_date:
        raise PreviewRequestValidationError("start_date must be before end_date")
    month_start = start_date.replace(day=1)
    if end_date > canonical_next_month_boundary(month_start):
        raise PreviewRequestValidationError("Daily preview range must stay within one UTC calendar month")
    return PreviewInterval("daily", start_date, end_date)


def normalize_column_selection(
    *,
    profile: PreviewColumnProfile,
    requested_columns: Sequence[str] | None,
) -> PreviewColumnSelection:
    if profile in {"full", "summary"}:
        if requested_columns is not None:
            raise PreviewRequestValidationError("columns may be supplied only when column_profile is custom")
        effective = FOCUS_1_4_FULL_PROFILE_COLUMNS if profile == "full" else FOCUS_1_4_SUMMARY_COLUMNS
        validate_preview_effective_columns(profile, effective)
        return PreviewColumnSelection(effective)

    allowed = set(FOCUS_1_4_FULL_PROFILE_COLUMNS)
    effective_values: list[str] = []
    ignored_unknown: list[str] = []
    ignored_duplicates: list[str] = []
    seen: set[str] = set()
    for column in requested_columns or ():
        if column not in allowed:
            ignored_unknown.append(column)
        elif column in seen:
            ignored_duplicates.append(column)
        else:
            seen.add(column)
            effective_values.append(column)
    effective = tuple(effective_values)
    if not effective:
        raise PreviewColumnSelectionEmptyError(
            ignored_unknown=tuple(ignored_unknown),
            ignored_duplicates=tuple(ignored_duplicates),
        )
    validate_preview_effective_columns(profile, effective)
    return PreviewColumnSelection(effective, tuple(ignored_unknown), tuple(ignored_duplicates))


def resolve_preview_evidence_interval(
    *, request: PreviewRequest, policy: _EligibilityPolicy
) -> PreviewEvidenceInterval:
    if request.grain == "daily":
        return PreviewEvidenceInterval(request.start_date, request.end_date, None)
    resolution = resolve_monthly_evidence(
        start_date=request.start_date,
        end_date=request.end_date,
        submitted_at=request.created_at,
        availability_cutoff_end_date=policy.acquisition_end_date,
    )
    if resolution.monthly_stage == "future":
        raise PreviewEvidencePendingError("monthly evidence is not yet available")
    return resolution
