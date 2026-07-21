from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from enum import StrEnum
from typing import Literal

logger = logging.getLogger(__name__)

type PreviewGrain = Literal["daily", "monthly"]
type PreviewColumnProfile = Literal["full", "summary", "custom"]
type PreviewMonthlyStatus = Literal["provisional", "settled"]
type PreviewMonthlyStage = Literal["future", "provisional", "settlement_candidate"]
type PreviewSnapshotValidationMode = Literal["candidate_ready", "strict_materialized"]


def _require_aware(value: datetime, field: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must be timezone-aware")
    return value.astimezone(UTC)


class PreviewRequestStatus(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    READY = "ready"
    FAILED = "failed"
    EXPIRED = "expired"


def preview_request_status(value: object) -> PreviewRequestStatus:
    if not isinstance(value, str):
        raise ValueError(f"unsupported preview request status: {value!r}")
    try:
        return PreviewRequestStatus(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"unsupported preview request status: {value!r}") from exc


def _validate_preview_request_identity(
    *,
    request_id: object,
    tenant_name: object,
    ecosystem: object,
    tenant_id: object,
    status: object,
) -> PreviewRequestStatus:
    for field, value in (
        ("request_id", request_id),
        ("tenant_name", tenant_name),
        ("ecosystem", ecosystem),
        ("tenant_id", tenant_id),
    ):
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{field} must not be blank")
    if ecosystem != "confluent_cloud":
        raise ValueError(f"unsupported preview ecosystem: {ecosystem!r}")
    return preview_request_status(status)


@dataclass(frozen=True)
class PreviewDiagnostic:
    code: str
    message: str
    retryable: bool
    source_correlation_ids: tuple[str, ...] = ()


@dataclass(frozen=True)
class PreviewCalculationCoverageEntry:
    tracking_date: date
    calculation_id: str
    calculation_completed_at: datetime
    calculation_run_id: int | None

    def __post_init__(self) -> None:
        if not self.calculation_id:
            raise ValueError("calculation_id must not be empty")
        object.__setattr__(
            self,
            "calculation_completed_at",
            _require_aware(self.calculation_completed_at, "calculation_completed_at"),
        )


@dataclass(frozen=True)
class PreviewColumnSelection:
    effective_columns: tuple[str, ...]
    ignored_unknown: tuple[str, ...] = ()
    ignored_duplicates: tuple[str, ...] = ()


@dataclass(frozen=True)
class PreviewInterval:
    grain: PreviewGrain
    start_date: date
    end_date: date


@dataclass(frozen=True)
class PreviewEvidenceInterval:
    start_date: date
    end_date: date
    monthly_stage: PreviewMonthlyStage | None


def canonical_next_month_boundary(month_start: date) -> date:
    if month_start.day != 1:
        raise ValueError("month_start must be the first day of a month")
    if month_start.month == 12:
        if month_start.year == 9999:
            raise ValueError("next month is not representable")
        return date(month_start.year + 1, 1, 1)
    return date(month_start.year, month_start.month + 1, 1)


def preview_month(*, grain: PreviewGrain, start_date: date, end_date: date) -> str | None:
    if grain == "daily":
        return None
    if end_date != canonical_next_month_boundary(start_date):
        raise ValueError("monthly preview bounds must cover one exact calendar month")
    return f"{start_date.year:04d}-{start_date.month:02d}"


def resolve_monthly_evidence(
    *,
    start_date: date,
    end_date: date,
    submitted_at: datetime,
    availability_cutoff_end_date: date,
) -> PreviewEvidenceInterval:
    if end_date != canonical_next_month_boundary(start_date):
        raise ValueError("monthly preview bounds must cover one exact calendar month")
    submitted = _require_aware(submitted_at, "submitted_at")
    month_start_at = datetime.combine(start_date, time.min, tzinfo=UTC)
    month_end_at = datetime.combine(end_date, time.min, tzinfo=UTC)
    if submitted < month_start_at:
        return PreviewEvidenceInterval(start_date, start_date, "future")
    if submitted >= month_end_at + timedelta(hours=72) and availability_cutoff_end_date >= end_date:
        return PreviewEvidenceInterval(start_date, end_date, "settlement_candidate")
    effective_end = max(start_date, min(end_date, availability_cutoff_end_date))
    return PreviewEvidenceInterval(start_date, effective_end, "provisional")


@dataclass(frozen=True)
class PreviewArtifactMetadata:
    name: str
    media_type: str
    size_bytes: int
    sha256: str
    order: int | None

    def __post_init__(self) -> None:
        if (
            not isinstance(self.name, str)
            or not self.name
            or self.name in {".", ".."}
            or "/" in self.name
            or "\\" in self.name
        ):
            raise ValueError("artifact name must be a safe basename")
        if not isinstance(self.media_type, str) or not self.media_type.strip():
            raise ValueError("artifact media_type must not be blank")
        if not isinstance(self.size_bytes, int) or isinstance(self.size_bytes, bool) or self.size_bytes < 0:
            raise ValueError("artifact size_bytes must be a non-negative integer")
        if not isinstance(self.sha256, str) or re.fullmatch(r"[0-9a-f]{64}", self.sha256) is None:
            raise ValueError("artifact sha256 must be canonical lowercase hexadecimal")
        if self.order is not None and (
            not isinstance(self.order, int) or isinstance(self.order, bool) or self.order <= 0
        ):
            raise ValueError("artifact order must be a positive integer or None")


@dataclass(frozen=True)
class PreviewSourceSnapshot:
    calculation_timestamp: datetime | None
    calculation_coverage: tuple[PreviewCalculationCoverageEntry, ...]
    source_through: datetime | None
    effective_coverage_start_date: date
    effective_coverage_end_date: date
    availability_cutoff_end_date: date | None
    monthly_status: PreviewMonthlyStatus | None

    def __post_init__(self) -> None:
        if self.calculation_timestamp is not None:
            object.__setattr__(
                self,
                "calculation_timestamp",
                _require_aware(self.calculation_timestamp, "calculation_timestamp"),
            )
        if self.source_through is not None:
            object.__setattr__(self, "source_through", _require_aware(self.source_through, "source_through"))
        if self.monthly_status not in {None, "provisional", "settled"}:
            raise ValueError(f"unsupported monthly_status: {self.monthly_status!r}")
        dates = tuple(entry.tracking_date for entry in self.calculation_coverage)
        if dates != tuple(sorted(dates)) or len(set(dates)) != len(dates):
            raise ValueError("calculation_coverage must contain unique date-ordered entries")
        start = self.effective_coverage_start_date
        end = self.effective_coverage_end_date
        if not isinstance(start, date) or not isinstance(end, date):
            raise ValueError("effective coverage bounds must be dates")
        if self.availability_cutoff_end_date is not None and not isinstance(self.availability_cutoff_end_date, date):
            raise ValueError("availability cutoff must be a date")
        if start > end:
            raise ValueError("effective coverage bounds are invalid")
        expected_dates = tuple(start + timedelta(days=offset) for offset in range((end - start).days))
        if dates != expected_dates:
            raise ValueError("calculation_coverage must exactly match effective coverage")
        if dates and self.calculation_timestamp is None:
            raise ValueError("nonempty calculation coverage requires calculation_timestamp")
        if not dates and (self.calculation_timestamp is not None or self.source_through is not None):
            raise ValueError("empty calculation coverage requires null timestamps")
        expected = max((entry.calculation_completed_at for entry in self.calculation_coverage), default=None)
        if self.calculation_timestamp != expected:
            raise ValueError("calculation_timestamp must equal the maximum coverage timestamp")


@dataclass(frozen=True)
class PreviewPackageMetadata:
    manifest: PreviewArtifactMetadata
    files: tuple[PreviewArtifactMetadata, ...]

    def __post_init__(self) -> None:
        if self.manifest.order is not None:
            raise ValueError("manifest order must be None")
        names = (self.manifest.name, *(item.name for item in self.files))
        if len(names) != len(set(names)):
            raise ValueError("package artifact names must be unique")
        if tuple(item.order for item in self.files) != tuple(range(1, len(self.files) + 1)):
            raise ValueError("package file order must be contiguous")


@dataclass(frozen=True)
class PreviewRequest:
    request_id: str
    tenant_name: str
    ecosystem: str
    tenant_id: str
    grain: PreviewGrain
    start_date: date
    end_date: date
    column_profile: PreviewColumnProfile
    status: PreviewRequestStatus
    created_at: datetime
    started_at: datetime | None
    completed_at: datetime | None
    expires_at: datetime | None
    source_snapshot: PreviewSourceSnapshot | None
    diagnostic: PreviewDiagnostic | None
    storage_key: str | None
    package: PreviewPackageMetadata | None
    effective_columns: tuple[str, ...]

    def __post_init__(self) -> None:
        status = _validate_preview_request_identity(
            request_id=self.request_id,
            tenant_name=self.tenant_name,
            ecosystem=self.ecosystem,
            tenant_id=self.tenant_id,
            status=self.status,
        )
        object.__setattr__(self, "status", status)
        created = _require_aware(self.created_at, "created_at")
        object.__setattr__(self, "created_at", created)
        started = None if self.started_at is None else _require_aware(self.started_at, "started_at")
        completed = None if self.completed_at is None else _require_aware(self.completed_at, "completed_at")
        expires = None if self.expires_at is None else _require_aware(self.expires_at, "expires_at")
        object.__setattr__(self, "started_at", started)
        object.__setattr__(self, "completed_at", completed)
        object.__setattr__(self, "expires_at", expires)
        if started is not None and started < created:
            raise ValueError("started_at must not precede created_at")
        if completed is not None and completed < created:
            raise ValueError("completed_at must not precede created_at")
        if completed is not None and started is not None and completed < started:
            raise ValueError("completed_at must not precede started_at")
        if status is PreviewRequestStatus.QUEUED:
            if started is not None or completed is not None:
                raise ValueError("queued request timestamps are invalid")
        elif status is PreviewRequestStatus.RUNNING:
            if started is None or completed is not None:
                raise ValueError("running request timestamps are invalid")
        elif status in {PreviewRequestStatus.READY, PreviewRequestStatus.EXPIRED}:
            if started is None or completed is None:
                raise ValueError("ready request timestamps are incomplete")
            if expires is None or expires != completed + timedelta(days=7):
                raise ValueError("ready and expired requests require expires_at exactly seven days after completed_at")
        elif status is PreviewRequestStatus.FAILED and completed is None:
            raise ValueError("failed request requires completed_at")
        if status not in {PreviewRequestStatus.READY, PreviewRequestStatus.EXPIRED} and expires is not None:
            raise ValueError("non-ready request expires_at must be null")

        ready_payload = self.source_snapshot is not None or self.storage_key is not None or self.package is not None
        if status in {PreviewRequestStatus.QUEUED, PreviewRequestStatus.RUNNING}:
            if ready_payload or self.diagnostic is not None:
                raise ValueError("pending request payload is invalid")
        elif status in {PreviewRequestStatus.READY, PreviewRequestStatus.EXPIRED}:
            if self.source_snapshot is None or self.package is None or self.diagnostic is not None:
                raise ValueError("ready request payload is incomplete")
            if status is PreviewRequestStatus.READY and self.storage_key is None:
                raise ValueError("ready request payload is incomplete")
        elif status is PreviewRequestStatus.FAILED and (self.diagnostic is None or ready_payload):
            raise ValueError("failed request payload is invalid")


def validate_preview_request_snapshot(
    *,
    request: PreviewRequest,
    snapshot: PreviewSourceSnapshot | None,
    resulting_status: PreviewRequestStatus,
    mode: PreviewSnapshotValidationMode,
) -> None:
    request_status = _validate_preview_request_identity(
        request_id=request.request_id,
        tenant_name=request.tenant_name,
        ecosystem=request.ecosystem,
        tenant_id=request.tenant_id,
        status=request.status,
    )
    result_status = preview_request_status(resulting_status)
    if request.grain == "daily":
        if request.start_date >= request.end_date:
            raise ValueError("daily request start_date must be before end_date")
        month_start = request.start_date.replace(day=1)
        if request.end_date > canonical_next_month_boundary(month_start):
            raise ValueError("daily request bounds must stay within one UTC calendar month")
    elif request.grain == "monthly":
        if request.end_date != canonical_next_month_boundary(request.start_date):
            raise ValueError("monthly request bounds must cover one exact calendar month")
    else:
        raise ValueError(f"unsupported preview grain: {request.grain!r}")

    if mode == "candidate_ready":
        if request_status is not PreviewRequestStatus.RUNNING:
            raise ValueError("candidate_ready requires a running request")
        if result_status is not PreviewRequestStatus.READY or snapshot is None:
            raise ValueError("candidate_ready requires a proposed ready snapshot")
    elif mode == "strict_materialized":
        if result_status is not request_status:
            raise ValueError("resulting_status must match materialized request status")
        if snapshot != request.source_snapshot:
            raise ValueError("snapshot must match materialized request snapshot")
    else:
        raise ValueError(f"unsupported snapshot validation mode: {mode!r}")

    if snapshot is None:
        return
    start = snapshot.effective_coverage_start_date
    end = snapshot.effective_coverage_end_date
    if start != request.start_date or end > request.end_date:
        raise ValueError("snapshot effective coverage lies outside requested bounds")
    if request.grain == "daily":
        if end != request.end_date:
            raise ValueError("daily effective coverage must equal requested bounds")
        if snapshot.monthly_status is not None or snapshot.availability_cutoff_end_date is not None:
            raise ValueError("daily snapshot cannot contain monthly state")
        return
    if request.grain != "monthly":
        raise ValueError(f"unsupported preview grain: {request.grain!r}")
    if snapshot.monthly_status is None or snapshot.availability_cutoff_end_date is None:
        raise ValueError("monthly snapshot requires status and availability cutoff")
    resolution = resolve_monthly_evidence(
        start_date=request.start_date,
        end_date=request.end_date,
        submitted_at=request.created_at,
        availability_cutoff_end_date=snapshot.availability_cutoff_end_date,
    )
    if resolution.monthly_stage == "future":
        raise ValueError("future monthly evidence cannot be ready")
    expected_status: PreviewMonthlyStatus = (
        "settled" if resolution.monthly_stage == "settlement_candidate" else "provisional"
    )
    if (start, end) != (resolution.start_date, resolution.end_date) or snapshot.monthly_status != expected_status:
        raise ValueError("monthly snapshot disagrees with frozen settlement state")


@dataclass(frozen=True)
class PreviewArtifactPayload:
    name: str
    media_type: str
    order: int
    body: bytes


@dataclass(frozen=True)
class PreviewStoredPackage:
    storage_key: str
    manifest: PreviewArtifactMetadata
    files: tuple[PreviewArtifactMetadata, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.storage_key, str) or not self.storage_key.strip():
            raise ValueError("storage_key must not be blank")
        PreviewPackageMetadata(manifest=self.manifest, files=self.files)
