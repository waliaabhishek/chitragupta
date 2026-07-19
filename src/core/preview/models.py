from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, date, datetime
from enum import StrEnum
from typing import Literal

logger = logging.getLogger(__name__)


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
class PreviewArtifactMetadata:
    name: str
    media_type: str
    size_bytes: int
    sha256: str
    order: int | None


@dataclass(frozen=True)
class PreviewSourceSnapshot:
    calculation_timestamp: datetime
    calculation_coverage: tuple[PreviewCalculationCoverageEntry, ...]
    source_through: datetime

    def __post_init__(self) -> None:
        _require_aware(self.calculation_timestamp, "calculation_timestamp")
        _require_aware(self.source_through, "source_through")
        dates = tuple(entry.tracking_date for entry in self.calculation_coverage)
        if not dates or dates != tuple(sorted(dates)) or len(set(dates)) != len(dates):
            raise ValueError("calculation_coverage must contain unique date-ordered entries")
        expected = max(entry.calculation_completed_at for entry in self.calculation_coverage)
        if self.calculation_timestamp != expected:
            raise ValueError("calculation_timestamp must equal the maximum coverage timestamp")


@dataclass(frozen=True)
class PreviewPackageMetadata:
    manifest: PreviewArtifactMetadata
    files: tuple[PreviewArtifactMetadata, ...]


@dataclass(frozen=True)
class PreviewRequest:
    request_id: str
    tenant_name: str
    ecosystem: str
    tenant_id: str
    grain: Literal["daily"]
    start_date: date
    end_date: date
    column_profile: Literal["full"]
    status: PreviewRequestStatus
    created_at: datetime
    started_at: datetime | None
    completed_at: datetime | None
    source_snapshot: PreviewSourceSnapshot | None
    diagnostic: PreviewDiagnostic | None
    storage_key: str | None
    package: PreviewPackageMetadata | None


@dataclass(frozen=True)
class PreviewArtifactPayload:
    name: str
    media_type: str
    order: int
    body: bytes


@dataclass(frozen=True)
class PreviewPackagePayload:
    manifest_body: bytes
    data_files: tuple[PreviewArtifactPayload, ...]


@dataclass(frozen=True)
class PreviewStoredPackage:
    storage_key: str
    manifest: PreviewArtifactMetadata
    files: tuple[PreviewArtifactMetadata, ...]
