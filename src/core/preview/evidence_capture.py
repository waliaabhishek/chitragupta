from __future__ import annotations

import logging
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import TYPE_CHECKING, Protocol, runtime_checkable

from core.preview.evidence import PreviewSourceReadiness

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from core.models import BillingLineItem
    from core.preview.evidence import PreviewSourceEvidence
    from core.preview.persistence import PreviewSourceReadinessWriter


def _aware(value: object) -> bool:
    return isinstance(value, datetime) and value.tzinfo is not None and value.utcoffset() is not None


@dataclass(frozen=True)
class NativeSourceWindow:
    start: datetime
    end: datetime

    def __post_init__(self) -> None:
        if not _aware(self.start) or not _aware(self.end) or self.start >= self.end:
            raise ValueError("native source window must have aware ordered bounds")


@dataclass(frozen=True)
class SourceWindowCount:
    window: NativeSourceWindow
    source_count: int

    def __post_init__(self) -> None:
        if not isinstance(self.window, NativeSourceWindow) or self.source_count < 0:
            raise ValueError("invalid source window count")


@dataclass(frozen=True)
class SourceWindowWriteResult:
    records_written: int
    window_counts: tuple[SourceWindowCount, ...] = ()

    def __post_init__(self) -> None:
        if (
            self.records_written < 0
            or not isinstance(self.window_counts, tuple)
            or len({item.window for item in self.window_counts}) != len(self.window_counts)
            or sum(item.source_count for item in self.window_counts) != self.records_written
        ):
            raise ValueError("invalid source window write result")

    def count_for(self, window: NativeSourceWindow) -> int:
        return next(
            (item.source_count for item in self.window_counts if item.window == window),
            0,
        )


class SourceCaptureFailure(StrEnum):
    ATTEMPT_BEGIN_FAILED = "attempt_begin_failed"
    CONSTRUCTION_FAILED = "construction_failed"
    CAPABILITY_UNAVAILABLE = "capability_unavailable"


class PreviewEvidenceBootstrapConflictError(RuntimeError):
    """Legacy source association lost a compare-and-set race."""


@runtime_checkable
class PreviewSourceWindowWriter(Protocol):
    def replace_capture(
        self,
        capture: NativeSourceEvidenceCapture,
        *,
        attempt_sequence: int,
        captured_at: datetime,
    ) -> SourceWindowWriteResult: ...

    def list_unassociated_windows(
        self, ecosystem: str, tenant_id: str, start: datetime, end: datetime
    ) -> tuple[NativeSourceWindow, ...]: ...

    def iter_unassociated_window(
        self, ecosystem: str, tenant_id: str, window: NativeSourceWindow
    ) -> Iterator[PreviewSourceEvidence]: ...

    def associate_legacy_window(
        self,
        ecosystem: str,
        tenant_id: str,
        window: NativeSourceWindow,
        *,
        capture_id: str,
        expected_source_count: int,
    ) -> int: ...

    def delete_before(self, ecosystem: str, tenant_id: str, before: datetime) -> int: ...


@runtime_checkable
class NativeSourceEvidenceCapture(Protocol):
    @property
    def ecosystem(self) -> str: ...

    @property
    def tenant_id(self) -> str: ...

    @property
    def refresh_start(self) -> datetime: ...

    @property
    def refresh_end(self) -> datetime: ...

    @property
    def windows(self) -> tuple[NativeSourceWindow, ...]: ...

    def persist(
        self,
        source_windows: PreviewSourceWindowWriter,
        source_readiness: PreviewSourceReadinessWriter,
        *,
        attempt_sequence: int,
        captured_at: datetime,
    ) -> PreviewSourceCaptureReceipt: ...

    def write(
        self,
        source_windows: PreviewSourceWindowWriter,
        source_readiness: PreviewSourceReadinessWriter,
        *,
        attempt_sequence: int,
        captured_at: datetime,
    ) -> PreviewSourceCaptureReceipt: ...


@dataclass(frozen=True)
class NativeSourceGatherResult:
    billing_lines: tuple[BillingLineItem, ...]
    capture: NativeSourceEvidenceCapture | None
    capture_failure: SourceCaptureFailure | None

    def __post_init__(self) -> None:
        if not isinstance(self.billing_lines, tuple):
            raise ValueError("native source billing lines must be an immutable tuple")
        if (self.capture is None) == (self.capture_failure is None):
            raise ValueError("native source gather requires exactly one capture outcome")
        if self.capture_failure is not None:
            if not isinstance(self.capture_failure, SourceCaptureFailure) or self.capture_failure not in {
                SourceCaptureFailure.CONSTRUCTION_FAILED,
                SourceCaptureFailure.CAPABILITY_UNAVAILABLE,
            }:
                raise ValueError("native source gather has an invalid capture failure")
            return
        if not isinstance(self.capture, NativeSourceEvidenceCapture):
            raise ValueError("native source gather has an invalid capture")
        if (
            not isinstance(self.capture.ecosystem, str)
            or not isinstance(self.capture.tenant_id, str)
            or not self.capture.ecosystem.strip()
            or not self.capture.tenant_id.strip()
        ):
            raise ValueError("native source capture owner must not be blank")
        if (
            not _aware(self.capture.refresh_start)
            or not _aware(self.capture.refresh_end)
            or self.capture.refresh_start >= self.capture.refresh_end
        ):
            raise ValueError("native source capture bounds are invalid")
        if (
            not isinstance(self.capture.windows, tuple)
            or not self.capture.windows
            or any(not isinstance(window, NativeSourceWindow) for window in self.capture.windows)
        ):
            raise ValueError("native source capture windows must be a nonempty tuple")
        if len(set(self.capture.windows)) != len(self.capture.windows):
            raise ValueError("native source capture windows must be unique")
        cursor = self.capture.refresh_start
        for window in self.capture.windows:
            if window.start != cursor:
                raise ValueError("native source capture windows must form an exact partition")
            cursor = window.end
        if cursor != self.capture.refresh_end:
            raise ValueError("native source capture windows must cover the refresh interval")


@runtime_checkable
class NativeSourceEvidenceCostInput(Protocol):
    def gather_with_native_source_evidence(
        self, tenant_id: str, start: datetime, end: datetime
    ) -> NativeSourceGatherResult: ...


@dataclass(frozen=True)
class SourceAttemptBeginFailure:
    refresh_token: str
    ecosystem: str
    tenant_id: str
    refresh_start: datetime
    refresh_end: datetime
    started_at: datetime

    def __post_init__(self) -> None:
        if not all(value.strip() for value in (self.refresh_token, self.ecosystem, self.tenant_id)):
            raise ValueError("source attempt begin failure identity is required")
        if (
            not _aware(self.refresh_start)
            or not _aware(self.refresh_end)
            or self.refresh_start >= self.refresh_end
            or not _aware(self.started_at)
        ):
            raise ValueError("source attempt begin failure bounds are invalid")


@dataclass(frozen=True)
class SourceEvidenceStorageUnavailable:
    ecosystem: str
    tenant_id: str
    refresh_start: datetime
    refresh_end: datetime

    def __post_init__(self) -> None:
        if not self.ecosystem.strip() or not self.tenant_id.strip():
            raise ValueError("source evidence owner is required")
        if not _aware(self.refresh_start) or not _aware(self.refresh_end) or self.refresh_start >= self.refresh_end:
            raise ValueError("source evidence bounds are invalid")


@dataclass(frozen=True)
class PreviewSourceCaptureReceipt:
    ecosystem: str
    tenant_id: str
    attempt_sequence: int
    refresh_start: datetime
    refresh_end: datetime
    captures: tuple[PreviewSourceReadiness, ...]
    source_count: int

    def __post_init__(self) -> None:
        if (
            not isinstance(self.ecosystem, str)
            or not isinstance(self.tenant_id, str)
            or not self.ecosystem.strip()
            or not self.tenant_id.strip()
            or type(self.attempt_sequence) is not int
            or self.attempt_sequence <= 0
        ):
            raise ValueError("invalid source capture identity")
        if not _aware(self.refresh_start) or not _aware(self.refresh_end) or self.refresh_start >= self.refresh_end:
            raise ValueError("invalid source capture bounds")
        if (
            not isinstance(self.captures, tuple)
            or not self.captures
            or any(not isinstance(item, PreviewSourceReadiness) for item in self.captures)
        ):
            raise ValueError("source readiness must be a nonempty immutable tuple")
        if type(self.source_count) is not int or self.source_count < 0:
            raise ValueError("source capture count must be nonnegative")
        if len({item.capture_id for item in self.captures}) != len(self.captures):
            raise ValueError("source readiness capture ids must be unique")
        if sum(item.source_count for item in self.captures) != self.source_count:
            raise ValueError("source capture count does not match readiness")
        cursor = self.refresh_start
        for item in self.captures:
            if (
                item.ecosystem != self.ecosystem
                or item.tenant_id != self.tenant_id
                or item.attempt_sequence != self.attempt_sequence
                or item.window_start != cursor
            ):
                raise ValueError("source readiness is not an exact ordered partition")
            cursor = item.window_end
        if cursor != self.refresh_end:
            raise ValueError("source readiness does not cover the refresh interval")
