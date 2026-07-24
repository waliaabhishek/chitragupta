from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING

from core.preview.evidence import PreviewSourceReadiness, SourceAttemptFinalStatus
from core.preview.evidence_capture import (
    NativeSourceWindow,
    PreviewSourceCaptureReceipt,
    PreviewSourceWindowWriter,
)
from core.time_precision import canonical_utc_second

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from core.preview.persistence import PreviewSourceReadinessWriter
    from plugins.confluent_cloud.models.billing import CCloudCostSourceRecord


class PreviewSourceCapturePersistenceError(RuntimeError):
    """Native source capture could not be persisted consistently."""


@dataclass(frozen=True)
class CCloudNativeSourceEvidenceCapture:
    ecosystem: str
    tenant_id: str
    refresh_start: datetime
    refresh_end: datetime
    windows: tuple[NativeSourceWindow, ...]
    records: tuple[CCloudCostSourceRecord, ...]

    def __post_init__(self) -> None:
        if not self.ecosystem.strip() or not self.tenant_id.strip() or not self.windows:
            raise ValueError("source capture identity and windows are required")
        cursor = self.refresh_start
        for window in self.windows:
            if window.start != cursor:
                raise ValueError("source capture windows must be an exact ordered partition")
            cursor = window.end
        if cursor != self.refresh_end:
            raise ValueError("source capture windows must cover the refresh interval")

    def persist(
        self,
        source_windows: PreviewSourceWindowWriter,
        source_readiness: PreviewSourceReadinessWriter,
        *,
        attempt_sequence: int,
        captured_at: datetime,
    ) -> PreviewSourceCaptureReceipt:
        receipt = self.write(
            source_windows,
            source_readiness,
            attempt_sequence=attempt_sequence,
            captured_at=captured_at,
        )
        source_readiness.finalize_attempt(
            attempt_sequence,
            SourceAttemptFinalStatus.COMPLETE,
            completed_at=captured_at,
            reason=None,
        )
        return receipt

    def write(
        self,
        source_windows: PreviewSourceWindowWriter,
        source_readiness: PreviewSourceReadinessWriter,
        *,
        attempt_sequence: int,
        captured_at: datetime,
    ) -> PreviewSourceCaptureReceipt:
        result = source_windows.replace_capture(
            self,
            attempt_sequence=attempt_sequence,
            captured_at=captured_at,
        )
        refresh_start = canonical_utc_second(self.refresh_start, field="refresh_start")
        refresh_end = canonical_utc_second(self.refresh_end, field="refresh_end")
        captured_at = canonical_utc_second(captured_at, field="captured_at")
        windows = tuple(
            NativeSourceWindow(
                canonical_utc_second(
                    window.start,
                    field="window.start",
                ),
                canonical_utc_second(
                    window.end,
                    field="window.end",
                ),
            )
            for window in self.windows
        )
        counts_by_window = {item.window: item.source_count for item in result.window_counts}
        captures = tuple(
            PreviewSourceReadiness(
                ecosystem=self.ecosystem,
                tenant_id=self.tenant_id,
                window_start=window.start,
                window_end=window.end,
                capture_id=self._capture_id(window.start, window.end),
                captured_at=captured_at,
                source_count=counts_by_window.get(window, 0),
                attempt_sequence=attempt_sequence,
            )
            for window in windows
        )
        persisted = source_readiness.replace_overlapping(
            self.ecosystem,
            self.tenant_id,
            refresh_start,
            refresh_end,
            captures,
        )
        if persisted != captures or result.records_written != sum(item.source_count for item in captures):
            raise PreviewSourceCapturePersistenceError("persisted source capture does not match its plan")
        return PreviewSourceCaptureReceipt(
            ecosystem=self.ecosystem,
            tenant_id=self.tenant_id,
            attempt_sequence=attempt_sequence,
            refresh_start=refresh_start,
            refresh_end=refresh_end,
            captures=persisted,
            source_count=result.records_written,
        )

    def capture_id(self, window: NativeSourceWindow) -> str:
        start = canonical_utc_second(window.start, field="window.start")
        end = canonical_utc_second(window.end, field="window.end")
        return self._capture_id(start, end)

    def _capture_id(self, start: datetime, end: datetime) -> str:
        digest = hashlib.sha256(
            f"{self.ecosystem}\0{self.tenant_id}\0{start.isoformat()}\0{end.isoformat()}".encode()
        ).hexdigest()
        return f"capture:v1:{digest}"
