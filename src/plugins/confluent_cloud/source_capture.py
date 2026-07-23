from __future__ import annotations

import hashlib
import logging
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING

from core.preview.evidence import PreviewSourceReadiness, SourceAttemptFinalStatus
from core.preview.evidence_capture import (
    NativeSourceWindow,
    PreviewSourceCaptureReceipt,
    PreviewSourceWindowWriter,
)

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
        result = source_windows.replace_capture(
            self,
            attempt_sequence=attempt_sequence,
            captured_at=captured_at,
        )
        counts = Counter((record.collection_window_start, record.collection_window_end) for record in self.records)
        captures = tuple(
            PreviewSourceReadiness(
                ecosystem=self.ecosystem,
                tenant_id=self.tenant_id,
                window_start=window.start,
                window_end=window.end,
                capture_id=self.capture_id(window),
                captured_at=captured_at,
                source_count=counts[(window.start, window.end)],
                attempt_sequence=attempt_sequence,
            )
            for window in self.windows
        )
        persisted = source_readiness.replace_overlapping(
            self.ecosystem,
            self.tenant_id,
            self.refresh_start,
            self.refresh_end,
            captures,
        )
        if persisted != captures or result.records_written != sum(item.source_count for item in captures):
            raise PreviewSourceCapturePersistenceError("persisted source capture does not match its plan")
        source_readiness.finalize_attempt(
            attempt_sequence,
            SourceAttemptFinalStatus.COMPLETE,
            completed_at=captured_at,
            reason=None,
        )
        return PreviewSourceCaptureReceipt(
            ecosystem=self.ecosystem,
            tenant_id=self.tenant_id,
            attempt_sequence=attempt_sequence,
            refresh_start=self.refresh_start,
            refresh_end=self.refresh_end,
            captures=persisted,
            source_count=result.records_written,
        )

    def capture_id(self, window: NativeSourceWindow) -> str:
        digest = hashlib.sha256(
            f"{self.ecosystem}\0{self.tenant_id}\0{window.start.isoformat()}\0{window.end.isoformat()}".encode()
        ).hexdigest()
        return f"capture:v1:{digest}"
