from __future__ import annotations

import logging
from collections.abc import Sequence
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING

from sqlalchemy import delete, exists, update
from sqlmodel import Session, col, select

from core.preview.evidence import (
    AllocationLineageRun,
    AllocationLineageRunStatus,
    AllocationLineageUnavailableRun,
    PreviewSourceAttempt,
    PreviewSourceEvidence,
    PreviewSourceReadiness,
    SourceAttemptFailureReason,
    SourceAttemptFinalStatus,
    SourceAttemptStatus,
)
from core.preview.evidence_capture import (
    NativeSourceEvidenceCapture,
    NativeSourceWindow,
    PreviewEvidenceBootstrapConflictError,
    SourceAttemptBeginFailure,
    SourceWindowWriteResult,
)
from core.preview.organization_authority import (
    OrganizationAuthorityAttempt,
    OrganizationAuthorityAttemptStatus,
    OrganizationAuthorityFailureReason,
    OrganizationAuthorityFinalStatus,
    PreviewOrganizationAuthorityConflictError,
    PreviewOrganizationAuthorityDecodeError,
)
from plugins.confluent_cloud.storage.preview_tables import (
    CCloudAllocationLineagePortionTable,
    CCloudAllocationLineageRunTable,
    CCloudCostSourceRecordTable,
    CCloudOrganizationAuthorityAttemptTable,
    CCloudSourceCaptureReadinessTable,
    CCloudSourceEvidenceAttemptTable,
)
from plugins.confluent_cloud.storage.repositories import (
    CCloudBillingRepository,
    CCloudChargebackRepository,
    _source_table_to_preview,
)

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from collections.abc import Iterator

    from core.preview.persistence import LineageDeletionCount
    from core.storage.interface import AllocationLineageRunCapture


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _require_aware(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(UTC)


def _source_attempt(row: CCloudSourceEvidenceAttemptTable) -> PreviewSourceAttempt:
    if row.attempt_sequence is None:
        raise ValueError("source attempt sequence was not assigned")
    return PreviewSourceAttempt(
        attempt_sequence=row.attempt_sequence,
        ecosystem=row.ecosystem,
        tenant_id=row.tenant_id,
        refresh_token=row.refresh_token,
        refresh_start=_utc(row.refresh_start),
        refresh_end=_utc(row.refresh_end),
        status=SourceAttemptStatus(row.status),
        started_at=_utc(row.started_at),
        completed_at=None if row.completed_at is None else _utc(row.completed_at),
        failure_reason=None if row.failure_reason is None else SourceAttemptFailureReason(row.failure_reason),
    )


def _readiness(row: CCloudSourceCaptureReadinessTable) -> PreviewSourceReadiness:
    return PreviewSourceReadiness(
        ecosystem=row.ecosystem,
        tenant_id=row.tenant_id,
        window_start=_utc(row.window_start),
        window_end=_utc(row.window_end),
        capture_id=row.capture_id,
        captured_at=_utc(row.captured_at),
        source_count=row.source_count,
        attempt_sequence=row.attempt_sequence,
    )


class SQLModelPreviewSourceWindowRepository:
    """Evidence-only source writer and legacy bootstrap repository."""

    def __init__(self, session: Session) -> None:
        self._session = session
        self._sources = CCloudBillingRepository(session)

    def replace_capture(
        self,
        capture: NativeSourceEvidenceCapture,
        *,
        attempt_sequence: int,
        captured_at: datetime,
    ) -> SourceWindowWriteResult:
        from plugins.confluent_cloud.source_capture import CCloudNativeSourceEvidenceCapture

        if not isinstance(capture, CCloudNativeSourceEvidenceCapture):
            raise TypeError("Confluent source writer requires a Confluent native capture")
        if attempt_sequence <= 0:
            raise ValueError("attempt_sequence must be positive")
        _require_aware(captured_at, "captured_at")
        self._sources.replace_source_window(
            capture.ecosystem,
            capture.tenant_id,
            capture.refresh_start,
            capture.refresh_end,
            capture.records,
        )
        for window in capture.windows:
            capture_id = capture.capture_id(window)
            self._session.execute(
                update(CCloudCostSourceRecordTable)
                .where(
                    col(CCloudCostSourceRecordTable.ecosystem) == capture.ecosystem,
                    col(CCloudCostSourceRecordTable.tenant_id) == capture.tenant_id,
                    col(CCloudCostSourceRecordTable.collection_window_start) == window.start,
                    col(CCloudCostSourceRecordTable.collection_window_end) == window.end,
                )
                .values(capture_id=capture_id)
            )
        self._session.flush()
        return SourceWindowWriteResult(records_written=len(capture.records))

    def list_unassociated_windows(
        self, ecosystem: str, tenant_id: str, start: datetime, end: datetime
    ) -> tuple[NativeSourceWindow, ...]:
        start = _require_aware(start, "start")
        end = _require_aware(end, "end")
        rows = self._session.exec(
            select(
                CCloudCostSourceRecordTable.collection_window_start,
                CCloudCostSourceRecordTable.collection_window_end,
            )
            .where(
                col(CCloudCostSourceRecordTable.ecosystem) == ecosystem,
                col(CCloudCostSourceRecordTable.tenant_id) == tenant_id,
                col(CCloudCostSourceRecordTable.capture_id).is_(None),
                col(CCloudCostSourceRecordTable.collection_window_start) < end,
                col(CCloudCostSourceRecordTable.collection_window_end) > start,
            )
            .distinct()
            .order_by(
                col(CCloudCostSourceRecordTable.collection_window_start),
                col(CCloudCostSourceRecordTable.collection_window_end),
            )
        ).yield_per(256)
        return tuple(NativeSourceWindow(_utc(row[0]), _utc(row[1])) for row in rows)

    def iter_unassociated_window(
        self, ecosystem: str, tenant_id: str, window: NativeSourceWindow
    ) -> Iterator[PreviewSourceEvidence]:
        rows = self._session.exec(
            select(CCloudCostSourceRecordTable)
            .where(
                col(CCloudCostSourceRecordTable.ecosystem) == ecosystem,
                col(CCloudCostSourceRecordTable.tenant_id) == tenant_id,
                col(CCloudCostSourceRecordTable.collection_window_start) == window.start,
                col(CCloudCostSourceRecordTable.collection_window_end) == window.end,
                col(CCloudCostSourceRecordTable.capture_id).is_(None),
            )
            .order_by(
                col(CCloudCostSourceRecordTable.evidence_scope_start),
                col(CCloudCostSourceRecordTable.evidence_scope_end),
                col(CCloudCostSourceRecordTable.source_record_id),
                col(CCloudCostSourceRecordTable.identity_scheme),
            )
        )
        for row in rows:
            yield _source_table_to_preview(row)

    def associate_legacy_window(
        self,
        ecosystem: str,
        tenant_id: str,
        window: NativeSourceWindow,
        *,
        capture_id: str,
        expected_source_count: int,
    ) -> int:
        result = self._session.execute(
            update(CCloudCostSourceRecordTable)
            .where(
                col(CCloudCostSourceRecordTable.ecosystem) == ecosystem,
                col(CCloudCostSourceRecordTable.tenant_id) == tenant_id,
                col(CCloudCostSourceRecordTable.collection_window_start) == window.start,
                col(CCloudCostSourceRecordTable.collection_window_end) == window.end,
                col(CCloudCostSourceRecordTable.capture_id).is_(None),
            )
            .values(capture_id=capture_id)
        )
        changed = int(getattr(result, "rowcount", 0))
        if changed != expected_source_count:
            raise PreviewEvidenceBootstrapConflictError("legacy source association changed concurrently")
        return changed

    def delete_before(self, ecosystem: str, tenant_id: str, before: datetime) -> int:
        result = self._session.execute(
            delete(CCloudCostSourceRecordTable).where(
                col(CCloudCostSourceRecordTable.ecosystem) == ecosystem,
                col(CCloudCostSourceRecordTable.tenant_id) == tenant_id,
                col(CCloudCostSourceRecordTable.retention_timestamp) < _require_aware(before, "before"),
            )
        )
        return int(getattr(result, "rowcount", 0))


class SQLModelPreviewAllocationLineageRepository:
    def __init__(self, session: Session) -> None:
        self._session = session
        self._lineage = CCloudChargebackRepository(session)

    def replace_calculation_lineage(
        self,
        capture: AllocationLineageRunCapture,
        *,
        calculation_completed_at: datetime,
    ) -> AllocationLineageRun:
        self._lineage.replace_calculation_lineage(capture, calculation_completed_at=calculation_completed_at)
        portion_count = sum(len(item.facts) for item in capture.captures)
        return AllocationLineageRun(
            ecosystem=capture.ecosystem,
            tenant_id=capture.tenant_id,
            tracking_date=capture.tracking_date,
            calculation_id=capture.calculation_id,
            calculation_completed_at=_require_aware(calculation_completed_at, "calculation_completed_at"),
            status=AllocationLineageRunStatus.COMPLETE,
            portion_count=portion_count,
        )

    def mark_calculation_lineage_unavailable(
        self, value: AllocationLineageUnavailableRun
    ) -> AllocationLineageUnavailableRun:
        self._session.execute(
            delete(CCloudAllocationLineagePortionTable).where(
                col(CCloudAllocationLineagePortionTable.ecosystem) == value.ecosystem,
                col(CCloudAllocationLineagePortionTable.tenant_id) == value.tenant_id,
                col(CCloudAllocationLineagePortionTable.tracking_date) == value.tracking_date,
            )
        )
        self._session.execute(
            delete(CCloudAllocationLineageRunTable).where(
                col(CCloudAllocationLineageRunTable.ecosystem) == value.ecosystem,
                col(CCloudAllocationLineageRunTable.tenant_id) == value.tenant_id,
                col(CCloudAllocationLineageRunTable.tracking_date) == value.tracking_date,
            )
        )
        self._session.add(
            CCloudAllocationLineageRunTable(
                ecosystem=value.ecosystem,
                tenant_id=value.tenant_id,
                tracking_date=value.tracking_date,
                calculation_id=value.calculation_id,
                calculation_completed_at=value.calculation_completed_at,
                capture_status=value.status.value,
                capture_reason=value.reason.value,
                portion_count=0,
            )
        )
        self._session.flush()
        return value

    def delete_before(self, ecosystem: str, tenant_id: str, before: date) -> LineageDeletionCount:
        from core.preview.persistence import LineageDeletionCount

        portions = self._session.execute(
            delete(CCloudAllocationLineagePortionTable).where(
                col(CCloudAllocationLineagePortionTable.ecosystem) == ecosystem,
                col(CCloudAllocationLineagePortionTable.tenant_id) == tenant_id,
                col(CCloudAllocationLineagePortionTable.tracking_date) < before,
            )
        )
        runs = self._session.execute(
            delete(CCloudAllocationLineageRunTable).where(
                col(CCloudAllocationLineageRunTable.ecosystem) == ecosystem,
                col(CCloudAllocationLineageRunTable.tenant_id) == tenant_id,
                col(CCloudAllocationLineageRunTable.tracking_date) < before,
            )
        )
        return LineageDeletionCount(
            portions=int(getattr(portions, "rowcount", 0)),
            runs=int(getattr(runs, "rowcount", 0)),
        )


class SQLModelPreviewSourceReadinessRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def begin_attempt(
        self,
        ecosystem: str,
        tenant_id: str,
        refresh_token: str,
        refresh_start: datetime,
        refresh_end: datetime,
        started_at: datetime,
    ) -> PreviewSourceAttempt:
        start = _require_aware(refresh_start, "refresh_start")
        end = _require_aware(refresh_end, "refresh_end")
        started = _require_aware(started_at, "started_at")
        if start >= end or not all(value.strip() for value in (ecosystem, tenant_id, refresh_token)):
            raise ValueError("invalid source attempt")
        existing = self._session.exec(
            select(CCloudSourceEvidenceAttemptTable).where(
                col(CCloudSourceEvidenceAttemptTable.ecosystem) == ecosystem,
                col(CCloudSourceEvidenceAttemptTable.tenant_id) == tenant_id,
                col(CCloudSourceEvidenceAttemptTable.refresh_token) == refresh_token,
            )
        ).first()
        if existing is not None:
            value = _source_attempt(existing)
            if (value.refresh_start, value.refresh_end, value.started_at) != (start, end, started):
                raise ValueError("source attempt token conflicts with persisted bounds")
            return value
        row = CCloudSourceEvidenceAttemptTable(
            ecosystem=ecosystem,
            tenant_id=tenant_id,
            refresh_token=refresh_token,
            refresh_start=start,
            refresh_end=end,
            status=SourceAttemptStatus.PENDING.value,
            started_at=started,
        )
        self._session.add(row)
        self._session.flush([row])
        return _source_attempt(row)

    def get_by_token(self, ecosystem: str, tenant_id: str, refresh_token: str) -> PreviewSourceAttempt | None:
        row = self._session.exec(
            select(CCloudSourceEvidenceAttemptTable).where(
                col(CCloudSourceEvidenceAttemptTable.ecosystem) == ecosystem,
                col(CCloudSourceEvidenceAttemptTable.tenant_id) == tenant_id,
                col(CCloudSourceEvidenceAttemptTable.refresh_token) == refresh_token,
            )
        ).first()
        return None if row is None else _source_attempt(row)

    def ensure_begin_failed(
        self,
        value: SourceAttemptBeginFailure,
        *,
        completed_at: datetime,
    ) -> PreviewSourceAttempt:
        from core.preview.persistence import PreviewSourceAttemptConflictError

        completed = _require_aware(completed_at, "completed_at")
        row = self._session.exec(
            select(CCloudSourceEvidenceAttemptTable).where(
                col(CCloudSourceEvidenceAttemptTable.ecosystem) == value.ecosystem,
                col(CCloudSourceEvidenceAttemptTable.tenant_id) == value.tenant_id,
                col(CCloudSourceEvidenceAttemptTable.refresh_token) == value.refresh_token,
            )
        ).first()
        if row is None:
            row = CCloudSourceEvidenceAttemptTable(
                ecosystem=value.ecosystem,
                tenant_id=value.tenant_id,
                refresh_token=value.refresh_token,
                refresh_start=value.refresh_start,
                refresh_end=value.refresh_end,
                status=SourceAttemptFinalStatus.FAILED.value,
                started_at=value.started_at,
                completed_at=completed,
                failure_reason=SourceAttemptFailureReason.ATTEMPT_BEGIN_FAILED.value,
            )
            self._session.add(row)
            self._session.flush([row])
            return _source_attempt(row)
        persisted = _source_attempt(row)
        if (
            persisted.refresh_start != value.refresh_start
            or persisted.refresh_end != value.refresh_end
            or persisted.started_at != value.started_at
        ):
            raise PreviewSourceAttemptConflictError("source attempt token bounds conflict")
        if persisted.status is SourceAttemptStatus.PENDING:
            return self.finalize_attempt(
                persisted.attempt_sequence,
                SourceAttemptFinalStatus.FAILED,
                completed_at=completed,
                reason=SourceAttemptFailureReason.ATTEMPT_BEGIN_FAILED,
            )
        if (
            persisted.status is SourceAttemptStatus.FAILED
            and persisted.failure_reason is SourceAttemptFailureReason.ATTEMPT_BEGIN_FAILED
        ):
            return persisted
        raise PreviewSourceAttemptConflictError("source attempt token status conflicts")

    def finalize_attempt(
        self,
        attempt_sequence: int,
        status: SourceAttemptFinalStatus,
        *,
        completed_at: datetime,
        reason: SourceAttemptFailureReason | None,
    ) -> PreviewSourceAttempt:
        if not isinstance(status, SourceAttemptFinalStatus):
            raise ValueError("invalid source attempt final status")
        if reason is not None and not isinstance(reason, SourceAttemptFailureReason):
            raise ValueError("invalid source attempt failure reason")
        failed_reasons = {
            SourceAttemptFailureReason.ATTEMPT_BEGIN_FAILED,
            SourceAttemptFailureReason.CONSTRUCTION_FAILED,
            SourceAttemptFailureReason.PERSISTENCE_FAILED,
            SourceAttemptFailureReason.CAPABILITY_UNAVAILABLE,
            SourceAttemptFailureReason.BOOTSTRAP_INVALID,
            SourceAttemptFailureReason.BOOTSTRAP_CONCURRENT_CHANGE,
        }
        aborted_reasons = {
            SourceAttemptFailureReason.GENERIC_GATHER_FAILED,
            SourceAttemptFailureReason.GENERIC_COMMIT_FAILED,
        }
        if (
            (status is SourceAttemptFinalStatus.COMPLETE and reason is not None)
            or (status is SourceAttemptFinalStatus.FAILED and reason not in failed_reasons)
            or (status is SourceAttemptFinalStatus.ABORTED and reason not in aborted_reasons)
        ):
            raise ValueError("source attempt status and reason do not match")
        row = self._session.get(CCloudSourceEvidenceAttemptTable, attempt_sequence)
        normalized_completed = _require_aware(completed_at, "completed_at")
        expected_reason = None if reason is None else reason.value
        if (
            row is not None
            and row.status == status.value
            and row.completed_at is not None
            and _utc(row.completed_at) == normalized_completed
            and row.failure_reason == expected_reason
        ):
            return _source_attempt(row)
        if row is None or row.status != SourceAttemptStatus.PENDING.value:
            raise RuntimeError("source attempt is missing or already finalized")
        row.status = status.value
        row.completed_at = normalized_completed
        row.failure_reason = expected_reason
        self._session.add(row)
        self._session.flush([row])
        return _source_attempt(row)

    def get_current_authority(self, ecosystem: str, tenant_id: str) -> PreviewSourceAttempt | None:
        row = self._session.exec(
            select(CCloudSourceEvidenceAttemptTable)
            .where(
                col(CCloudSourceEvidenceAttemptTable.ecosystem) == ecosystem,
                col(CCloudSourceEvidenceAttemptTable.tenant_id) == tenant_id,
                col(CCloudSourceEvidenceAttemptTable.status) != SourceAttemptStatus.ABORTED.value,
            )
            .order_by(col(CCloudSourceEvidenceAttemptTable.attempt_sequence).desc())
            .limit(1)
        ).first()
        return None if row is None else _source_attempt(row)

    def list_covering(
        self,
        ecosystem: str,
        tenant_id: str,
        start: datetime,
        end: datetime,
    ) -> tuple[PreviewSourceReadiness, ...]:
        start = _require_aware(start, "start")
        end = _require_aware(end, "end")
        if start >= end:
            raise ValueError("readiness bounds must be ordered")
        rows = self._session.exec(
            select(CCloudSourceCaptureReadinessTable)
            .where(
                col(CCloudSourceCaptureReadinessTable.ecosystem) == ecosystem,
                col(CCloudSourceCaptureReadinessTable.tenant_id) == tenant_id,
                col(CCloudSourceCaptureReadinessTable.window_start) < end,
                col(CCloudSourceCaptureReadinessTable.window_end) > start,
            )
            .order_by(
                col(CCloudSourceCaptureReadinessTable.window_start),
                col(CCloudSourceCaptureReadinessTable.window_end),
                col(CCloudSourceCaptureReadinessTable.capture_id),
            )
        ).all()
        return tuple(_readiness(row) for row in rows)

    def replace_overlapping(
        self,
        ecosystem: str,
        tenant_id: str,
        refresh_start: datetime,
        refresh_end: datetime,
        captures: Sequence[PreviewSourceReadiness],
    ) -> tuple[PreviewSourceReadiness, ...]:
        start = _require_aware(refresh_start, "refresh_start")
        end = _require_aware(refresh_end, "refresh_end")
        if start >= end:
            raise ValueError("readiness bounds must be ordered")
        rows: list[CCloudSourceCaptureReadinessTable] = []
        cursor = start
        attempt_sequence: int | None = None
        for capture in captures:
            if capture.ecosystem != ecosystem or capture.tenant_id != tenant_id:
                raise ValueError("readiness owner mismatch")
            if capture.window_start != cursor or capture.window_end > end:
                raise ValueError("readiness rows must exactly partition the refresh interval")
            if attempt_sequence is None:
                attempt_sequence = capture.attempt_sequence
            elif capture.attempt_sequence != attempt_sequence:
                raise ValueError("readiness rows must use one attempt")
            cursor = capture.window_end
            rows.append(
                CCloudSourceCaptureReadinessTable(
                    ecosystem=capture.ecosystem,
                    tenant_id=capture.tenant_id,
                    window_start=capture.window_start,
                    window_end=capture.window_end,
                    capture_id=capture.capture_id,
                    captured_at=capture.captured_at,
                    source_count=capture.source_count,
                    attempt_sequence=capture.attempt_sequence,
                )
            )
        if cursor != end or attempt_sequence is None:
            raise ValueError("readiness rows must exactly partition the refresh interval")
        attempt = self._session.get(CCloudSourceEvidenceAttemptTable, attempt_sequence)
        if (
            attempt is None
            or attempt.status != SourceAttemptStatus.PENDING.value
            or attempt.ecosystem != ecosystem
            or attempt.tenant_id != tenant_id
            or _utc(attempt.refresh_start) != start
            or _utc(attempt.refresh_end) != end
        ):
            raise ValueError("readiness attempt is not the current pending refresh")
        self._session.exec(
            delete(CCloudSourceCaptureReadinessTable).where(
                col(CCloudSourceCaptureReadinessTable.ecosystem) == ecosystem,
                col(CCloudSourceCaptureReadinessTable.tenant_id) == tenant_id,
                col(CCloudSourceCaptureReadinessTable.window_start) < end,
                col(CCloudSourceCaptureReadinessTable.window_end) > start,
            )
        )
        self._session.add_all(rows)
        self._session.flush(rows)
        return tuple(_readiness(row) for row in rows)

    def delete_orphaned_before(self, ecosystem: str, tenant_id: str, before: datetime) -> int:
        cutoff = _require_aware(before, "before")
        source_exists = exists().where(
            col(CCloudCostSourceRecordTable.ecosystem) == ecosystem,
            col(CCloudCostSourceRecordTable.tenant_id) == tenant_id,
            col(CCloudCostSourceRecordTable.capture_id) == col(CCloudSourceCaptureReadinessTable.capture_id),
        )
        result = self._session.exec(
            delete(CCloudSourceCaptureReadinessTable).where(
                col(CCloudSourceCaptureReadinessTable.ecosystem) == ecosystem,
                col(CCloudSourceCaptureReadinessTable.tenant_id) == tenant_id,
                col(CCloudSourceCaptureReadinessTable.window_end) < cutoff,
                ~source_exists,
            )
        )
        deleted = int(getattr(result, "rowcount", 0))
        current = self.get_current_authority(ecosystem, tenant_id)
        readiness_exists = exists().where(
            col(CCloudSourceCaptureReadinessTable.attempt_sequence)
            == col(CCloudSourceEvidenceAttemptTable.attempt_sequence)
        )
        conditions = [
            col(CCloudSourceEvidenceAttemptTable.ecosystem) == ecosystem,
            col(CCloudSourceEvidenceAttemptTable.tenant_id) == tenant_id,
            col(CCloudSourceEvidenceAttemptTable.status) != SourceAttemptStatus.PENDING.value,
            col(CCloudSourceEvidenceAttemptTable.completed_at).is_not(None),
            col(CCloudSourceEvidenceAttemptTable.completed_at) < cutoff,
            ~readiness_exists,
        ]
        if current is not None:
            conditions.append(col(CCloudSourceEvidenceAttemptTable.attempt_sequence) != current.attempt_sequence)
        self._session.exec(delete(CCloudSourceEvidenceAttemptTable).where(*conditions))
        return deleted


def _organization_attempt(row: CCloudOrganizationAuthorityAttemptTable) -> OrganizationAuthorityAttempt:
    if row.attempt_sequence is None:
        raise PreviewOrganizationAuthorityDecodeError("organization attempt sequence was not assigned")
    try:
        return OrganizationAuthorityAttempt(
            attempt_sequence=row.attempt_sequence,
            ecosystem=row.ecosystem,
            tenant_id=row.tenant_id,
            status=OrganizationAuthorityAttemptStatus(row.status),
            started_at=_utc(row.started_at),
            completed_at=None if row.completed_at is None else _utc(row.completed_at),
            organization_id=row.organization_id,
            failure_reason=(
                None if row.failure_reason is None else OrganizationAuthorityFailureReason(row.failure_reason)
            ),
        )
    except ValueError as exc:
        raise PreviewOrganizationAuthorityDecodeError("invalid persisted organization authority") from exc


class SQLModelPreviewOrganizationAuthorityRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def begin(self, ecosystem: str, tenant_id: str, started_at: datetime) -> OrganizationAuthorityAttempt:
        if not ecosystem.strip() or not tenant_id.strip():
            raise ValueError("organization authority owner must not be blank")
        row = CCloudOrganizationAuthorityAttemptTable(
            ecosystem=ecosystem,
            tenant_id=tenant_id,
            status=OrganizationAuthorityAttemptStatus.PENDING.value,
            started_at=_require_aware(started_at, "started_at"),
        )
        self._session.add(row)
        self._session.flush([row])
        return _organization_attempt(row)

    def get_latest(self, ecosystem: str, tenant_id: str) -> OrganizationAuthorityAttempt | None:
        row = self._session.exec(
            select(CCloudOrganizationAuthorityAttemptTable)
            .where(
                col(CCloudOrganizationAuthorityAttemptTable.ecosystem) == ecosystem,
                col(CCloudOrganizationAuthorityAttemptTable.tenant_id) == tenant_id,
            )
            .order_by(col(CCloudOrganizationAuthorityAttemptTable.attempt_sequence).desc())
            .limit(1)
        ).first()
        return None if row is None else _organization_attempt(row)

    def finalize(
        self,
        attempt_sequence: int,
        status: OrganizationAuthorityFinalStatus,
        *,
        completed_at: datetime,
        organization_id: str | None,
        reason: OrganizationAuthorityFailureReason | None,
    ) -> OrganizationAuthorityAttempt:
        row = self._session.get(CCloudOrganizationAuthorityAttemptTable, attempt_sequence)
        normalized_completed = _require_aware(completed_at, "completed_at")
        expected_reason = None if reason is None else reason.value
        if (
            row is not None
            and row.status == status.value
            and row.completed_at is not None
            and _utc(row.completed_at) == normalized_completed
            and row.organization_id == organization_id
            and row.failure_reason == expected_reason
        ):
            return _organization_attempt(row)
        if row is None or row.status != OrganizationAuthorityAttemptStatus.PENDING.value:
            raise PreviewOrganizationAuthorityConflictError("organization authority attempt is not pending")
        row.status = status.value
        row.completed_at = normalized_completed
        row.organization_id = organization_id
        row.failure_reason = expected_reason
        self._session.add(row)
        self._session.flush([row])
        return _organization_attempt(row)

    def delete_superseded_before(self, ecosystem: str, tenant_id: str, before: datetime) -> int:
        latest = self.get_latest(ecosystem, tenant_id)
        conditions = [
            col(CCloudOrganizationAuthorityAttemptTable.ecosystem) == ecosystem,
            col(CCloudOrganizationAuthorityAttemptTable.tenant_id) == tenant_id,
            col(CCloudOrganizationAuthorityAttemptTable.completed_at).is_not(None),
            col(CCloudOrganizationAuthorityAttemptTable.completed_at) < _require_aware(before, "before"),
        ]
        if latest is not None:
            conditions.append(col(CCloudOrganizationAuthorityAttemptTable.attempt_sequence) != latest.attempt_sequence)
        result = self._session.exec(delete(CCloudOrganizationAuthorityAttemptTable).where(*conditions))
        return int(getattr(result, "rowcount", 0))
