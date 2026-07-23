from __future__ import annotations

import json
import logging
from collections.abc import Sequence
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING, Literal

from sqlalchemy import delete, exists, update
from sqlmodel import Session, col, select

from core.preview.evidence import (
    AllocationLineageRun,
    AllocationLineageRunStatus,
    AllocationLineageUnavailableRun,
    PreviewSourceAttempt,
    PreviewSourceAttemptOrigin,
    PreviewSourceAuthoritySlice,
    PreviewSourceEvidence,
    PreviewSourceReadiness,
    SourceAttemptFailureReason,
    SourceAttemptFinalStatus,
    SourceAttemptStatus,
    source_attempt_origin,
)
from core.preview.evidence_capture import (
    NativeSourceEvidenceCapture,
    NativeSourceWindow,
    PreviewEvidenceBootstrapConflictError,
    SourceAttemptBeginFailure,
    SourceWindowWriteResult,
)
from core.preview.models import PreviewDiagnostic
from core.preview.organization_authority import (
    OrganizationAuthorityAttempt,
    OrganizationAuthorityAttemptStatus,
    OrganizationAuthorityFailureReason,
    OrganizationAuthorityFinalStatus,
    PreviewOrganizationAuthorityConflictError,
    PreviewOrganizationAuthorityDecodeError,
)
from core.preview.repair import (
    PreviewRepair,
    PreviewRepairDate,
    PreviewRepairDateStatus,
    PreviewRepairFailureStage,
    PreviewRepairStatus,
)
from plugins.confluent_cloud.storage.preview_tables import (
    CCloudAllocationLineagePortionTable,
    CCloudAllocationLineageRunTable,
    CCloudCostSourceRecordTable,
    CCloudFocusPreviewRepairDateTable,
    CCloudFocusPreviewRepairTable,
    CCloudOrganizationAuthorityAttemptTable,
    CCloudSourceCaptureReadinessHistoryTable,
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


def _readiness(
    row: CCloudSourceCaptureReadinessTable | CCloudSourceCaptureReadinessHistoryTable,
) -> PreviewSourceReadiness:
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


def _diagnostic(
    code: str | None,
    message: str | None,
    retryable: bool | None,
    source_correlation_ids_json: str | None = None,
) -> PreviewDiagnostic | None:
    if code is None and message is None and retryable is None:
        return None
    if code is None or message is None or retryable is None:
        raise ValueError("incomplete persisted repair diagnostic")
    correlations: tuple[str, ...] = ()
    if source_correlation_ids_json is not None:
        decoded = json.loads(source_correlation_ids_json)
        if not isinstance(decoded, list) or not all(isinstance(item, str) for item in decoded):
            raise ValueError("invalid persisted repair diagnostic correlations")
        correlations = tuple(decoded)
    return PreviewDiagnostic(
        code=code,
        message=message,
        retryable=retryable,
        source_correlation_ids=correlations,
    )


def _repair_date(row: CCloudFocusPreviewRepairDateTable) -> PreviewRepairDate:
    return PreviewRepairDate(
        repair_id=row.repair_id,
        tracking_date=row.tracking_date,
        status=PreviewRepairDateStatus(row.status),
        started_at=None if row.started_at is None else _utc(row.started_at),
        completed_at=None if row.completed_at is None else _utc(row.completed_at),
        calculation_id=row.calculation_id,
        calculation_completed_at=(None if row.calculation_completed_at is None else _utc(row.calculation_completed_at)),
        rows_written=row.rows_written,
        failure_stage=None if row.failure_stage is None else PreviewRepairFailureStage(row.failure_stage),
        diagnostic=_diagnostic(
            row.diagnostic_code,
            row.diagnostic_message,
            row.diagnostic_retryable,
            row.source_correlation_ids_json,
        ),
    )


class SQLModelPreviewRepairRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def _get(self, repair_id: str) -> PreviewRepair | None:
        row = self._session.get(CCloudFocusPreviewRepairTable, repair_id)
        if row is None:
            return None
        date_rows = self._session.exec(
            select(CCloudFocusPreviewRepairDateTable)
            .where(col(CCloudFocusPreviewRepairDateTable.repair_id) == repair_id)
            .order_by(col(CCloudFocusPreviewRepairDateTable.tracking_date))
        ).all()
        return PreviewRepair(
            repair_id=row.repair_id,
            tenant_name=row.tenant_name,
            ecosystem=row.ecosystem,
            tenant_id=row.tenant_id,
            start_date=row.start_date,
            end_date=row.end_date,
            status=PreviewRepairStatus(row.status),
            created_at=_utc(row.created_at),
            started_at=None if row.started_at is None else _utc(row.started_at),
            completed_at=None if row.completed_at is None else _utc(row.completed_at),
            diagnostic=_diagnostic(
                row.diagnostic_code,
                row.diagnostic_message,
                row.diagnostic_retryable,
            ),
            dates=tuple(_repair_date(item) for item in date_rows),
        )

    def create_queued(self, repair: PreviewRepair) -> PreviewRepair:
        if repair.status is not PreviewRepairStatus.QUEUED:
            raise ValueError("repair must be queued")
        self._session.add(
            CCloudFocusPreviewRepairTable(
                repair_id=repair.repair_id,
                tenant_name=repair.tenant_name,
                ecosystem=repair.ecosystem,
                tenant_id=repair.tenant_id,
                start_date=repair.start_date,
                end_date=repair.end_date,
                status=repair.status.value,
                created_at=repair.created_at,
            )
        )
        self._session.add_all(
            [
                CCloudFocusPreviewRepairDateTable(
                    repair_id=item.repair_id,
                    tracking_date=item.tracking_date,
                    status=item.status.value,
                )
                for item in repair.dates
            ]
        )
        self._session.flush()
        created = self._get(repair.repair_id)
        if created is None:
            raise RuntimeError("queued repair was not persisted")
        return created

    def get_for_owner(
        self,
        repair_id: str,
        ecosystem: str,
        tenant_id: str,
    ) -> PreviewRepair | None:
        value = self._get(repair_id)
        if value is None or value.ecosystem != ecosystem or value.tenant_id != tenant_id:
            return None
        return value

    def find_active_for_owner(
        self,
        ecosystem: str,
        tenant_id: str,
    ) -> PreviewRepair | None:
        row = self._session.exec(
            select(CCloudFocusPreviewRepairTable)
            .where(
                col(CCloudFocusPreviewRepairTable.ecosystem) == ecosystem,
                col(CCloudFocusPreviewRepairTable.tenant_id) == tenant_id,
                col(CCloudFocusPreviewRepairTable.status).in_(
                    (PreviewRepairStatus.QUEUED.value, PreviewRepairStatus.RUNNING.value)
                ),
            )
            .order_by(col(CCloudFocusPreviewRepairTable.created_at))
            .limit(1)
        ).first()
        return None if row is None else self._get(row.repair_id)

    def mark_running(self, repair_id: str, *, started_at: datetime) -> PreviewRepair | None:
        result = self._session.exec(
            update(CCloudFocusPreviewRepairTable)
            .where(
                col(CCloudFocusPreviewRepairTable.repair_id) == repair_id,
                col(CCloudFocusPreviewRepairTable.status) == PreviewRepairStatus.QUEUED.value,
            )
            .values(status=PreviewRepairStatus.RUNNING.value, started_at=_require_aware(started_at, "started_at"))
        )
        if int(getattr(result, "rowcount", 0)) != 1:
            return None
        self._session.flush()
        return self._get(repair_id)

    def mark_date_running(
        self,
        repair_id: str,
        tracking_date: date,
        *,
        started_at: datetime,
    ) -> PreviewRepairDate | None:
        parent = self._session.get(CCloudFocusPreviewRepairTable, repair_id)
        if parent is None or parent.status != PreviewRepairStatus.RUNNING.value:
            return None
        result = self._session.exec(
            update(CCloudFocusPreviewRepairDateTable)
            .where(
                col(CCloudFocusPreviewRepairDateTable.repair_id) == repair_id,
                col(CCloudFocusPreviewRepairDateTable.tracking_date) == tracking_date,
                col(CCloudFocusPreviewRepairDateTable.status) == PreviewRepairDateStatus.QUEUED.value,
            )
            .values(
                status=PreviewRepairDateStatus.RUNNING.value,
                started_at=_require_aware(started_at, "started_at"),
            )
        )
        if int(getattr(result, "rowcount", 0)) != 1:
            return None
        self._session.flush()
        row = self._session.get(CCloudFocusPreviewRepairDateTable, (repair_id, tracking_date))
        return None if row is None else _repair_date(row)

    def _mark_date_result(
        self,
        repair_id: str,
        tracking_date: date,
        *,
        status: PreviewRepairDateStatus,
        completed_at: datetime | None,
        calculation_id: str,
        calculation_completed_at: datetime,
        rows_written: int,
    ) -> PreviewRepairDate | None:
        if not calculation_id.strip() or rows_written < 0:
            raise ValueError("invalid repair calculation result")
        result = self._session.exec(
            update(CCloudFocusPreviewRepairDateTable)
            .where(
                col(CCloudFocusPreviewRepairDateTable.repair_id) == repair_id,
                col(CCloudFocusPreviewRepairDateTable.tracking_date) == tracking_date,
                col(CCloudFocusPreviewRepairDateTable.status) == PreviewRepairDateStatus.RUNNING.value,
            )
            .values(
                status=status.value,
                completed_at=None if completed_at is None else _require_aware(completed_at, "completed_at"),
                calculation_id=calculation_id,
                calculation_completed_at=_require_aware(
                    calculation_completed_at,
                    "calculation_completed_at",
                ),
                rows_written=rows_written,
            )
        )
        if int(getattr(result, "rowcount", 0)) != 1:
            return None
        self._session.flush()
        row = self._session.get(CCloudFocusPreviewRepairDateTable, (repair_id, tracking_date))
        return None if row is None else _repair_date(row)

    def mark_date_daily_validated(
        self,
        repair_id: str,
        tracking_date: date,
        *,
        calculation_id: str,
        calculation_completed_at: datetime,
        rows_written: int,
    ) -> PreviewRepairDate | None:
        return self._mark_date_result(
            repair_id,
            tracking_date,
            status=PreviewRepairDateStatus.DAILY_VALIDATED,
            completed_at=None,
            calculation_id=calculation_id,
            calculation_completed_at=calculation_completed_at,
            rows_written=rows_written,
        )

    def mark_date_succeeded_from_running(
        self,
        repair_id: str,
        tracking_date: date,
        *,
        completed_at: datetime,
        calculation_id: str,
        calculation_completed_at: datetime,
        rows_written: int,
    ) -> PreviewRepairDate | None:
        return self._mark_date_result(
            repair_id,
            tracking_date,
            status=PreviewRepairDateStatus.SUCCEEDED,
            completed_at=completed_at,
            calculation_id=calculation_id,
            calculation_completed_at=calculation_completed_at,
            rows_written=rows_written,
        )

    def mark_date_failed_from_running(
        self,
        repair_id: str,
        tracking_date: date,
        *,
        completed_at: datetime,
        stage: PreviewRepairFailureStage,
        diagnostic: PreviewDiagnostic,
    ) -> PreviewRepairDate | None:
        result = self._session.exec(
            update(CCloudFocusPreviewRepairDateTable)
            .where(
                col(CCloudFocusPreviewRepairDateTable.repair_id) == repair_id,
                col(CCloudFocusPreviewRepairDateTable.tracking_date) == tracking_date,
                col(CCloudFocusPreviewRepairDateTable.status) == PreviewRepairDateStatus.RUNNING.value,
            )
            .values(
                status=PreviewRepairDateStatus.FAILED.value,
                completed_at=_require_aware(completed_at, "completed_at"),
                calculation_id=None,
                calculation_completed_at=None,
                rows_written=None,
                failure_stage=stage.value,
                diagnostic_code=diagnostic.code,
                diagnostic_message=diagnostic.message,
                diagnostic_retryable=diagnostic.retryable,
                source_correlation_ids_json=json.dumps(list(diagnostic.source_correlation_ids)),
            )
        )
        if int(getattr(result, "rowcount", 0)) != 1:
            return None
        self._session.flush()
        row = self._session.get(CCloudFocusPreviewRepairDateTable, (repair_id, tracking_date))
        return None if row is None else _repair_date(row)

    def finalize_month_dates(
        self,
        repair_id: str,
        tracking_dates: tuple[date, ...],
        *,
        terminal_status: Literal[
            PreviewRepairDateStatus.SUCCEEDED,
            PreviewRepairDateStatus.FAILED,
        ],
        completed_at: datetime,
        stage: PreviewRepairFailureStage | None,
        diagnostic: PreviewDiagnostic | None,
    ) -> tuple[PreviewRepairDate, ...] | None:
        if (
            not tracking_dates
            or tuple(sorted(set(tracking_dates))) != tracking_dates
            or terminal_status not in {PreviewRepairDateStatus.SUCCEEDED, PreviewRepairDateStatus.FAILED}
            or (terminal_status is PreviewRepairDateStatus.SUCCEEDED and (stage is not None or diagnostic is not None))
            or (terminal_status is PreviewRepairDateStatus.FAILED and (stage is None or diagnostic is None))
        ):
            raise ValueError("invalid monthly repair finalization")
        rows = self._session.exec(
            select(CCloudFocusPreviewRepairDateTable).where(
                col(CCloudFocusPreviewRepairDateTable.repair_id) == repair_id,
                col(CCloudFocusPreviewRepairDateTable.tracking_date).in_(tracking_dates),
            )
        ).all()
        if len(rows) != len(tracking_dates) or any(
            row.status != PreviewRepairDateStatus.DAILY_VALIDATED.value for row in rows
        ):
            return None
        values: dict[str, object] = {
            "status": terminal_status.value,
            "completed_at": _require_aware(completed_at, "completed_at"),
        }
        if terminal_status is PreviewRepairDateStatus.FAILED:
            assert stage is not None and diagnostic is not None
            values.update(
                calculation_id=None,
                calculation_completed_at=None,
                rows_written=None,
                failure_stage=stage.value,
                diagnostic_code=diagnostic.code,
                diagnostic_message=diagnostic.message,
                diagnostic_retryable=diagnostic.retryable,
                source_correlation_ids_json=json.dumps(list(diagnostic.source_correlation_ids)),
            )
        result = self._session.exec(
            update(CCloudFocusPreviewRepairDateTable)
            .where(
                col(CCloudFocusPreviewRepairDateTable.repair_id) == repair_id,
                col(CCloudFocusPreviewRepairDateTable.tracking_date).in_(tracking_dates),
                col(CCloudFocusPreviewRepairDateTable.status) == PreviewRepairDateStatus.DAILY_VALIDATED.value,
            )
            .values(**values)
        )
        if int(getattr(result, "rowcount", 0)) != len(tracking_dates):
            self._session.rollback()
            return None
        self._session.flush()
        updated = self._session.exec(
            select(CCloudFocusPreviewRepairDateTable)
            .where(
                col(CCloudFocusPreviewRepairDateTable.repair_id) == repair_id,
                col(CCloudFocusPreviewRepairDateTable.tracking_date).in_(tracking_dates),
            )
            .order_by(col(CCloudFocusPreviewRepairDateTable.tracking_date))
        ).all()
        return tuple(_repair_date(item) for item in updated)

    def _fail(
        self,
        repair_id: str,
        *,
        expected_status: PreviewRepairStatus,
        completed_at: datetime,
        diagnostic: PreviewDiagnostic,
    ) -> PreviewRepair | None:
        completed = _require_aware(completed_at, "completed_at")
        result = self._session.exec(
            update(CCloudFocusPreviewRepairTable)
            .where(
                col(CCloudFocusPreviewRepairTable.repair_id) == repair_id,
                col(CCloudFocusPreviewRepairTable.status) == expected_status.value,
            )
            .values(
                status=PreviewRepairStatus.FAILED.value,
                completed_at=completed,
                diagnostic_code=diagnostic.code,
                diagnostic_message=diagnostic.message,
                diagnostic_retryable=diagnostic.retryable,
            )
        )
        if int(getattr(result, "rowcount", 0)) != 1:
            return None
        nonterminal = (
            (PreviewRepairDateStatus.QUEUED.value,)
            if expected_status is PreviewRepairStatus.QUEUED
            else (
                PreviewRepairDateStatus.QUEUED.value,
                PreviewRepairDateStatus.RUNNING.value,
                PreviewRepairDateStatus.DAILY_VALIDATED.value,
            )
        )
        expected_children = len(
            self._session.exec(
                select(CCloudFocusPreviewRepairDateTable).where(
                    col(CCloudFocusPreviewRepairDateTable.repair_id) == repair_id,
                    col(CCloudFocusPreviewRepairDateTable.status).in_(nonterminal),
                )
            ).all()
        )
        child_result = self._session.exec(
            update(CCloudFocusPreviewRepairDateTable)
            .where(
                col(CCloudFocusPreviewRepairDateTable.repair_id) == repair_id,
                col(CCloudFocusPreviewRepairDateTable.status).in_(nonterminal),
            )
            .values(
                status=PreviewRepairDateStatus.FAILED.value,
                completed_at=completed,
                calculation_id=None,
                calculation_completed_at=None,
                rows_written=None,
                failure_stage=PreviewRepairFailureStage.WORKER.value,
                diagnostic_code=diagnostic.code,
                diagnostic_message=diagnostic.message,
                diagnostic_retryable=diagnostic.retryable,
                source_correlation_ids_json=json.dumps(list(diagnostic.source_correlation_ids)),
            )
        )
        if int(getattr(child_result, "rowcount", 0)) != expected_children:
            self._session.rollback()
            return None
        self._session.flush()
        return self._get(repair_id)

    def fail_queued_before_execution(
        self,
        repair_id: str,
        *,
        completed_at: datetime,
        diagnostic: PreviewDiagnostic,
    ) -> PreviewRepair | None:
        return self._fail(
            repair_id,
            expected_status=PreviewRepairStatus.QUEUED,
            completed_at=completed_at,
            diagnostic=diagnostic,
        )

    def fail_running_worker(
        self,
        repair_id: str,
        *,
        completed_at: datetime,
        diagnostic: PreviewDiagnostic,
    ) -> PreviewRepair | None:
        return self._fail(
            repair_id,
            expected_status=PreviewRepairStatus.RUNNING,
            completed_at=completed_at,
            diagnostic=diagnostic,
        )

    def _finalize(
        self,
        repair_id: str,
        *,
        completed_at: datetime,
        with_failures: bool,
    ) -> PreviewRepair | None:
        children = self._session.exec(
            select(CCloudFocusPreviewRepairDateTable).where(
                col(CCloudFocusPreviewRepairDateTable.repair_id) == repair_id
            )
        ).all()
        statuses = {item.status for item in children}
        if not children or not statuses <= {
            PreviewRepairDateStatus.SUCCEEDED.value,
            PreviewRepairDateStatus.FAILED.value,
        }:
            return None
        if with_failures != (PreviewRepairDateStatus.FAILED.value in statuses):
            return None
        final_status = PreviewRepairStatus.COMPLETED_WITH_FAILURES if with_failures else PreviewRepairStatus.COMPLETED
        result = self._session.exec(
            update(CCloudFocusPreviewRepairTable)
            .where(
                col(CCloudFocusPreviewRepairTable.repair_id) == repair_id,
                col(CCloudFocusPreviewRepairTable.status) == PreviewRepairStatus.RUNNING.value,
            )
            .values(status=final_status.value, completed_at=_require_aware(completed_at, "completed_at"))
        )
        if int(getattr(result, "rowcount", 0)) != 1:
            return None
        self._session.flush()
        return self._get(repair_id)

    def finalize_completed(self, repair_id: str, *, completed_at: datetime) -> PreviewRepair | None:
        return self._finalize(repair_id, completed_at=completed_at, with_failures=False)

    def finalize_completed_with_failures(
        self,
        repair_id: str,
        *,
        completed_at: datetime,
    ) -> PreviewRepair | None:
        return self._finalize(repair_id, completed_at=completed_at, with_failures=True)

    def fail_interrupted_before(
        self,
        ecosystem: str,
        tenant_id: str,
        process_started_at: datetime,
        *,
        completed_at: datetime,
        diagnostic: PreviewDiagnostic,
    ) -> int:
        rows = self._session.exec(
            select(CCloudFocusPreviewRepairTable).where(
                col(CCloudFocusPreviewRepairTable.ecosystem) == ecosystem,
                col(CCloudFocusPreviewRepairTable.tenant_id) == tenant_id,
                col(CCloudFocusPreviewRepairTable.created_at)
                < _require_aware(process_started_at, "process_started_at"),
                col(CCloudFocusPreviewRepairTable.status).in_(
                    (PreviewRepairStatus.QUEUED.value, PreviewRepairStatus.RUNNING.value)
                ),
            )
        ).all()
        changed = 0
        for row in rows:
            result = self._fail(
                row.repair_id,
                expected_status=PreviewRepairStatus(row.status),
                completed_at=completed_at,
                diagnostic=diagnostic,
            )
            changed += result is not None
        return changed


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

    def resolve_authority(
        self,
        ecosystem: str,
        tenant_id: str,
        start: datetime,
        end: datetime,
    ) -> tuple[PreviewSourceAuthoritySlice, ...]:
        start = _require_aware(start, "start")
        end = _require_aware(end, "end")
        if start > end:
            raise ValueError("authority bounds must be ordered")
        base = select(CCloudSourceEvidenceAttemptTable).where(
            col(CCloudSourceEvidenceAttemptTable.ecosystem) == ecosystem,
            col(CCloudSourceEvidenceAttemptTable.tenant_id) == tenant_id,
            col(CCloudSourceEvidenceAttemptTable.status) != SourceAttemptStatus.ABORTED.value,
        )
        if start == end:
            rows = self._session.exec(
                base.order_by(col(CCloudSourceEvidenceAttemptTable.attempt_sequence).desc())
            ).all()
            selected = next(
                (
                    row
                    for row in rows
                    if source_attempt_origin(row.refresh_token) is PreviewSourceAttemptOrigin.ORDINARY
                    or _utc(row.refresh_start) <= start < _utc(row.refresh_end)
                ),
                None,
            )
            return (
                PreviewSourceAuthoritySlice(
                    start,
                    end,
                    None if selected is None else _source_attempt(selected),
                ),
            )
        rows = self._session.exec(
            base.where(
                col(CCloudSourceEvidenceAttemptTable.refresh_start) < end,
                col(CCloudSourceEvidenceAttemptTable.refresh_end) > start,
            ).order_by(col(CCloudSourceEvidenceAttemptTable.attempt_sequence).desc())
        ).all()
        unresolved: list[tuple[datetime, datetime]] = [(start, end)]
        assigned: list[PreviewSourceAuthoritySlice] = []
        for row in rows:
            attempt = _source_attempt(row)
            next_unresolved: list[tuple[datetime, datetime]] = []
            for segment_start, segment_end in unresolved:
                overlap_start = max(segment_start, attempt.refresh_start)
                overlap_end = min(segment_end, attempt.refresh_end)
                if overlap_start >= overlap_end:
                    next_unresolved.append((segment_start, segment_end))
                    continue
                if segment_start < overlap_start:
                    next_unresolved.append((segment_start, overlap_start))
                assigned.append(PreviewSourceAuthoritySlice(overlap_start, overlap_end, attempt))
                if overlap_end < segment_end:
                    next_unresolved.append((overlap_end, segment_end))
            unresolved = next_unresolved
            if not unresolved:
                break
        assigned.extend(PreviewSourceAuthoritySlice(left, right, None) for left, right in unresolved)
        ordered = sorted(assigned, key=lambda item: (item.start, item.end))
        merged: list[PreviewSourceAuthoritySlice] = []
        for item in ordered:
            if merged and merged[-1].end == item.start and merged[-1].attempt == item.attempt:
                previous = merged[-1]
                merged[-1] = PreviewSourceAuthoritySlice(previous.start, item.end, item.attempt)
            else:
                merged.append(item)
        return tuple(merged)

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
            select(CCloudSourceCaptureReadinessHistoryTable)
            .where(
                col(CCloudSourceCaptureReadinessHistoryTable.ecosystem) == ecosystem,
                col(CCloudSourceCaptureReadinessHistoryTable.tenant_id) == tenant_id,
                col(CCloudSourceCaptureReadinessHistoryTable.window_start) < end,
                col(CCloudSourceCaptureReadinessHistoryTable.window_end) > start,
            )
            .order_by(
                col(CCloudSourceCaptureReadinessHistoryTable.window_start),
                col(CCloudSourceCaptureReadinessHistoryTable.window_end),
                col(CCloudSourceCaptureReadinessHistoryTable.attempt_sequence),
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
        for row in rows:
            self._session.merge(
                CCloudSourceCaptureReadinessHistoryTable(
                    ecosystem=row.ecosystem,
                    tenant_id=row.tenant_id,
                    attempt_sequence=row.attempt_sequence,
                    window_start=row.window_start,
                    window_end=row.window_end,
                    capture_id=row.capture_id,
                    captured_at=row.captured_at,
                    source_count=row.source_count,
                )
            )
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
        history_source_exists = exists().where(
            col(CCloudCostSourceRecordTable.ecosystem) == ecosystem,
            col(CCloudCostSourceRecordTable.tenant_id) == tenant_id,
            col(CCloudCostSourceRecordTable.capture_id) == col(CCloudSourceCaptureReadinessHistoryTable.capture_id),
        )
        history_result = self._session.exec(
            delete(CCloudSourceCaptureReadinessHistoryTable).where(
                col(CCloudSourceCaptureReadinessHistoryTable.ecosystem) == ecosystem,
                col(CCloudSourceCaptureReadinessHistoryTable.tenant_id) == tenant_id,
                col(CCloudSourceCaptureReadinessHistoryTable.window_end) < cutoff,
                ~history_source_exists,
            )
        )
        deleted += int(getattr(history_result, "rowcount", 0))
        current = self.get_current_authority(ecosystem, tenant_id)
        current_readiness_exists = exists().where(
            col(CCloudSourceCaptureReadinessTable.attempt_sequence)
            == col(CCloudSourceEvidenceAttemptTable.attempt_sequence)
        )
        history_readiness_exists = exists().where(
            col(CCloudSourceCaptureReadinessHistoryTable.attempt_sequence)
            == col(CCloudSourceEvidenceAttemptTable.attempt_sequence)
        )
        conditions = [
            col(CCloudSourceEvidenceAttemptTable.ecosystem) == ecosystem,
            col(CCloudSourceEvidenceAttemptTable.tenant_id) == tenant_id,
            col(CCloudSourceEvidenceAttemptTable.status) != SourceAttemptStatus.PENDING.value,
            col(CCloudSourceEvidenceAttemptTable.completed_at).is_not(None),
            col(CCloudSourceEvidenceAttemptTable.completed_at) < cutoff,
            ~current_readiness_exists,
            ~history_readiness_exists,
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
