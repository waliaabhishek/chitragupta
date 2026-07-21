from __future__ import annotations

import json
import logging
import sqlite3
from dataclasses import asdict, dataclass, replace
from datetime import date, datetime, timedelta
from typing import Literal, Protocol, Self, runtime_checkable

from sqlalchemy import (
    Boolean,
    Column,
    Date,
    DateTime,
    Index,
    String,
    Text,
    and_,
    case,
    cast,
    func,
    or_,
    text,
    true,
    update,
)
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlmodel import Field, Session, SQLModel, col, select

from core.preview.eligibility import capped_correlations
from core.preview.evidence import (  # noqa: TC001  # resolved by get_type_hints contract test
    PreviewAllocationEvidenceReader,
    PreviewCostEvidenceReader,
)
from core.preview.mapping import LEGACY_DAILY_FULL_V4_COLUMNS, validate_preview_effective_columns
from core.preview.models import (
    PreviewArtifactMetadata,
    PreviewCalculationCoverageEntry,
    PreviewDiagnostic,
    PreviewPackageMetadata,
    PreviewRequest,
    PreviewRequestStatus,
    PreviewRevision,
    PreviewRevisionCandidate,
    PreviewSourceSnapshot,
    PreviewStoredPackage,
    preview_request_status,
    validate_preview_request_snapshot,
    validate_preview_revision_invariant,
)
from core.storage.backends.sqlmodel.mappers import ensure_utc, ensure_utc_strict
from core.storage.backends.sqlmodel.tables import PipelineStateTable
from core.storage.interface import (  # noqa: TC001  # resolved by get_type_hints contract test
    EntityTagRepository,
    IdentityRepository,
    ResourceRepository,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CompleteCalculationCoverage:
    entries: tuple[PreviewCalculationCoverageEntry, ...]


@dataclass(frozen=True)
class NoUsableCalculationCoverage:
    missing_dates: tuple[date, ...]
    incomplete_correlation_dates: tuple[date, ...]


@dataclass(frozen=True)
class PartialCalculationCoverage:
    entries: tuple[PreviewCalculationCoverageEntry, ...]
    missing_dates: tuple[date, ...]
    incomplete_correlation_dates: tuple[date, ...]


PreviewCalculationCoverageResult = (
    CompleteCalculationCoverage | NoUsableCalculationCoverage | PartialCalculationCoverage
)


def _require_safe_identifier(value: str, field: str) -> None:
    if not value or not value.strip() or "/" in value or "\\" in value or value in {".", ".."}:
        raise ValueError(f"{field} must be a safe nonblank identifier")


@dataclass(frozen=True)
class PreviewRequestPage:
    items: tuple[PreviewRequest, ...]
    next_cursor: str | None

    def __post_init__(self) -> None:
        if self.next_cursor is not None:
            _require_safe_identifier(self.next_cursor, "next_cursor")


@dataclass(frozen=True)
class PreviewExpiredArtifact:
    request_id: str
    storage_key: str

    def __post_init__(self) -> None:
        _require_safe_identifier(self.request_id, "request_id")
        _require_safe_identifier(self.storage_key, "storage_key")


@dataclass(frozen=True)
class PreviewInterruptionRecoveryResult:
    failed_count: int
    protected_count: int

    def __post_init__(self) -> None:
        if self.failed_count < 0 or self.protected_count < 0:
            raise ValueError("preview interruption recovery counts must be non-negative")


@dataclass(frozen=True)
class PreviewStaleLeaseRecoveryResult:
    failed_count: int
    has_more: bool

    def __post_init__(self) -> None:
        if self.failed_count < 0:
            raise ValueError("preview stale-lease recovery count must be non-negative")


class PreviewRequestCursorError(ValueError):
    def __init__(self, cursor_request_id: str) -> None:
        self.cursor_request_id = cursor_request_id
        super().__init__("preview request cursor is invalid")


@runtime_checkable
class PreviewCalculationRepository(Protocol):
    def find_current_coverage(
        self, *, ecosystem: str, tenant_id: str, start_date: date, end_date: date
    ) -> PreviewCalculationCoverageResult: ...


@runtime_checkable
class PreviewRequestRepository(Protocol):
    def create_queued(
        self,
        request: PreviewRequest,
        *,
        worker_id: str | None = None,
        lease_expires_at: datetime | None = None,
    ) -> PreviewRequest: ...

    def get_for_owner(self, request_id: str, ecosystem: str, tenant_id: str) -> PreviewRequest | None: ...

    def list_recent_for_owner(
        self,
        *,
        ecosystem: str,
        tenant_id: str,
        limit: int,
        cursor_request_id: str | None,
    ) -> PreviewRequestPage: ...

    def mark_running(
        self,
        request_id: str,
        started_at: datetime,
        *,
        worker_id: str | None = None,
        lease_expires_at: datetime | None = None,
    ) -> PreviewRequest | None: ...

    def renew_lease(self, request_id: str, worker_id: str, lease_expires_at: datetime) -> bool: ...

    def mark_ready(
        self,
        request_id: str,
        completed_at: datetime,
        expires_at: datetime,
        source_snapshot: PreviewSourceSnapshot,
        stored_package: PreviewStoredPackage,
        *,
        worker_id: str | None = None,
    ) -> bool: ...

    def mark_failed(
        self,
        request_id: str,
        completed_at: datetime,
        diagnostic: PreviewDiagnostic,
        *,
        worker_id: str | None = None,
    ) -> bool: ...

    def expire_ready_due(
        self, *, ecosystem: str, tenant_id: str, now: datetime, limit: int
    ) -> tuple[PreviewExpiredArtifact, ...]: ...

    def expire_ready_request(
        self, *, request_id: str, ecosystem: str, tenant_id: str, now: datetime
    ) -> PreviewExpiredArtifact | None: ...

    def list_expired_artifacts(
        self, *, ecosystem: str, tenant_id: str, limit: int
    ) -> tuple[PreviewExpiredArtifact, ...]: ...

    def clear_expired_storage_key(self, request_id: str, storage_key: str) -> bool: ...

    def fail_interrupted_before(
        self,
        *,
        ecosystem: str,
        tenant_id: str,
        startup_at: datetime,
        lease_stale_at: datetime,
        diagnostic: PreviewDiagnostic,
    ) -> PreviewInterruptionRecoveryResult: ...

    def fail_stale_foreign_leases(
        self,
        *,
        ecosystem: str,
        tenant_id: str,
        current_worker_id: str,
        lease_stale_at: datetime,
        limit: int,
        diagnostic: PreviewDiagnostic,
    ) -> PreviewStaleLeaseRecoveryResult: ...


class PreviewRequestTable(SQLModel, table=True):
    __tablename__ = "preview_requests"
    __table_args__ = (
        Index("ix_preview_requests_owner_created", "ecosystem", "tenant_id", "created_at"),
        Index("ix_preview_requests_owner_status", "ecosystem", "tenant_id", "status"),
        Index("ix_preview_requests_owner_expiry", "ecosystem", "tenant_id", "status", "expires_at"),
        Index(
            "ix_preview_requests_owner_recovery",
            "ecosystem",
            "tenant_id",
            "status",
            "created_at",
            "lease_expires_at",
        ),
        Index(
            "ix_preview_requests_owner_lease",
            "ecosystem",
            "tenant_id",
            "status",
            "lease_expires_at",
            "worker_id",
        ),
    )

    request_id: str = Field(primary_key=True)
    tenant_name: str
    ecosystem: str
    tenant_id: str
    grain: str
    start_date: date = Field(sa_column=Column(Date()))
    end_date: date = Field(sa_column=Column(Date()))
    column_profile: str
    status: str
    created_at: datetime = Field(sa_column=Column(DateTime(timezone=True)))
    started_at: datetime | None = Field(default=None, sa_column=Column(DateTime(timezone=True)))
    completed_at: datetime | None = Field(default=None, sa_column=Column(DateTime(timezone=True)))
    expires_at: datetime | None = Field(default=None, sa_column=Column(DateTime(timezone=True)))
    worker_id: str | None = Field(default=None, sa_column=Column(String(), nullable=True))
    lease_expires_at: datetime | None = Field(default=None, sa_column=Column(DateTime(timezone=True), nullable=True))
    calculation_timestamp: datetime | None = Field(default=None, sa_column=Column(DateTime(timezone=True)))
    source_through: datetime | None = Field(default=None, sa_column=Column(DateTime(timezone=True)))
    calculation_coverage_json: str | None = None
    diagnostic_code: str | None = None
    diagnostic_message: str | None = None
    diagnostic_retryable: bool | None = None
    diagnostic_source_correlation_ids_json: str | None = Field(
        default=None,
        sa_column=Column(Text(), nullable=True),
    )
    storage_key: str | None = None
    manifest_metadata_json: str | None = None
    data_files_json: str | None = None
    effective_columns_json: str | None = Field(default=None, sa_column=Column(Text(), nullable=True))
    effective_coverage_start_date: date | None = Field(default=None, sa_column=Column(Date(), nullable=True))
    effective_coverage_end_date: date | None = Field(default=None, sa_column=Column(Date(), nullable=True))
    availability_cutoff_end_date: date | None = Field(default=None, sa_column=Column(Date(), nullable=True))
    monthly_status: str | None = None


class PreviewRevisionTable(SQLModel, table=True):
    __tablename__ = "preview_revisions"
    __table_args__ = (
        Index("ix_preview_revisions_supersedes", "supersedes_revision_id"),
        Index("ix_preview_revisions_superseded_by", "superseded_by_revision_id"),
        Index(
            "ux_preview_revisions_owner_month_current",
            "ecosystem",
            "tenant_id",
            "month_start",
            unique=True,
            sqlite_where=text("is_current = 1"),
            postgresql_where=text("is_current IS TRUE"),
        ),
    )

    revision_id: str = Field(primary_key=True)
    tenant_name_at_publication: str
    ecosystem: str
    tenant_id: str
    month_start: date = Field(sa_column=Column(Date(), nullable=False))
    month_end: date = Field(sa_column=Column(Date(), nullable=False))
    monthly_status: str
    material_sha256: str
    source_snapshot_json: str = Field(sa_column=Column(Text(), nullable=False))
    published_at: datetime = Field(sa_column=Column(DateTime(timezone=True), nullable=False))
    supersedes_revision_id: str | None = Field(default=None, sa_column=Column(String(), nullable=True))
    superseded_by_revision_id: str | None = Field(default=None, sa_column=Column(String(), nullable=True))
    is_current: bool = Field(sa_column=Column(Boolean(), nullable=False))
    storage_key: str
    manifest_metadata_json: str = Field(sa_column=Column(Text(), nullable=False))
    file_metadata_json: str = Field(sa_column=Column(Text(), nullable=False))


def _canonical_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _artifact_dict(item: PreviewArtifactMetadata) -> dict[str, object]:
    return asdict(item)


def _coverage_json(snapshot: PreviewSourceSnapshot | None) -> str | None:
    if snapshot is None:
        return None
    return _canonical_json(
        [
            {
                "tracking_date": entry.tracking_date.isoformat(),
                "calculation_id": entry.calculation_id,
                "calculation_completed_at": entry.calculation_completed_at.isoformat(),
                "calculation_run_id": entry.calculation_run_id,
            }
            for entry in snapshot.calculation_coverage
        ]
    )


def request_to_table(request: PreviewRequest) -> PreviewRequestTable:
    validate_preview_effective_columns(request.column_profile, request.effective_columns)
    validate_preview_request_snapshot(
        request=request,
        snapshot=request.source_snapshot,
        resulting_status=request.status,
        mode="strict_materialized",
    )
    package = request.package
    return PreviewRequestTable(
        request_id=request.request_id,
        tenant_name=request.tenant_name,
        ecosystem=request.ecosystem,
        tenant_id=request.tenant_id,
        grain=request.grain,
        start_date=request.start_date,
        end_date=request.end_date,
        column_profile=request.column_profile,
        status=request.status.value,
        created_at=ensure_utc_strict(request.created_at),
        started_at=ensure_utc_strict(request.started_at),
        completed_at=ensure_utc_strict(request.completed_at),
        expires_at=ensure_utc_strict(request.expires_at),
        worker_id=None,
        lease_expires_at=None,
        calculation_timestamp=ensure_utc_strict(
            request.source_snapshot.calculation_timestamp if request.source_snapshot else None
        ),
        source_through=ensure_utc_strict(request.source_snapshot.source_through if request.source_snapshot else None),
        calculation_coverage_json=_coverage_json(request.source_snapshot),
        diagnostic_code=request.diagnostic.code if request.diagnostic else None,
        diagnostic_message=request.diagnostic.message if request.diagnostic else None,
        diagnostic_retryable=request.diagnostic.retryable if request.diagnostic else None,
        diagnostic_source_correlation_ids_json=(
            _canonical_json(capped_correlations(request.diagnostic.source_correlation_ids))
            if request.diagnostic
            else None
        ),
        storage_key=request.storage_key,
        manifest_metadata_json=_canonical_json(_artifact_dict(package.manifest)) if package else None,
        data_files_json=_canonical_json([_artifact_dict(item) for item in package.files]) if package else None,
        effective_columns_json=_canonical_json(request.effective_columns),
        effective_coverage_start_date=(
            request.source_snapshot.effective_coverage_start_date if request.source_snapshot else None
        ),
        effective_coverage_end_date=(
            request.source_snapshot.effective_coverage_end_date if request.source_snapshot else None
        ),
        availability_cutoff_end_date=(
            request.source_snapshot.availability_cutoff_end_date if request.source_snapshot else None
        ),
        monthly_status=request.source_snapshot.monthly_status if request.source_snapshot else None,
    )


def _artifact_from_dict(value: dict[str, object]) -> PreviewArtifactMetadata:
    return PreviewArtifactMetadata(
        name=str(value["name"]),
        media_type=str(value["media_type"]),
        size_bytes=int(str(value["size_bytes"])),
        sha256=str(value["sha256"]),
        order=int(str(value["order"])) if value.get("order") is not None else None,
    )


def _supported_grain(value: str) -> Literal["daily", "monthly"]:
    if value == "daily":
        return "daily"
    if value == "monthly":
        return "monthly"
    raise ValueError(f"unsupported persisted preview grain: {value!r}")


def _supported_column_profile(value: str) -> Literal["full", "summary", "custom"]:
    if value == "full":
        return "full"
    if value == "summary":
        return "summary"
    if value == "custom":
        return "custom"
    raise ValueError(f"unsupported persisted preview column profile: {value!r}")


def _supported_monthly_status(value: str | None) -> Literal["provisional", "settled"] | None:
    if value is None:
        return None
    if value == "provisional":
        return "provisional"
    if value == "settled":
        return "settled"
    raise ValueError(f"unsupported persisted preview monthly status: {value!r}")


def request_to_domain(row: PreviewRequestTable) -> PreviewRequest:
    profile = _supported_column_profile(row.column_profile)
    grain = _supported_grain(row.grain)
    legacy_v4 = row.effective_columns_json is None
    if legacy_v4:
        if profile != "full" or grain != "daily":
            raise ValueError("legacy preview columns are valid only for Daily Full rows")
        effective_columns = LEGACY_DAILY_FULL_V4_COLUMNS
    else:
        explicit_columns_json = row.effective_columns_json
        assert explicit_columns_json is not None
        decoded_columns = json.loads(explicit_columns_json)
        if not isinstance(decoded_columns, list) or not all(isinstance(value, str) for value in decoded_columns):
            raise ValueError("preview effective columns must be a string list")
        effective_columns = tuple(decoded_columns)
        validate_preview_effective_columns(profile, effective_columns)

    coverage: tuple[PreviewCalculationCoverageEntry, ...] = ()
    if row.calculation_coverage_json:
        raw = json.loads(row.calculation_coverage_json)
        coverage = tuple(
            PreviewCalculationCoverageEntry(
                tracking_date=date.fromisoformat(item["tracking_date"]),
                calculation_id=item["calculation_id"],
                calculation_completed_at=datetime.fromisoformat(item["calculation_completed_at"]),
                calculation_run_id=item.get("calculation_run_id"),
            )
            for item in raw
        )
    snapshot = None
    if row.status in {PreviewRequestStatus.READY.value, PreviewRequestStatus.EXPIRED.value}:
        effective_coverage_start_date: date
        effective_coverage_end_date: date
        if legacy_v4:
            effective_coverage_start_date = row.start_date
            effective_coverage_end_date = row.end_date
        else:
            persisted_start = row.effective_coverage_start_date
            persisted_end = row.effective_coverage_end_date
            if persisted_start is None or persisted_end is None:
                raise ValueError("v5 ready preview requires persisted effective coverage bounds")
            effective_coverage_start_date = persisted_start
            effective_coverage_end_date = persisted_end
        snapshot = PreviewSourceSnapshot(
            calculation_timestamp=ensure_utc(row.calculation_timestamp),
            calculation_coverage=coverage,
            source_through=ensure_utc(row.source_through),
            effective_coverage_start_date=effective_coverage_start_date,
            effective_coverage_end_date=effective_coverage_end_date,
            availability_cutoff_end_date=row.availability_cutoff_end_date,
            monthly_status=_supported_monthly_status(row.monthly_status),
        )
    diagnostic = None
    if row.diagnostic_code is not None:
        if row.diagnostic_message is None or row.diagnostic_retryable is None:
            raise ValueError("preview diagnostic metadata is incomplete")
        correlations: tuple[str, ...] = ()
        if row.diagnostic_source_correlation_ids_json:
            values = json.loads(row.diagnostic_source_correlation_ids_json)
            if not isinstance(values, list) or not all(isinstance(value, str) for value in values):
                raise ValueError("preview diagnostic correlations must be a string list")
            correlations = capped_correlations(values)
        diagnostic = PreviewDiagnostic(
            row.diagnostic_code,
            row.diagnostic_message,
            row.diagnostic_retryable,
            correlations,
        )
    package = None
    if row.manifest_metadata_json is not None and row.data_files_json is not None:
        raw_files = json.loads(row.data_files_json)
        if legacy_v4:
            raw_files = [{**item, "order": index} for index, item in enumerate(raw_files, start=1)]
        package = PreviewPackageMetadata(
            manifest=_artifact_from_dict(json.loads(row.manifest_metadata_json)),
            files=tuple(_artifact_from_dict(item) for item in raw_files),
        )
    request = PreviewRequest(
        request_id=row.request_id,
        tenant_name=row.tenant_name,
        ecosystem=row.ecosystem,
        tenant_id=row.tenant_id,
        grain=grain,
        start_date=row.start_date,
        end_date=row.end_date,
        column_profile=profile,
        status=preview_request_status(row.status),
        created_at=ensure_utc(row.created_at),
        started_at=ensure_utc(row.started_at),
        completed_at=ensure_utc(row.completed_at),
        expires_at=ensure_utc(row.expires_at),
        source_snapshot=snapshot,
        diagnostic=diagnostic,
        storage_key=row.storage_key,
        package=package,
        effective_columns=effective_columns,
    )
    validate_preview_request_snapshot(
        request=request,
        snapshot=request.source_snapshot,
        resulting_status=request.status,
        mode="strict_materialized",
    )
    return request


class SQLModelPreviewCalculationRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def find_current_coverage(
        self, *, ecosystem: str, tenant_id: str, start_date: date, end_date: date
    ) -> PreviewCalculationCoverageResult:
        requested_days = (end_date - start_date).days
        if requested_days < 1 or requested_days > 31:
            raise ValueError("preview coverage range must contain 1 through 31 days")
        statement = (
            select(  # type: ignore[call-overload]  # SQLModel select() overload stubs stop at four columns
                PipelineStateTable.tracking_date,
                PipelineStateTable.chargeback_calculated,
                PipelineStateTable.calculation_id,
                cast(PipelineStateTable.calculation_completed_at, String),
                PipelineStateTable.calculation_run_id,
            )
            .where(
                col(PipelineStateTable.ecosystem) == ecosystem,
                col(PipelineStateTable.tenant_id) == tenant_id,
                col(PipelineStateTable.tracking_date) >= start_date,
                col(PipelineStateTable.tracking_date) < end_date,
            )
            .order_by(col(PipelineStateTable.tracking_date))
            .limit(requested_days + 1)
        )
        rows = self._session.exec(statement).all()
        if len(rows) > requested_days:
            raise RuntimeError("preview calculation coverage cardinality exceeded")
        by_date = {row[0]: row for row in rows}
        entries: list[PreviewCalculationCoverageEntry] = []
        missing: list[date] = []
        incomplete: list[date] = []
        for offset in range(requested_days):
            current = start_date + timedelta(days=offset)
            row = by_date.get(current)
            if row is None or not row[1]:
                missing.append(current)
                continue
            calculation_id = row[2]
            calculation_completed_at = row[3]
            if not calculation_id or calculation_completed_at is None:
                incomplete.append(current)
                continue
            try:
                entries.append(
                    PreviewCalculationCoverageEntry(
                        tracking_date=current,
                        calculation_id=calculation_id,
                        calculation_completed_at=ensure_utc(datetime.fromisoformat(calculation_completed_at)),
                        calculation_run_id=row[4],
                    )
                )
            except ValueError:
                incomplete.append(current)
        if len(entries) == requested_days:
            return CompleteCalculationCoverage(tuple(entries))
        if not entries:
            return NoUsableCalculationCoverage(tuple(missing), tuple(incomplete))
        return PartialCalculationCoverage(tuple(entries), tuple(missing), tuple(incomplete))


class SQLModelPreviewRequestRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def create_queued(
        self,
        request: PreviewRequest,
        *,
        worker_id: str | None = None,
        lease_expires_at: datetime | None = None,
    ) -> PreviewRequest:
        if request.status is not PreviewRequestStatus.QUEUED:
            raise ValueError("new preview request must be queued")
        if (worker_id is None) != (lease_expires_at is None):
            raise ValueError("preview worker ownership requires both worker_id and lease_expires_at")
        if worker_id is not None:
            _require_safe_identifier(worker_id, "worker_id")
        row = request_to_table(request)
        row.worker_id = worker_id
        row.lease_expires_at = ensure_utc_strict(lease_expires_at)
        self._session.add(row)
        self._session.flush()
        return request

    def get_for_owner(self, request_id: str, ecosystem: str, tenant_id: str) -> PreviewRequest | None:
        statement = select(PreviewRequestTable).where(
            col(PreviewRequestTable.request_id) == request_id,
            col(PreviewRequestTable.ecosystem) == ecosystem,
            col(PreviewRequestTable.tenant_id) == tenant_id,
        )
        row = self._session.exec(statement).first()
        return request_to_domain(row) if row else None

    def list_recent_for_owner(
        self,
        *,
        ecosystem: str,
        tenant_id: str,
        limit: int,
        cursor_request_id: str | None,
    ) -> PreviewRequestPage:
        if limit < 1 or limit > 100:
            raise ValueError("preview request page limit must be between 1 and 100")
        statement = select(PreviewRequestTable).where(
            col(PreviewRequestTable.ecosystem) == ecosystem,
            col(PreviewRequestTable.tenant_id) == tenant_id,
        )
        if cursor_request_id is not None:
            cursor = self._session.exec(
                select(PreviewRequestTable).where(
                    col(PreviewRequestTable.request_id) == cursor_request_id,
                    col(PreviewRequestTable.ecosystem) == ecosystem,
                    col(PreviewRequestTable.tenant_id) == tenant_id,
                )
            ).first()
            if cursor is None:
                raise PreviewRequestCursorError(cursor_request_id)
            statement = statement.where(
                or_(
                    col(PreviewRequestTable.created_at) < cursor.created_at,
                    and_(
                        col(PreviewRequestTable.created_at) == cursor.created_at,
                        col(PreviewRequestTable.request_id) < cursor.request_id,
                    ),
                )
            )
        rows = self._session.exec(
            statement.order_by(
                col(PreviewRequestTable.created_at).desc(),
                col(PreviewRequestTable.request_id).desc(),
            ).limit(limit + 1)
        ).all()
        has_more = len(rows) > limit
        emitted = rows[:limit]
        items = tuple(request_to_domain(row) for row in emitted)
        return PreviewRequestPage(items=items, next_cursor=items[-1].request_id if has_more and items else None)

    def _current(self, request_id: str) -> PreviewRequest | None:
        row = self._session.get(PreviewRequestTable, request_id)
        return None if row is None else request_to_domain(row)

    def mark_running(
        self,
        request_id: str,
        started_at: datetime,
        *,
        worker_id: str | None = None,
        lease_expires_at: datetime | None = None,
    ) -> PreviewRequest | None:
        if (worker_id is None) != (lease_expires_at is None):
            raise ValueError("preview worker ownership requires both worker_id and lease_expires_at")
        if worker_id is not None:
            _require_safe_identifier(worker_id, "worker_id")
        current = self._current(request_id)
        if current is None or current.status is not PreviewRequestStatus.QUEUED:
            return None
        candidate = replace(
            current,
            status=PreviewRequestStatus.RUNNING,
            started_at=started_at,
        )
        validate_preview_request_snapshot(
            request=candidate,
            snapshot=candidate.source_snapshot,
            resulting_status=candidate.status,
            mode="strict_materialized",
        )
        statement = update(PreviewRequestTable).where(
            col(PreviewRequestTable.request_id) == request_id,
            col(PreviewRequestTable.status) == PreviewRequestStatus.QUEUED.value,
        )
        if worker_id is not None:
            statement = statement.where(col(PreviewRequestTable.worker_id) == worker_id)
        result = self._session.execute(
            statement.values(
                status=candidate.status.value,
                started_at=ensure_utc_strict(candidate.started_at),
                lease_expires_at=ensure_utc_strict(lease_expires_at),
            )
        )
        return candidate if getattr(result, "rowcount", 0) == 1 else None

    def renew_lease(self, request_id: str, worker_id: str, lease_expires_at: datetime) -> bool:
        _require_safe_identifier(worker_id, "worker_id")
        result = self._session.execute(
            update(PreviewRequestTable)
            .where(
                col(PreviewRequestTable.request_id) == request_id,
                col(PreviewRequestTable.worker_id) == worker_id,
                col(PreviewRequestTable.status).in_(
                    [PreviewRequestStatus.QUEUED.value, PreviewRequestStatus.RUNNING.value]
                ),
            )
            .values(lease_expires_at=ensure_utc_strict(lease_expires_at))
        )
        return getattr(result, "rowcount", 0) == 1

    def mark_ready(
        self,
        request_id: str,
        completed_at: datetime,
        expires_at: datetime,
        source_snapshot: PreviewSourceSnapshot,
        stored_package: PreviewStoredPackage,
        *,
        worker_id: str | None = None,
    ) -> bool:
        if worker_id is not None:
            _require_safe_identifier(worker_id, "worker_id")
        current = self._current(request_id)
        if current is None or current.status is not PreviewRequestStatus.RUNNING:
            return False
        package = PreviewPackageMetadata(stored_package.manifest, stored_package.files)
        candidate = replace(
            current,
            status=PreviewRequestStatus.READY,
            completed_at=completed_at,
            expires_at=expires_at,
            source_snapshot=source_snapshot,
            storage_key=stored_package.storage_key,
            package=package,
        )
        validate_preview_request_snapshot(
            request=candidate,
            snapshot=candidate.source_snapshot,
            resulting_status=candidate.status,
            mode="strict_materialized",
        )
        statement = update(PreviewRequestTable).where(
            col(PreviewRequestTable.request_id) == request_id,
            col(PreviewRequestTable.status) == PreviewRequestStatus.RUNNING.value,
        )
        if worker_id is not None:
            statement = statement.where(col(PreviewRequestTable.worker_id) == worker_id)
        result = self._session.execute(
            statement.values(
                status=PreviewRequestStatus.READY.value,
                completed_at=ensure_utc_strict(candidate.completed_at),
                expires_at=ensure_utc_strict(candidate.expires_at),
                calculation_timestamp=ensure_utc_strict(source_snapshot.calculation_timestamp),
                source_through=ensure_utc_strict(source_snapshot.source_through),
                calculation_coverage_json=_coverage_json(source_snapshot),
                effective_coverage_start_date=source_snapshot.effective_coverage_start_date,
                effective_coverage_end_date=source_snapshot.effective_coverage_end_date,
                availability_cutoff_end_date=source_snapshot.availability_cutoff_end_date,
                monthly_status=source_snapshot.monthly_status,
                storage_key=candidate.storage_key,
                manifest_metadata_json=_canonical_json(_artifact_dict(package.manifest)),
                data_files_json=_canonical_json([_artifact_dict(item) for item in package.files]),
                worker_id=None,
                lease_expires_at=None,
            )
        )
        return getattr(result, "rowcount", 0) == 1

    def expire_ready_request(
        self,
        *,
        request_id: str,
        ecosystem: str,
        tenant_id: str,
        now: datetime,
    ) -> PreviewExpiredArtifact | None:
        now = ensure_utc_strict(now)
        current = self.get_for_owner(request_id, ecosystem, tenant_id)
        if (
            current is None
            or current.status is not PreviewRequestStatus.READY
            or current.expires_at is None
            or current.expires_at > now
            or current.storage_key is None
        ):
            return None
        result = self._session.execute(
            update(PreviewRequestTable)
            .where(
                col(PreviewRequestTable.request_id) == request_id,
                col(PreviewRequestTable.ecosystem) == ecosystem,
                col(PreviewRequestTable.tenant_id) == tenant_id,
                col(PreviewRequestTable.status) == PreviewRequestStatus.READY.value,
                col(PreviewRequestTable.expires_at) <= now,
            )
            .values(status=PreviewRequestStatus.EXPIRED.value)
            .execution_options(synchronize_session=False)
        )
        if getattr(result, "rowcount", 0) != 1:
            return None
        return PreviewExpiredArtifact(request_id=request_id, storage_key=current.storage_key)

    def expire_ready_due(
        self,
        *,
        ecosystem: str,
        tenant_id: str,
        now: datetime,
        limit: int,
    ) -> tuple[PreviewExpiredArtifact, ...]:
        if limit < 1 or limit > 100:
            raise ValueError("expiry limit must be between 1 and 100")
        due = self._session.execute(
            select(PreviewRequestTable.request_id, PreviewRequestTable.storage_key)
            .where(
                col(PreviewRequestTable.ecosystem) == ecosystem,
                col(PreviewRequestTable.tenant_id) == tenant_id,
                col(PreviewRequestTable.status) == PreviewRequestStatus.READY.value,
                col(PreviewRequestTable.expires_at) <= ensure_utc_strict(now),
            )
            .order_by(col(PreviewRequestTable.expires_at), col(PreviewRequestTable.request_id))
            .limit(limit)
        ).all()
        selected = tuple((request_id, storage_key) for request_id, storage_key in due)
        if not selected:
            return ()
        result = self._session.execute(
            update(PreviewRequestTable)
            .where(
                col(PreviewRequestTable.request_id).in_(request_id for request_id, _ in selected),
                col(PreviewRequestTable.ecosystem) == ecosystem,
                col(PreviewRequestTable.tenant_id) == tenant_id,
                col(PreviewRequestTable.status) == PreviewRequestStatus.READY.value,
                col(PreviewRequestTable.expires_at) <= ensure_utc_strict(now),
            )
            .values(status=PreviewRequestStatus.EXPIRED.value)
            .returning(col(PreviewRequestTable.request_id))
            .execution_options(synchronize_session=False)
        )
        updated_ids = {request_id for (request_id,) in result}
        return tuple(
            PreviewExpiredArtifact(request_id, storage_key)
            for request_id, storage_key in selected
            if request_id in updated_ids and storage_key is not None
        )

    def list_expired_artifacts(
        self,
        *,
        ecosystem: str,
        tenant_id: str,
        limit: int,
    ) -> tuple[PreviewExpiredArtifact, ...]:
        if limit < 1 or limit > 100:
            raise ValueError("expiry limit must be between 1 and 100")
        rows = self._session.exec(
            select(PreviewRequestTable)
            .where(
                col(PreviewRequestTable.ecosystem) == ecosystem,
                col(PreviewRequestTable.tenant_id) == tenant_id,
                col(PreviewRequestTable.status) == PreviewRequestStatus.EXPIRED.value,
                col(PreviewRequestTable.storage_key).is_not(None),
            )
            .order_by(col(PreviewRequestTable.expires_at), col(PreviewRequestTable.request_id))
            .limit(limit)
        ).all()
        return tuple(
            PreviewExpiredArtifact(row.request_id, row.storage_key) for row in rows if row.storage_key is not None
        )

    def clear_expired_storage_key(self, request_id: str, storage_key: str) -> bool:
        result = self._session.execute(
            update(PreviewRequestTable)
            .where(
                col(PreviewRequestTable.request_id) == request_id,
                col(PreviewRequestTable.status) == PreviewRequestStatus.EXPIRED.value,
                col(PreviewRequestTable.storage_key) == storage_key,
            )
            .values(storage_key=None)
        )
        return getattr(result, "rowcount", 0) == 1

    def fail_interrupted_before(
        self,
        *,
        ecosystem: str,
        tenant_id: str,
        startup_at: datetime,
        lease_stale_at: datetime,
        diagnostic: PreviewDiagnostic,
    ) -> PreviewInterruptionRecoveryResult:
        cutoff = ensure_utc_strict(startup_at)
        stale = ensure_utc_strict(lease_stale_at)
        result = self._session.execute(
            update(PreviewRequestTable)
            .where(
                col(PreviewRequestTable.ecosystem) == ecosystem,
                col(PreviewRequestTable.tenant_id) == tenant_id,
                col(PreviewRequestTable.status).in_(
                    [PreviewRequestStatus.QUEUED.value, PreviewRequestStatus.RUNNING.value]
                ),
                col(PreviewRequestTable.created_at) < cutoff,
                or_(
                    col(PreviewRequestTable.worker_id).is_(None),
                    col(PreviewRequestTable.lease_expires_at).is_(None),
                ),
            )
            .values(
                status=PreviewRequestStatus.FAILED.value,
                completed_at=case(
                    (
                        and_(
                            col(PreviewRequestTable.started_at).is_not(None),
                            col(PreviewRequestTable.started_at) > cutoff,
                        ),
                        col(PreviewRequestTable.started_at),
                    ),
                    else_=cutoff,
                ),
                diagnostic_code=diagnostic.code,
                diagnostic_message=diagnostic.message,
                diagnostic_retryable=diagnostic.retryable,
                diagnostic_source_correlation_ids_json=_canonical_json(
                    capped_correlations(diagnostic.source_correlation_ids)
                ),
                worker_id=None,
                lease_expires_at=None,
            )
        )
        failed_count = int(getattr(result, "rowcount", 0))
        protected_count = int(
            self._session.execute(
                select(func.count())
                .select_from(PreviewRequestTable)
                .where(
                    col(PreviewRequestTable.ecosystem) == ecosystem,
                    col(PreviewRequestTable.tenant_id) == tenant_id,
                    col(PreviewRequestTable.status).in_(
                        [PreviewRequestStatus.QUEUED.value, PreviewRequestStatus.RUNNING.value]
                    ),
                    col(PreviewRequestTable.created_at) < cutoff,
                    col(PreviewRequestTable.worker_id).is_not(None),
                    col(PreviewRequestTable.lease_expires_at).is_not(None),
                    col(PreviewRequestTable.lease_expires_at) > stale,
                )
            ).scalar_one()
        )
        return PreviewInterruptionRecoveryResult(
            failed_count=failed_count,
            protected_count=protected_count,
        )

    def fail_stale_foreign_leases(
        self,
        *,
        ecosystem: str,
        tenant_id: str,
        current_worker_id: str,
        lease_stale_at: datetime,
        limit: int,
        diagnostic: PreviewDiagnostic,
    ) -> PreviewStaleLeaseRecoveryResult:
        _require_safe_identifier(current_worker_id, "current_worker_id")
        if limit < 1 or limit > 100:
            raise ValueError("stale lease recovery limit must be between 1 and 100")
        stale = ensure_utc_strict(lease_stale_at)
        due = self._session.execute(
            select(PreviewRequestTable.request_id)
            .where(
                col(PreviewRequestTable.ecosystem) == ecosystem,
                col(PreviewRequestTable.tenant_id) == tenant_id,
                col(PreviewRequestTable.status).in_(
                    [PreviewRequestStatus.QUEUED.value, PreviewRequestStatus.RUNNING.value]
                ),
                col(PreviewRequestTable.worker_id).is_not(None),
                col(PreviewRequestTable.worker_id) != current_worker_id,
                col(PreviewRequestTable.lease_expires_at).is_not(None),
                col(PreviewRequestTable.lease_expires_at) <= stale,
            )
            .order_by(col(PreviewRequestTable.lease_expires_at), col(PreviewRequestTable.request_id))
            .limit(limit + 1)
        ).all()
        request_ids = tuple(request_id for (request_id,) in due[:limit])
        if not request_ids:
            return PreviewStaleLeaseRecoveryResult(failed_count=0, has_more=False)
        result = self._session.execute(
            update(PreviewRequestTable)
            .where(
                col(PreviewRequestTable.request_id).in_(request_ids),
                col(PreviewRequestTable.ecosystem) == ecosystem,
                col(PreviewRequestTable.tenant_id) == tenant_id,
                col(PreviewRequestTable.status).in_(
                    [PreviewRequestStatus.QUEUED.value, PreviewRequestStatus.RUNNING.value]
                ),
                col(PreviewRequestTable.worker_id).is_not(None),
                col(PreviewRequestTable.worker_id) != current_worker_id,
                col(PreviewRequestTable.lease_expires_at).is_not(None),
                col(PreviewRequestTable.lease_expires_at) <= stale,
            )
            .values(
                status=PreviewRequestStatus.FAILED.value,
                completed_at=case(
                    (
                        and_(
                            col(PreviewRequestTable.started_at).is_not(None),
                            col(PreviewRequestTable.started_at) > stale,
                        ),
                        col(PreviewRequestTable.started_at),
                    ),
                    (col(PreviewRequestTable.created_at) > stale, col(PreviewRequestTable.created_at)),
                    else_=stale,
                ),
                diagnostic_code=diagnostic.code,
                diagnostic_message=diagnostic.message,
                diagnostic_retryable=diagnostic.retryable,
                diagnostic_source_correlation_ids_json=_canonical_json(
                    capped_correlations(diagnostic.source_correlation_ids)
                ),
                worker_id=None,
                lease_expires_at=None,
            )
        )
        return PreviewStaleLeaseRecoveryResult(
            failed_count=int(getattr(result, "rowcount", 0)),
            has_more=len(due) > limit,
        )

    def mark_failed(
        self,
        request_id: str,
        completed_at: datetime,
        diagnostic: PreviewDiagnostic,
        *,
        worker_id: str | None = None,
    ) -> bool:
        if worker_id is not None:
            _require_safe_identifier(worker_id, "worker_id")
        current = self._current(request_id)
        if current is None or current.status not in {PreviewRequestStatus.QUEUED, PreviewRequestStatus.RUNNING}:
            return False
        candidate = replace(
            current,
            status=PreviewRequestStatus.FAILED,
            completed_at=completed_at,
            diagnostic=diagnostic,
        )
        validate_preview_request_snapshot(
            request=candidate,
            snapshot=candidate.source_snapshot,
            resulting_status=candidate.status,
            mode="strict_materialized",
        )
        assert candidate.diagnostic is not None
        statement = update(PreviewRequestTable).where(
            col(PreviewRequestTable.request_id) == request_id,
            col(PreviewRequestTable.status).in_(
                [PreviewRequestStatus.QUEUED.value, PreviewRequestStatus.RUNNING.value]
            ),
        )
        if worker_id is not None:
            statement = statement.where(col(PreviewRequestTable.worker_id) == worker_id)
        result = self._session.execute(
            statement.values(
                status=PreviewRequestStatus.FAILED.value,
                completed_at=ensure_utc_strict(candidate.completed_at),
                diagnostic_code=candidate.diagnostic.code,
                diagnostic_message=candidate.diagnostic.message,
                diagnostic_retryable=candidate.diagnostic.retryable,
                diagnostic_source_correlation_ids_json=_canonical_json(
                    capped_correlations(candidate.diagnostic.source_correlation_ids)
                ),
                worker_id=None,
                lease_expires_at=None,
            )
        )
        return getattr(result, "rowcount", 0) == 1


class PreviewRevisionConflictError(RuntimeError):
    """A concurrent publisher changed the current revision."""


@runtime_checkable
class PreviewRevisionRepository(Protocol):
    def get_current_for_owner(
        self,
        *,
        ecosystem: str,
        tenant_id: str,
        month_start: date,
    ) -> PreviewRevision | None: ...

    def replace_current(
        self,
        *,
        candidate: PreviewRevisionCandidate,
        package: PreviewStoredPackage,
        expected_current_revision_id: str | None,
    ) -> PreviewRevision: ...


def _revision_snapshot_dict(snapshot: PreviewSourceSnapshot) -> dict[str, object]:
    return {
        "calculation_timestamp": (
            None if snapshot.calculation_timestamp is None else snapshot.calculation_timestamp.isoformat()
        ),
        "calculation_coverage": [
            {
                "tracking_date": entry.tracking_date.isoformat(),
                "calculation_id": entry.calculation_id,
                "calculation_completed_at": entry.calculation_completed_at.isoformat(),
                "calculation_run_id": entry.calculation_run_id,
            }
            for entry in snapshot.calculation_coverage
        ],
        "source_through": None if snapshot.source_through is None else snapshot.source_through.isoformat(),
        "effective_coverage_start_date": snapshot.effective_coverage_start_date.isoformat(),
        "effective_coverage_end_date": snapshot.effective_coverage_end_date.isoformat(),
        "availability_cutoff_end_date": (
            None if snapshot.availability_cutoff_end_date is None else snapshot.availability_cutoff_end_date.isoformat()
        ),
        "monthly_status": snapshot.monthly_status,
    }


def _revision_snapshot_from_json(body: str) -> PreviewSourceSnapshot:
    raw = json.loads(body)
    if not isinstance(raw, dict):
        raise ValueError("persisted revision source snapshot must be an object")
    coverage_raw = raw.get("calculation_coverage")
    if not isinstance(coverage_raw, list):
        raise ValueError("persisted revision calculation coverage must be a list")
    coverage = tuple(
        PreviewCalculationCoverageEntry(
            tracking_date=date.fromisoformat(item["tracking_date"]),
            calculation_id=item["calculation_id"],
            calculation_completed_at=datetime.fromisoformat(item["calculation_completed_at"]),
            calculation_run_id=item.get("calculation_run_id"),
        )
        for item in coverage_raw
    )
    cutoff = raw.get("availability_cutoff_end_date")
    status = _supported_monthly_status(raw.get("monthly_status"))
    return PreviewSourceSnapshot(
        calculation_timestamp=(
            None if raw.get("calculation_timestamp") is None else datetime.fromisoformat(raw["calculation_timestamp"])
        ),
        calculation_coverage=coverage,
        source_through=(None if raw.get("source_through") is None else datetime.fromisoformat(raw["source_through"])),
        effective_coverage_start_date=date.fromisoformat(raw["effective_coverage_start_date"]),
        effective_coverage_end_date=date.fromisoformat(raw["effective_coverage_end_date"]),
        availability_cutoff_end_date=None if cutoff is None else date.fromisoformat(cutoff),
        monthly_status=status,
    )


def _revision_to_table(
    candidate: PreviewRevisionCandidate,
    package: PreviewStoredPackage,
) -> PreviewRevisionTable:
    return PreviewRevisionTable(
        revision_id=candidate.revision_id,
        tenant_name_at_publication=candidate.tenant_name_at_publication,
        ecosystem=candidate.ecosystem,
        tenant_id=candidate.tenant_id,
        month_start=candidate.start_date,
        month_end=candidate.end_date,
        monthly_status=candidate.monthly_status,
        material_sha256=candidate.material_sha256,
        source_snapshot_json=_canonical_json(_revision_snapshot_dict(candidate.source_snapshot)),
        published_at=ensure_utc_strict(candidate.published_at),
        supersedes_revision_id=candidate.supersedes_revision_id,
        superseded_by_revision_id=None,
        is_current=True,
        storage_key=package.storage_key,
        manifest_metadata_json=_canonical_json(_artifact_dict(package.manifest)),
        file_metadata_json=_canonical_json([_artifact_dict(item) for item in package.files]),
    )


def _revision_to_domain(row: PreviewRevisionTable) -> PreviewRevision:
    snapshot = _revision_snapshot_from_json(row.source_snapshot_json)
    status = _supported_monthly_status(row.monthly_status)
    if status is None:
        raise ValueError("persisted revision monthly status is required")
    validate_preview_revision_invariant(
        month=f"{row.month_start.year:04d}-{row.month_start.month:02d}",
        start_date=row.month_start,
        end_date=row.month_end,
        monthly_status=status,
        source_snapshot=snapshot,
    )
    manifest_raw = json.loads(row.manifest_metadata_json)
    files_raw = json.loads(row.file_metadata_json)
    if not isinstance(manifest_raw, dict) or not isinstance(files_raw, list):
        raise ValueError("persisted revision artifact metadata is invalid")
    return PreviewRevision(
        revision_id=row.revision_id,
        tenant_name_at_publication=row.tenant_name_at_publication,
        ecosystem=row.ecosystem,
        tenant_id=row.tenant_id,
        month=f"{row.month_start.year:04d}-{row.month_start.month:02d}",
        start_date=row.month_start,
        end_date=row.month_end,
        monthly_status=status,
        material_sha256=row.material_sha256,
        source_snapshot=snapshot,
        published_at=ensure_utc(row.published_at),
        supersedes_revision_id=row.supersedes_revision_id,
        superseded_by_revision_id=row.superseded_by_revision_id,
        is_current=row.is_current,
        package=PreviewStoredPackage(
            storage_key=row.storage_key,
            manifest=_artifact_from_dict(manifest_raw),
            files=tuple(_artifact_from_dict(item) for item in files_raw),
        ),
    )


def _is_revision_conflict(exc: IntegrityError | OperationalError) -> bool:
    original = exc.orig
    if isinstance(exc, IntegrityError) and isinstance(original, sqlite3.IntegrityError):
        code = getattr(original, "sqlite_errorcode", None)
        return code in {sqlite3.SQLITE_CONSTRAINT_UNIQUE, sqlite3.SQLITE_CONSTRAINT_PRIMARYKEY}
    if isinstance(exc, OperationalError) and isinstance(original, sqlite3.OperationalError):
        code = getattr(original, "sqlite_errorcode", None)
        return isinstance(code, int) and code & 0xFF in {sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED}
    if isinstance(exc, IntegrityError):
        diag = getattr(original, "diag", None)
        return getattr(original, "pgcode", None) == "23505" and getattr(diag, "constraint_name", None) in {
            "ux_preview_revisions_owner_month_current",
            "preview_revisions_pkey",
        }
    return False


class SQLModelPreviewRevisionRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def get_current_for_owner(
        self,
        *,
        ecosystem: str,
        tenant_id: str,
        month_start: date,
    ) -> PreviewRevision | None:
        is_current = col(PreviewRevisionTable.is_current)
        current_predicate = (
            is_current.is_(True) if self._session.get_bind().dialect.name == "postgresql" else is_current == true()
        )
        row = self._session.exec(
            select(PreviewRevisionTable).where(
                col(PreviewRevisionTable.ecosystem) == ecosystem,
                col(PreviewRevisionTable.tenant_id) == tenant_id,
                col(PreviewRevisionTable.month_start) == month_start,
                current_predicate,
            )
        ).first()
        return None if row is None else _revision_to_domain(row)

    def replace_current(
        self,
        *,
        candidate: PreviewRevisionCandidate,
        package: PreviewStoredPackage,
        expected_current_revision_id: str | None,
    ) -> PreviewRevision:
        if candidate.supersedes_revision_id != expected_current_revision_id:
            raise ValueError("candidate supersedes identity does not match expected current revision")
        candidate_row = _revision_to_table(candidate, package)
        try:
            with self._session.no_autoflush:
                if expected_current_revision_id is not None:
                    result = self._session.execute(
                        update(PreviewRevisionTable)
                        .where(
                            col(PreviewRevisionTable.ecosystem) == candidate.ecosystem,
                            col(PreviewRevisionTable.tenant_id) == candidate.tenant_id,
                            col(PreviewRevisionTable.month_start) == candidate.start_date,
                            col(PreviewRevisionTable.revision_id) == expected_current_revision_id,
                            col(PreviewRevisionTable.is_current) == true(),
                            col(PreviewRevisionTable.superseded_by_revision_id).is_(None),
                        )
                        .values(is_current=False, superseded_by_revision_id=candidate.revision_id)
                    )
                    if getattr(result, "rowcount", 0) != 1:
                        raise PreviewRevisionConflictError("current preview revision changed")
                self._session.add(candidate_row)
                self._session.flush([candidate_row])
        except (IntegrityError, OperationalError) as exc:
            if _is_revision_conflict(exc):
                raise PreviewRevisionConflictError("current preview revision changed") from exc
            raise
        return PreviewRevision(
            **candidate.__dict__,
            superseded_by_revision_id=None,
            is_current=True,
            package=package,
        )


@runtime_checkable
class PreviewWriteUnitOfWork(Protocol):
    requests: PreviewRequestRepository
    revisions: PreviewRevisionRepository

    def __enter__(self) -> Self: ...
    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: object,
    ) -> None: ...
    def commit(self) -> None: ...
    def rollback(self) -> None: ...


@runtime_checkable
class PreviewReadUnitOfWork(Protocol):
    requests: PreviewRequestRepository
    revisions: PreviewRevisionRepository
    calculations: PreviewCalculationRepository
    cost_evidence: PreviewCostEvidenceReader
    allocation_evidence: PreviewAllocationEvidenceReader
    resources: ResourceRepository
    identities: IdentityRepository
    tags: EntityTagRepository

    def __enter__(self) -> Self: ...
    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: object,
    ) -> None: ...


@runtime_checkable
class PreviewStorageBackend(Protocol):
    def create_preview_write_unit_of_work(self) -> PreviewWriteUnitOfWork: ...
    def create_preview_read_unit_of_work(self) -> PreviewReadUnitOfWork: ...
