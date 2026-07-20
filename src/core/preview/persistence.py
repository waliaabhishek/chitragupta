from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass, replace
from datetime import date, datetime, timedelta
from typing import Literal, Protocol, Self, runtime_checkable

from sqlalchemy import Column, Date, DateTime, Index, String, Text, cast, update
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
    PreviewSourceSnapshot,
    PreviewStoredPackage,
    preview_request_status,
    validate_preview_request_snapshot,
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


@runtime_checkable
class PreviewCalculationRepository(Protocol):
    def find_current_coverage(
        self, *, ecosystem: str, tenant_id: str, start_date: date, end_date: date
    ) -> PreviewCalculationCoverageResult: ...


@runtime_checkable
class PreviewRequestRepository(Protocol):
    def create_queued(self, request: PreviewRequest) -> PreviewRequest: ...

    def get_for_owner(self, request_id: str, ecosystem: str, tenant_id: str) -> PreviewRequest | None: ...

    def mark_running(self, request_id: str, started_at: datetime) -> PreviewRequest | None: ...

    def mark_ready(
        self,
        request_id: str,
        completed_at: datetime,
        source_snapshot: PreviewSourceSnapshot,
        stored_package: PreviewStoredPackage,
    ) -> bool: ...

    def mark_failed(self, request_id: str, completed_at: datetime, diagnostic: PreviewDiagnostic) -> bool: ...


class PreviewRequestTable(SQLModel, table=True):
    __tablename__ = "preview_requests"
    __table_args__ = (
        Index("ix_preview_requests_owner_created", "ecosystem", "tenant_id", "created_at"),
        Index("ix_preview_requests_owner_status", "ecosystem", "tenant_id", "status"),
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
        package = PreviewPackageMetadata(
            manifest=_artifact_from_dict(json.loads(row.manifest_metadata_json)),
            files=tuple(_artifact_from_dict(item) for item in json.loads(row.data_files_json)),
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

    def create_queued(self, request: PreviewRequest) -> PreviewRequest:
        if request.status is not PreviewRequestStatus.QUEUED:
            raise ValueError("new preview request must be queued")
        self._session.add(request_to_table(request))
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

    def _current(self, request_id: str) -> PreviewRequest | None:
        row = self._session.get(PreviewRequestTable, request_id)
        return None if row is None else request_to_domain(row)

    def mark_running(self, request_id: str, started_at: datetime) -> PreviewRequest | None:
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
        result = self._session.execute(
            update(PreviewRequestTable)
            .where(
                col(PreviewRequestTable.request_id) == request_id,
                col(PreviewRequestTable.status) == PreviewRequestStatus.QUEUED.value,
            )
            .values(status=candidate.status.value, started_at=ensure_utc_strict(candidate.started_at))
        )
        return candidate if getattr(result, "rowcount", 0) == 1 else None

    def mark_ready(
        self,
        request_id: str,
        completed_at: datetime,
        source_snapshot: PreviewSourceSnapshot,
        stored_package: PreviewStoredPackage,
    ) -> bool:
        current = self._current(request_id)
        if current is None or current.status is not PreviewRequestStatus.RUNNING:
            return False
        package = PreviewPackageMetadata(stored_package.manifest, stored_package.files)
        candidate = replace(
            current,
            status=PreviewRequestStatus.READY,
            completed_at=completed_at,
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
        result = self._session.execute(
            update(PreviewRequestTable)
            .where(
                col(PreviewRequestTable.request_id) == request_id,
                col(PreviewRequestTable.status) == PreviewRequestStatus.RUNNING.value,
            )
            .values(
                status=PreviewRequestStatus.READY.value,
                completed_at=ensure_utc_strict(candidate.completed_at),
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
            )
        )
        return getattr(result, "rowcount", 0) == 1

    def mark_failed(self, request_id: str, completed_at: datetime, diagnostic: PreviewDiagnostic) -> bool:
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
        result = self._session.execute(
            update(PreviewRequestTable)
            .where(
                col(PreviewRequestTable.request_id) == request_id,
                col(PreviewRequestTable.status).in_(
                    [PreviewRequestStatus.QUEUED.value, PreviewRequestStatus.RUNNING.value]
                ),
            )
            .values(
                status=PreviewRequestStatus.FAILED.value,
                completed_at=ensure_utc_strict(candidate.completed_at),
                diagnostic_code=candidate.diagnostic.code,
                diagnostic_message=candidate.diagnostic.message,
                diagnostic_retryable=candidate.diagnostic.retryable,
                diagnostic_source_correlation_ids_json=_canonical_json(
                    capped_correlations(candidate.diagnostic.source_correlation_ids)
                ),
            )
        )
        return getattr(result, "rowcount", 0) == 1


@runtime_checkable
class PreviewWriteUnitOfWork(Protocol):
    requests: PreviewRequestRepository

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
