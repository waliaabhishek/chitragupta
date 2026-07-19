from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
from typing import Literal, Protocol, Self, runtime_checkable

from sqlalchemy import Column, Date, DateTime, Index, String, cast, update
from sqlmodel import Field, Session, SQLModel, col, select

from core.preview.evidence import (  # noqa: TC001  # resolved by get_type_hints contract test
    PreviewAllocationEvidenceReader,
    PreviewCostEvidenceReader,
)
from core.preview.models import (
    PreviewArtifactMetadata,
    PreviewCalculationCoverageEntry,
    PreviewDiagnostic,
    PreviewPackageMetadata,
    PreviewRequest,
    PreviewRequestStatus,
    PreviewSourceSnapshot,
    PreviewStoredPackage,
)
from core.storage.backends.sqlmodel.mappers import ensure_utc, ensure_utc_strict
from core.storage.backends.sqlmodel.tables import PipelineStateTable
from core.storage.interface import (  # noqa: TC001  # resolved by get_type_hints contract test
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

    def mark_running(self, request_id: str, started_at: datetime) -> bool: ...

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
    storage_key: str | None = None
    manifest_metadata_json: str | None = None
    data_files_json: str | None = None


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
        storage_key=request.storage_key,
        manifest_metadata_json=_canonical_json(_artifact_dict(package.manifest)) if package else None,
        data_files_json=_canonical_json([_artifact_dict(item) for item in package.files]) if package else None,
    )


def _artifact_from_dict(value: dict[str, object]) -> PreviewArtifactMetadata:
    return PreviewArtifactMetadata(
        name=str(value["name"]),
        media_type=str(value["media_type"]),
        size_bytes=int(str(value["size_bytes"])),
        sha256=str(value["sha256"]),
        order=int(str(value["order"])) if value.get("order") is not None else None,
    )


def _supported_grain(value: str) -> Literal["daily"]:
    if value != "daily":
        raise ValueError(f"unsupported persisted preview grain: {value!r}")
    return "daily"


def _supported_column_profile(value: str) -> Literal["full"]:
    if value != "full":
        raise ValueError(f"unsupported persisted preview column profile: {value!r}")
    return "full"


def request_to_domain(row: PreviewRequestTable) -> PreviewRequest:
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
    if coverage:
        if row.calculation_timestamp is None or row.source_through is None:
            raise ValueError("preview source snapshot metadata is incomplete")
        snapshot = PreviewSourceSnapshot(
            calculation_timestamp=ensure_utc(row.calculation_timestamp),
            calculation_coverage=coverage,
            source_through=ensure_utc(row.source_through),
        )
    diagnostic = None
    if row.diagnostic_code is not None:
        if row.diagnostic_message is None or row.diagnostic_retryable is None:
            raise ValueError("preview diagnostic metadata is incomplete")
        diagnostic = PreviewDiagnostic(row.diagnostic_code, row.diagnostic_message, row.diagnostic_retryable)
    package = None
    if row.manifest_metadata_json is not None and row.data_files_json is not None:
        package = PreviewPackageMetadata(
            manifest=_artifact_from_dict(json.loads(row.manifest_metadata_json)),
            files=tuple(_artifact_from_dict(item) for item in json.loads(row.data_files_json)),
        )
    return PreviewRequest(
        request_id=row.request_id,
        tenant_name=row.tenant_name,
        ecosystem=row.ecosystem,
        tenant_id=row.tenant_id,
        grain=_supported_grain(row.grain),
        start_date=row.start_date,
        end_date=row.end_date,
        column_profile=_supported_column_profile(row.column_profile),
        status=PreviewRequestStatus(row.status),
        created_at=ensure_utc(row.created_at),
        started_at=ensure_utc(row.started_at),
        completed_at=ensure_utc(row.completed_at),
        source_snapshot=snapshot,
        diagnostic=diagnostic,
        storage_key=row.storage_key,
        package=package,
    )


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

    def mark_running(self, request_id: str, started_at: datetime) -> bool:
        result = self._session.execute(
            update(PreviewRequestTable)
            .where(
                col(PreviewRequestTable.request_id) == request_id,
                col(PreviewRequestTable.status) == PreviewRequestStatus.QUEUED.value,
            )
            .values(status=PreviewRequestStatus.RUNNING.value, started_at=ensure_utc_strict(started_at))
        )
        return getattr(result, "rowcount", 0) == 1

    def mark_ready(
        self,
        request_id: str,
        completed_at: datetime,
        source_snapshot: PreviewSourceSnapshot,
        stored_package: PreviewStoredPackage,
    ) -> bool:
        result = self._session.execute(
            update(PreviewRequestTable)
            .where(
                col(PreviewRequestTable.request_id) == request_id,
                col(PreviewRequestTable.status) == PreviewRequestStatus.RUNNING.value,
            )
            .values(
                status=PreviewRequestStatus.READY.value,
                completed_at=ensure_utc_strict(completed_at),
                calculation_timestamp=source_snapshot.calculation_timestamp,
                source_through=source_snapshot.source_through,
                calculation_coverage_json=_coverage_json(source_snapshot),
                storage_key=stored_package.storage_key,
                manifest_metadata_json=_canonical_json(_artifact_dict(stored_package.manifest)),
                data_files_json=_canonical_json([_artifact_dict(item) for item in stored_package.files]),
            )
        )
        return getattr(result, "rowcount", 0) == 1

    def mark_failed(self, request_id: str, completed_at: datetime, diagnostic: PreviewDiagnostic) -> bool:
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
                completed_at=ensure_utc_strict(completed_at),
                diagnostic_code=diagnostic.code,
                diagnostic_message=diagnostic.message,
                diagnostic_retryable=diagnostic.retryable,
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
