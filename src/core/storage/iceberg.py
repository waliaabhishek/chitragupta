from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import TYPE_CHECKING, Literal, Protocol, runtime_checkable

from core.storage.interface import ReadOnlyUnitOfWork, UnitOfWork

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator

    from core.models.chargeback import ChargebackRow

type IcebergExportOutcome = Literal["unconfirmed", "succeeded", "failed"]


class IcebergStorageUnsupportedError(ValueError):
    """The configured SQL connection cannot enforce export deadlines."""


class IcebergStorageDeadlineError(TimeoutError):
    """The time available to the export SQL operation expired."""


class IcebergSourceWidthError(ValueError):
    """A source row exceeds the Iceberg read budget."""


@dataclass(frozen=True)
class IcebergExportKey:
    table_uuid: str
    ecosystem: str
    tenant_id: str
    date: date


@dataclass(frozen=True)
class IcebergExportCheckpoint:
    key: IcebergExportKey
    attempted_revision: str | None
    attempted_at: datetime | None
    outcome: IcebergExportOutcome | None
    error_code: str | None
    confirmed_revision: str | None
    source_completed_at: datetime | None
    snapshot_id: int | None
    confirmed_at: datetime | None
    row_count: int | None
    replay_requested: bool


class IcebergExportRepository(Protocol):
    """Track publication state within the caller's SQL transaction."""

    def get(self, key: IcebergExportKey) -> IcebergExportCheckpoint | None: ...

    def find_by_range(
        self, table_uuid: str, ecosystem: str, tenant_id: str, start: date, end: date
    ) -> list[IcebergExportCheckpoint]:
        """Return checkpoints in the half-open UTC day range [start, end)."""
        ...

    def record_attempt(self, key: IcebergExportKey, revision: str | None, attempted_at: datetime) -> None: ...

    def confirm_attempt(
        self,
        key: IcebergExportKey,
        revision: str | None,
        source_completed_at: datetime | None,
        snapshot_id: int,
        confirmed_at: datetime,
        row_count: int,
    ) -> None: ...

    def fail_attempt(self, key: IcebergExportKey, error_code: str, *, terminal: bool = False) -> None: ...

    def request_replay(self, table_uuid: str, ecosystem: str, tenant_id: str, dates: Iterable[date]) -> None: ...

    def delete_before(self, ecosystem: str, tenant_id: str, before: date) -> int: ...


@runtime_checkable
class IcebergExportUnitOfWork(Protocol):
    iceberg_exports: IcebergExportRepository


class IcebergExportReadUnitOfWork(ReadOnlyUnitOfWork, IcebergExportUnitOfWork, Protocol):
    def iter_iceberg_chargebacks(
        self,
        ecosystem: str,
        tenant_id: str,
        start: datetime,
        end: datetime,
        *,
        max_row_bytes: int,
        max_batch_bytes: int,
        batch_size: int = 256,
    ) -> Iterator[ChargebackRow]: ...


class IcebergExportWriteUnitOfWork(UnitOfWork, IcebergExportUnitOfWork, Protocol): ...


@runtime_checkable
class IcebergExportStorageBackend(Protocol):
    def create_iceberg_read_unit_of_work(self, *, deadline: float) -> IcebergExportReadUnitOfWork: ...

    def create_iceberg_write_unit_of_work(self, *, deadline: float) -> IcebergExportWriteUnitOfWork: ...
