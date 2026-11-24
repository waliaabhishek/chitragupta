from __future__ import annotations

from datetime import date, datetime
from typing import TYPE_CHECKING, Any, Protocol, cast, runtime_checkable

from sqlalchemy import BigInteger, Index, LargeBinary, delete, false, func, literal
from sqlalchemy import cast as sql_cast
from sqlmodel import Field, SQLModel, col, select

from core.storage.backends.sqlmodel.tables import ChargebackDimensionTable, ChargebackFactTable, EntityTagTable
from core.storage.backends.sqlmodel.timestamps import UTCSecondDateTime
from core.storage.iceberg import IcebergExportCheckpoint, IcebergExportKey, IcebergSourceWidthError

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Iterator

    from sqlalchemy.sql.elements import ColumnElement, SQLCoreOperations
    from sqlmodel import Session

    from core.models.chargeback import ChargebackRow
    from core.storage.iceberg import IcebergExportOutcome
    from core.storage.interface import ChargebackRepository, EntityTagRepository


@runtime_checkable
class _IcebergSourceWidthExtension(Protocol):
    def iceberg_source_extra_width(
        self, byte_length: Callable[[SQLCoreOperations[Any]], ColumnElement[int]]
    ) -> ColumnElement[int]: ...


def iter_iceberg_chargebacks(
    session: Session,
    chargebacks: ChargebackRepository,
    tags: EntityTagRepository,
    ecosystem: str,
    tenant_id: str,
    start: datetime,
    end: datetime,
    *,
    max_row_bytes: int,
    max_batch_bytes: int,
    batch_size: int,
) -> Iterator[ChargebackRow]:
    """Bound source payloads before the existing repository hydrates a batch."""
    dim, fact, tag = ChargebackDimensionTable, ChargebackFactTable, EntityTagTable
    sqlite = session.get_bind().dialect.name == "sqlite"

    def byte_length(column: SQLCoreOperations[Any]) -> ColumnElement[int]:
        if sqlite:
            return func.coalesce(func.length(sql_cast(column, LargeBinary)), 0)
        return func.coalesce(func.octet_length(column), 0)

    def text_width(*fields: str | None) -> ColumnElement[int]:
        return sum((byte_length(col(field)) for field in fields), literal(0))

    def tag_width(entity_type: str, entity_id: SQLCoreOperations[Any]) -> ColumnElement[int]:
        tag_payload = text_width(
            tag.tenant_id, tag.entity_type, tag.entity_id, tag.tag_key, tag.tag_value, tag.created_by
        )
        return (
            # Include the numeric identifier and timestamp as fixed-width values.
            select(func.coalesce(func.sum(tag_payload + 32), 0))
            .where(
                col(tag.tenant_id) == tenant_id,
                col(tag.entity_type) == entity_type,
                col(tag.entity_id) == entity_id,
            )
            .correlate(dim)
            .scalar_subquery()
        )

    # Include stored fields fetched by the ORM even when the mapper discards them, such as legacy tags_json.
    row_payload = text_width(
        dim.ecosystem,
        dim.tenant_id,
        dim.resource_id,
        dim.product_category,
        dim.product_type,
        dim.identity_id,
        dim.cost_type,
        dim.allocation_method,
        dim.allocation_detail,
        dim.env_id,
        fact.amount,
        fact.tags_json,
    )
    width = 64 + row_payload + tag_width("resource", col(dim.resource_id))
    width += tag_width("identity", func.nullif(col(dim.identity_id), ""))
    if isinstance(chargebacks, _IcebergSourceWidthExtension):
        width += chargebacks.iceberg_source_extra_width(byte_length)
    stmt = (
        select(func.max(width))
        .select_from(dim)
        .join(fact, col(fact.dimension_id) == col(dim.dimension_id))
        .where(
            col(dim.ecosystem) == ecosystem,
            col(dim.tenant_id) == tenant_id,
            col(fact.timestamp) >= start,
            col(fact.timestamp) < end,
        )
    )
    # Both queries run in the caller's consistent read transaction. Counting shared tags once per
    # fact is conservative and lets the existing iterator use one bounded batch size for the day.
    widest = session.exec(stmt).one()
    if widest is None:
        return
    if widest > min(max_row_bytes, max_batch_bytes):
        raise IcebergSourceWidthError("Chargeback source row exceeds the Iceberg read budget")
    yield from chargebacks.iter_by_filters(
        ecosystem, tenant_id, start, end, batch_size=min(batch_size, max_batch_bytes // widest), tags_repo=tags
    )


class IcebergExportCheckpointTable(SQLModel, table=True):
    __tablename__ = "iceberg_export_checkpoints"
    __table_args__ = (Index("ix_iceberg_export_checkpoints_retention", "ecosystem", "tenant_id", "tracking_date"),)

    table_uuid: str = Field(primary_key=True)
    ecosystem: str = Field(primary_key=True)
    tenant_id: str = Field(primary_key=True)
    tracking_date: date = Field(primary_key=True)
    attempted_revision: str | None = None
    attempted_at: datetime | None = Field(default=None, sa_type=UTCSecondDateTime)
    outcome: str | None = None
    error_code: str | None = None
    confirmed_revision: str | None = None
    source_completed_at: datetime | None = Field(default=None, sa_type=UTCSecondDateTime)
    snapshot_id: int | None = Field(default=None, sa_type=BigInteger)
    confirmed_at: datetime | None = Field(default=None, sa_type=UTCSecondDateTime)
    row_count: int | None = None
    replay_requested: bool = Field(default=False, sa_column_kwargs={"server_default": false()})

    def to_checkpoint(self) -> IcebergExportCheckpoint:
        return IcebergExportCheckpoint(
            key=IcebergExportKey(self.table_uuid, self.ecosystem, self.tenant_id, self.tracking_date),
            attempted_revision=self.attempted_revision,
            attempted_at=self.attempted_at,
            outcome=cast("IcebergExportOutcome | None", self.outcome),
            error_code=self.error_code,
            confirmed_revision=self.confirmed_revision,
            source_completed_at=self.source_completed_at,
            snapshot_id=self.snapshot_id,
            confirmed_at=self.confirmed_at,
            row_count=self.row_count,
            replay_requested=self.replay_requested,
        )


class SQLModelIcebergExportRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def get(self, key: IcebergExportKey) -> IcebergExportCheckpoint | None:
        row = self._get(key)
        return row.to_checkpoint() if row is not None else None

    def find_by_range(
        self, table_uuid: str, ecosystem: str, tenant_id: str, start: date, end: date
    ) -> list[IcebergExportCheckpoint]:
        stmt = (
            select(IcebergExportCheckpointTable)
            .where(
                col(IcebergExportCheckpointTable.table_uuid) == table_uuid,
                col(IcebergExportCheckpointTable.ecosystem) == ecosystem,
                col(IcebergExportCheckpointTable.tenant_id) == tenant_id,
                col(IcebergExportCheckpointTable.tracking_date) >= start,
                col(IcebergExportCheckpointTable.tracking_date) < end,
            )
            .order_by(col(IcebergExportCheckpointTable.tracking_date))
        )
        return [row.to_checkpoint() for row in self._session.exec(stmt)]

    def record_attempt(self, key: IcebergExportKey, revision: str | None, attempted_at: datetime) -> None:
        row = self._get_or_create(key)
        row.attempted_revision = revision
        row.attempted_at = attempted_at
        row.outcome = "unconfirmed"
        row.error_code = None

    def confirm_attempt(
        self,
        key: IcebergExportKey,
        revision: str | None,
        source_completed_at: datetime | None,
        snapshot_id: int,
        confirmed_at: datetime,
        row_count: int,
    ) -> None:
        row = self._unconfirmed_attempt(key)
        if row.attempted_revision != revision:
            raise ValueError("Iceberg publication revision differs from the attempted revision")
        row.confirmed_revision = revision
        row.source_completed_at = source_completed_at
        row.snapshot_id = snapshot_id
        row.confirmed_at = confirmed_at
        row.row_count = row_count
        row.outcome = "succeeded"
        row.replay_requested = False

    def fail_attempt(self, key: IcebergExportKey, error_code: str, *, terminal: bool = False) -> None:
        row = self._unconfirmed_attempt(key)
        row.outcome = "failed"
        row.error_code = error_code
        if terminal:
            row.replay_requested = False

    def request_replay(self, table_uuid: str, ecosystem: str, tenant_id: str, dates: Iterable[date]) -> None:
        for day in dates:
            row = self._get_or_create(IcebergExportKey(table_uuid, ecosystem, tenant_id, day))
            row.replay_requested = True

    def delete_before(self, ecosystem: str, tenant_id: str, before: date) -> int:
        stmt = delete(IcebergExportCheckpointTable).where(
            col(IcebergExportCheckpointTable.ecosystem) == ecosystem,
            col(IcebergExportCheckpointTable.tenant_id) == tenant_id,
            col(IcebergExportCheckpointTable.tracking_date) < before,
        )
        return self._session.exec(stmt).rowcount

    def _get(self, key: IcebergExportKey) -> IcebergExportCheckpointTable | None:
        return self._session.get(IcebergExportCheckpointTable, (key.table_uuid, key.ecosystem, key.tenant_id, key.date))

    def _get_or_create(self, key: IcebergExportKey) -> IcebergExportCheckpointTable:
        row = self._get(key)
        if row is None:
            row = IcebergExportCheckpointTable(
                table_uuid=key.table_uuid,
                ecosystem=key.ecosystem,
                tenant_id=key.tenant_id,
                tracking_date=key.date,
            )
            self._session.add(row)
        return row

    def _unconfirmed_attempt(self, key: IcebergExportKey) -> IcebergExportCheckpointTable:
        row = self._get(key)
        if row is None or row.outcome != "unconfirmed":
            raise ValueError("Iceberg publication has no unconfirmed attempt")
        return row
