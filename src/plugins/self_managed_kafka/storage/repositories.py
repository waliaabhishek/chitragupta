from __future__ import annotations

import logging
from collections.abc import Iterator, Sequence
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from typing import TYPE_CHECKING, Any

from sqlalchemy import delete, tuple_
from sqlmodel import col, select

from core.storage.backends.sqlmodel.repositories import SQLModelChargebackRepository
from core.storage.backends.sqlmodel.tables import ChargebackDimensionTable, ChargebackFactTable
from core.storage.backends.sqlmodel.time_bounds import exact_utc_half_open_bounds
from core.storage.backends.sqlmodel.timestamps import canonical_utc_second, exclusive_utc_second_upper_bound

if TYPE_CHECKING:
    from sqlmodel import Session

    from core.models.chargeback import ChargebackRow
    from core.storage.interface import EntityTagRepository

from core.storage.backends.sqlmodel.repositories import (
    SQLModelBillingRepository as SMKBillingRepository,
)
from core.storage.backends.sqlmodel.repositories import (
    SQLModelIdentityRepository as SMKIdentityRepository,
)
from core.storage.backends.sqlmodel.repositories import (
    SQLModelResourceRepository as SMKResourceRepository,
)
from plugins.self_managed_kafka.storage.tables import (
    SelfManagedKafkaPrincipalTeamSnapshotTable,
    SelfManagedKafkaScopeStateTable,
)

logger = logging.getLogger(__name__)


class SelfManagedKafkaChargebackRepository(SQLModelChargebackRepository):
    """Core chargeback persistence plus typed historical team snapshots."""

    _SNAPSHOT_CHUNK_SIZE = 400

    def _snapshot_keys_for_rows(self, rows: Sequence[ChargebackRow]) -> list[tuple[datetime, int]]:
        return [(canonical_utc_second(row.timestamp), row.dimension_id) for row in rows if row.dimension_id is not None]

    def _snapshot_for_keys(self, keys: Sequence[tuple[datetime, int]]) -> dict[tuple[datetime, int], str]:
        result: dict[tuple[datetime, int], str] = {}
        for start in range(0, len(keys), self._SNAPSHOT_CHUNK_SIZE):
            chunk = keys[start : start + self._SNAPSHOT_CHUNK_SIZE]
            if not chunk:
                continue
            statement = select(SelfManagedKafkaPrincipalTeamSnapshotTable).where(
                tuple_(
                    col(SelfManagedKafkaPrincipalTeamSnapshotTable.timestamp),
                    col(SelfManagedKafkaPrincipalTeamSnapshotTable.dimension_id),
                ).in_(chunk)
            )
            for snapshot in self._session.exec(statement).all():
                result[(snapshot.timestamp, snapshot.dimension_id)] = snapshot.team
        return result

    def _hydrate(self, rows: list[ChargebackRow]) -> list[ChargebackRow]:
        keys = self._snapshot_keys_for_rows(rows)
        snapshots = self._snapshot_for_keys(keys)
        for row in rows:
            if row.dimension_id is None:
                continue
            team = snapshots.get((canonical_utc_second(row.timestamp), row.dimension_id))
            if team is not None:
                row.metadata["team"] = team
        return rows

    def _delete_snapshot_keys(self, keys: Sequence[tuple[datetime, int]]) -> None:
        for start in range(0, len(keys), self._SNAPSHOT_CHUNK_SIZE):
            chunk = keys[start : start + self._SNAPSHOT_CHUNK_SIZE]
            if chunk:
                self._session.execute(
                    delete(SelfManagedKafkaPrincipalTeamSnapshotTable).where(
                        tuple_(
                            col(SelfManagedKafkaPrincipalTeamSnapshotTable.timestamp),
                            col(SelfManagedKafkaPrincipalTeamSnapshotTable.dimension_id),
                        ).in_(chunk)
                    )
                )

    def _delete_snapshots_for_period(
        self,
        ecosystem: str,
        tenant_id: str,
        *,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> None:
        dimension_ids = select(ChargebackDimensionTable.dimension_id).where(
            col(ChargebackDimensionTable.ecosystem) == ecosystem,
            col(ChargebackDimensionTable.tenant_id) == tenant_id,
        )
        predicates: list[Any] = [
            col(SelfManagedKafkaPrincipalTeamSnapshotTable.dimension_id).in_(dimension_ids),
        ]
        if start is not None:
            predicates.append(col(SelfManagedKafkaPrincipalTeamSnapshotTable.timestamp) >= start)
        if end is not None:
            predicates.append(col(SelfManagedKafkaPrincipalTeamSnapshotTable.timestamp) < end)
        self._session.execute(delete(SelfManagedKafkaPrincipalTeamSnapshotTable).where(*predicates))

    def _delete_fact_keys(self, keys: Sequence[tuple[datetime, int]]) -> None:
        for start in range(0, len(keys), self._SNAPSHOT_CHUNK_SIZE):
            chunk = keys[start : start + self._SNAPSHOT_CHUNK_SIZE]
            if chunk:
                self._session.execute(
                    delete(ChargebackFactTable).where(
                        tuple_(
                            col(ChargebackFactTable.timestamp),
                            col(ChargebackFactTable.dimension_id),
                        ).in_(chunk)
                    )
                )

    def _store_snapshot(self, key: tuple[datetime, int], team: object) -> None:
        self._delete_snapshot_keys([key])
        if team is not None:
            if not isinstance(team, str):
                raise TypeError("self-managed Kafka team snapshot must be a string")
            self._session.add(
                SelfManagedKafkaPrincipalTeamSnapshotTable(
                    timestamp=key[0],
                    dimension_id=key[1],
                    team=team,
                )
            )

    def upsert(self, row: ChargebackRow) -> ChargebackRow:
        team = row.metadata.get("team")
        core_row = replace(row, metadata={"team": team} if team is not None else {})
        stored = super().upsert(core_row)
        self._session.flush()
        if stored.dimension_id is None:
            raise RuntimeError("chargeback fact dimension was not assigned")
        key = (canonical_utc_second(stored.timestamp), stored.dimension_id)
        self._store_snapshot(key, team)
        if team is not None:
            stored.metadata["team"] = team
        return stored

    def upsert_batch(self, rows: list[ChargebackRow]) -> int:
        core_rows = [
            replace(row, metadata={"team": row.metadata["team"]} if row.metadata.get("team") is not None else {})
            for row in rows
        ]
        if not core_rows:
            return 0
        teams: dict[tuple[datetime, int], object] = {}
        unique_rows: dict[tuple[datetime, int], ChargebackRow] = {}
        for row in core_rows:
            dimension = self._get_or_create_dimension(row)
            if dimension.dimension_id is None:
                raise RuntimeError("chargeback dimension was not assigned")
            key = (canonical_utc_second(row.timestamp), dimension.dimension_id)
            teams[key] = row.metadata.get("team")
            unique_rows[key] = row

        keys = list(teams)
        self._session.flush()
        self._delete_snapshot_keys(keys)
        # The core batch repository intentionally uses add_all() rather than merge().
        # Remove matching facts first so this plugin retains replacement semantics
        # while delegating the complete deduplicated batch through that one path.
        self._delete_fact_keys(keys)
        count = super().upsert_batch(list(unique_rows.values()))
        self._session.flush()
        self._session.add_all(
            SelfManagedKafkaPrincipalTeamSnapshotTable(
                timestamp=key[0],
                dimension_id=key[1],
                team=team,
            )
            for key, team in teams.items()
            if team is not None
        )
        return count

    def find_by_date(self, ecosystem: str, tenant_id: str, target_date: date) -> list[ChargebackRow]:
        return self._hydrate(super().find_by_date(ecosystem, tenant_id, target_date))

    def find_by_range(self, ecosystem: str, tenant_id: str, start: datetime, end: datetime) -> list[ChargebackRow]:
        return self._hydrate(super().find_by_range(ecosystem, tenant_id, start, end))

    def find_by_identity(self, ecosystem: str, tenant_id: str, identity_id: str) -> list[ChargebackRow]:
        return self._hydrate(super().find_by_identity(ecosystem, tenant_id, identity_id))

    def find_by_filters(
        self,
        ecosystem: str,
        tenant_id: str,
        start: datetime | None = None,
        end: datetime | None = None,
        identity_id: str | None = None,
        product_type: str | None = None,
        resource_id: str | None = None,
        cost_type: str | None = None,
        limit: int = 1000,
        offset: int = 0,
        tag_key: str | None = None,
        tag_value: str | None = None,
        tags_repo: EntityTagRepository | None = None,
    ) -> tuple[list[ChargebackRow], int]:
        rows, total = super().find_by_filters(
            ecosystem,
            tenant_id,
            start,
            end,
            identity_id,
            product_type,
            resource_id,
            cost_type,
            limit,
            offset,
            tag_key,
            tag_value,
            tags_repo,
        )
        return self._hydrate(rows), total

    def iter_by_filters(
        self,
        ecosystem: str,
        tenant_id: str,
        start: datetime | None = None,
        end: datetime | None = None,
        identity_id: str | None = None,
        product_type: str | None = None,
        resource_id: str | None = None,
        cost_type: str | None = None,
        batch_size: int = 5000,
        tag_key: str | None = None,
        tag_value: str | None = None,
        tags_repo: EntityTagRepository | None = None,
    ) -> Iterator[ChargebackRow]:
        batch: list[ChargebackRow] = []
        for row in super().iter_by_filters(
            ecosystem,
            tenant_id,
            start,
            end,
            identity_id,
            product_type,
            resource_id,
            cost_type,
            batch_size,
            tag_key,
            tag_value,
            tags_repo,
        ):
            batch.append(row)
            if len(batch) >= batch_size:
                yield from self._hydrate(batch)
                batch = []
        if batch:
            yield from self._hydrate(batch)

    def delete_by_date(self, ecosystem: str, tenant_id: str, target_date: date) -> int:
        start = datetime(target_date.year, target_date.month, target_date.day, tzinfo=UTC)
        end = start + timedelta(days=1)
        start, end = exact_utc_half_open_bounds(self._session, start, end)
        self._delete_snapshots_for_period(ecosystem, tenant_id, start=start, end=end)
        return super().delete_by_date(ecosystem, tenant_id, target_date)

    def delete_before(self, ecosystem: str, tenant_id: str, before: datetime) -> int:
        cutoff = exclusive_utc_second_upper_bound(before)
        self._delete_snapshots_for_period(ecosystem, tenant_id, end=cutoff)
        return super().delete_before(ecosystem, tenant_id, before)


class SelfManagedKafkaScopeStateRepository:
    """Repository for target-scope breaker state."""

    def __init__(self, session: Session) -> None:
        self._session = session

    def get(self, ecosystem: str, tenant_id: str, cluster_id: str) -> SelfManagedKafkaScopeStateTable | None:
        return self._session.get(SelfManagedKafkaScopeStateTable, (ecosystem, tenant_id, cluster_id))

    def open(
        self,
        *,
        ecosystem: str,
        tenant_id: str,
        cluster_id: str,
        metrics_identifier_label: str,
        metrics_identifier: str,
        window_start: datetime,
        window_end: datetime,
        reason: str,
        status: str,
        detail: str,
        opened_at: datetime,
    ) -> None:
        state = self.get(ecosystem, tenant_id, cluster_id)
        if state is None:
            state = SelfManagedKafkaScopeStateTable(
                ecosystem=ecosystem,
                tenant_id=tenant_id,
                cluster_id=cluster_id,
                metrics_identifier_label=metrics_identifier_label,
                metrics_identifier=metrics_identifier,
                status="open",
                opened_at=opened_at,
                first_blocked_window_start=window_start,
                first_blocked_window_end=window_end,
                last_failure_reason=reason,
                last_failure_status=status,
                last_failure_detail=detail,
            )
            self._session.add(state)
            return
        state.status = "open"
        state.metrics_identifier_label = metrics_identifier_label
        state.metrics_identifier = metrics_identifier
        state.opened_at = opened_at
        state.first_blocked_window_start = window_start
        state.first_blocked_window_end = window_end
        state.last_failure_reason = reason
        state.last_failure_status = status
        state.last_failure_detail = detail
        state.last_probe_at = None
        state.last_probe_status = None
        state.recovered_at = None
        state.recovery_cursor_date = None
        state.retention_gap_start = None
        state.retention_gap_end = None

    def record_probe(
        self,
        ecosystem: str,
        tenant_id: str,
        cluster_id: str,
        *,
        probed_at: datetime,
        status: str,
    ) -> None:
        state = self._require(ecosystem, tenant_id, cluster_id)
        state.last_probe_at = probed_at
        state.last_probe_status = status

    def mark_recovering(
        self,
        ecosystem: str,
        tenant_id: str,
        cluster_id: str,
        *,
        recovered_at: datetime,
        recovery_cursor_date: date,
    ) -> None:
        state = self._require(ecosystem, tenant_id, cluster_id)
        state.status = "recovering"
        state.recovered_at = recovered_at
        state.recovery_cursor_date = recovery_cursor_date

    def mark_retention_gap(
        self,
        ecosystem: str,
        tenant_id: str,
        cluster_id: str,
        *,
        gap_start: datetime,
        gap_end: datetime,
    ) -> None:
        state = self._require(ecosystem, tenant_id, cluster_id)
        state.status = "retention_gap"
        state.retention_gap_start = gap_start
        state.retention_gap_end = gap_end

    def close(
        self,
        ecosystem: str,
        tenant_id: str,
        cluster_id: str,
        *,
        recovery_cursor_date: date | None = None,
    ) -> None:
        state = self._require(ecosystem, tenant_id, cluster_id)
        if recovery_cursor_date is not None:
            state.recovery_cursor_date = recovery_cursor_date
        state.status = "closed"

    def _require(self, ecosystem: str, tenant_id: str, cluster_id: str) -> SelfManagedKafkaScopeStateTable:
        state = self.get(ecosystem, tenant_id, cluster_id)
        if state is None:
            raise ValueError("self-managed Kafka scope state does not exist")
        return state


__all__ = [
    "SelfManagedKafkaChargebackRepository",
    "SMKBillingRepository",
    "SMKIdentityRepository",
    "SMKResourceRepository",
    "SelfManagedKafkaScopeStateRepository",
]
