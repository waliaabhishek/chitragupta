"""Plugin-owned historical team snapshot repository behavior."""

from __future__ import annotations

from collections.abc import Generator
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text

from core.models.chargeback import ChargebackRow, CostType
from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend
from plugins.self_managed_kafka.storage.module import SelfManagedKafkaStorageModule

_ECOSYSTEM = "self_managed_kafka"
_TENANT_ID = "tenant-1"
_TIMESTAMP = datetime(2026, 8, 1, tzinfo=UTC)
_SNAPSHOT_TABLE = "self_managed_kafka_principal_team_snapshots"


@pytest.fixture
def backend(tmp_path: Path) -> Generator[SQLModelBackend]:
    result = SQLModelBackend(
        f"sqlite:///{tmp_path / 'team-snapshots.db'}",
        SelfManagedKafkaStorageModule(),
        use_migrations=False,
    )
    result.create_tables()
    try:
        yield result
    finally:
        result.dispose()


def _row(
    *,
    timestamp: datetime = _TIMESTAMP,
    tenant_id: str = _TENANT_ID,
    identity_id: str = "User:alice",
    amount: Decimal = Decimal("1.0000"),
    product_type: str = "SELF_KAFKA_NETWORK_INGRESS",
    metadata: dict[str, object] | None = None,
) -> ChargebackRow:
    return ChargebackRow(
        ecosystem=_ECOSYSTEM,
        tenant_id=tenant_id,
        timestamp=timestamp,
        resource_id="cluster-1",
        product_category="network",
        product_type=product_type,
        identity_id=identity_id,
        cost_type=CostType.USAGE,
        amount=amount,
        allocation_method="principal_quota_ready_v1",
        allocation_detail="usage_ratio_allocation",
        metadata=dict(metadata or {}),
    )


def _rows_for_date(
    backend: SQLModelBackend,
    target_date: date,
    *,
    tenant_id: str = _TENANT_ID,
) -> list[ChargebackRow]:
    with backend.create_read_only_unit_of_work() as uow:
        return uow.chargebacks.find_by_date(_ECOSYSTEM, tenant_id, target_date)


def _snapshot_count(backend: SQLModelBackend) -> int:
    engine = create_engine(backend._connection_string)  # noqa: SLF001
    try:
        with engine.connect() as connection:
            return connection.execute(text(f"SELECT COUNT(*) FROM {_SNAPSHOT_TABLE}")).scalar_one()
    finally:
        engine.dispose()


def test_measured_team_snapshot_round_trips_through_plugin_repository(backend: SQLModelBackend) -> None:
    measured = _row(metadata={"team": "team-data", "transient": "ignored"})
    unrelated = _row(identity_id="User:bob", metadata={"unrelated": "ignored"})

    with backend.create_unit_of_work() as uow:
        stored = uow.chargebacks.upsert(measured)
        uow.chargebacks.upsert(unrelated)
        uow.commit()

    found = {row.identity_id: row for row in _rows_for_date(backend, _TIMESTAMP.date())}
    with backend.create_read_only_unit_of_work() as uow:
        by_range = uow.chargebacks.find_by_range(
            _ECOSYSTEM,
            _TENANT_ID,
            _TIMESTAMP,
            _TIMESTAMP + timedelta(days=1),
        )
        by_identity = uow.chargebacks.find_by_identity(_ECOSYSTEM, _TENANT_ID, "User:alice")
        filtered, total = uow.chargebacks.find_by_filters(
            _ECOSYSTEM,
            _TENANT_ID,
            identity_id="User:alice",
        )
    assert stored.metadata == {"team": "team-data"}
    assert found["User:alice"].metadata == {"team": "team-data"}
    assert found["User:bob"].metadata == {}
    assert [row.metadata for row in by_range if row.identity_id == "User:alice"] == [{"team": "team-data"}]
    assert [row.metadata for row in by_identity] == [{"team": "team-data"}]
    assert total == 1
    assert [row.metadata for row in filtered] == [{"team": "team-data"}]


def test_batch_without_team_clears_stale_snapshot(backend: SQLModelBackend) -> None:
    with backend.create_unit_of_work() as uow:
        uow.chargebacks.upsert(_row(metadata={"team": "team-data"}))
        uow.commit()

    assert _rows_for_date(backend, _TIMESTAMP.date())[0].metadata == {"team": "team-data"}

    with backend.create_unit_of_work() as uow:
        assert uow.chargebacks.upsert_batch([_row(metadata={})]) == 1
        uow.commit()

    assert _rows_for_date(backend, _TIMESTAMP.date())[0].metadata == {}
    assert _snapshot_count(backend) == 0


def test_batch_last_row_wins_for_fact_and_team_snapshot(backend: SQLModelBackend) -> None:
    first = _row(amount=Decimal("1.0000"), metadata={"team": "team-first"})
    second = _row(amount=Decimal("2.0000"), metadata={"team": "team-second"})

    with backend.create_unit_of_work() as uow:
        assert uow.chargebacks.upsert_batch([first, second]) == 1
        uow.commit()

    [stored] = _rows_for_date(backend, _TIMESTAMP.date())
    assert stored.amount == Decimal("2.0000")
    assert stored.metadata == {"team": "team-second"}


def test_snapshot_date_and_retention_deletes_match_core_facts(backend: SQLModelBackend) -> None:
    first_day = _row(metadata={"team": "team-first"})
    second_day = _row(timestamp=_TIMESTAMP + timedelta(days=1), metadata={"team": "team-second"})

    with backend.create_unit_of_work() as uow:
        assert uow.chargebacks.upsert_batch([first_day, second_day]) == 2
        uow.commit()

    with backend.create_unit_of_work() as uow:
        assert uow.chargebacks.delete_by_date(_ECOSYSTEM, _TENANT_ID, _TIMESTAMP.date()) == 1
        uow.commit()

    assert _rows_for_date(backend, _TIMESTAMP.date()) == []
    assert _rows_for_date(backend, (_TIMESTAMP + timedelta(days=1)).date())[0].metadata == {"team": "team-second"}

    with backend.create_unit_of_work() as uow:
        assert uow.chargebacks.delete_before(_ECOSYSTEM, _TENANT_ID, _TIMESTAMP + timedelta(days=2)) == 1
        uow.commit()

    assert _rows_for_date(backend, (_TIMESTAMP + timedelta(days=1)).date()) == []
    assert _snapshot_count(backend) == 0


def test_date_delete_scopes_snapshot_cleanup_to_tenant(backend: SQLModelBackend) -> None:
    selected = _row(metadata={"team": "team-selected"})
    other_tenant = _row(tenant_id="tenant-2", metadata={"team": "team-other"})
    with backend.create_unit_of_work() as uow:
        uow.chargebacks.upsert_batch([selected, other_tenant])
        uow.commit()

    with backend.create_unit_of_work() as uow:
        assert uow.chargebacks.delete_by_date(_ECOSYSTEM, _TENANT_ID, _TIMESTAMP.date()) == 1
        uow.commit()

    assert _rows_for_date(backend, _TIMESTAMP.date()) == []
    assert _rows_for_date(backend, _TIMESTAMP.date(), tenant_id="tenant-2")[0].metadata == {"team": "team-other"}
    assert _snapshot_count(backend) == 1


def test_batch_replaces_existing_fact_and_snapshot_in_one_unit_of_work(backend: SQLModelBackend) -> None:
    with backend.create_unit_of_work() as uow:
        uow.chargebacks.upsert(_row(metadata={"team": "team-before"}))
        assert uow.chargebacks.upsert_batch([_row(amount=Decimal("2.0000"), metadata={})]) == 1
        uow.commit()

    [stored] = _rows_for_date(backend, _TIMESTAMP.date())
    assert stored.amount == Decimal("2.0000")
    assert stored.metadata == {}


def test_team_snapshot_changes_only_when_explicit_recalculation_replaces_date(backend: SQLModelBackend) -> None:
    first_day = _row(metadata={"team": "team-first"})
    second_day = _row(timestamp=_TIMESTAMP + timedelta(days=1), metadata={"team": "team-second"})
    with backend.create_unit_of_work() as uow:
        uow.chargebacks.upsert_batch([first_day, second_day])
        uow.commit()

    assert _rows_for_date(backend, _TIMESTAMP.date())[0].metadata == {"team": "team-first"}
    assert _rows_for_date(backend, (_TIMESTAMP + timedelta(days=1)).date())[0].metadata == {"team": "team-second"}

    with backend.create_unit_of_work() as uow:
        uow.chargebacks.delete_by_date(_ECOSYSTEM, _TENANT_ID, (_TIMESTAMP + timedelta(days=1)).date())
        uow.chargebacks.upsert(_row(timestamp=_TIMESTAMP + timedelta(days=1), metadata={"team": "team-third"}))
        uow.commit()

    assert _rows_for_date(backend, _TIMESTAMP.date())[0].metadata == {"team": "team-first"}
    assert _rows_for_date(backend, (_TIMESTAMP + timedelta(days=1)).date())[0].metadata == {"team": "team-third"}


def test_iter_by_filters_hydrates_each_streamed_partition(backend: SQLModelBackend) -> None:
    rows = [
        _row(identity_id="User:alice", metadata={"team": "team-data"}),
        _row(identity_id="User:bob", product_type="SELF_KAFKA_NETWORK_EGRESS", metadata={"team": "team-platform"}),
    ]
    with backend.create_unit_of_work() as uow:
        uow.chargebacks.upsert_batch(rows)
        uow.commit()

    with backend.create_read_only_unit_of_work() as uow:
        streamed = list(uow.chargebacks.iter_by_filters(_ECOSYSTEM, _TENANT_ID, batch_size=1))

    assert {row.identity_id: row.metadata for row in streamed} == {
        "User:alice": {"team": "team-data"},
        "User:bob": {"team": "team-platform"},
    }


def test_unrelated_metadata_is_not_persisted_as_team(backend: SQLModelBackend) -> None:
    with backend.create_unit_of_work() as uow:
        uow.chargebacks.upsert(_row(metadata={"env_id": "env-1", "transient": "value"}))
        uow.commit()

    [stored] = _rows_for_date(backend, _TIMESTAMP.date())
    assert stored.metadata == {}
    assert _snapshot_count(backend) == 0


def test_residual_static_unavailable_and_zero_usage_rows_have_no_snapshot(backend: SQLModelBackend) -> None:
    rows = [
        _row(identity_id="UNALLOCATED", metadata={}),
        _row(identity_id="Team:fixed", product_type="SELF_KAFKA_COMPUTE", metadata={}),
        _row(identity_id="UNALLOCATED", product_type="SELF_KAFKA_STORAGE", metadata={}),
    ]
    with backend.create_unit_of_work() as uow:
        uow.chargebacks.upsert_batch(rows)
        uow.commit()

    assert _snapshot_count(backend) == 0
    assert all(row.metadata == {} for row in _rows_for_date(backend, _TIMESTAMP.date()))


def test_snapshot_and_fact_updates_roll_back_together(backend: SQLModelBackend) -> None:
    with backend.create_unit_of_work() as uow:
        uow.chargebacks.upsert(_row(metadata={"team": "team-committed"}))
        uow.commit()

    with backend.create_unit_of_work() as uow:
        uow.chargebacks.upsert(_row(metadata={"team": "team-rolled-back"}))

    [stored] = _rows_for_date(backend, _TIMESTAMP.date())
    assert stored.metadata == {"team": "team-committed"}
