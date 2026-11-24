from __future__ import annotations

import sqlite3
import subprocess
import sys
from contextlib import closing
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from time import monotonic
from typing import TYPE_CHECKING

import pytest
from sqlalchemy.exc import OperationalError

from core.models.chargeback import ChargebackRow, CostType
from core.models.pipeline import PipelineState
from core.storage.backends.sqlmodel.module import CoreStorageModule
from core.storage.backends.sqlmodel.repositories import SQLModelChargebackRepository
from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend
from core.storage.iceberg import (
    IcebergExportKey,
    IcebergExportUnitOfWork,
    IcebergSourceWidthError,
    IcebergStorageDeadlineError,
    IcebergStorageUnsupportedError,
)
from plugins.self_managed_kafka.storage.module import SelfManagedKafkaStorageModule

if TYPE_CHECKING:
    from collections.abc import Iterator
    from typing import Any

DAY = date(2026, 11, 20)
NEXT_DAY = DAY + timedelta(days=1)
NOW = datetime(2026, 11, 22, tzinfo=UTC)
KEY = IcebergExportKey("table-a", "generic_metrics_only", "source", DAY)
SNAPSHOT_ID = (1 << 63) - 1
START = datetime.combine(DAY, datetime.min.time(), tzinfo=UTC)
END = START + timedelta(days=1)


@pytest.fixture(params=[False, True], ids=["direct", "migrated"])
def backend(request: pytest.FixtureRequest, tmp_path: Path) -> Iterator[SQLModelBackend]:
    storage = SQLModelBackend(f"sqlite:///{tmp_path / 'source.db'}", CoreStorageModule(), use_migrations=request.param)
    storage.create_tables()
    try:
        yield storage
    finally:
        storage.dispose()


def _chargeback() -> ChargebackRow:
    return ChargebackRow(
        KEY.ecosystem, KEY.tenant_id, START, "resource", "product", "type", "identity", CostType.USAGE, Decimal("1.23")
    )


def _read_chargebacks(backend: SQLModelBackend, ecosystem: str = KEY.ecosystem, *, budget: int) -> list[ChargebackRow]:
    with backend.create_iceberg_read_unit_of_work(deadline=monotonic() + 2) as uow:
        return list(
            uow.iter_iceberg_chargebacks(
                ecosystem, KEY.tenant_id, START, END, max_row_bytes=budget, max_batch_bytes=budget
            )
        )


def test_source_width_caps_batch_and_uses_the_guarded_snapshot(
    backend: SQLModelBackend, monkeypatch: pytest.MonkeyPatch
) -> None:
    row = _chargeback()
    with backend.create_unit_of_work() as uow:
        uow.chargebacks.upsert_batch(
            [
                row,
                replace(row, timestamp=START + timedelta(hours=1)),
                replace(row, tenant_id="different", allocation_detail="x" * 2000),
                replace(row, ecosystem="different", allocation_detail="x" * 2000),
                replace(row, timestamp=END, allocation_detail="x" * 2000),
            ]
        )
        uow.commit()

    iter_by_filters = SQLModelChargebackRepository.iter_by_filters
    batch_sizes: list[int] = []

    def record_batch(self: SQLModelChargebackRepository, *args: Any, batch_size: int, **kwargs: Any) -> Any:
        batch_sizes.append(batch_size)
        # This tag must stay invisible to the read that already counted the batch widths.
        with backend.create_unit_of_work() as writer:
            writer.tags.add_tag(KEY.tenant_id, "resource", "resource", "new", "x" * 2000, "creator")
            writer.commit()
        return iter_by_filters(self, *args, batch_size=batch_size, **kwargs)

    monkeypatch.setattr(SQLModelChargebackRepository, "iter_by_filters", record_batch)
    assert [row.tags for row in _read_chargebacks(backend, budget=220)] == [{}, {}]
    assert batch_sizes == [1]
    with pytest.raises(IcebergSourceWidthError):
        _read_chargebacks(backend, budget=220)
    assert batch_sizes == [1]


@pytest.mark.parametrize("body", ["fact", "tags"])
def test_source_width_rejects_fact_and_combined_tag_payloads_before_hydration(
    backend: SQLModelBackend, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, body: str
) -> None:
    with backend.create_unit_of_work() as uow:
        uow.chargebacks.upsert(_chargeback())
        uow.commit()

    if body == "fact":
        with closing(sqlite3.connect(tmp_path / "source.db")) as connection, connection:
            connection.execute("UPDATE chargeback_facts SET tags_json = ?", ("a\0" + "🙂" * 200,))
    else:
        with backend.create_unit_of_work() as uow:
            uow.tags.add_tag(KEY.tenant_id, "identity", "identity", "owner", "a\0" + "🙂" * 40, "creator")
            uow.commit()
        assert len(_read_chargebacks(backend, budget=450)) == 1
        with backend.create_unit_of_work() as uow:
            uow.tags.add_tag(KEY.tenant_id, "resource", "resource", "owner", "🙂" * 40, "creator")
            uow.commit()

    def unexpected_read(*args: object, **kwargs: object) -> None:
        pytest.fail("The source row body was read before the width check")

    monkeypatch.setattr(SQLModelChargebackRepository, "iter_by_filters", unexpected_read)
    with pytest.raises(IcebergSourceWidthError):
        _read_chargebacks(backend, budget=450)


def test_source_width_counts_historic_team_for_legacy_timestamp(tmp_path: Path) -> None:
    path = tmp_path / "source.db"
    backend = SQLModelBackend(f"sqlite:///{path}", SelfManagedKafkaStorageModule(), use_migrations=False)
    backend.create_tables()
    ecosystem = "self_managed_kafka"
    team = "team" * 125
    try:
        with backend.create_unit_of_work() as uow:
            uow.chargebacks.upsert(replace(_chargeback(), ecosystem=ecosystem, metadata={"team": team}))
            uow.commit()
        with closing(sqlite3.connect(path)) as connection, connection:
            connection.execute("UPDATE chargeback_facts SET timestamp = timestamp || '.123456'")

        assert [row.metadata for row in _read_chargebacks(backend, ecosystem, budget=1000)] == [{"team": team}]
        with pytest.raises(IcebergSourceWidthError):
            _read_chargebacks(backend, ecosystem, budget=450)
    finally:
        backend.dispose()


def test_checkpoint_preserves_empty_confirmation_during_replay(backend: SQLModelBackend) -> None:
    with backend.create_unit_of_work() as uow:
        assert isinstance(uow, IcebergExportUnitOfWork)
        uow.iceberg_exports.record_attempt(KEY, None, NOW)
        uow.commit()

    with backend.create_unit_of_work() as uow:
        uow.iceberg_exports.confirm_attempt(KEY, None, None, SNAPSHOT_ID, NOW, 0)
        uow.iceberg_exports.request_replay(KEY.table_uuid, KEY.ecosystem, KEY.tenant_id, [DAY])
        uow.commit()

    with backend.create_unit_of_work() as uow:
        uow.iceberg_exports.record_attempt(KEY, "recalculation", NOW)
        uow.iceberg_exports.fail_attempt(KEY, "storage_unavailable")
        uow.commit()

    with backend.create_consistent_read_unit_of_work() as uow:
        checkpoint = uow.iceberg_exports.get(KEY)
        assert checkpoint is not None
        assert checkpoint.confirmed_revision is None
        assert checkpoint.confirmed_at == NOW
        assert checkpoint.row_count == 0
        assert checkpoint.snapshot_id == SNAPSHOT_ID
        assert checkpoint.attempted_revision == "recalculation"
        assert checkpoint.attempted_at == NOW
        assert checkpoint.outcome == "failed"
        assert checkpoint.error_code == "storage_unavailable"
        assert checkpoint.replay_requested

    with backend.create_unit_of_work() as uow:
        uow.iceberg_exports.record_attempt(KEY, "recalculation", NOW)
        uow.iceberg_exports.fail_attempt(KEY, "amount_not_representable", terminal=True)
        uow.commit()

    with backend.create_consistent_read_unit_of_work() as uow:
        checkpoint = uow.iceberg_exports.get(KEY)
        assert checkpoint is not None
        assert checkpoint.snapshot_id == SNAPSHOT_ID
        assert checkpoint.error_code == "amount_not_representable"
        assert not checkpoint.replay_requested


def test_replay_registration_and_retention_use_caller_transaction(backend: SQLModelBackend) -> None:
    table, ecosystem, tenant = KEY.table_uuid, KEY.ecosystem, KEY.tenant_id
    with backend.create_unit_of_work() as uow:
        uow.iceberg_exports.request_replay(table, ecosystem, tenant, [DAY])
    with backend.create_consistent_read_unit_of_work() as uow:
        assert uow.iceberg_exports.get(KEY) is None

    with backend.create_unit_of_work() as uow:
        uow.pipeline_state.upsert(PipelineState(ecosystem, tenant, DAY, chargeback_calculated=True))
        uow.iceberg_exports.request_replay(table, ecosystem, tenant, [DAY, DAY, NEXT_DAY])
        uow.iceberg_exports.request_replay("former-table", ecosystem, tenant, [DAY])
        uow.iceberg_exports.request_replay(table, ecosystem, "other-source", [DAY])
        uow.commit()

    with backend.create_unit_of_work() as uow:
        assert uow.pipeline_state.delete_before(ecosystem, tenant, NEXT_DAY) == 1
        assert uow.iceberg_exports.delete_before(ecosystem, tenant, NEXT_DAY) == 2

    with backend.create_consistent_read_unit_of_work() as uow:
        assert uow.pipeline_state.get(ecosystem, tenant, DAY) is not None
        checkpoints = uow.iceberg_exports.find_by_range(table, ecosystem, tenant, DAY, NEXT_DAY)
        assert [checkpoint.key for checkpoint in checkpoints] == [KEY]
        assert checkpoints[0].replay_requested
        assert checkpoints[0].outcome is None

    with backend.create_unit_of_work() as uow:
        assert uow.pipeline_state.delete_before(ecosystem, tenant, NEXT_DAY) == 1
        assert uow.iceberg_exports.delete_before(ecosystem, tenant, NEXT_DAY) == 2
        uow.commit()

    with backend.create_consistent_read_unit_of_work() as uow:
        assert uow.pipeline_state.get(ecosystem, tenant, DAY) is None
        assert uow.iceberg_exports.find_by_range("former-table", ecosystem, tenant, DAY, NEXT_DAY) == []
        assert uow.iceberg_exports.find_by_range(table, ecosystem, tenant, DAY, NEXT_DAY) == []
        assert uow.iceberg_exports.get(IcebergExportKey(table, ecosystem, tenant, NEXT_DAY)) is not None
        assert uow.iceberg_exports.get(IcebergExportKey(table, ecosystem, "other-source", DAY)) is not None


def test_sqlite_checkpoint_write_timeout_recovers_without_registration(tmp_path: Path) -> None:
    db = tmp_path / "source db.sqlite"
    storage = SQLModelBackend(f"sqlite:///{db}", CoreStorageModule(), use_migrations=False)
    storage.create_tables()
    replay_scope = (KEY.table_uuid, KEY.ecosystem, KEY.tenant_id, [DAY])
    try:
        with closing(sqlite3.connect(db)) as locker:
            locker.execute("BEGIN IMMEDIATE")
            start = monotonic()
            with (
                pytest.raises((OperationalError, IcebergStorageDeadlineError)),
                storage.create_iceberg_write_unit_of_work(deadline=start + 0.12) as uow,
            ):
                uow.iceberg_exports.request_replay(*replay_scope)
                uow.commit()
            assert monotonic() - start < 0.55

        with storage.create_iceberg_read_unit_of_work(deadline=monotonic() + 2) as uow:
            assert uow.iceberg_exports.get(KEY) is None
        with storage.create_iceberg_write_unit_of_work(deadline=monotonic() + 2) as uow:
            uow.iceberg_exports.request_replay(*replay_scope)
            uow.commit()
        with storage.create_iceberg_read_unit_of_work(deadline=monotonic() + 2) as uow:
            checkpoint = uow.iceberg_exports.get(KEY)
            assert checkpoint is not None and checkpoint.replay_requested
    finally:
        storage.dispose()


def test_sqlite_export_deadlines_do_not_create_a_missing_database(tmp_path: Path) -> None:
    db = tmp_path / "missing.db"
    storage = SQLModelBackend(f"sqlite:///{db}", CoreStorageModule())
    try:
        for create_uow in (storage.create_iceberg_read_unit_of_work, storage.create_iceberg_write_unit_of_work):
            with pytest.raises(OperationalError), create_uow(deadline=monotonic() + 2) as uow:
                uow.iceberg_exports.get(KEY)
        assert not list(tmp_path.iterdir())
    finally:
        storage.dispose()


def test_sqlite_source_read_interrupts_long_running_query(tmp_path: Path) -> None:
    storage = SQLModelBackend(f"sqlite:///{tmp_path / 'source.db'}", CoreStorageModule(), use_migrations=False)
    storage.create_tables()
    try:
        start = monotonic()
        with (
            pytest.raises(IcebergStorageDeadlineError) as failure,
            storage.create_iceberg_read_unit_of_work(deadline=start + 0.2) as uow,
        ):
            assert uow._connection is not None
            uow._connection.exec_driver_sql(
                "WITH RECURSIVE series(n) AS "
                "(VALUES(0) UNION ALL SELECT n+1 FROM series WHERE n < 100000000) "
                "SELECT sum(n) FROM series"
            )
        assert isinstance(failure.value.__cause__, sqlite3.OperationalError)
        assert failure.value.__cause__.sqlite_errorcode == sqlite3.SQLITE_INTERRUPT
        assert monotonic() - start < 0.75
    finally:
        storage.dispose()


def test_sqlite_source_read_preserves_explicit_interrupts(backend: SQLModelBackend) -> None:
    with backend.create_iceberg_read_unit_of_work(deadline=monotonic() + 2) as uow:
        assert uow._connection is not None
        connection = uow._connection.connection.driver_connection
        assert isinstance(connection, sqlite3.Connection)
        connection.create_function("interrupt_source", 0, connection.interrupt)
        with pytest.raises(OperationalError) as failure:
            uow._connection.exec_driver_sql("SELECT interrupt_source()").one()
        assert failure.value.orig.sqlite_errorcode == sqlite3.SQLITE_INTERRUPT


def test_sqlite_checkpoint_deadlines_require_persistent_storage() -> None:
    storage = SQLModelBackend("sqlite:///:memory:", CoreStorageModule())
    try:
        with pytest.raises(IcebergStorageUnsupportedError, match="requires a database file"):
            storage.create_iceberg_read_unit_of_work(deadline=monotonic() + 2)
    finally:
        storage.dispose()


def test_base_sqlite_storage_does_not_import_the_optional_postgresql_driver(tmp_path: Path) -> None:
    source = Path(__file__).resolve().parents[4] / "src"
    script = """
import sys
from time import monotonic

sys.modules["psycopg"] = None
sys.path.insert(0, sys.argv[1])

from core.storage.backends.sqlmodel.module import CoreStorageModule
from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend
from core.storage.iceberg import IcebergExportKey
from datetime import date

backend = SQLModelBackend(f"sqlite:///{sys.argv[2]}", CoreStorageModule(), use_migrations=False)
backend.create_tables()
with backend.create_iceberg_read_unit_of_work(deadline=monotonic() + 2) as uow:
    assert uow.iceberg_exports.get(IcebergExportKey("table", "plugin", "tenant", date.today())) is None
backend.dispose()
"""
    subprocess.run([sys.executable, "-c", script, str(source), str(tmp_path / "source.db")], check=True, timeout=10)
