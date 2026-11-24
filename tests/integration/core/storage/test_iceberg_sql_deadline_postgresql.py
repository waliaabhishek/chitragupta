from __future__ import annotations

import socket
from collections.abc import Iterator
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from time import monotonic

import pytest
from sqlalchemy import create_engine, make_url
from sqlalchemy.exc import OperationalError

from core.models.chargeback import ChargebackRow, CostType
from core.storage.backends.sqlmodel.module import CoreStorageModule
from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend
from core.storage.iceberg import (
    IcebergExportKey,
    IcebergSourceWidthError,
    IcebergStorageDeadlineError,
    IcebergStorageUnsupportedError,
)
from plugins.self_managed_kafka.storage.module import SelfManagedKafkaStorageModule
from tests.integration.core.storage.test_migration_029_postgresql import (
    postgresql_schema_url as postgresql_schema_url,
)
from tests.integration.core.storage.test_migration_029_postgresql import pytestmark as pytestmark

KEY = IcebergExportKey("iceberg-table", "generic_metrics_only", "tenant", date(2026, 11, 20))


@pytest.fixture
def storage_url(postgresql_schema_url: str) -> str:
    url = make_url(postgresql_schema_url)
    if url.host == "localhost":
        url = url.update_query_dict({"hostaddr": "127.0.0.1"})
    return url.render_as_string(hide_password=False)


@pytest.fixture
def backend(storage_url: str) -> Iterator[SQLModelBackend]:
    storage = SQLModelBackend(storage_url, CoreStorageModule(), use_migrations=False)
    storage.create_tables()
    try:
        yield storage
    finally:
        storage.dispose()


def test_postgresql_source_width_counts_team_and_combined_tag_payload(storage_url: str) -> None:
    storage = SQLModelBackend(storage_url, SelfManagedKafkaStorageModule(), use_migrations=False)
    storage.create_tables()
    ecosystem = "self_managed_kafka"
    start = datetime.combine(KEY.date, datetime.min.time(), tzinfo=UTC)
    end = start + timedelta(days=1)
    team = "team" * 45
    tag_value = "🙂" * 40
    row = ChargebackRow(
        ecosystem,
        KEY.tenant_id,
        start,
        "res",
        "category",
        "type",
        "id",
        CostType.USAGE,
        Decimal("1.2"),
        metadata={"team": team},
    )

    def read(budget: int) -> list[ChargebackRow]:
        with storage.create_iceberg_read_unit_of_work(deadline=monotonic() + 2) as uow:
            return list(
                uow.iter_iceberg_chargebacks(
                    ecosystem, KEY.tenant_id, start, end, max_row_bytes=budget, max_batch_bytes=budget
                )
            )

    try:
        with storage.create_unit_of_work() as uow:
            uow.chargebacks.upsert(row)
            uow.tags.add_tag(KEY.tenant_id, "identity", "id", "owner", tag_value, "creator")
            uow.commit()
        with pytest.raises(IcebergSourceWidthError):
            read(460)
        (stored,) = read(600)
        assert stored.metadata == {"team": team}
        assert stored.tags == {"owner": tag_value}

        with storage.create_unit_of_work() as uow:
            uow.tags.add_tag(KEY.tenant_id, "resource", "res", "owner", tag_value, "creator")
            uow.commit()
        with pytest.raises(IcebergSourceWidthError):
            read(600)
    finally:
        storage.dispose()


def test_postgresql_source_reads_and_checkpoint_commit_recover_after_lock_timeout(
    backend: SQLModelBackend, storage_url: str
) -> None:
    locker = create_engine(storage_url)
    try:
        with locker.begin() as connection:
            connection.exec_driver_sql("LOCK TABLE pipeline_state IN ACCESS EXCLUSIVE MODE")
            start = monotonic()
            with (
                pytest.raises((OperationalError, IcebergStorageDeadlineError)),
                backend.create_iceberg_read_unit_of_work(deadline=start + 0.2) as uow,
            ):
                uow.pipeline_state.get(KEY.ecosystem, KEY.tenant_id, KEY.date)
            assert monotonic() - start < 0.75

        with locker.begin() as connection:
            connection.exec_driver_sql("LOCK TABLE iceberg_export_checkpoints IN SHARE MODE")
            start = monotonic()
            with (
                pytest.raises((OperationalError, IcebergStorageDeadlineError)),
                backend.create_iceberg_write_unit_of_work(deadline=start + 0.2) as uow,
            ):
                uow.iceberg_exports.request_replay(KEY.table_uuid, KEY.ecosystem, KEY.tenant_id, [KEY.date])
                uow.commit()
            assert monotonic() - start < 0.75

        with backend.create_iceberg_read_unit_of_work(deadline=monotonic() + 2) as uow:
            assert uow.iceberg_exports.get(KEY) is None
        with backend.create_iceberg_write_unit_of_work(deadline=monotonic() + 2) as uow:
            uow.iceberg_exports.request_replay(KEY.table_uuid, KEY.ecosystem, KEY.tenant_id, [KEY.date])
            uow.commit()
        with backend.create_iceberg_read_unit_of_work(deadline=monotonic() + 2) as uow:
            checkpoint = uow.iceberg_exports.get(KEY)
            assert checkpoint is not None and checkpoint.replay_requested
    finally:
        locker.dispose()


def test_postgresql_connection_stall_obeys_monotonic_deadline() -> None:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        listener.listen()
        port = listener.getsockname()[1]
        backend = SQLModelBackend(f"postgresql+psycopg://127.0.0.1:{port}/source", CoreStorageModule())
        try:
            start = monotonic()
            with (
                pytest.raises(IcebergStorageDeadlineError),
                backend.create_iceberg_read_unit_of_work(deadline=start + 0.12),
            ):
                pytest.fail("A connection without a PostgreSQL handshake opened")
            assert monotonic() - start < 0.55
        finally:
            backend.dispose()


def test_postgresql_source_statement_deadline_survives_driver_wrapping(backend: SQLModelBackend) -> None:
    from psycopg.errors import ConnectionTimeout

    start = monotonic()
    with (
        pytest.raises(IcebergStorageDeadlineError) as failure,
        backend.create_iceberg_read_unit_of_work(deadline=start + 0.2) as uow,
    ):
        assert uow._connection is not None
        uow._connection.exec_driver_sql("SELECT pg_sleep(60)")
    assert isinstance(failure.value.__cause__, ConnectionTimeout)
    assert monotonic() - start < 0.75


def test_postgresql_source_preserves_stricter_server_timeout(storage_url: str) -> None:
    from psycopg.errors import QueryCanceled

    url = make_url(storage_url)
    options = url.query.get("options", "")
    url = url.update_query_dict({"options": f"{options} -cstatement_timeout=150"})
    storage = SQLModelBackend(url.render_as_string(hide_password=False), CoreStorageModule(), use_migrations=False)
    try:
        with (
            pytest.raises(OperationalError) as failure,
            storage.create_iceberg_read_unit_of_work(deadline=monotonic() + 2) as uow,
        ):
            assert uow._connection is not None
            uow._connection.exec_driver_sql("SELECT pg_sleep(60)")
        assert isinstance(failure.value.orig, QueryCanceled)
    finally:
        storage.dispose()


def test_postgresql_commit_timeout_preserves_unknown_outcome_and_safe_reissue(
    backend: SQLModelBackend, storage_url: str
) -> None:
    engine = create_engine(storage_url)
    try:
        with engine.begin() as connection:
            connection.exec_driver_sql(
                "CREATE FUNCTION delay_checkpoint_commit() RETURNS trigger LANGUAGE plpgsql AS "
                "$$ BEGIN PERFORM pg_sleep(2); RETURN NULL; END; $$"
            )
            connection.exec_driver_sql(
                "CREATE CONSTRAINT TRIGGER delay_checkpoint_commit AFTER INSERT ON iceberg_export_checkpoints "
                "DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION delay_checkpoint_commit()"
            )
        start = monotonic()
        with (
            pytest.raises(IcebergStorageDeadlineError),
            backend.create_iceberg_write_unit_of_work(deadline=start + 0.2) as uow,
        ):
            uow.iceberg_exports.request_replay(KEY.table_uuid, KEY.ecosystem, KEY.tenant_id, [KEY.date])
            uow.commit()
        assert monotonic() - start < 0.75
        with engine.begin() as connection:
            connection.exec_driver_sql("DROP TRIGGER delay_checkpoint_commit ON iceberg_export_checkpoints")
        with backend.create_iceberg_read_unit_of_work(deadline=monotonic() + 2) as uow:
            checkpoint = uow.iceberg_exports.get(KEY)
            assert checkpoint is None or checkpoint.replay_requested

        # The caller did not receive COMMIT acknowledgement. Registering the request again is safe either way.
        with backend.create_iceberg_write_unit_of_work(deadline=monotonic() + 2) as uow:
            uow.iceberg_exports.request_replay(KEY.table_uuid, KEY.ecosystem, KEY.tenant_id, [KEY.date])
            uow.commit()
        with backend.create_iceberg_read_unit_of_work(deadline=monotonic() + 2) as uow:
            checkpoint = uow.iceberg_exports.get(KEY)
            assert checkpoint is not None and checkpoint.replay_requested
    finally:
        engine.dispose()


def test_postgresql_hostname_requires_a_bounded_address() -> None:
    backend = SQLModelBackend("postgresql+psycopg://postgres.example.invalid/source", CoreStorageModule())
    try:
        with pytest.raises(IcebergStorageUnsupportedError, match="numeric hostaddr"):
            backend.create_iceberg_read_unit_of_work(deadline=monotonic() + 2)
    finally:
        backend.dispose()


@pytest.mark.parametrize("service_from_environment", [False, True])
def test_postgresql_service_cannot_override_bounded_address(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, service_from_environment: bool
) -> None:
    service_file = tmp_path / "pg_service.conf"
    service_file.write_text("[export]\nhost=localhost\n", encoding="utf-8")
    monkeypatch.setenv("PGSERVICEFILE", str(service_file))
    monkeypatch.setenv("PGHOST", "127.0.0.1")
    monkeypatch.setenv("PGHOSTADDR", "127.0.0.1")
    url = "postgresql+psycopg:///source"
    if service_from_environment:
        monkeypatch.setenv("PGSERVICE", "export")
    else:
        url += "?service=export"
    backend = SQLModelBackend(url, CoreStorageModule())
    try:
        with pytest.raises(IcebergStorageUnsupportedError, match="service connections require an explicit hostaddr"):
            backend.create_iceberg_read_unit_of_work(deadline=monotonic() + 2)
    finally:
        backend.dispose()
