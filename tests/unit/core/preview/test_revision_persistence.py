from __future__ import annotations

import io
import sqlite3
import threading
from dataclasses import replace
from datetime import UTC, date, datetime
from importlib import import_module
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.dialects import postgresql, sqlite
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.schema import CreateIndex
from sqlmodel import Session, SQLModel, select

from tests.unit.core.preview.test_revision_models import _candidate, _package
from tests.unit.core.storage.test_migration_019_focus_preview import _alembic_config


def _persistence() -> Any:
    return import_module("core.preview.persistence")


def test_revision_table_registers_complete_immutable_replacement_metadata() -> None:
    persistence = _persistence()
    table = persistence.PreviewRevisionTable.__table__

    assert table.name == "preview_revisions"
    assert set(table.columns) >= {
        table.c.revision_id,
        table.c.tenant_name_at_publication,
        table.c.ecosystem,
        table.c.tenant_id,
        table.c.month_start,
        table.c.month_end,
        table.c.monthly_status,
        table.c.material_sha256,
        table.c.source_snapshot_json,
        table.c.published_at,
        table.c.supersedes_revision_id,
        table.c.superseded_by_revision_id,
        table.c.is_current,
        table.c.storage_key,
        table.c.manifest_metadata_json,
        table.c.file_metadata_json,
        table.c.retention_pending_at,
    }
    assert tuple(column.name for column in table.primary_key.columns) == ("revision_id",)
    assert {index.name: tuple(column.name for column in index.columns) for index in table.indexes}.items() >= {
        "ix_preview_revisions_owner_month_visible_history": (
            "ecosystem",
            "tenant_id",
            "month_start",
            "retention_pending_at",
            "published_at",
            "revision_id",
        ),
        "ix_preview_revisions_owner_retention_due": (
            "ecosystem",
            "tenant_id",
            "retention_pending_at",
            "month_end",
            "published_at",
            "revision_id",
        ),
        "ix_preview_revisions_owner_retention_pending": (
            "ecosystem",
            "tenant_id",
            "retention_pending_at",
            "revision_id",
        ),
    }.items()


def test_current_revision_partial_index_compiles_for_sqlite_and_postgresql() -> None:
    persistence = _persistence()
    index = next(
        item
        for item in persistence.PreviewRevisionTable.__table__.indexes
        if item.name == "ux_preview_revisions_owner_month_current"
    )

    assert index.unique is True
    assert tuple(column.name for column in index.columns) == (
        "ecosystem",
        "tenant_id",
        "month_start",
    )
    sqlite_sql = str(CreateIndex(index).compile(dialect=sqlite.dialect()))
    postgres_sql = str(CreateIndex(index).compile(dialect=postgresql.dialect()))
    assert "WHERE is_current = 1" in sqlite_sql
    assert "WHERE is_current IS TRUE" in postgres_sql


def test_current_lookup_compiles_to_partial_index_predicate_and_uses_index(tmp_path: Path) -> None:
    persistence = _persistence()
    engine = create_engine(f"sqlite:///{tmp_path / 'query-plan.db'}")
    _create_revision_schema(engine)
    statements: list[Any] = []
    with Session(engine) as session:
        original = session.exec

        def capture(statement: Any, *args: Any, **kwargs: Any) -> Any:
            statements.append(statement)
            return original(statement, *args, **kwargs)

        session.exec = capture  # type: ignore[method-assign]
        assert (
            persistence.SQLModelPreviewRevisionRepository(session).get_current_for_owner(
                ecosystem="confluent_cloud",
                tenant_id="tenant-1",
                month_start=date(2026, 7, 1),
            )
            is None
        )

    sql = str(statements[-1].compile(engine, compile_kwargs={"literal_binds": True}))
    assert "preview_revisions.is_current = 1" in sql
    with engine.connect() as connection:
        plan = connection.execute(text(f"EXPLAIN QUERY PLAN {sql}")).all()
    assert any(
        index_name in str(row)
        for row in plan
        for index_name in (
            "ux_preview_revisions_owner_month_current",
            "ix_preview_revisions_owner_month_visible_history",
        )
    )
    engine.dispose()


def test_postgresql_current_lookup_compiles_to_partial_index_is_true_predicate() -> None:
    persistence = _persistence()
    statements: list[Any] = []

    class Result:
        def first(self) -> None:
            return None

    class PostgreSQLSession:
        def get_bind(self) -> Any:
            return SimpleNamespace(dialect=postgresql.dialect())

        def exec(self, statement: Any) -> Result:
            statements.append(statement)
            return Result()

    repository = persistence.SQLModelPreviewRevisionRepository(PostgreSQLSession())
    assert (
        repository.get_current_for_owner(
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
            month_start=date(2026, 7, 1),
        )
        is None
    )

    sql = str(statements[-1].compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))
    assert "preview_revisions.is_current IS true" in sql
    assert "preview_revisions.is_current = true" not in sql


def test_postgresql_retention_queries_compile_with_index_matched_predicates_and_order() -> None:
    persistence = _persistence()
    statements: list[Any] = []

    class Result:
        def all(self) -> list[Any]:
            return []

        def first(self) -> None:
            return None

    class PostgreSQLSession:
        def get_bind(self) -> Any:
            return SimpleNamespace(dialect=postgresql.dialect())

        def exec(self, statement: Any) -> Result:
            statements.append(statement)
            return Result()

    repository = persistence.SQLModelPreviewRevisionRepository(PostgreSQLSession())
    assert (
        repository.mark_retention_due(
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
            cutoff_date=date(2026, 8, 1),
            pending_at=datetime(2026, 8, 5, tzinfo=UTC),
            limit=100,
        )
        == ()
    )
    assert (
        repository.list_retention_pending(
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
            limit=100,
        )
        == ()
    )
    assert (
        repository.get_retention_pending_tail(
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
        )
        is None
    )

    sql = [
        str(
            statement.compile(
                dialect=postgresql.dialect(),
                compile_kwargs={"literal_binds": True},
            )
        )
        for statement in statements
    ]
    due, pending, tail = sql
    assert "retention_pending_at IS NULL" in due
    assert "month_end <= '2026-08-01'" in due
    assert "ORDER BY preview_revisions.month_end, preview_revisions.published_at, preview_revisions.revision_id" in due
    assert "LIMIT 100" in due
    assert "retention_pending_at IS NOT NULL" in pending
    assert "ORDER BY preview_revisions.retention_pending_at, preview_revisions.revision_id" in pending
    assert "LIMIT 100" in pending
    assert "retention_pending_at IS NOT NULL" in tail
    assert "ORDER BY preview_revisions.retention_pending_at DESC, preview_revisions.revision_id DESC" in tail
    assert "LIMIT 1" in tail


def test_repository_rejects_supersedes_mismatch_before_sql() -> None:
    persistence = _persistence()
    session = MagicMock()
    repository = persistence.SQLModelPreviewRevisionRepository(session)

    with pytest.raises(ValueError, match="supersedes identity"):
        repository.replace_current(
            candidate=_candidate(supersedes_revision_id="revision-old"),
            package=_package(),
            expected_current_revision_id=None,
        )

    session.execute.assert_not_called()
    session.add.assert_not_called()

    with pytest.raises(ValueError, match="supersedes identity"):
        repository.replace_current(
            candidate=_candidate(supersedes_revision_id="revision-old"),
            package=_package(),
            expected_current_revision_id="revision-other",
        )

    session.execute.assert_not_called()
    session.add.assert_not_called()


def test_sqlmodel_preview_read_and_write_uows_expose_revision_repository(tmp_path: Path) -> None:
    unit_of_work = import_module("core.storage.backends.sqlmodel.unit_of_work")
    storage_module = import_module("plugins.confluent_cloud.storage.module")
    backend = unit_of_work.SQLModelBackend(
        f"sqlite:///{tmp_path / 'uow.db'}",
        storage_module.CCloudStorageModule(),
        use_migrations=False,
    )
    backend.create_tables()

    with backend.create_preview_write_unit_of_work() as write_uow:
        assert isinstance(write_uow.revisions, _persistence().PreviewRevisionRepository)
    with backend.create_preview_read_unit_of_work() as read_uow:
        assert isinstance(read_uow.revisions, _persistence().PreviewRevisionRepository)
    backend.dispose()


def _create_revision_schema(engine: Any) -> None:
    persistence = _persistence()
    SQLModel.metadata.create_all(engine, tables=[persistence.PreviewRevisionTable.__table__])


def _publish_race(
    engine: Any,
    candidates: tuple[Any, Any],
    expected_current_revision_id: str | None,
) -> tuple[list[str], list[BaseException]]:
    persistence = _persistence()
    barrier = threading.Barrier(2)
    winners: list[str] = []
    failures: list[BaseException] = []

    def publish(candidate: Any) -> None:
        try:
            with Session(engine) as session:
                repository = persistence.SQLModelPreviewRevisionRepository(session)
                observed = repository.get_current_for_owner(
                    ecosystem="confluent_cloud",
                    tenant_id="tenant-1",
                    month_start=date(2026, 7, 1),
                )
                assert (None if observed is None else observed.revision_id) == expected_current_revision_id
                barrier.wait(timeout=10)
                repository.replace_current(
                    candidate=candidate,
                    package=replace(_package(), storage_key=candidate.revision_id),
                    expected_current_revision_id=expected_current_revision_id,
                )
                session.commit()
                winners.append(candidate.revision_id)
        except BaseException as exc:
            failures.append(exc)

    threads = tuple(threading.Thread(target=publish, args=(candidate,)) for candidate in candidates)
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=15)
    assert all(not thread.is_alive() for thread in threads)
    return winners, failures


def test_concurrent_initial_publication_leaves_exactly_one_current(tmp_path: Path) -> None:
    persistence = _persistence()
    engine = create_engine(f"sqlite:///{tmp_path / 'initial-race.db'}")
    _create_revision_schema(engine)

    winners, failures = _publish_race(
        engine,
        (_candidate(revision_id="revision-a"), _candidate(revision_id="revision-b")),
        None,
    )

    with Session(engine) as session:
        rows = session.exec(select(persistence.PreviewRevisionTable)).all()
    assert len(winners) == 1
    assert len(failures) == 1
    assert isinstance(failures[0], persistence.PreviewRevisionConflictError)
    assert len(rows) == 1
    assert rows[0].revision_id == winners[0]
    assert rows[0].is_current is True
    engine.dispose()


def test_unrelated_sqlite_constraint_error_is_not_translated_to_revision_conflict(
    tmp_path: Path,
) -> None:
    persistence = _persistence()
    engine = create_engine(f"sqlite:///{tmp_path / 'unrelated-constraint.db'}")
    _create_revision_schema(engine)
    with engine.begin() as connection:
        connection.execute(
            text(
                "CREATE TRIGGER reject_revision BEFORE INSERT ON preview_revisions "
                "WHEN NEW.revision_id = 'revision-rejected' "
                "BEGIN SELECT RAISE(ABORT, 'unrelated trigger constraint'); END"
            )
        )

    with Session(engine) as session:
        repository = persistence.SQLModelPreviewRevisionRepository(session)
        with pytest.raises(IntegrityError) as raised:
            repository.replace_current(
                candidate=_candidate(revision_id="revision-rejected"),
                package=replace(_package(), storage_key="revision-rejected"),
                expected_current_revision_id=None,
            )
        assert not isinstance(raised.value, persistence.PreviewRevisionConflictError)
    engine.dispose()


def test_pending_unrelated_unique_failure_is_not_flushed_or_translated_by_revision_write(
    tmp_path: Path,
) -> None:
    persistence = _persistence()
    engine = create_engine(f"sqlite:///{tmp_path / 'pending-unrelated.db'}")
    SQLModel.metadata.create_all(
        engine,
        tables=[persistence.PreviewRequestTable.__table__, persistence.PreviewRevisionTable.__table__],
    )
    request_values = {
        "request_id": "duplicate-request",
        "tenant_name": "production",
        "ecosystem": "confluent_cloud",
        "tenant_id": "tenant-1",
        "grain": "daily",
        "start_date": date(2026, 7, 1),
        "end_date": date(2026, 7, 2),
        "column_profile": "full",
        "status": "queued",
        "created_at": datetime(2026, 7, 1, tzinfo=UTC),
    }
    with Session(engine) as session:
        session.add(persistence.PreviewRequestTable(**request_values))
        session.commit()
        session.add(persistence.PreviewRequestTable(**request_values))
        published = persistence.SQLModelPreviewRevisionRepository(session).replace_current(
            candidate=_candidate(revision_id="revision-isolated"),
            package=replace(_package(), storage_key="revision-isolated"),
            expected_current_revision_id=None,
        )
        assert published.revision_id == "revision-isolated"
        with pytest.raises(IntegrityError) as raised:
            session.commit()
        assert not isinstance(raised.value, persistence.PreviewRevisionConflictError)
    engine.dispose()


def _sqlite_error(code: int, *, operational: bool = False) -> IntegrityError | OperationalError:
    original_type = sqlite3.OperationalError if operational else sqlite3.IntegrityError
    original = original_type("synthetic database error")
    original.sqlite_errorcode = code  # type: ignore[attr-defined]
    wrapper = OperationalError if operational else IntegrityError
    return wrapper("statement", {}, original)


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (_sqlite_error(sqlite3.SQLITE_CONSTRAINT_UNIQUE), True),
        (_sqlite_error(sqlite3.SQLITE_CONSTRAINT_PRIMARYKEY), True),
        (_sqlite_error(sqlite3.SQLITE_CONSTRAINT_CHECK), False),
        (_sqlite_error(sqlite3.SQLITE_CONSTRAINT_FOREIGNKEY), False),
        (_sqlite_error(sqlite3.SQLITE_CONSTRAINT_NOTNULL), False),
        (_sqlite_error(sqlite3.SQLITE_BUSY, operational=True), True),
        (_sqlite_error(sqlite3.SQLITE_LOCKED, operational=True), True),
        (_sqlite_error(sqlite3.SQLITE_IOERR, operational=True), False),
    ],
)
def test_sqlite_revision_conflict_classification_is_narrow(
    error: IntegrityError | OperationalError,
    expected: bool,
) -> None:
    assert _persistence()._is_revision_conflict(error) is expected


@pytest.mark.parametrize(
    ("code", "constraint", "expected"),
    [
        ("23505", "ux_preview_revisions_owner_month_current", True),
        ("23505", "preview_revisions_pkey", True),
        ("23505", "unrelated_unique", False),
        ("23503", "ux_preview_revisions_owner_month_current", False),
        ("23514", "preview_revisions_pkey", False),
    ],
)
def test_postgresql_revision_conflict_classification_is_narrow(
    code: str,
    constraint: str,
    expected: bool,
) -> None:
    original = SimpleNamespace(pgcode=code, diag=SimpleNamespace(constraint_name=constraint))
    error = IntegrityError("statement", {}, original)

    assert _persistence()._is_revision_conflict(error) is expected


def test_concurrent_replacement_atomically_links_prior_to_one_winner(tmp_path: Path) -> None:
    persistence = _persistence()
    engine = create_engine(f"sqlite:///{tmp_path / 'replacement-race.db'}")
    _create_revision_schema(engine)
    with Session(engine) as session:
        repository = persistence.SQLModelPreviewRevisionRepository(session)
        repository.replace_current(
            candidate=_candidate(revision_id="revision-old"),
            package=replace(_package(), storage_key="revision-old"),
            expected_current_revision_id=None,
        )
        session.commit()

    winners, failures = _publish_race(
        engine,
        (
            _candidate(revision_id="revision-a", supersedes_revision_id="revision-old"),
            _candidate(revision_id="revision-b", supersedes_revision_id="revision-old"),
        ),
        "revision-old",
    )

    with Session(engine) as session:
        rows = session.exec(select(persistence.PreviewRevisionTable)).all()
    current = [row for row in rows if row.is_current]
    prior = next(row for row in rows if row.revision_id == "revision-old")
    assert len(winners) == 1
    assert len(failures) == 1
    assert isinstance(failures[0], persistence.PreviewRevisionConflictError)
    assert [row.revision_id for row in current] == winners
    assert prior.is_current is False
    assert prior.superseded_by_revision_id == winners[0]
    assert len(rows) == 2
    engine.dispose()


def test_same_month_isolated_by_configured_storage_owner(tmp_path: Path) -> None:
    persistence = _persistence()
    engine = create_engine(f"sqlite:///{tmp_path / 'owners.db'}")
    _create_revision_schema(engine)
    with Session(engine) as session:
        repository = persistence.SQLModelPreviewRevisionRepository(session)
        repository.replace_current(
            candidate=_candidate(revision_id="revision-a", tenant_id="tenant-a"),
            package=replace(_package(), storage_key="revision-a"),
            expected_current_revision_id=None,
        )
        repository.replace_current(
            candidate=_candidate(revision_id="revision-b", tenant_id="tenant-b"),
            package=replace(_package(), storage_key="revision-b"),
            expected_current_revision_id=None,
        )
        session.commit()

    with Session(engine) as session:
        repository = persistence.SQLModelPreviewRevisionRepository(session)
        owner_a = repository.get_current_for_owner(
            ecosystem="confluent_cloud", tenant_id="tenant-a", month_start=date(2026, 7, 1)
        )
        owner_b = repository.get_current_for_owner(
            ecosystem="confluent_cloud", tenant_id="tenant-b", month_start=date(2026, 7, 1)
        )
        missing = repository.get_current_for_owner(
            ecosystem="confluent_cloud", tenant_id="tenant-c", month_start=date(2026, 7, 1)
        )
    assert owner_a is not None and owner_a.revision_id == "revision-a"
    assert owner_b is not None and owner_b.revision_id == "revision-b"
    assert missing is None
    engine.dispose()


def test_table_hydration_revalidates_shared_revision_invariant(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    persistence = _persistence()
    models = import_module("core.preview.models")
    engine = create_engine(f"sqlite:///{tmp_path / 'hydrate.db'}")
    _create_revision_schema(engine)
    with Session(engine) as session:
        repository = persistence.SQLModelPreviewRevisionRepository(session)
        repository.replace_current(candidate=_candidate(), package=_package(), expected_current_revision_id=None)
        session.commit()

    calls: list[str] = []
    original = models.validate_preview_revision_invariant

    def capture(**kwargs: object) -> None:
        calls.append(str(kwargs["month"]))
        original(**kwargs)

    monkeypatch.setattr(persistence, "validate_preview_revision_invariant", capture)
    with Session(engine) as session:
        hydrated = persistence.SQLModelPreviewRevisionRepository(session).get_current_for_owner(
            ecosystem="confluent_cloud", tenant_id="tenant-1", month_start=date(2026, 7, 1)
        )

    assert hydrated is not None
    assert calls == ["2026-07"]
    engine.dispose()


def test_migration_024_upgrade_and_downgrade(tmp_path: Path) -> None:
    from alembic import command

    migrated_url = f"sqlite:///{tmp_path / 'migrated.db'}"
    command.upgrade(_alembic_config(migrated_url), "024")
    migrated = create_engine(migrated_url)
    migrated_inspector = inspect(migrated)
    assert "preview_revisions" in migrated_inspector.get_table_names()
    assert "ux_preview_revisions_owner_month_current" in {
        index["name"] for index in migrated_inspector.get_indexes("preview_revisions")
    }

    command.downgrade(_alembic_config(migrated_url), "023")
    assert "preview_revisions" not in inspect(migrated).get_table_names()
    migrated.dispose()


def test_migration_024_revision_chain_is_exact() -> None:
    migration = import_module("core.storage.migrations.versions.024_add_preview_revisions")

    assert migration.revision == "024"
    assert migration.down_revision == "023"


@pytest.mark.parametrize(
    ("url", "predicate"),
    [("sqlite://", "WHERE is_current = 1"), ("postgresql://", "WHERE is_current IS TRUE")],
)
def test_migration_024_compiles_cross_dialect_current_predicate(url: str, predicate: str) -> None:
    migration = import_module("core.storage.migrations.versions.024_add_preview_revisions")
    output = io.StringIO()
    context = MigrationContext.configure(url=url, opts={"as_sql": True, "output_buffer": output})

    with Operations(context).context(context):
        migration.upgrade()

    sql = output.getvalue()
    assert "CREATE UNIQUE INDEX ux_preview_revisions_owner_month_current" in sql
    assert predicate in sql
