from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from importlib import import_module
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import create_engine, delete, update
from sqlmodel import Session, SQLModel, select

from tests.unit.core.preview.test_revision_models import _candidate, _package, _revision


def _persistence() -> Any:
    return import_module("core.preview.persistence")


def _schema(engine: Any) -> None:
    persistence = _persistence()
    SQLModel.metadata.create_all(engine, tables=[persistence.PreviewRevisionTable.__table__])


def _publish_chain(session: Session) -> Any:
    persistence = _persistence()
    repository = persistence.SQLModelPreviewRevisionRepository(session)
    prior: str | None = None
    for revision_id, published_at in (
        ("revision-a", datetime(2026, 8, 4, 10, tzinfo=UTC)),
        ("revision-b", datetime(2026, 8, 4, 11, tzinfo=UTC)),
        ("revision-c", datetime(2026, 8, 4, 11, tzinfo=UTC)),
    ):
        repository.replace_current(
            candidate=_candidate(
                revision_id=revision_id,
                published_at=published_at,
                supersedes_revision_id=prior,
            ),
            package=replace(_package(), storage_key=revision_id),
            expected_current_revision_id=prior,
        )
        prior = revision_id
    session.commit()
    return repository


def test_revision_page_and_retention_candidate_validate_invariants() -> None:
    persistence = _persistence()
    revision = _revision()
    page = persistence.PreviewRevisionPage(items=(revision,), next_cursor=revision.revision_id)
    candidate = persistence.PreviewRetentionCandidate(
        revision_id="revision-1",
        ecosystem="confluent_cloud",
        tenant_id="tenant-1",
        storage_key="revision-1",
        retention_pending_at=datetime(2026, 8, 5, tzinfo=UTC),
    )

    assert page.items == (revision,)
    assert candidate.retention_pending_at.tzinfo is UTC

    with pytest.raises(ValueError):
        persistence.PreviewRevisionPage(items=[revision], next_cursor=None)
    with pytest.raises(ValueError):
        persistence.PreviewRevisionPage(items=(revision,), next_cursor="revision-other")
    with pytest.raises(ValueError):
        replace(candidate, retention_pending_at=datetime(2026, 8, 5))


def test_visible_history_is_newest_first_keyset_paginated_and_owner_scoped(tmp_path: Path) -> None:
    engine = create_engine(f"sqlite:///{tmp_path / 'history.db'}")
    _schema(engine)
    with Session(engine) as session:
        repository = _publish_chain(session)

        first = repository.list_for_owner_month(
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
            month_start=date(2026, 7, 1),
            limit=2,
            cursor_revision_id=None,
        )
        second = repository.list_for_owner_month(
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
            month_start=date(2026, 7, 1),
            limit=2,
            cursor_revision_id=first.next_cursor,
        )
        foreign = repository.list_for_owner_month(
            ecosystem="confluent_cloud",
            tenant_id="tenant-other",
            month_start=date(2026, 7, 1),
            limit=20,
            cursor_revision_id=None,
        )

    assert [item.revision_id for item in first.items] == ["revision-c", "revision-b"]
    assert first.next_cursor == "revision-b"
    assert [item.revision_id for item in second.items] == ["revision-a"]
    assert second.next_cursor is None
    assert foreign.items == ()
    engine.dispose()


@pytest.mark.parametrize("state", ["missing", "foreign", "wrong-month", "pending", "removed"])
def test_invalid_or_hidden_history_cursor_raises_one_cursor_error(tmp_path: Path, state: str) -> None:
    persistence = _persistence()
    engine = create_engine(f"sqlite:///{tmp_path / f'{state}.db'}")
    _schema(engine)
    with Session(engine) as session:
        repository = _publish_chain(session)
        cursor_revision_id = "revision-missing"
        if state in {"foreign", "wrong-month"}:
            cursor_revision_id = f"revision-{state}"
            repository.replace_current(
                candidate=_candidate(
                    revision_id=cursor_revision_id,
                    tenant_id="tenant-other",
                ),
                package=replace(_package(), storage_key=cursor_revision_id),
                expected_current_revision_id=None,
            )
            session.commit()
            if state == "wrong-month":
                session.execute(
                    update(persistence.PreviewRevisionTable)
                    .where(persistence.PreviewRevisionTable.revision_id == cursor_revision_id)
                    .values(
                        tenant_id="tenant-1",
                        month_start=date(2026, 6, 1),
                        month_end=date(2026, 7, 1),
                    )
                )
                session.commit()
        elif state == "pending":
            cursor_revision_id = "revision-a"
            repository.mark_retention_due(
                ecosystem="confluent_cloud",
                tenant_id="tenant-1",
                cutoff_date=date(2026, 8, 1),
                pending_at=datetime(2026, 8, 5, tzinfo=UTC),
                limit=1,
            )
            session.commit()
        elif state == "removed":
            cursor_revision_id = "revision-a"
            session.execute(
                delete(persistence.PreviewRevisionTable).where(
                    persistence.PreviewRevisionTable.revision_id == cursor_revision_id
                )
            )
            session.commit()

        persisted = session.exec(
            select(persistence.PreviewRevisionTable).where(
                persistence.PreviewRevisionTable.revision_id == cursor_revision_id
            )
        ).first()
        assert (persisted is None) is (state in {"missing", "removed"})
        with pytest.raises(persistence.PreviewRevisionCursorError, match="cursor is invalid"):
            repository.list_for_owner_month(
                ecosystem="confluent_cloud",
                tenant_id="tenant-1",
                month_start=date(2026, 7, 1),
                limit=20,
                cursor_revision_id=cursor_revision_id,
            )
    engine.dispose()


def test_pending_current_is_hidden_from_reads_but_visible_to_publication(tmp_path: Path) -> None:
    persistence = _persistence()
    engine = create_engine(f"sqlite:///{tmp_path / 'pending-current.db'}")
    _schema(engine)
    pending_at = datetime(2026, 8, 5, 1, 2, 3, 4, tzinfo=UTC)
    with Session(engine) as session:
        repository = _publish_chain(session)
        claimed = repository.mark_retention_due(
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
            cutoff_date=date(2026, 8, 1),
            pending_at=pending_at,
            limit=100,
        )
        session.commit()

    with Session(engine) as session:
        repository = persistence.SQLModelPreviewRevisionRepository(session)
        visible = repository.get_current_for_owner(
            ecosystem="confluent_cloud", tenant_id="tenant-1", month_start=date(2026, 7, 1)
        )
        publication = repository.get_current_for_publication(
            ecosystem="confluent_cloud", tenant_id="tenant-1", month_start=date(2026, 7, 1)
        )
        direct = repository.get_for_owner(ecosystem="confluent_cloud", tenant_id="tenant-1", revision_id="revision-c")
        history = repository.list_for_owner_month(
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
            month_start=date(2026, 7, 1),
            limit=20,
            cursor_revision_id=None,
        )

    assert len(claimed) == 3
    assert visible is None
    assert direct is None
    assert history.items == ()
    assert publication is not None
    assert publication.revision_id == "revision-c"
    assert publication.retention_pending_at == pending_at
    assert publication.retention_pending_at.tzinfo is UTC
    engine.dispose()


def test_direct_lookup_masks_distinct_foreign_pending_and_removed_states(tmp_path: Path) -> None:
    persistence = _persistence()
    engine = create_engine(f"sqlite:///{tmp_path / 'direct-masking.db'}")
    _schema(engine)
    with Session(engine) as session:
        repository = _publish_chain(session)
        repository.replace_current(
            candidate=_candidate(
                revision_id="revision-foreign",
                tenant_id="tenant-other",
            ),
            package=replace(_package(), storage_key="revision-foreign"),
            expected_current_revision_id=None,
        )
        session.commit()
        repository.mark_retention_due(
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
            cutoff_date=date(2026, 8, 1),
            pending_at=datetime(2026, 8, 5, tzinfo=UTC),
            limit=1,
        )
        session.execute(
            delete(persistence.PreviewRevisionTable).where(persistence.PreviewRevisionTable.revision_id == "revision-b")
        )
        session.commit()

        raw = {row.revision_id: row for row in session.exec(select(persistence.PreviewRevisionTable)).all()}
        assert raw["revision-foreign"].tenant_id == "tenant-other"
        assert raw["revision-a"].retention_pending_at is not None
        assert "revision-b" not in raw
        assert (
            repository.get_for_owner(
                ecosystem="confluent_cloud",
                tenant_id="tenant-1",
                revision_id="revision-foreign",
            )
            is None
        )
        assert (
            repository.get_for_owner(
                ecosystem="confluent_cloud",
                tenant_id="tenant-1",
                revision_id="revision-a",
            )
            is None
        )
        assert (
            repository.get_for_owner(
                ecosystem="confluent_cloud",
                tenant_id="tenant-1",
                revision_id="revision-b",
            )
            is None
        )
    engine.dispose()


@pytest.mark.parametrize("mismatch", ["owner", "storage-key", "pending-at"])
def test_retention_claim_uses_exact_month_end_boundary_and_guarded_finalization(
    tmp_path: Path,
    mismatch: str,
) -> None:
    persistence = _persistence()
    engine = create_engine(f"sqlite:///{tmp_path / 'retention-boundary.db'}")
    _schema(engine)
    pending_at = datetime(2026, 8, 5, tzinfo=UTC)
    with Session(engine) as session:
        repository = _publish_chain(session)
        candidates = repository.mark_retention_due(
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
            cutoff_date=date(2026, 8, 1),
            pending_at=pending_at,
            limit=2,
        )
        session.commit()

    assert [item.revision_id for item in candidates] == ["revision-a", "revision-b"]
    assert all(item.retention_pending_at == pending_at for item in candidates)

    with Session(engine) as session:
        repository = persistence.SQLModelPreviewRevisionRepository(session)
        changes = {
            "owner": {"tenant_id": "tenant-other"},
            "storage-key": {"storage_key": "wrong-key"},
            "pending-at": {"retention_pending_at": pending_at + timedelta(microseconds=1)},
        }
        wrong = replace(candidates[0], **changes[mismatch])
        assert repository.delete_retention_pending(candidate=wrong) is False
        retry_at = pending_at + timedelta(microseconds=1)
        assert repository.defer_retention_pending(candidate=candidates[0], retry_at=retry_at) is True
        session.commit()

    with Session(engine) as session:
        repository = persistence.SQLModelPreviewRevisionRepository(session)
        pending = repository.list_retention_pending(ecosystem="confluent_cloud", tenant_id="tenant-1", limit=100)
    assert [item.revision_id for item in pending] == ["revision-b", "revision-a"]
    assert pending[-1].retention_pending_at == pending_at + timedelta(microseconds=1)
    engine.dispose()


def test_retention_claim_protects_rows_one_day_inside_policy_and_is_owner_scoped(tmp_path: Path) -> None:
    engine = create_engine(f"sqlite:///{tmp_path / 'protected.db'}")
    _schema(engine)
    with Session(engine) as session:
        repository = _publish_chain(session)
        protected = repository.mark_retention_due(
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
            cutoff_date=date(2026, 7, 31),
            pending_at=datetime(2026, 8, 5, tzinfo=UTC),
            limit=100,
        )
        foreign = repository.mark_retention_due(
            ecosystem="confluent_cloud",
            tenant_id="tenant-other",
            cutoff_date=date(2026, 8, 1),
            pending_at=datetime(2026, 8, 5, tzinfo=UTC),
            limit=100,
        )
        session.commit()

    assert protected == ()
    assert foreign == ()
    engine.dispose()


def test_repeated_bounded_claims_never_return_the_same_row_twice(tmp_path: Path) -> None:
    engine = create_engine(f"sqlite:///{tmp_path / 'bounded-claims.db'}")
    _schema(engine)
    with Session(engine) as session:
        repository = _publish_chain(session)
        first = repository.mark_retention_due(
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
            cutoff_date=date(2026, 8, 1),
            pending_at=datetime(2026, 8, 5, tzinfo=UTC),
            limit=2,
        )
        session.commit()
        second = repository.mark_retention_due(
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
            cutoff_date=date(2026, 8, 1),
            pending_at=datetime(2026, 8, 5, 1, tzinfo=UTC),
            limit=2,
        )
        session.commit()

    assert [item.revision_id for item in first] == ["revision-a", "revision-b"]
    assert [item.revision_id for item in second] == ["revision-c"]
    assert {item.revision_id for item in first}.isdisjoint(item.revision_id for item in second)
    engine.dispose()


def test_concurrent_double_claim_returns_every_row_exactly_once(tmp_path: Path) -> None:
    engine = create_engine(f"sqlite:///{tmp_path / 'concurrent-claims.db'}")
    _schema(engine)
    with Session(engine) as session:
        _publish_chain(session)
    barrier = threading.Barrier(2)

    def claim() -> tuple[str, ...]:
        with Session(engine) as session:
            repository = _persistence().SQLModelPreviewRevisionRepository(session)
            barrier.wait(timeout=5)
            claimed = repository.mark_retention_due(
                ecosystem="confluent_cloud",
                tenant_id="tenant-1",
                cutoff_date=date(2026, 8, 1),
                pending_at=datetime(2026, 8, 5, tzinfo=UTC),
                limit=100,
            )
            session.commit()
            return tuple(candidate.revision_id for candidate in claimed)

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = tuple(pool.map(lambda _index: claim(), range(2)))

    flattened = [revision_id for result in results for revision_id in result]
    assert sorted(flattened) == ["revision-a", "revision-b", "revision-c"]
    assert set(results) == {(), ("revision-a", "revision-b", "revision-c")}
    engine.dispose()


def test_bounded_claim_selects_only_ids_and_uses_one_guarded_bulk_update(
    tmp_path: Path,
) -> None:
    engine = create_engine(f"sqlite:///{tmp_path / 'bulk-claim.db'}")
    _schema(engine)
    with Session(engine) as session:
        repository = _publish_chain(session)
        exec_statements: list[Any] = []
        execute_statements: list[Any] = []
        original_exec = session.exec
        original_execute = session.execute

        def capture_exec(statement: Any, *args: Any, **kwargs: Any) -> Any:
            exec_statements.append(statement)
            return original_exec(statement, *args, **kwargs)

        def capture_execute(statement: Any, *args: Any, **kwargs: Any) -> Any:
            execute_statements.append(statement)
            return original_execute(statement, *args, **kwargs)

        session.exec = capture_exec  # type: ignore[method-assign]
        session.execute = capture_execute  # type: ignore[method-assign]
        claimed = repository.mark_retention_due(
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
            cutoff_date=date(2026, 8, 1),
            pending_at=datetime(2026, 8, 5, tzinfo=UTC),
            limit=100,
        )

    assert {item.revision_id for item in claimed} == {
        "revision-a",
        "revision-b",
        "revision-c",
    }
    assert len(exec_statements) == 2
    assert [column.name for column in exec_statements[0].selected_columns] == ["revision_id"]
    assert len(execute_statements) == 1
    bulk_sql = str(
        execute_statements[0].compile(
            engine,
            compile_kwargs={"literal_binds": True},
        )
    )
    assert bulk_sql.startswith("UPDATE preview_revisions SET retention_pending_at=")
    assert "preview_revisions.ecosystem = 'confluent_cloud'" in bulk_sql
    assert "preview_revisions.tenant_id = 'tenant-1'" in bulk_sql
    assert "preview_revisions.month_end <= '2026-08-01'" in bulk_sql
    assert "revision_id IN ('revision-a', 'revision-b', 'revision-c')" in bulk_sql
    assert "retention_pending_at IS NULL" in bulk_sql
    engine.dispose()


def test_current_partial_unique_index_still_reserves_pending_current(tmp_path: Path) -> None:
    persistence = _persistence()
    engine = create_engine(f"sqlite:///{tmp_path / 'pending-slot.db'}")
    _schema(engine)
    with Session(engine) as session:
        repository = _publish_chain(session)
        repository.mark_retention_due(
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
            cutoff_date=date(2026, 8, 1),
            pending_at=datetime(2026, 8, 5, tzinfo=UTC),
            limit=100,
        )
        session.commit()

    with Session(engine) as session:
        rows = session.exec(select(persistence.PreviewRevisionTable)).all()
        current = next(row for row in rows if row.is_current)
        assert current.retention_pending_at is not None
    engine.dispose()
