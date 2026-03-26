from __future__ import annotations

from collections.abc import Generator
from typing import TYPE_CHECKING

import pytest
from sqlmodel import Session, SQLModel, create_engine

if TYPE_CHECKING:
    from sqlalchemy import Engine

from core.storage.backends.sqlmodel.repositories import SQLModelEntityTagRepository

# ---------------------------------------------------------------------------
# DB fixtures for integration tests
# ---------------------------------------------------------------------------


@pytest.fixture
def engine() -> Generator[Engine]:
    eng = create_engine("sqlite://", echo=False)
    SQLModel.metadata.create_all(eng)
    yield eng
    eng.dispose(close=True)


@pytest.fixture
def session(engine: Engine) -> Generator[Session]:
    with Session(engine) as s:
        yield s


# ---------------------------------------------------------------------------
# bulk_add_tags: correctness
# ---------------------------------------------------------------------------


class TestBulkAddTagsCorrectness:
    def test_bulk_create_three_items(self, session: Session) -> None:
        """Bulk add 3 distinct tags → created=3, updated=0, skipped=0."""
        repo = SQLModelEntityTagRepository(session)
        items = [
            {"entity_type": "resource", "entity_id": "r1", "tag_key": "env", "tag_value": "prod"},
            {"entity_type": "resource", "entity_id": "r2", "tag_key": "env", "tag_value": "prod"},
            {"entity_type": "resource", "entity_id": "r3", "tag_key": "env", "tag_value": "prod"},
        ]
        created, updated, skipped = repo.bulk_add_tags("t1", items, override_existing=False, created_by="admin")
        session.commit()
        assert created == 3
        assert updated == 0
        assert skipped == 0

    def test_bulk_same_call_twice_skips_all(self, session: Session) -> None:
        """Bulk add same tags twice → second call skips all 3."""
        repo = SQLModelEntityTagRepository(session)
        items = [
            {"entity_type": "resource", "entity_id": "r1", "tag_key": "env", "tag_value": "prod"},
            {"entity_type": "resource", "entity_id": "r2", "tag_key": "env", "tag_value": "prod"},
            {"entity_type": "resource", "entity_id": "r3", "tag_key": "env", "tag_value": "prod"},
        ]
        repo.bulk_add_tags("t1", items, override_existing=False, created_by="admin")
        session.commit()
        created, updated, skipped = repo.bulk_add_tags("t1", items, override_existing=False, created_by="admin")
        session.commit()
        assert created == 0
        assert updated == 0
        assert skipped == 3

    def test_bulk_override_existing_updates_all(self, session: Session) -> None:
        """Bulk add with override_existing=True → updated=3."""
        repo = SQLModelEntityTagRepository(session)
        items_create = [
            {"entity_type": "resource", "entity_id": "r1", "tag_key": "env", "tag_value": "prod"},
            {"entity_type": "resource", "entity_id": "r2", "tag_key": "env", "tag_value": "prod"},
            {"entity_type": "resource", "entity_id": "r3", "tag_key": "env", "tag_value": "prod"},
        ]
        repo.bulk_add_tags("t1", items_create, override_existing=False, created_by="admin")
        session.commit()

        items_update = [
            {"entity_type": "resource", "entity_id": "r1", "tag_key": "env", "tag_value": "staging"},
            {"entity_type": "resource", "entity_id": "r2", "tag_key": "env", "tag_value": "staging"},
            {"entity_type": "resource", "entity_id": "r3", "tag_key": "env", "tag_value": "staging"},
        ]
        created, updated, skipped = repo.bulk_add_tags("t1", items_update, override_existing=True, created_by="admin")
        session.commit()
        assert created == 0
        assert updated == 3
        assert skipped == 0

    def test_bulk_mixed_create_and_skip(self, session: Session) -> None:
        """3 existing + 2 new → created=2, skipped=3, updated=0."""
        repo = SQLModelEntityTagRepository(session)
        existing = [
            {"entity_type": "resource", "entity_id": f"r{i}", "tag_key": "env", "tag_value": "prod"} for i in range(3)
        ]
        repo.bulk_add_tags("t1", existing, override_existing=False, created_by="admin")
        session.commit()

        all_items = [
            {"entity_type": "resource", "entity_id": f"r{i}", "tag_key": "env", "tag_value": "prod"} for i in range(5)
        ]
        created, updated, skipped = repo.bulk_add_tags("t1", all_items, override_existing=False, created_by="admin")
        session.commit()
        assert created == 2
        assert updated == 0
        assert skipped == 3


# ---------------------------------------------------------------------------
# bulk_add_tags: chunking — 600 > _CHUNK_SIZE=500
# ---------------------------------------------------------------------------


class TestBulkAddTagsChunking:
    def test_bulk_add_600_items_no_sql_error(self, session: Session) -> None:
        """600 distinct entity_ids cross the chunk boundary; no OperationalError raised."""
        repo = SQLModelEntityTagRepository(session)
        items = [
            {"entity_type": "resource", "entity_id": f"r{i}", "tag_key": "env", "tag_value": f"val-{i}"}
            for i in range(600)
        ]
        # Must not raise OperationalError (SQLite param limit)
        created, updated, skipped = repo.bulk_add_tags("t1", items, override_existing=False, created_by="admin")
        session.commit()
        assert created == 600
        assert updated == 0
        assert skipped == 0
