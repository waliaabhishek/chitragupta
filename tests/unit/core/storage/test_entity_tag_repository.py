from __future__ import annotations

from collections.abc import Generator
from typing import TYPE_CHECKING

import pytest
from sqlalchemy.exc import IntegrityError
from sqlmodel import Session, SQLModel, create_engine

if TYPE_CHECKING:
    from sqlalchemy import Engine

from core.storage.backends.sqlmodel.repositories import SQLModelEntityTagRepository


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


class TestEntityTagRepositoryAddTag:
    def test_add_tag_returns_entity_tag(self, session: Session) -> None:
        repo = SQLModelEntityTagRepository(session)
        tag = repo.add_tag(
            tenant_id="t1",
            entity_type="resource",
            entity_id="r1",
            tag_key="env",
            tag_value="prod",
            created_by="admin",
        )
        session.commit()

        assert tag.tag_id is not None
        assert tag.tenant_id == "t1"
        assert tag.entity_type == "resource"
        assert tag.entity_id == "r1"
        assert tag.tag_key == "env"
        assert tag.tag_value == "prod"
        assert tag.created_by == "admin"

    def test_add_tag_different_keys_same_entity(self, session: Session) -> None:
        repo = SQLModelEntityTagRepository(session)
        repo.add_tag("t1", "resource", "r1", "env", "prod", "admin")
        repo.add_tag("t1", "resource", "r1", "team", "platform", "admin")
        session.commit()

        tags = repo.get_tags("t1", "resource", "r1")
        assert len(tags) == 2

    def test_add_tag_same_key_different_entities_allowed(self, session: Session) -> None:
        repo = SQLModelEntityTagRepository(session)
        repo.add_tag("t1", "resource", "r1", "env", "prod", "admin")
        repo.add_tag("t1", "resource", "r2", "env", "staging", "admin")
        session.commit()  # Should not raise

    def test_add_tag_same_key_different_entity_types_allowed(self, session: Session) -> None:
        repo = SQLModelEntityTagRepository(session)
        repo.add_tag("t1", "resource", "abc", "env", "prod", "admin")
        repo.add_tag("t1", "identity", "abc", "env", "staging", "admin")
        session.commit()  # Should not raise — different entity_type


class TestEntityTagRepositoryDuplicateKey:
    def test_duplicate_composite_key_raises_integrity_error(self, session: Session) -> None:
        repo = SQLModelEntityTagRepository(session)
        repo.add_tag("t1", "resource", "r1", "env", "prod", "admin")
        session.commit()

        with pytest.raises(IntegrityError):
            repo.add_tag("t1", "resource", "r1", "env", "staging", "admin")


class TestEntityTagRepositoryGetTags:
    def test_get_tags_empty_for_unknown_entity(self, session: Session) -> None:
        repo = SQLModelEntityTagRepository(session)
        tags = repo.get_tags("t1", "resource", "no-such-resource")
        assert tags == []

    def test_get_tags_returns_all_tags_for_entity(self, session: Session) -> None:
        repo = SQLModelEntityTagRepository(session)
        repo.add_tag("t1", "resource", "r1", "env", "prod", "admin")
        repo.add_tag("t1", "resource", "r1", "team", "platform", "admin")
        session.commit()

        tags = repo.get_tags("t1", "resource", "r1")
        assert len(tags) == 2
        keys = {t.tag_key for t in tags}
        assert keys == {"env", "team"}

    def test_get_tags_does_not_cross_entity_types(self, session: Session) -> None:
        repo = SQLModelEntityTagRepository(session)
        repo.add_tag("t1", "resource", "abc", "env", "prod", "admin")
        repo.add_tag("t1", "identity", "abc", "env", "staging", "admin")
        session.commit()

        resource_tags = repo.get_tags("t1", "resource", "abc")
        assert len(resource_tags) == 1
        assert resource_tags[0].tag_value == "prod"

        identity_tags = repo.get_tags("t1", "identity", "abc")
        assert len(identity_tags) == 1
        assert identity_tags[0].tag_value == "staging"

    def test_get_tags_does_not_cross_tenants(self, session: Session) -> None:
        repo = SQLModelEntityTagRepository(session)
        repo.add_tag("t1", "resource", "r1", "env", "prod", "admin")
        repo.add_tag("t2", "resource", "r1", "env", "staging", "admin")
        session.commit()

        t1_tags = repo.get_tags("t1", "resource", "r1")
        assert len(t1_tags) == 1
        assert t1_tags[0].tag_value == "prod"


class TestEntityTagRepositoryUpdateTag:
    def test_update_tag_value(self, session: Session) -> None:
        repo = SQLModelEntityTagRepository(session)
        tag = repo.add_tag("t1", "resource", "r1", "env", "prod", "admin")
        session.commit()

        updated = repo.update_tag(tag.tag_id, "staging")  # type: ignore[arg-type]
        session.commit()

        assert updated.tag_value == "staging"
        assert updated.tag_key == "env"

    def test_update_tag_preserves_key(self, session: Session) -> None:
        repo = SQLModelEntityTagRepository(session)
        tag = repo.add_tag("t1", "resource", "r1", "env", "prod", "admin")
        session.commit()

        updated = repo.update_tag(tag.tag_id, "staging")  # type: ignore[arg-type]
        assert updated.tag_key == "env"
        assert updated.entity_id == "r1"

    def test_update_tag_not_found_raises_key_error(self, session: Session) -> None:
        repo = SQLModelEntityTagRepository(session)
        with pytest.raises(KeyError):
            repo.update_tag(99999, "new-value")


class TestEntityTagRepositoryDeleteTag:
    def test_delete_tag_removes_it(self, session: Session) -> None:
        repo = SQLModelEntityTagRepository(session)
        tag = repo.add_tag("t1", "resource", "r1", "env", "prod", "admin")
        session.commit()

        repo.delete_tag(tag.tag_id)  # type: ignore[arg-type]
        session.commit()

        tags = repo.get_tags("t1", "resource", "r1")
        assert tags == []

    def test_delete_nonexistent_tag_no_error(self, session: Session) -> None:
        repo = SQLModelEntityTagRepository(session)
        repo.delete_tag(99999)  # Should not raise


class TestEntityTagRepositoryFindTagsForTenant:
    def test_find_tags_for_tenant_returns_all(self, session: Session) -> None:
        repo = SQLModelEntityTagRepository(session)
        repo.add_tag("t1", "resource", "r1", "env", "prod", "admin")
        repo.add_tag("t1", "identity", "u1", "team", "platform", "admin")
        session.commit()

        items, total = repo.find_tags_for_tenant("t1")
        assert total == 2

    def test_find_tags_for_tenant_pagination(self, session: Session) -> None:
        repo = SQLModelEntityTagRepository(session)
        for i in range(5):
            repo.add_tag("t1", "resource", f"r{i}", "env", "prod", "admin")
        session.commit()

        items, total = repo.find_tags_for_tenant("t1", limit=2, offset=0)
        assert total == 5
        assert len(items) == 2

        items2, total2 = repo.find_tags_for_tenant("t1", limit=2, offset=2)
        assert total2 == 5
        assert len(items2) == 2

    def test_find_tags_for_tenant_entity_type_filter(self, session: Session) -> None:
        repo = SQLModelEntityTagRepository(session)
        repo.add_tag("t1", "resource", "r1", "env", "prod", "admin")
        repo.add_tag("t1", "identity", "u1", "env", "staging", "admin")
        session.commit()

        items, total = repo.find_tags_for_tenant("t1", entity_type="resource")
        assert total == 1
        assert items[0].entity_type == "resource"

    def test_find_tags_for_tenant_tag_key_filter_case_insensitive(self, session: Session) -> None:
        repo = SQLModelEntityTagRepository(session)
        repo.add_tag("t1", "resource", "r1", "Environment", "prod", "admin")
        repo.add_tag("t1", "resource", "r2", "team", "platform", "admin")
        session.commit()

        items, total = repo.find_tags_for_tenant("t1", tag_key="environ")
        assert total == 1
        assert items[0].tag_key == "Environment"

    def test_find_tags_for_tenant_no_cross_tenant_leakage(self, session: Session) -> None:
        repo = SQLModelEntityTagRepository(session)
        repo.add_tag("t1", "resource", "r1", "env", "prod", "admin")
        repo.add_tag("t2", "resource", "r1", "env", "staging", "admin")
        session.commit()

        items, total = repo.find_tags_for_tenant("t1")
        assert total == 1
        assert items[0].tag_value == "prod"


class TestEntityTagRepositoryFindTagsForEntities:
    def test_find_tags_for_entities_returns_dict_keyed_by_entity_id(self, session: Session) -> None:
        repo = SQLModelEntityTagRepository(session)
        repo.add_tag("t1", "resource", "r1", "env", "prod", "admin")
        repo.add_tag("t1", "resource", "r2", "env", "staging", "admin")
        repo.add_tag("t1", "resource", "r1", "team", "platform", "admin")
        session.commit()

        result = repo.find_tags_for_entities("t1", "resource", ["r1", "r2"])
        assert "r1" in result
        assert "r2" in result
        assert len(result["r1"]) == 2
        assert len(result["r2"]) == 1

    def test_find_tags_for_entities_empty_input_returns_empty_dict(self, session: Session) -> None:
        repo = SQLModelEntityTagRepository(session)
        result = repo.find_tags_for_entities("t1", "resource", [])
        assert result == {}

    def test_find_tags_for_entities_missing_entity_omitted(self, session: Session) -> None:
        repo = SQLModelEntityTagRepository(session)
        repo.add_tag("t1", "resource", "r1", "env", "prod", "admin")
        session.commit()

        result = repo.find_tags_for_entities("t1", "resource", ["r1", "r-not-exists"])
        assert "r1" in result
        assert "r-not-exists" not in result

    def test_find_tags_for_entities_does_not_cross_entity_types(self, session: Session) -> None:
        repo = SQLModelEntityTagRepository(session)
        repo.add_tag("t1", "resource", "abc", "env", "prod", "admin")
        repo.add_tag("t1", "identity", "abc", "env", "staging", "admin")
        session.commit()

        resource_result = repo.find_tags_for_entities("t1", "resource", ["abc"])
        assert resource_result["abc"][0].tag_value == "prod"

        identity_result = repo.find_tags_for_entities("t1", "identity", ["abc"])
        assert identity_result["abc"][0].tag_value == "staging"


class TestEntityTagRepositoryBulkAddTags:
    def test_bulk_add_tags_creates_all(self, session: Session) -> None:
        repo = SQLModelEntityTagRepository(session)
        items = [
            {"entity_type": "resource", "entity_id": "r1", "tag_key": "env", "tag_value": "prod"},
            {"entity_type": "resource", "entity_id": "r2", "tag_key": "env", "tag_value": "staging"},
            {"entity_type": "identity", "entity_id": "u1", "tag_key": "team", "tag_value": "platform"},
        ]
        created, updated, skipped = repo.bulk_add_tags("t1", items, override_existing=False, created_by="admin")
        session.commit()

        assert created == 3
        assert updated == 0
        assert skipped == 0

    def test_bulk_add_tags_skips_existing_when_no_override(self, session: Session) -> None:
        repo = SQLModelEntityTagRepository(session)
        repo.add_tag("t1", "resource", "r1", "env", "prod", "admin")
        session.commit()

        items = [{"entity_type": "resource", "entity_id": "r1", "tag_key": "env", "tag_value": "staging"}]
        created, updated, skipped = repo.bulk_add_tags("t1", items, override_existing=False, created_by="admin")
        session.commit()

        assert created == 0
        assert updated == 0
        assert skipped == 1

    def test_bulk_add_tags_updates_when_override(self, session: Session) -> None:
        repo = SQLModelEntityTagRepository(session)
        repo.add_tag("t1", "resource", "r1", "env", "prod", "admin")
        session.commit()

        items = [{"entity_type": "resource", "entity_id": "r1", "tag_key": "env", "tag_value": "staging"}]
        created, updated, skipped = repo.bulk_add_tags("t1", items, override_existing=True, created_by="admin")
        session.commit()

        assert created == 0
        assert updated == 1
        assert skipped == 0

        tags = repo.get_tags("t1", "resource", "r1")
        assert tags[0].tag_value == "staging"
