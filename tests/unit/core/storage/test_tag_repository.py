from __future__ import annotations

from collections.abc import Generator
from datetime import UTC, datetime
from decimal import Decimal

import pytest
from sqlalchemy import Engine, inspect
from sqlmodel import Session, SQLModel, create_engine

from core.models.chargeback import ChargebackRow, CostType
from core.storage.backends.sqlmodel.repositories import SQLModelChargebackRepository, SQLModelEntityTagRepository


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


def _make_dim(session: Session, ecosystem: str = "eco", tenant: str = "t1") -> int:
    """Insert a dimension and fact row, return dimension_id."""
    row = ChargebackRow(
        ecosystem=ecosystem,
        tenant_id=tenant,
        timestamp=datetime(2026, 2, 15, tzinfo=UTC),
        resource_id="r1",
        product_category="compute",
        product_type="kafka",
        identity_id="user-1",
        cost_type=CostType.USAGE,
        amount=Decimal("10.00"),
    )
    repo = SQLModelChargebackRepository(session)
    result = repo.upsert(row)
    session.commit()
    assert result.dimension_id is not None
    return result.dimension_id


class TestEntityTagRepositoryAddTag:
    def test_add_tag_stores_provided_value(self, session: Session) -> None:
        repo = SQLModelEntityTagRepository(session)
        tag = repo.add_tag("t1", "resource", "r1", "env", "Production", "admin")
        session.commit()

        assert tag.tag_id is not None
        assert tag.tag_key == "env"
        assert tag.tag_value == "Production"
        assert tag.entity_type == "resource"
        assert tag.entity_id == "r1"

    def test_add_tag_different_keys_same_entity(self, session: Session) -> None:
        repo = SQLModelEntityTagRepository(session)
        repo.add_tag("t1", "resource", "r1", "env", "prod", "admin")
        repo.add_tag("t1", "resource", "r1", "team", "platform", "admin")
        session.commit()

        tags = repo.get_tags("t1", "resource", "r1")
        assert len(tags) == 2


class TestEntityTagRepositoryFindWithFilter:
    def test_find_tags_with_tag_key_filter(self, session: Session) -> None:
        repo = SQLModelEntityTagRepository(session)
        repo.add_tag("t1", "resource", "r1", "env", "prod", "admin")
        repo.add_tag("t1", "resource", "r1", "team", "platform", "admin")
        session.commit()

        items, total = repo.find_tags_for_tenant("t1", tag_key="env")
        assert total == 1
        assert items[0].tag_key == "env"

    def test_find_tags_with_entity_type_filter(self, session: Session) -> None:
        repo = SQLModelEntityTagRepository(session)
        repo.add_tag("t1", "resource", "r1", "env", "prod", "admin")
        repo.add_tag("t1", "identity", "u1", "team", "platform", "admin")
        session.commit()

        items, total = repo.find_tags_for_tenant("t1", entity_type="resource")
        assert total == 1
        assert items[0].entity_type == "resource"

    def test_find_tags_no_filter_returns_all(self, session: Session) -> None:
        repo = SQLModelEntityTagRepository(session)
        repo.add_tag("t1", "resource", "r1", "env", "prod", "admin")
        repo.add_tag("t1", "resource", "r1", "team", "platform", "admin")
        session.commit()

        items, total = repo.find_tags_for_tenant("t1")
        assert total == 2


class TestEntityTagRepositoryUpdate:
    def test_update_tag_value(self, session: Session) -> None:
        repo = SQLModelEntityTagRepository(session)
        tag = repo.add_tag("t1", "resource", "r1", "env", "prod", "admin")
        session.commit()

        assert tag.tag_id is not None
        updated = repo.update_tag(tag.tag_id, "staging")
        session.commit()

        assert updated.tag_value == "staging"
        assert updated.tag_key == "env"

    def test_update_tag_not_found_raises(self, session: Session) -> None:
        repo = SQLModelEntityTagRepository(session)
        with pytest.raises(KeyError):
            repo.update_tag(99999, "new-value")


class TestEntityTagRepositoryFindByEntity:
    def test_find_by_entity_found(self, session: Session) -> None:
        repo = SQLModelEntityTagRepository(session)
        repo.add_tag("t1", "resource", "r1", "env", "prod", "admin")
        session.commit()

        tags = repo.get_tags("t1", "resource", "r1")
        assert len(tags) == 1
        assert tags[0].tag_key == "env"

    def test_find_by_entity_not_found(self, session: Session) -> None:
        repo = SQLModelEntityTagRepository(session)
        session.commit()

        tags = repo.get_tags("t1", "resource", "nonexistent")
        assert tags == []


class TestEntityTagUniqueConstraint:
    def test_unique_constraint_enforced(self, session: Session) -> None:
        from sqlalchemy.exc import IntegrityError

        repo = SQLModelEntityTagRepository(session)
        repo.add_tag("t1", "resource", "r1", "env", "prod", "admin")
        session.commit()

        with pytest.raises(IntegrityError):
            repo.add_tag("t1", "resource", "r1", "env", "staging", "admin")

    def test_same_key_different_entities_allowed(self, session: Session) -> None:
        repo = SQLModelEntityTagRepository(session)
        repo.add_tag("t1", "resource", "r1", "env", "prod", "admin")
        repo.add_tag("t1", "resource", "r2", "env", "staging", "admin")
        session.commit()  # Should not raise


class TestTagsTableSchema:
    def test_tags_table_exists(self, engine: Engine) -> None:
        inspector = inspect(engine)
        table_names = inspector.get_table_names()
        assert "tags" in table_names

    def test_unique_constraint_on_entity_key(self, engine: Engine) -> None:
        inspector = inspect(engine)
        uqs = inspector.get_unique_constraints("tags")
        uq_names = [u["name"] for u in uqs]
        assert "uq_tags_entity_key" in uq_names


class TestChargebackRepositoryGetDimensionsBatch:
    def test_get_dimensions_batch_returns_dict(self, session: Session) -> None:
        dim_id = _make_dim(session)
        repo = SQLModelChargebackRepository(session)

        result = repo.get_dimensions_batch([dim_id])
        assert dim_id in result
        assert result[dim_id].dimension_id == dim_id

    def test_get_dimensions_batch_missing_ids_omitted(self, session: Session) -> None:
        dim_id = _make_dim(session)
        repo = SQLModelChargebackRepository(session)

        result = repo.get_dimensions_batch([dim_id, 99999])
        assert dim_id in result
        assert 99999 not in result

    def test_get_dimensions_batch_empty_input(self, session: Session) -> None:
        repo = SQLModelChargebackRepository(session)
        result = repo.get_dimensions_batch([])
        assert result == {}


class TestChargebackRepositoryFindDimensionIdsByFilters:
    def test_find_dimension_ids_by_filters_returns_ids(self, session: Session) -> None:
        dim_id = _make_dim(session)
        repo = SQLModelChargebackRepository(session)

        from datetime import UTC, datetime

        ids = repo.find_dimension_ids_by_filters(
            ecosystem="eco",
            tenant_id="t1",
            start=datetime(2026, 2, 1, tzinfo=UTC),
            end=datetime(2026, 2, 28, tzinfo=UTC),
        )
        assert dim_id in ids

    def test_find_dimension_ids_by_filters_no_match(self, session: Session) -> None:
        _make_dim(session)
        repo = SQLModelChargebackRepository(session)

        from datetime import UTC, datetime

        ids = repo.find_dimension_ids_by_filters(
            ecosystem="eco",
            tenant_id="t1",
            start=datetime(2025, 1, 1, tzinfo=UTC),
            end=datetime(2025, 1, 31, tzinfo=UTC),
        )
        assert ids == []

    def test_find_dimension_ids_respects_identity_filter(self, session: Session) -> None:
        cb_repo = SQLModelChargebackRepository(session)
        row1 = ChargebackRow(
            ecosystem="eco",
            tenant_id="t1",
            timestamp=datetime(2026, 2, 15, tzinfo=UTC),
            resource_id="r1",
            product_category="compute",
            product_type="kafka",
            identity_id="user-A",
            cost_type=CostType.USAGE,
            amount=Decimal("10.00"),
        )
        row2 = ChargebackRow(
            ecosystem="eco",
            tenant_id="t1",
            timestamp=datetime(2026, 2, 15, tzinfo=UTC),
            resource_id="r1",
            product_category="compute",
            product_type="flink",
            identity_id="user-B",
            cost_type=CostType.USAGE,
            amount=Decimal("5.00"),
        )
        r1 = cb_repo.upsert(row1)
        r2 = cb_repo.upsert(row2)
        session.commit()

        ids = cb_repo.find_dimension_ids_by_filters(
            ecosystem="eco",
            tenant_id="t1",
            start=datetime(2026, 2, 1, tzinfo=UTC),
            end=datetime(2026, 2, 28, tzinfo=UTC),
            identity_id="user-A",
        )
        assert r1.dimension_id in ids
        assert r2.dimension_id not in ids
