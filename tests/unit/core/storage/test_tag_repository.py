from __future__ import annotations

from collections.abc import Generator
from datetime import UTC, datetime
from decimal import Decimal

import pytest
from sqlalchemy import Engine, inspect
from sqlmodel import Session, SQLModel, create_engine

from core.models.chargeback import ChargebackRow, CostType
from core.storage.backends.sqlmodel.repositories import SQLModelChargebackRepository, SQLModelTagRepository


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


class TestTagRepositoryAddTag:
    def test_add_tag_auto_generates_value(self, session: Session) -> None:
        dim_id = _make_dim(session)
        repo = SQLModelTagRepository(session)
        tag = repo.add_tag(dim_id, "env", "Production", "admin")
        session.commit()

        assert tag.tag_id is not None
        assert tag.tag_key == "env"
        assert tag.display_name == "Production"
        assert len(tag.tag_value) == 36  # UUID format

    def test_add_tag_different_keys_same_dimension(self, session: Session) -> None:
        dim_id = _make_dim(session)
        repo = SQLModelTagRepository(session)
        repo.add_tag(dim_id, "env", "Production", "admin")
        repo.add_tag(dim_id, "team", "Platform", "admin")
        session.commit()

        tags = repo.get_tags(dim_id)
        assert len(tags) == 2


class TestTagRepositoryFindWithSearch:
    def test_find_tags_with_search_by_key(self, session: Session) -> None:
        dim_id = _make_dim(session)
        repo = SQLModelTagRepository(session)
        repo.add_tag(dim_id, "env", "Production", "admin")
        repo.add_tag(dim_id, "team", "Platform", "admin")
        session.commit()

        items, total = repo.find_tags_for_tenant("eco", "t1", search="env")
        assert total == 1
        assert items[0].tag_key == "env"

    def test_find_tags_with_search_by_display_name(self, session: Session) -> None:
        dim_id = _make_dim(session)
        repo = SQLModelTagRepository(session)
        repo.add_tag(dim_id, "env", "Production Environment", "admin")
        repo.add_tag(dim_id, "team", "Platform Team", "admin")
        session.commit()

        items, total = repo.find_tags_for_tenant("eco", "t1", search="platform")
        assert total == 1
        assert items[0].display_name == "Platform Team"

    def test_find_tags_with_search_by_value(self, session: Session) -> None:
        dim_id = _make_dim(session)
        repo = SQLModelTagRepository(session)
        tag = repo.add_tag(dim_id, "env", "Production", "admin")
        # Search by partial tag_value (UUID prefix is unpredictable — search by known prefix)
        session.commit()

        items, total = repo.find_tags_for_tenant("eco", "t1", search=tag.tag_value[:8])
        assert total == 1

    def test_find_tags_no_search_returns_all(self, session: Session) -> None:
        dim_id = _make_dim(session)
        repo = SQLModelTagRepository(session)
        repo.add_tag(dim_id, "env", "Production", "admin")
        repo.add_tag(dim_id, "team", "Platform", "admin")
        session.commit()

        items, total = repo.find_tags_for_tenant("eco", "t1")
        assert total == 2


class TestTagRepositoryUpdateDisplayName:
    def test_update_display_name(self, session: Session) -> None:
        dim_id = _make_dim(session)
        repo = SQLModelTagRepository(session)
        tag = repo.add_tag(dim_id, "env", "Old Name", "admin")
        session.commit()

        original_value = tag.tag_value
        updated = repo.update_display_name(tag.tag_id, "New Name")  # type: ignore[arg-type]
        session.commit()

        assert updated.display_name == "New Name"
        assert updated.tag_value == original_value  # immutable

    def test_update_display_name_not_found(self, session: Session) -> None:
        repo = SQLModelTagRepository(session)
        with pytest.raises(KeyError):
            repo.update_display_name(99999, "New Name")


class TestTagRepositoryFindByDimensionAndKey:
    def test_find_by_dimension_and_key_found(self, session: Session) -> None:
        dim_id = _make_dim(session)
        repo = SQLModelTagRepository(session)
        repo.add_tag(dim_id, "env", "Production", "admin")
        session.commit()

        found = repo.find_by_dimension_and_key(dim_id, "env")
        assert found is not None
        assert found.tag_key == "env"

    def test_find_by_dimension_and_key_not_found(self, session: Session) -> None:
        dim_id = _make_dim(session)
        repo = SQLModelTagRepository(session)
        session.commit()

        found = repo.find_by_dimension_and_key(dim_id, "nonexistent")
        assert found is None


class TestTagUniqueConstraint:
    def test_unique_constraint_enforced(self, session: Session) -> None:
        from sqlalchemy.exc import IntegrityError

        dim_id = _make_dim(session)
        repo = SQLModelTagRepository(session)
        repo.add_tag(dim_id, "env", "Production", "admin")
        session.commit()

        # Try to add same key to same dimension — flush raises on duplicate
        with pytest.raises(IntegrityError):
            repo.add_tag(dim_id, "env", "Staging", "admin")

    def test_same_key_different_dimensions_allowed(self, session: Session) -> None:
        dim_id1 = _make_dim(session, ecosystem="eco", tenant="t1")
        # Create a second dimension with different identity
        row2 = ChargebackRow(
            ecosystem="eco",
            tenant_id="t1",
            timestamp=datetime(2026, 2, 15, tzinfo=UTC),
            resource_id="r1",
            product_category="compute",
            product_type="flink",
            identity_id="user-2",
            cost_type=CostType.USAGE,
            amount=Decimal("5.00"),
        )
        cb_repo = SQLModelChargebackRepository(session)
        result2 = cb_repo.upsert(row2)
        session.commit()
        dim_id2 = result2.dimension_id
        assert dim_id1 != dim_id2

        repo = SQLModelTagRepository(session)
        repo.add_tag(dim_id1, "env", "Production", "admin")
        repo.add_tag(dim_id2, "env", "Staging", "admin")
        session.commit()  # Should not raise


class TestTagTableColumn:
    def test_display_name_column_exists(self, engine: Engine) -> None:
        inspector = inspect(engine)
        cols = {c["name"] for c in inspector.get_columns("custom_tags")}
        assert "display_name" in cols

    def test_unique_constraint_on_dimension_key(self, engine: Engine) -> None:
        inspector = inspect(engine)
        uqs = inspector.get_unique_constraints("custom_tags")
        uq_names = [u["name"] for u in uqs]
        assert "uq_custom_tag_dimension_key" in uq_names


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
