from __future__ import annotations

from collections.abc import Generator
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING

import pytest
from sqlmodel import Session, SQLModel, create_engine

if TYPE_CHECKING:
    from sqlalchemy import Engine

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


def _make_dim(
    session: Session,
    *,
    identity_id: str = "user-1",
    product_type: str = "kafka",
    ecosystem: str = "eco",
    tenant: str = "t1",
    timestamp: datetime = datetime(2026, 2, 15, tzinfo=UTC),
) -> int:
    """Insert a dimension+fact row and return dimension_id."""
    row = ChargebackRow(
        ecosystem=ecosystem,
        tenant_id=tenant,
        timestamp=timestamp,
        resource_id="r1",
        product_category="compute",
        product_type=product_type,
        identity_id=identity_id,
        cost_type=CostType.USAGE,
        amount=Decimal("10.00"),
    )
    repo = SQLModelChargebackRepository(session)
    result = repo.upsert(row)
    session.commit()
    assert result.dimension_id is not None
    return result.dimension_id


def _upsert_fact(
    session: Session,
    *,
    identity_id: str,
    product_type: str,
    timestamp: datetime,
    ecosystem: str = "eco",
    tenant: str = "t1",
) -> None:
    """Insert an additional fact row for an existing or new dimension."""
    row = ChargebackRow(
        ecosystem=ecosystem,
        tenant_id=tenant,
        timestamp=timestamp,
        resource_id="r1",
        product_category="compute",
        product_type=product_type,
        identity_id=identity_id,
        cost_type=CostType.USAGE,
        amount=Decimal("5.00"),
    )
    repo = SQLModelChargebackRepository(session)
    repo.upsert(row)
    session.commit()


# ---------------------------------------------------------------------------
# Issue #1: delete_before — subquery replaces Python-level list
# ---------------------------------------------------------------------------


class TestDeleteBeforeSubquery:
    def test_delete_before_subquery(self, session: Session) -> None:
        """1000 dims × 2 facts each: older facts deleted, newer preserved, orphans cleaned."""
        old_ts = datetime(2026, 1, 1, tzinfo=UTC)
        new_ts = datetime(2026, 3, 1, tzinfo=UTC)
        cutoff = datetime(2026, 2, 1, tzinfo=UTC)

        repo = SQLModelChargebackRepository(session)

        # Create 1000 dimensions, each with one old fact and one new fact
        for i in range(1000):
            identity_id = f"user-{i}"
            # old fact
            repo.upsert(
                ChargebackRow(
                    ecosystem="eco",
                    tenant_id="t1",
                    timestamp=old_ts,
                    resource_id="r1",
                    product_category="compute",
                    product_type="kafka",
                    identity_id=identity_id,
                    cost_type=CostType.USAGE,
                    amount=Decimal("1.00"),
                )
            )
            # new fact — different timestamp triggers separate fact row
            repo.upsert(
                ChargebackRow(
                    ecosystem="eco",
                    tenant_id="t1",
                    timestamp=new_ts,
                    resource_id="r1",
                    product_category="compute",
                    product_type="kafka",
                    identity_id=identity_id,
                    cost_type=CostType.USAGE,
                    amount=Decimal("2.00"),
                )
            )
        session.commit()

        deleted = repo.delete_before("eco", "t1", cutoff)

        # Exactly 1000 old facts deleted
        assert deleted == 1000

        # All 1000 dimensions still exist (new facts keep them alive)
        # Dimensions with new facts must still be present
        rows, total = repo.find_by_filters(
            ecosystem="eco",
            tenant_id="t1",
            start=datetime(2026, 2, 28, tzinfo=UTC),
            end=datetime(2026, 3, 31, tzinfo=UTC),
        )
        assert total == 1000

        # No old facts remain
        old_rows, old_total = repo.find_by_filters(
            ecosystem="eco",
            tenant_id="t1",
            start=datetime(2025, 12, 1, tzinfo=UTC),
            end=datetime(2026, 1, 31, tzinfo=UTC),
        )
        assert old_total == 0

    def test_delete_before_empty_tenant(self, session: Session) -> None:
        """delete_before on a tenant with no dimensions returns 0 without error."""
        repo = SQLModelChargebackRepository(session)
        result = repo.delete_before("eco", "no-such-tenant", datetime(2026, 1, 1, tzinfo=UTC))
        assert result == 0

    def test_delete_before_orphan_cleanup(self, session: Session) -> None:
        """Dimensions whose only fact is deleted become orphans and are removed."""
        old_ts = datetime(2026, 1, 1, tzinfo=UTC)
        cutoff = datetime(2026, 2, 1, tzinfo=UTC)

        repo = SQLModelChargebackRepository(session)

        # 5 dimensions each with only an old fact (will become orphans)
        for i in range(5):
            repo.upsert(
                ChargebackRow(
                    ecosystem="eco",
                    tenant_id="t1",
                    timestamp=old_ts,
                    resource_id="r1",
                    product_category="compute",
                    product_type="kafka",
                    identity_id=f"orphan-{i}",
                    cost_type=CostType.USAGE,
                    amount=Decimal("1.00"),
                )
            )
        session.commit()

        deleted = repo.delete_before("eco", "t1", cutoff)
        assert deleted == 5

        # No rows should remain
        rows, total = repo.find_by_filters(
            ecosystem="eco",
            tenant_id="t1",
            start=datetime(2025, 12, 1, tzinfo=UTC),
            end=datetime(2026, 3, 31, tzinfo=UTC),
        )
        assert total == 0


# ---------------------------------------------------------------------------
# Issue #4: _overlay_tags chunking — 600 > _CHUNK_SIZE=500
# ---------------------------------------------------------------------------


class TestOverlayTagsChunking:
    def test_overlay_tags_chunking(self, session: Session) -> None:
        """600 rows with distinct dimension_ids cross the chunk boundary; no SQL error."""
        cb_repo = SQLModelChargebackRepository(session)
        tag_repo = SQLModelTagRepository(session)

        dim_ids: list[int] = []
        for i in range(600):
            result = cb_repo.upsert(
                ChargebackRow(
                    ecosystem="eco",
                    tenant_id="t1",
                    timestamp=datetime(2026, 2, 15, tzinfo=UTC),
                    resource_id="r1",
                    product_category="compute",
                    product_type="kafka",
                    identity_id=f"user-{i}",
                    cost_type=CostType.USAGE,
                    amount=Decimal("1.00"),
                )
            )
            session.commit()
            assert result.dimension_id is not None
            dim_ids.append(result.dimension_id)
            # Add a tag to each dimension
            tag_repo.add_tag(result.dimension_id, "env", f"env-{i}", "admin")
        session.commit()

        # Build ChargebackRow objects with dimension_id set
        rows = [
            ChargebackRow(
                ecosystem="eco",
                tenant_id="t1",
                timestamp=datetime(2026, 2, 15, tzinfo=UTC),
                resource_id="r1",
                product_category="compute",
                product_type="kafka",
                identity_id=f"user-{i}",
                cost_type=CostType.USAGE,
                amount=Decimal("1.00"),
                dimension_id=dim_ids[i],
            )
            for i in range(600)
        ]

        # Must not raise OperationalError (SQLite param limit)
        cb_repo._overlay_tags(rows)

        # All rows should have tags overlaid
        rows_with_tags = [r for r in rows if r.tags]
        assert len(rows_with_tags) == 600


# ---------------------------------------------------------------------------
# Issue #5: get_dimensions_batch chunking — 1100 > SQLite 999 param limit
# ---------------------------------------------------------------------------


class TestGetDimensionsBatchChunking:
    def test_get_dimensions_batch_chunking(self, session: Session) -> None:
        """1100 dimension IDs exceed SQLite param limit; chunking makes it transparent."""
        cb_repo = SQLModelChargebackRepository(session)

        dim_ids: list[int] = []
        for i in range(1100):
            result = cb_repo.upsert(
                ChargebackRow(
                    ecosystem="eco",
                    tenant_id="t1",
                    timestamp=datetime(2026, 2, 15, tzinfo=UTC),
                    resource_id="r1",
                    product_category="compute",
                    product_type="kafka",
                    identity_id=f"user-{i}",
                    cost_type=CostType.USAGE,
                    amount=Decimal("1.00"),
                )
            )
            session.commit()
            assert result.dimension_id is not None
            dim_ids.append(result.dimension_id)

        # With current code (no chunking) this would raise OperationalError on SQLite
        result_map = cb_repo.get_dimensions_batch(dim_ids)

        assert len(result_map) == 1100
        for dim_id in dim_ids:
            assert dim_id in result_map


# ---------------------------------------------------------------------------
# Issue #2: find_tags_by_dimensions_and_key — new batch method
# ---------------------------------------------------------------------------


class TestFindTagsByDimensionsAndKey:
    def test_find_tags_by_dimensions_and_key_batch(self, session: Session) -> None:
        """Returns dict of {dimension_id: CustomTag} for dims that have matching key."""
        cb_repo = SQLModelChargebackRepository(session)
        tag_repo = SQLModelTagRepository(session)

        # Create 3 dimensions
        dim_ids: list[int] = []
        for i in range(3):
            result = cb_repo.upsert(
                ChargebackRow(
                    ecosystem="eco",
                    tenant_id="t1",
                    timestamp=datetime(2026, 2, 15, tzinfo=UTC),
                    resource_id="r1",
                    product_category="compute",
                    product_type="kafka",
                    identity_id=f"user-{i}",
                    cost_type=CostType.USAGE,
                    amount=Decimal("1.00"),
                )
            )
            session.commit()
            assert result.dimension_id is not None
            dim_ids.append(result.dimension_id)

        # Add "env" tag to dims 0 and 1
        tag_repo.add_tag(dim_ids[0], "env", "Production", "admin")
        tag_repo.add_tag(dim_ids[1], "env", "Staging", "admin")
        # Add different key to dim 2
        tag_repo.add_tag(dim_ids[2], "team", "Platform", "admin")
        session.commit()

        # Call the new batch method
        result = tag_repo.find_tags_by_dimensions_and_key(dim_ids, "env")

        # Only dims 0 and 1 should be present
        assert len(result) == 2
        assert dim_ids[0] in result
        assert dim_ids[1] in result
        assert dim_ids[2] not in result
        assert result[dim_ids[0]].tag_key == "env"
        assert result[dim_ids[1]].tag_key == "env"
