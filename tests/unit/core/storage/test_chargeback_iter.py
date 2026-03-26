from __future__ import annotations

from collections.abc import Generator
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest
from sqlmodel import Session, SQLModel, create_engine

from core.models.chargeback import ChargebackRow, CostType
from core.storage.backends.sqlmodel.repositories import SQLModelChargebackRepository, SQLModelEntityTagRepository
from core.storage.interface import ChargebackRepository


@pytest.fixture
def session() -> Generator[Session]:
    engine = create_engine("sqlite://", echo=False)
    SQLModel.metadata.create_all(engine)
    with Session(engine) as s:
        yield s
    engine.dispose(close=True)


def _make_chargeback(**overrides: Any) -> ChargebackRow:
    defaults: dict[str, Any] = dict(
        ecosystem="eco",
        tenant_id="t1",
        timestamp=datetime(2026, 2, 15, tzinfo=UTC),
        resource_id="r1",
        product_category="compute",
        product_type="kafka",
        identity_id="user-1",
        cost_type=CostType.USAGE,
        amount=Decimal("10.00"),
        allocation_method="direct",
        allocation_detail=None,
        tags={},
        metadata={},
    )
    defaults.update(overrides)
    return ChargebackRow(**defaults)


class TestIterByFilters:
    def test_iter_by_filters_yields_all_rows_no_truncation(self, session: Session) -> None:
        """iter_by_filters yields all rows even when count exceeds batch_size."""
        repo = SQLModelChargebackRepository(session)
        total = 25
        for i in range(total):
            repo.upsert(_make_chargeback(identity_id=f"user-{i}"))
        session.commit()

        rows = list(repo.iter_by_filters("eco", "t1", batch_size=10))
        assert len(rows) == total

    def test_iter_by_filters_with_identity_filter_returns_only_matching(self, session: Session) -> None:
        """iter_by_filters with identity_id returns only rows for that identity."""
        repo = SQLModelChargebackRepository(session)
        for i in range(5):
            repo.upsert(_make_chargeback(identity_id="user-A", timestamp=datetime(2026, 2, i + 1, tzinfo=UTC)))
        for i in range(3):
            repo.upsert(_make_chargeback(identity_id="user-B", timestamp=datetime(2026, 2, i + 1, tzinfo=UTC)))
        session.commit()

        rows = list(repo.iter_by_filters("eco", "t1", identity_id="user-A"))
        assert len(rows) == 5
        assert all(r.identity_id == "user-A" for r in rows)

    def test_iter_by_filters_empty_result_returns_empty_iterator(self, session: Session) -> None:
        """iter_by_filters returns an empty iterator when no rows match filters."""
        repo = SQLModelChargebackRepository(session)

        rows = list(repo.iter_by_filters("eco", "t1", identity_id="nonexistent-user"))
        assert rows == []

    def test_iter_by_filters_with_entity_tags_overlay(self, session: Session) -> None:
        """Rows with entity tags have tag dict populated when tags_repo is provided."""
        repo = SQLModelChargebackRepository(session)
        tag_repo = SQLModelEntityTagRepository(session)

        row = repo.upsert(_make_chargeback(resource_id="r1", identity_id="user-1"))
        session.flush()
        assert row.dimension_id is not None
        tag_repo.add_tag("t1", "resource", "r1", "env", "production", "test")
        session.commit()

        rows = list(repo.iter_by_filters("eco", "t1", tags_repo=tag_repo))
        assert len(rows) == 1
        assert rows[0].tags.get("env") == "production"

    def test_iter_by_filters_date_range_excludes_out_of_range_rows(self, session: Session) -> None:
        """iter_by_filters with start/end only returns rows within the half-open interval."""
        repo = SQLModelChargebackRepository(session)
        for day in [1, 10, 20]:
            repo.upsert(
                _make_chargeback(
                    identity_id=f"user-day-{day}",
                    timestamp=datetime(2026, 2, day, tzinfo=UTC),
                )
            )
        session.commit()

        start = datetime(2026, 2, 5, tzinfo=UTC)
        end = datetime(2026, 2, 15, tzinfo=UTC)
        rows = list(repo.iter_by_filters("eco", "t1", start=start, end=end))
        assert len(rows) == 1
        assert rows[0].identity_id == "user-day-10"

    def test_iter_by_filters_multiple_batches_all_tags_correct(self, session: Session) -> None:
        """Entity tags are overlaid correctly even when rows span multiple batches."""
        repo = SQLModelChargebackRepository(session)
        tag_repo = SQLModelEntityTagRepository(session)

        # Insert 15 rows; tag every 3rd identity. Use batch_size=6 to cross batch boundaries.
        tagged_identities: list[str] = []
        for i in range(15):
            identity_id = f"user-{i}"
            repo.upsert(_make_chargeback(identity_id=identity_id))
            if i % 3 == 0:
                tag_repo.add_tag("t1", "identity", identity_id, "tier", f"tier-{i}", "test")
                tagged_identities.append(identity_id)
        session.commit()

        rows = list(repo.iter_by_filters("eco", "t1", tags_repo=tag_repo, batch_size=6))
        assert len(rows) == 15

        tagged_rows = [r for r in rows if r.tags]
        assert len(tagged_rows) == 5
        for r in tagged_rows:
            assert len(r.tags) == 1
            assert list(r.tags.values())[0].startswith("tier-")


class TestProtocolCompliance:
    def test_sqlmodel_chargeback_repo_satisfies_protocol(self, session: Session) -> None:
        """SQLModelChargebackRepository is an instance of ChargebackRepository Protocol."""
        repo = SQLModelChargebackRepository(session)
        assert isinstance(repo, ChargebackRepository)

    def test_iter_by_filters_present_on_repo(self, session: Session) -> None:
        """iter_by_filters method exists and is callable on SQLModelChargebackRepository."""
        repo = SQLModelChargebackRepository(session)
        assert hasattr(repo, "iter_by_filters"), "iter_by_filters missing from SQLModelChargebackRepository"
        assert callable(repo.iter_by_filters)

    def test_iter_by_filters_present_in_protocol(self) -> None:
        """iter_by_filters is declared on the ChargebackRepository Protocol."""
        assert hasattr(ChargebackRepository, "iter_by_filters"), (
            "iter_by_filters not declared on ChargebackRepository Protocol"
        )
