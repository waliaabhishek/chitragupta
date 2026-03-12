from __future__ import annotations

from collections.abc import Generator
from datetime import UTC, date, datetime
from decimal import Decimal

import pytest
from sqlmodel import Session, SQLModel, create_engine

from core.models.chargeback import ChargebackRow, CostType
from core.storage.backends.sqlmodel.repositories import SQLModelChargebackRepository


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def session() -> Generator[Session]:
    engine = create_engine("sqlite://", echo=False)
    SQLModel.metadata.create_all(engine)
    with Session(engine) as s:
        yield s
    engine.dispose(close=True)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_TS = datetime(2026, 3, 1, 0, 0, 0, tzinfo=UTC)
_DATE = date(2026, 3, 1)


def _make_row(
    identity_id: str = "user-1",
    resource_id: str = "cluster-1",
    product_type: str = "KAFKA_CKU",
    amount: Decimal = Decimal("10.00"),
) -> ChargebackRow:
    return ChargebackRow(
        ecosystem="eco",
        tenant_id="t1",
        timestamp=_TS,
        resource_id=resource_id,
        product_category="kafka",
        product_type=product_type,
        identity_id=identity_id,
        cost_type=CostType.USAGE,
        amount=amount,
        allocation_method="even_split",
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestUpsertBatch:
    def test_upsert_batch_correctness(self, session: Session) -> None:
        """upsert_batch([r1, r2, r3]) — all 3 rows queryable via find_by_date."""
        repo = SQLModelChargebackRepository(session)
        rows = [
            _make_row(identity_id="user-1"),
            _make_row(identity_id="user-2"),
            _make_row(identity_id="user-3"),
        ]

        count = repo.upsert_batch(rows)

        session.commit()
        found = repo.find_by_date("eco", "t1", _DATE)
        assert count == 3
        assert len(found) == 3
        identity_ids = {r.identity_id for r in found}
        assert identity_ids == {"user-1", "user-2", "user-3"}

    def test_upsert_batch_dimension_dedup(self, session: Session) -> None:
        """Two rows with the same dimension key create only one ChargebackDimensionTable row."""
        from sqlmodel import select

        from core.storage.backends.sqlmodel.tables import ChargebackDimensionTable

        repo = SQLModelChargebackRepository(session)
        # Both rows share the same dimension key (same eco/tenant/resource/product/identity/cost_type/method)
        row1 = _make_row(identity_id="user-1", amount=Decimal("10.00"))
        row2 = _make_row(identity_id="user-1", amount=Decimal("20.00"))

        repo.upsert_batch([row1, row2])
        session.commit()

        dim_count = session.exec(select(ChargebackDimensionTable)).all()
        assert len(dim_count) == 1

    def test_upsert_batch_empty_returns_zero(self, session: Session) -> None:
        """upsert_batch([]) returns 0 and does not raise."""
        repo = SQLModelChargebackRepository(session)
        count = repo.upsert_batch([])
        assert count == 0

    def test_upsert_batch_delete_then_write_no_pk_conflict(self, session: Session) -> None:
        """delete_by_date then upsert_batch for same date — no PK conflict, rows correct."""
        repo = SQLModelChargebackRepository(session)
        initial = [_make_row(identity_id="user-1"), _make_row(identity_id="user-2")]
        repo.upsert_batch(initial)
        session.commit()

        repo.delete_by_date("eco", "t1", _DATE)
        session.commit()

        assert repo.find_by_date("eco", "t1", _DATE) == []

        fresh = [_make_row(identity_id="user-A"), _make_row(identity_id="user-B")]
        count = repo.upsert_batch(fresh)
        session.commit()

        found = repo.find_by_date("eco", "t1", _DATE)
        assert count == 2
        assert len(found) == 2
        assert {r.identity_id for r in found} == {"user-A", "user-B"}
