from __future__ import annotations

from collections.abc import Generator
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

import pytest
from sqlmodel import Session, SQLModel, create_engine

if TYPE_CHECKING:
    from sqlalchemy import Engine

ECOSYSTEM = "test-eco"
TENANT_ID = "tenant-1"


@pytest.fixture
def engine() -> Generator[Engine]:
    # Import tables to ensure SQLModel metadata is populated
    from core.storage.backends.sqlmodel.base_tables import BillingTable, IdentityTable, ResourceTable  # noqa: F401
    from core.storage.backends.sqlmodel.tables import (  # noqa: F401
        ChargebackDimensionTable,
        ChargebackFactTable,
        EmissionRecordTable,
    )

    eng = create_engine("sqlite://", echo=False)
    SQLModel.metadata.create_all(eng)
    yield eng
    eng.dispose(close=True)


@pytest.fixture
def session(engine: Engine) -> Generator[Session]:
    with Session(engine) as s:
        yield s


def _insert_chargeback(session: Session, dt: date, amount: Decimal = Decimal("10.00")) -> Any:
    from core.models.chargeback import ChargebackRow, CostType
    from core.storage.backends.sqlmodel.repositories import SQLModelChargebackRepository

    row = ChargebackRow(
        ecosystem=ECOSYSTEM,
        tenant_id=TENANT_ID,
        timestamp=datetime(dt.year, dt.month, dt.day, 0, 0, 0, tzinfo=UTC),
        resource_id="cluster-1",
        product_category="kafka",
        product_type="KAFKA_CKU",
        identity_id="user-1",
        cost_type=CostType.USAGE,
        amount=amount,
        allocation_method="even",
    )
    repo = SQLModelChargebackRepository(session)
    result = repo.upsert(row)
    session.commit()
    return result


def _insert_hourly_chargebacks(
    session: Session, day: date, hours: int = 24, amount_per_hour: Decimal = Decimal("1.00")
) -> list[Any]:
    from core.models.chargeback import ChargebackRow, CostType
    from core.storage.backends.sqlmodel.repositories import SQLModelChargebackRepository

    repo = SQLModelChargebackRepository(session)
    rows = []
    for hour in range(hours):
        row = ChargebackRow(
            ecosystem=ECOSYSTEM,
            tenant_id=TENANT_ID,
            timestamp=datetime(day.year, day.month, day.day, hour, 0, 0, tzinfo=UTC),
            resource_id="cluster-1",
            product_category="kafka",
            product_type="KAFKA_CKU",
            identity_id="user-1",
            cost_type=CostType.USAGE,
            amount=amount_per_hour,
            allocation_method="even",
        )
        rows.append(repo.upsert(row))
    session.commit()
    return rows


# ---------- SQLModelEmissionRepository ----------


class TestSQLModelEmissionRepositoryUpsert:
    def test_upsert_creates_new_record(self, session: Session) -> None:
        from core.emitters.models import EmissionRecord
        from core.storage.backends.sqlmodel.repositories import SQLModelEmissionRepository

        repo = SQLModelEmissionRepository(session)
        rec = EmissionRecord(
            ecosystem=ECOSYSTEM,
            tenant_id=TENANT_ID,
            emitter_name="csv",
            date=date(2025, 1, 1),
            status="emitted",
        )
        repo.upsert(rec)
        session.commit()

        emitted = repo.get_emitted_dates(ECOSYSTEM, TENANT_ID, "csv")
        assert date(2025, 1, 1) in emitted

    def test_upsert_updates_existing_record_status(self, session: Session) -> None:
        from core.emitters.models import EmissionRecord
        from core.storage.backends.sqlmodel.repositories import SQLModelEmissionRepository

        repo = SQLModelEmissionRepository(session)
        rec = EmissionRecord(
            ecosystem=ECOSYSTEM,
            tenant_id=TENANT_ID,
            emitter_name="csv",
            date=date(2025, 1, 2),
            status="failed",
        )
        repo.upsert(rec)
        session.commit()

        # Re-upsert as emitted
        rec2 = EmissionRecord(
            ecosystem=ECOSYSTEM,
            tenant_id=TENANT_ID,
            emitter_name="csv",
            date=date(2025, 1, 2),
            status="emitted",
        )
        repo.upsert(rec2)
        session.commit()

        emitted = repo.get_emitted_dates(ECOSYSTEM, TENANT_ID, "csv")
        assert date(2025, 1, 2) in emitted

    def test_upsert_increments_attempt_count(self, session: Session) -> None:
        from sqlmodel import select

        from core.emitters.models import EmissionRecord
        from core.storage.backends.sqlmodel.repositories import SQLModelEmissionRepository
        from core.storage.backends.sqlmodel.tables import EmissionRecordTable

        repo = SQLModelEmissionRepository(session)
        rec = EmissionRecord(
            ecosystem=ECOSYSTEM,
            tenant_id=TENANT_ID,
            emitter_name="csv",
            date=date(2025, 1, 3),
            status="emitted",
        )
        repo.upsert(rec)
        session.commit()

        repo.upsert(rec)
        session.commit()

        row = session.exec(
            select(EmissionRecordTable).where(
                EmissionRecordTable.date == date(2025, 1, 3),
                EmissionRecordTable.emitter_name == "csv",
            )
        ).first()
        assert row is not None
        assert row.attempt_count == 2


class TestSQLModelEmissionRepositoryGetEmittedDates:
    def test_get_emitted_dates_returns_only_emitted(self, session: Session) -> None:
        from core.emitters.models import EmissionRecord
        from core.storage.backends.sqlmodel.repositories import SQLModelEmissionRepository

        repo = SQLModelEmissionRepository(session)
        repo.upsert(EmissionRecord(ECOSYSTEM, TENANT_ID, "csv", date(2025, 1, 1), "emitted"))
        repo.upsert(EmissionRecord(ECOSYSTEM, TENANT_ID, "csv", date(2025, 1, 2), "failed"))
        session.commit()

        emitted = repo.get_emitted_dates(ECOSYSTEM, TENANT_ID, "csv")
        assert date(2025, 1, 1) in emitted
        assert date(2025, 1, 2) not in emitted

    def test_get_emitted_dates_empty_when_none(self, session: Session) -> None:
        from core.storage.backends.sqlmodel.repositories import SQLModelEmissionRepository

        repo = SQLModelEmissionRepository(session)
        emitted = repo.get_emitted_dates(ECOSYSTEM, TENANT_ID, "csv")
        assert emitted == set()

    def test_get_emitted_dates_respects_emitter_name(self, session: Session) -> None:
        from core.emitters.models import EmissionRecord
        from core.storage.backends.sqlmodel.repositories import SQLModelEmissionRepository

        repo = SQLModelEmissionRepository(session)
        repo.upsert(EmissionRecord(ECOSYSTEM, TENANT_ID, "csv", date(2025, 1, 5), "emitted"))
        repo.upsert(EmissionRecord(ECOSYSTEM, TENANT_ID, "prometheus", date(2025, 1, 5), "failed"))
        session.commit()

        csv_emitted = repo.get_emitted_dates(ECOSYSTEM, TENANT_ID, "csv")
        prom_emitted = repo.get_emitted_dates(ECOSYSTEM, TENANT_ID, "prometheus")
        assert date(2025, 1, 5) in csv_emitted
        assert date(2025, 1, 5) not in prom_emitted


class TestSQLModelEmissionRepositoryGetFailedDates:
    def test_get_failed_dates_returns_only_failed(self, session: Session) -> None:
        from core.emitters.models import EmissionRecord
        from core.storage.backends.sqlmodel.repositories import SQLModelEmissionRepository

        repo = SQLModelEmissionRepository(session)
        repo.upsert(EmissionRecord(ECOSYSTEM, TENANT_ID, "csv", date(2025, 2, 1), "failed"))
        repo.upsert(EmissionRecord(ECOSYSTEM, TENANT_ID, "csv", date(2025, 2, 2), "emitted"))
        session.commit()

        failed = repo.get_failed_dates(ECOSYSTEM, TENANT_ID, "csv")
        assert date(2025, 2, 1) in failed
        assert date(2025, 2, 2) not in failed

    def test_get_failed_dates_empty_when_none(self, session: Session) -> None:
        from core.storage.backends.sqlmodel.repositories import SQLModelEmissionRepository

        repo = SQLModelEmissionRepository(session)
        failed = repo.get_failed_dates(ECOSYSTEM, TENANT_ID, "csv")
        assert failed == set()


# ---------- Case 7: find_aggregated_for_emit daily ----------


class TestFindAggregatedForEmitDaily:
    def test_24_hourly_rows_aggregated_to_single_row(self, session: Session) -> None:
        """Case 7: 24 hourly rows for 2025-01-15 → 1 row with sum, timestamp=midnight UTC."""
        from core.storage.backends.sqlmodel.repositories import SQLModelChargebackRepository

        day = date(2025, 1, 15)
        _insert_hourly_chargebacks(session, day, hours=24, amount_per_hour=Decimal("1.00"))

        repo = SQLModelChargebackRepository(session)
        results = repo.find_aggregated_for_emit(ECOSYSTEM, TENANT_ID, day, day, "daily")

        assert len(results) == 1
        assert results[0].amount == Decimal("24.00")

    def test_daily_aggregation_timestamp_is_midnight_utc(self, session: Session) -> None:
        """Aggregated row timestamp must be 2025-01-15T00:00:00Z."""
        from core.storage.backends.sqlmodel.repositories import SQLModelChargebackRepository

        day = date(2025, 1, 15)
        _insert_hourly_chargebacks(session, day, hours=3)

        repo = SQLModelChargebackRepository(session)
        results = repo.find_aggregated_for_emit(ECOSYSTEM, TENANT_ID, day, day, "daily")

        assert len(results) == 1
        expected_ts = datetime(2025, 1, 15, 0, 0, 0, tzinfo=UTC)
        assert results[0].timestamp == expected_ts

    def test_daily_aggregation_dimension_id_is_none(self, session: Session) -> None:
        """Aggregated rows must have dimension_id=None (no single dimension maps to group)."""
        from core.storage.backends.sqlmodel.repositories import SQLModelChargebackRepository

        day = date(2025, 1, 15)
        _insert_hourly_chargebacks(session, day, hours=2)

        repo = SQLModelChargebackRepository(session)
        results = repo.find_aggregated_for_emit(ECOSYSTEM, TENANT_ID, day, day, "daily")

        assert all(r.dimension_id is None for r in results)

    def test_daily_aggregation_amount_is_sum(self, session: Session) -> None:
        """Sum of all hourly amounts must equal total in the aggregated row."""
        from core.storage.backends.sqlmodel.repositories import SQLModelChargebackRepository

        day = date(2025, 1, 20)
        _insert_hourly_chargebacks(session, day, hours=10, amount_per_hour=Decimal("2.50"))

        repo = SQLModelChargebackRepository(session)
        results = repo.find_aggregated_for_emit(ECOSYSTEM, TENANT_ID, day, day, "daily")

        assert len(results) == 1
        assert results[0].amount == Decimal("25.00")  # 10 * 2.50

    def test_daily_aggregation_different_identities_separate_rows(self, session: Session) -> None:
        """Different identity_ids produce separate aggregated rows."""
        from core.models.chargeback import ChargebackRow, CostType
        from core.storage.backends.sqlmodel.repositories import SQLModelChargebackRepository

        day = date(2025, 1, 10)
        repo = SQLModelChargebackRepository(session)
        for identity_id in ["user-1", "user-2"]:
            repo.upsert(
                ChargebackRow(
                    ecosystem=ECOSYSTEM,
                    tenant_id=TENANT_ID,
                    timestamp=datetime(2025, 1, 10, 0, 0, 0, tzinfo=UTC),
                    resource_id="cluster-1",
                    product_category="kafka",
                    product_type="KAFKA_CKU",
                    identity_id=identity_id,
                    cost_type=CostType.USAGE,
                    amount=Decimal("5.00"),
                    allocation_method="even",
                )
            )
        session.commit()

        results = repo.find_aggregated_for_emit(ECOSYSTEM, TENANT_ID, day, day, "daily")
        assert len(results) == 2


# ---------- Case 9: Monthly aggregation — full month range ----------


class TestFindAggregatedForEmitMonthly:
    def test_monthly_rows_have_first_of_month_timestamp(self, session: Session) -> None:
        """Case 9: Monthly aggregation timestamps must be 2025-01-01T00:00:00Z."""
        from core.storage.backends.sqlmodel.repositories import SQLModelChargebackRepository

        for day in [5, 10, 20]:
            _insert_chargeback(session, date(2025, 1, day), Decimal("10.00"))

        repo = SQLModelChargebackRepository(session)
        results = repo.find_aggregated_for_emit(ECOSYSTEM, TENANT_ID, date(2025, 1, 1), date(2025, 1, 31), "monthly")

        assert len(results) == 1
        expected_ts = datetime(2025, 1, 1, 0, 0, 0, tzinfo=UTC)
        assert results[0].timestamp == expected_ts

    def test_monthly_amounts_summed_across_full_date_range(self, session: Session) -> None:
        """Case 9: amounts for Jan 5, Jan 10, Jan 20 must be summed in single row."""
        from core.storage.backends.sqlmodel.repositories import SQLModelChargebackRepository

        for day in [5, 10, 20]:
            _insert_chargeback(session, date(2025, 1, day), Decimal("10.00"))

        repo = SQLModelChargebackRepository(session)
        results = repo.find_aggregated_for_emit(ECOSYSTEM, TENANT_ID, date(2025, 1, 1), date(2025, 1, 31), "monthly")

        assert len(results) == 1
        assert results[0].amount == Decimal("30.00")  # 3 × 10.00

    def test_monthly_aggregation_dimension_id_none(self, session: Session) -> None:
        from core.storage.backends.sqlmodel.repositories import SQLModelChargebackRepository

        for day in [5, 10]:
            _insert_chargeback(session, date(2025, 2, day))

        repo = SQLModelChargebackRepository(session)
        results = repo.find_aggregated_for_emit(ECOSYSTEM, TENANT_ID, date(2025, 2, 1), date(2025, 2, 28), "monthly")

        assert all(r.dimension_id is None for r in results)

    def test_monthly_different_months_separate_results(self, session: Session) -> None:
        """Rows in different months must produce separate aggregated rows."""
        from core.storage.backends.sqlmodel.repositories import SQLModelChargebackRepository

        _insert_chargeback(session, date(2025, 1, 15))
        _insert_chargeback(session, date(2025, 2, 15))

        repo = SQLModelChargebackRepository(session)
        # Query full two-month range — expect 2 rows (one per month)
        results = repo.find_aggregated_for_emit(ECOSYSTEM, TENANT_ID, date(2025, 1, 1), date(2025, 2, 28), "monthly")

        # Two distinct months should produce two rows
        timestamps = {r.timestamp for r in results}
        assert datetime(2025, 1, 1, 0, 0, 0, tzinfo=UTC) in timestamps
        assert datetime(2025, 2, 1, 0, 0, 0, tzinfo=UTC) in timestamps
