from __future__ import annotations

from collections.abc import Generator
from datetime import UTC, date, datetime
from decimal import Decimal

import pytest
from sqlmodel import Session, SQLModel, create_engine

from core.models.topic_attribution import TopicAttributionRow
from core.storage.backends.sqlmodel.repositories import TopicAttributionRepository


@pytest.fixture
def session() -> Generator[Session]:
    engine = create_engine("sqlite://", echo=False)
    SQLModel.metadata.create_all(engine)
    with Session(engine) as s:
        yield s
    engine.dispose(close=True)


def _make_row(
    topic_name: str = "orders",
    product_type: str = "KAFKA_NETWORK_WRITE",
    attribution_method: str = "bytes_ratio",
    amount: Decimal = Decimal("10.00"),
    timestamp: datetime | None = None,
    cluster_resource_id: str = "lkc-abc",
    ecosystem: str = "eco",
    tenant_id: str = "t1",
    env_id: str = "env-1",
) -> TopicAttributionRow:
    return TopicAttributionRow(
        ecosystem=ecosystem,
        tenant_id=tenant_id,
        timestamp=timestamp or datetime(2026, 1, 1, tzinfo=UTC),
        env_id=env_id,
        cluster_resource_id=cluster_resource_id,
        topic_name=topic_name,
        product_category="KAFKA",
        product_type=product_type,
        attribution_method=attribution_method,
        amount=amount,
    )


class TestTopicAttributionRepositorySchemaRoundTrip:
    def test_upsert_batch_and_find_by_date(self, session: Session) -> None:
        """Insert via upsert_batch(), read back via find_by_date() — amounts match."""
        repo = TopicAttributionRepository(session)
        rows = [
            _make_row(topic_name="orders", amount=Decimal("8.00")),
            _make_row(topic_name="payments", amount=Decimal("2.00")),
        ]
        count = repo.upsert_batch(rows)
        session.commit()

        assert count == 2
        found = repo.find_by_date("eco", "t1", date(2026, 1, 1))
        assert len(found) == 2
        by_topic = {r.topic_name: r.amount for r in found}
        assert by_topic["orders"] == Decimal("8.00")
        assert by_topic["payments"] == Decimal("2.00")

    def test_find_by_date_returns_empty_for_different_date(self, session: Session) -> None:
        repo = TopicAttributionRepository(session)
        repo.upsert_batch([_make_row(timestamp=datetime(2026, 1, 1, tzinfo=UTC))])
        session.commit()

        found = repo.find_by_date("eco", "t1", date(2026, 1, 2))
        assert found == []


class TestTopicAttributionRepositoryDimensionDedup:
    def test_same_dimension_twice_creates_one_dimension_one_fact(self, session: Session) -> None:
        """Same (cluster, topic, product_type, attribution_method) twice → one dimension row, one fact row."""
        repo = TopicAttributionRepository(session)
        row1 = _make_row(topic_name="orders", amount=Decimal("8.00"))
        row2 = _make_row(topic_name="orders", amount=Decimal("8.00"))  # exact same

        repo.upsert_batch([row1, row2])
        session.commit()

        from sqlmodel import select

        from core.storage.backends.sqlmodel.tables import (
            TopicAttributionDimensionTable,
            TopicAttributionFactTable,
        )

        dims = session.exec(
            select(TopicAttributionDimensionTable).where(TopicAttributionDimensionTable.topic_name == "orders")
        ).all()
        assert len(dims) == 1

        facts = session.exec(select(TopicAttributionFactTable)).all()
        assert len(facts) == 1

    def test_different_topics_create_separate_dimensions(self, session: Session) -> None:
        repo = TopicAttributionRepository(session)
        repo.upsert_batch(
            [
                _make_row(topic_name="orders", amount=Decimal("8.00")),
                _make_row(topic_name="payments", amount=Decimal("2.00")),
            ]
        )
        session.commit()

        from sqlmodel import select

        from core.storage.backends.sqlmodel.tables import TopicAttributionDimensionTable

        dims = session.exec(select(TopicAttributionDimensionTable)).all()
        assert len(dims) == 2


class TestTopicAttributionRepositoryFindByFilters:
    def _insert_rows(self, session: Session, count: int) -> None:
        repo = TopicAttributionRepository(session)
        rows = [
            _make_row(
                topic_name=f"topic-{i}",
                amount=Decimal("1.00"),
                timestamp=datetime(2026, 1, 1, tzinfo=UTC),
            )
            for i in range(count)
        ]
        repo.upsert_batch(rows)
        session.commit()

    def test_pagination_returns_correct_page(self, session: Session) -> None:
        """500 rows in DB, limit=10, offset=0 → returns 10 rows + total=500."""
        self._insert_rows(session, 500)
        repo = TopicAttributionRepository(session)
        items, total = repo.find_by_filters("eco", "t1", limit=10, offset=0)
        assert len(items) == 10
        assert total == 500

    def test_pagination_offset(self, session: Session) -> None:
        self._insert_rows(session, 50)
        repo = TopicAttributionRepository(session)
        items, total = repo.find_by_filters("eco", "t1", limit=10, offset=10)
        assert len(items) == 10
        assert total == 50

    def test_find_by_filters_cluster_filter(self, session: Session) -> None:
        """Filter by cluster_resource_id returns only matching rows."""
        repo = TopicAttributionRepository(session)
        repo.upsert_batch(
            [
                _make_row(cluster_resource_id="lkc-abc", topic_name="t1"),
                _make_row(cluster_resource_id="lkc-xyz", topic_name="t2"),
            ]
        )
        session.commit()

        items, total = repo.find_by_filters("eco", "t1", cluster_resource_id="lkc-abc")
        assert total == 1
        assert items[0].cluster_resource_id == "lkc-abc"


class TestTopicAttributionRepositoryAggregate:
    def test_aggregate_by_topic_name(self, session: Session) -> None:
        """3 topics × 2 dates = 6 rows; group_by=["topic_name"] → 3 buckets, each row_count=2."""
        repo = TopicAttributionRepository(session)
        rows = []
        for topic in ["a", "b", "c"]:
            for ts in [
                datetime(2026, 1, 1, tzinfo=UTC),
                datetime(2026, 1, 2, tzinfo=UTC),
            ]:
                rows.append(
                    _make_row(
                        topic_name=topic,
                        amount=Decimal("5.00"),
                        timestamp=ts,
                    )
                )
        repo.upsert_batch(rows)
        session.commit()

        result = repo.aggregate("eco", "t1", group_by=["topic_name"], time_bucket="day")
        # 3 topics × 2 dates = 6 buckets with group_by topic_name + day
        # But since time_bucket="day" and there are 2 dates, each topic has 2 buckets
        # Actually: 3 topics × 2 dates = 6 buckets
        # Let's verify we get correct aggregation
        topic_buckets: dict[str, list] = {}
        for bucket in result.buckets:
            tname = bucket.dimensions.get("topic_name", "")
            topic_buckets.setdefault(tname, []).append(bucket)

        assert len(topic_buckets) == 3
        for _tname, buckets in topic_buckets.items():
            # Each topic has 2 time buckets (2 dates)
            total_rows = sum(b.row_count for b in buckets)
            assert total_rows == 2


class TestTopicAttributionRepositoryGetDistinctDates:
    def test_get_distinct_dates_returns_sorted_dates(self, session: Session) -> None:
        """Facts for 2026-01-01 and 2026-01-02 → returns [date(2026, 1, 1), date(2026, 1, 2)]."""
        repo = TopicAttributionRepository(session)
        repo.upsert_batch(
            [
                _make_row(timestamp=datetime(2026, 1, 2, tzinfo=UTC), topic_name="t1"),
                _make_row(timestamp=datetime(2026, 1, 1, tzinfo=UTC), topic_name="t2"),
            ]
        )
        session.commit()

        dates = repo.get_distinct_dates("eco", "t1")
        assert len(dates) == 2
        # Should be sorted ascending
        assert dates[0] <= dates[1]

    def test_get_distinct_dates_empty_when_no_facts(self, session: Session) -> None:
        repo = TopicAttributionRepository(session)
        dates = repo.get_distinct_dates("eco", "t1")
        assert dates == []


class TestTopicAttributionRepositoryDeleteBefore:
    def test_delete_before_removes_old_facts(self, session: Session) -> None:
        """delete_before() removes facts older than cutoff."""
        repo = TopicAttributionRepository(session)
        old_ts = datetime(2025, 10, 1, tzinfo=UTC)
        new_ts = datetime(2026, 1, 1, tzinfo=UTC)
        repo.upsert_batch(
            [
                _make_row(timestamp=old_ts, topic_name="old-topic"),
                _make_row(timestamp=new_ts, topic_name="new-topic"),
            ]
        )
        session.commit()

        cutoff = datetime(2025, 12, 1, tzinfo=UTC)
        deleted = repo.delete_before("eco", "t1", cutoff)
        session.commit()

        assert deleted == 1

        remaining = repo.find_by_date("eco", "t1", new_ts.date())
        assert len(remaining) == 1
        assert remaining[0].topic_name == "new-topic"

    def test_delete_before_prunes_orphaned_dimensions(self, session: Session) -> None:
        """delete_before() prunes orphaned dimension rows after fact deletion."""
        from sqlmodel import select

        from core.storage.backends.sqlmodel.tables import TopicAttributionDimensionTable

        repo = TopicAttributionRepository(session)
        old_ts = datetime(2025, 10, 1, tzinfo=UTC)
        repo.upsert_batch(
            [
                _make_row(timestamp=old_ts, topic_name="old-only"),
            ]
        )
        session.commit()

        cutoff = datetime(2025, 12, 1, tzinfo=UTC)
        repo.delete_before("eco", "t1", cutoff)
        session.commit()

        dims = session.exec(
            select(TopicAttributionDimensionTable).where(TopicAttributionDimensionTable.topic_name == "old-only")
        ).all()
        assert dims == []

    def test_delete_before_keeps_dimensions_with_remaining_facts(self, session: Session) -> None:
        from sqlmodel import select

        from core.storage.backends.sqlmodel.tables import TopicAttributionDimensionTable

        repo = TopicAttributionRepository(session)
        old_ts = datetime(2025, 10, 1, tzinfo=UTC)
        new_ts = datetime(2026, 1, 1, tzinfo=UTC)

        repo.upsert_batch(
            [
                _make_row(timestamp=old_ts, topic_name="mixed"),
                _make_row(timestamp=new_ts, topic_name="mixed"),
            ]
        )
        session.commit()

        # delete old fact — dimension should stay (new fact exists)
        repo.delete_before("eco", "t1", datetime(2025, 12, 1, tzinfo=UTC))
        session.commit()

        dims = session.exec(
            select(TopicAttributionDimensionTable).where(TopicAttributionDimensionTable.topic_name == "mixed")
        ).all()
        assert len(dims) == 1  # dimension kept


class TestTopicAttributionDimensionTableAttributionMethodSchema:
    """Tests for TASK-166: attribution_method must be NOT NULL with default ''."""

    def test_insert_without_attribution_method_defaults_to_empty_string(self, session: Session) -> None:
        """Insert TopicAttributionDimensionTable without attribution_method → reads back as ''."""
        from sqlmodel import select

        from core.storage.backends.sqlmodel.tables import TopicAttributionDimensionTable

        row = TopicAttributionDimensionTable(
            ecosystem="eco",
            tenant_id="t1",
            env_id="env-1",
            cluster_resource_id="lkc-abc",
            topic_name="orders",
            product_category="KAFKA",
            product_type="KAFKA_NETWORK_WRITE",
            # attribution_method intentionally omitted
        )
        session.add(row)
        session.commit()

        saved = session.exec(
            select(TopicAttributionDimensionTable).where(TopicAttributionDimensionTable.topic_name == "orders")
        ).one()
        assert saved.attribution_method == ""

    def test_duplicate_dimension_rows_with_empty_attribution_method_raises_integrity_error(
        self, session: Session
    ) -> None:
        """Two rows with identical 8-column unique constraint values → second insert raises IntegrityError."""
        from sqlalchemy.exc import IntegrityError

        from core.storage.backends.sqlmodel.tables import TopicAttributionDimensionTable

        def _make_dim() -> TopicAttributionDimensionTable:
            return TopicAttributionDimensionTable(
                ecosystem="eco",
                tenant_id="t1",
                env_id="env-1",
                cluster_resource_id="lkc-abc",
                topic_name="orders",
                product_category="KAFKA",
                product_type="KAFKA_NETWORK_WRITE",
                attribution_method="",
            )

        session.add(_make_dim())
        session.commit()

        session.add(_make_dim())
        with pytest.raises(IntegrityError):
            session.commit()

    def test_attribution_method_orm_field_is_str_with_empty_string_default(self) -> None:
        """TopicAttributionDimensionTable.attribution_method is str with default '' — no None accepted."""
        from core.storage.backends.sqlmodel.tables import TopicAttributionDimensionTable

        field_info = TopicAttributionDimensionTable.model_fields["attribution_method"]
        # default must be "" not None
        assert field_info.default == ""
        # annotation must be str (not str | None)
        import typing

        annotation = field_info.annotation
        # Should not be Optional[str] / str | None
        args = typing.get_args(annotation)
        assert type(None) not in args, "attribution_method must not be Optional[str]"
