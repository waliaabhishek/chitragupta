"""End-to-end integration tests for topic attribution pipeline.

Tests the full flow using real SQLite + CCloud storage module:
 - Billing rows seeded → resources seeded → TopicAttributionPhase runs → rows in DB
 - Recalculation: mark_needs_recalculation resets state → re-run produces fresh rows
 - Cost mapping override: even_split override applied at runtime
"""

from __future__ import annotations

from collections.abc import Generator
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend
from plugins.confluent_cloud.storage.module import CCloudStorageModule

ECOSYSTEM = "confluent_cloud"
TENANT_ID = "org-e2e-test"
TRACKING_DATE = date(2024, 1, 15)
TRACKING_TS = datetime(2024, 1, 15, 0, 0, 0, tzinfo=UTC)
CLUSTER_ID = "lkc-e2etest"
ENV_ID = "env-abc"


@pytest.fixture
def storage(tmp_path: Path) -> Generator[SQLModelBackend]:
    """File-based SQLite backend so write and read-only engines share the same DB."""
    db_path = tmp_path / "test.db"
    backend = SQLModelBackend(f"sqlite:///{db_path}", CCloudStorageModule(), use_migrations=False)
    backend.create_tables()
    yield backend
    backend.dispose()


def _insert_billing(storage: SQLModelBackend, product_type: str, amount: Decimal) -> None:
    from plugins.confluent_cloud.models.billing import CCloudBillingLineItem

    line = CCloudBillingLineItem(
        ecosystem=ECOSYSTEM,
        tenant_id=TENANT_ID,
        timestamp=TRACKING_TS,
        env_id=ENV_ID,
        resource_id=CLUSTER_ID,
        product_category="KAFKA",
        product_type=product_type,
        quantity=Decimal("1"),
        unit_price=amount,
        total_cost=amount,
    )
    with storage.create_unit_of_work() as uow:
        uow.billing.upsert(line)
        uow.commit()


def _insert_topic_resources(storage: SQLModelBackend, topics: list[str]) -> None:
    """Seed topic resources as children of CLUSTER_ID so _get_cluster_topics() finds them."""
    from core.models.resource import CoreResource

    with storage.create_unit_of_work() as uow:
        for topic in topics:
            resource = CoreResource(
                ecosystem=ECOSYSTEM,
                tenant_id=TENANT_ID,
                resource_id=f"{CLUSTER_ID}/topic/{topic}",
                resource_type="topic",
                display_name=topic,
                parent_id=CLUSTER_ID,
                created_at=TRACKING_TS,
            )
            uow.resources.upsert(resource)
        uow.commit()


def _seed_pipeline_state(
    storage: SQLModelBackend,
    tracking_date: date = TRACKING_DATE,
    *,
    topic_overlay_gathered: bool = True,
) -> None:
    """Seed a pipeline_state row with topic_overlay_gathered=True by default."""
    from core.models.pipeline import PipelineState

    state = PipelineState(
        ecosystem=ECOSYSTEM,
        tenant_id=TENANT_ID,
        tracking_date=tracking_date,
        billing_gathered=True,
        resources_gathered=True,
        chargeback_calculated=True,
        topic_overlay_gathered=topic_overlay_gathered,
        topic_attribution_calculated=False,
    )
    with storage.create_unit_of_work() as uow:
        uow.pipeline_state.upsert(state)
        uow.commit()


def _make_ta_config(
    enabled: bool = True,
    cost_mapping_overrides: dict[str, str] | None = None,
) -> Any:
    from plugins.confluent_cloud.config import TopicAttributionConfig

    return TopicAttributionConfig(
        enabled=enabled,
        cost_mapping_overrides=cost_mapping_overrides or {},
    )


def _make_metrics_source_with_topics(
    topics: list[str],
    bytes_per_topic: dict[str, float] | None = None,
) -> MagicMock:
    """MagicMock metrics_source that returns synthetic per-topic byte metrics."""
    from core.models.metrics import MetricRow

    metrics_source = MagicMock()
    bytes_per_topic = bytes_per_topic or {t: 100.0 for t in topics}

    def _query(
        queries: Any, start: Any, end: Any, step: Any, resource_id_filter: Any = None
    ) -> dict[str, list[MetricRow]]:
        result: dict[str, list[MetricRow]] = {}
        for q in queries:
            rows = [
                MetricRow(
                    timestamp=TRACKING_TS,
                    metric_key=q.key,
                    value=bytes_per_topic.get(topic, 100.0),
                    labels={"kafka_id": CLUSTER_ID, "topic": topic},
                )
                for topic in topics
            ]
            result[q.key] = rows
        return result

    metrics_source.query = _query
    return metrics_source


class TestTopicAttributionE2EFullPipeline:
    def test_phase_writes_rows_to_db(self, storage: SQLModelBackend) -> None:
        """TopicAttributionPhase.run() stores rows queryable via repository."""
        from core.engine.topic_attribution import TopicAttributionPhase

        _insert_billing(storage, "KAFKA_NETWORK_WRITE", Decimal("100.00"))
        topics = ["orders-events", "payments-events"]
        _insert_topic_resources(storage, topics)
        metrics_source = _make_metrics_source_with_topics(topics)

        ta_config = _make_ta_config()
        phase = TopicAttributionPhase(
            ecosystem=ECOSYSTEM,
            tenant_id=TENANT_ID,
            metrics_source=metrics_source,
            config=ta_config,
            metrics_step=timedelta(hours=1),
        )

        with storage.create_unit_of_work() as uow:
            count = phase.run(uow, TRACKING_DATE)
            uow.commit()

        assert count >= 2  # at least one row per topic

        with storage.create_read_only_unit_of_work() as uow:
            rows = uow.topic_attributions.find_by_date(ECOSYSTEM, TENANT_ID, TRACKING_DATE)

        assert len(rows) >= 2
        topic_names = {r.topic_name for r in rows}
        assert "orders-events" in topic_names
        assert "payments-events" in topic_names

    def test_phase_marks_topic_attribution_calculated(self, storage: SQLModelBackend) -> None:
        """After run(), pipeline_state.topic_attribution_calculated must be True."""
        from core.engine.topic_attribution import TopicAttributionPhase

        _insert_billing(storage, "KAFKA_NETWORK_WRITE", Decimal("50.00"))
        _insert_topic_resources(storage, ["topic-a"])
        _seed_pipeline_state(storage)
        metrics_source = _make_metrics_source_with_topics(["topic-a"])

        ta_config = _make_ta_config()
        phase = TopicAttributionPhase(
            ecosystem=ECOSYSTEM,
            tenant_id=TENANT_ID,
            metrics_source=metrics_source,
            config=ta_config,
            metrics_step=timedelta(hours=1),
        )

        with storage.create_unit_of_work() as uow:
            phase.run(uow, TRACKING_DATE)
            uow.commit()

        with storage.create_read_only_unit_of_work() as uow:
            pending = uow.pipeline_state.find_needing_topic_attribution(ECOSYSTEM, TENANT_ID)

        # No pending dates — calculated flag was set
        assert not any(p.tracking_date == TRACKING_DATE for p in pending)

    def test_rows_are_queryable_by_date(self, storage: SQLModelBackend) -> None:
        """Rows stored on TRACKING_DATE are not returned for a different date."""
        from core.engine.topic_attribution import TopicAttributionPhase

        _insert_billing(storage, "KAFKA_NETWORK_WRITE", Decimal("30.00"))
        _insert_topic_resources(storage, ["topic-a"])
        metrics_source = _make_metrics_source_with_topics(["topic-a"])

        phase = TopicAttributionPhase(
            ecosystem=ECOSYSTEM,
            tenant_id=TENANT_ID,
            metrics_source=metrics_source,
            config=_make_ta_config(),
            metrics_step=timedelta(hours=1),
        )

        with storage.create_unit_of_work() as uow:
            phase.run(uow, TRACKING_DATE)
            uow.commit()

        other_date = date(2024, 1, 16)
        with storage.create_read_only_unit_of_work() as uow:
            rows_other = uow.topic_attributions.find_by_date(ECOSYSTEM, TENANT_ID, other_date)

        assert rows_other == []

    def test_find_needing_topic_attribution_returns_pending(self, storage: SQLModelBackend) -> None:
        """Seeded pipeline state with topic_overlay_gathered=True shows in find_needing_topic_attribution."""
        _seed_pipeline_state(storage)

        with storage.create_read_only_unit_of_work() as uow:
            pending = uow.pipeline_state.find_needing_topic_attribution(ECOSYSTEM, TENANT_ID)

        assert any(p.tracking_date == TRACKING_DATE for p in pending)

    def test_mark_overlay_gathered_with_zero_topics_enables_attribution(self, storage: SQLModelBackend) -> None:
        """mark_topic_overlay_gathered with no topics upserted → date appears in find_needing_topic_attribution."""
        _seed_pipeline_state(storage, topic_overlay_gathered=False)

        with storage.create_unit_of_work() as uow:
            uow.pipeline_state.mark_topic_overlay_gathered(ECOSYSTEM, TENANT_ID, TRACKING_DATE)
            uow.commit()

        with storage.create_read_only_unit_of_work() as uow:
            pending = uow.pipeline_state.find_needing_topic_attribution(ECOSYSTEM, TENANT_ID)

        assert any(p.tracking_date == TRACKING_DATE for p in pending)


class TestTopicAttributionE2ERecalculation:
    def test_mark_needs_recalculation_resets_topic_attribution_calculated(self, storage: SQLModelBackend) -> None:
        """mark_needs_recalculation must reset topic_attribution_calculated=False."""
        from core.engine.topic_attribution import TopicAttributionPhase

        _insert_billing(storage, "KAFKA_NETWORK_WRITE", Decimal("60.00"))
        _insert_topic_resources(storage, ["topic-a"])
        _seed_pipeline_state(storage)
        metrics_source = _make_metrics_source_with_topics(["topic-a"])

        phase = TopicAttributionPhase(
            ecosystem=ECOSYSTEM,
            tenant_id=TENANT_ID,
            metrics_source=metrics_source,
            config=_make_ta_config(),
            metrics_step=timedelta(hours=1),
        )

        # First run — marks calculated
        with storage.create_unit_of_work() as uow:
            phase.run(uow, TRACKING_DATE)
            uow.commit()

        # Verify no pending before reset
        with storage.create_read_only_unit_of_work() as uow:
            pending_before = uow.pipeline_state.find_needing_topic_attribution(ECOSYSTEM, TENANT_ID)
        assert not any(p.tracking_date == TRACKING_DATE for p in pending_before)

        # Reset via mark_needs_recalculation
        with storage.create_unit_of_work() as uow:
            uow.pipeline_state.mark_needs_recalculation(ECOSYSTEM, TENANT_ID, TRACKING_DATE)
            uow.commit()

        # Restore topic_overlay_gathered so it shows up as pending again
        with storage.create_unit_of_work() as uow:
            uow.pipeline_state.mark_topic_overlay_gathered(ECOSYSTEM, TENANT_ID, TRACKING_DATE)
            uow.commit()

        with storage.create_read_only_unit_of_work() as uow:
            pending_after = uow.pipeline_state.find_needing_topic_attribution(ECOSYSTEM, TENANT_ID)

        assert any(p.tracking_date == TRACKING_DATE for p in pending_after)

    def test_rerun_after_recalculation_produces_fresh_rows(self, storage: SQLModelBackend) -> None:
        """Re-running TopicAttributionPhase after recalculation upserts rows without errors."""
        from core.engine.topic_attribution import TopicAttributionPhase

        _insert_billing(storage, "KAFKA_NETWORK_WRITE", Decimal("90.00"))
        _insert_topic_resources(storage, ["topic-a"])
        _seed_pipeline_state(storage)
        metrics_source = _make_metrics_source_with_topics(["topic-a"])

        phase = TopicAttributionPhase(
            ecosystem=ECOSYSTEM,
            tenant_id=TENANT_ID,
            metrics_source=metrics_source,
            config=_make_ta_config(),
            metrics_step=timedelta(hours=1),
        )

        # First run
        with storage.create_unit_of_work() as uow:
            count1 = phase.run(uow, TRACKING_DATE)
            uow.commit()

        assert count1 > 0

        # Reset and re-run
        with storage.create_unit_of_work() as uow:
            uow.pipeline_state.mark_needs_recalculation(ECOSYSTEM, TENANT_ID, TRACKING_DATE)
            uow.pipeline_state.mark_topic_overlay_gathered(ECOSYSTEM, TENANT_ID, TRACKING_DATE)
            uow.commit()

        with storage.create_unit_of_work() as uow:
            count2 = phase.run(uow, TRACKING_DATE)
            uow.commit()

        assert count2 > 0

        with storage.create_read_only_unit_of_work() as uow:
            rows = uow.topic_attributions.find_by_date(ECOSYSTEM, TENANT_ID, TRACKING_DATE)

        assert len(rows) > 0


class TestTopicAttributionE2ECostMappingOverride:
    def test_even_split_override_produces_equal_amounts(self, storage: SQLModelBackend) -> None:
        """With even_split override, all topics receive equal cost share."""
        from core.engine.topic_attribution import TopicAttributionPhase

        _insert_billing(storage, "KAFKA_NUM_CKU", Decimal("100.00"))  # normally bytes_ratio

        # KAFKA_NUM_CKU normally uses bytes_ratio — override to even_split
        topics = ["topic-a", "topic-b", "topic-c", "topic-d"]
        _insert_topic_resources(storage, topics)
        metrics_source = _make_metrics_source_with_topics(
            topics,
            bytes_per_topic={"topic-a": 1000.0, "topic-b": 10.0, "topic-c": 5.0, "topic-d": 500.0},
        )

        ta_config = _make_ta_config(cost_mapping_overrides={"KAFKA_NUM_CKU": "even_split"})
        phase = TopicAttributionPhase(
            ecosystem=ECOSYSTEM,
            tenant_id=TENANT_ID,
            metrics_source=metrics_source,
            config=ta_config,
            metrics_step=timedelta(hours=1),
        )

        with storage.create_unit_of_work() as uow:
            phase.run(uow, TRACKING_DATE)
            uow.commit()

        with storage.create_read_only_unit_of_work() as uow:
            rows = uow.topic_attributions.find_by_date(ECOSYSTEM, TENANT_ID, TRACKING_DATE)

        assert len(rows) == 4
        amounts = {r.topic_name: r.amount for r in rows}
        # All four topics should have the same amount under even_split
        unique_amounts = set(amounts.values())
        assert len(unique_amounts) == 1, f"Expected even split, got {amounts}"

    def test_bytes_ratio_default_produces_proportional_amounts(self, storage: SQLModelBackend) -> None:
        """Without override, KAFKA_NETWORK_WRITE uses bytes_ratio — amounts differ."""
        from core.engine.topic_attribution import TopicAttributionPhase

        _insert_billing(storage, "KAFKA_NETWORK_WRITE", Decimal("100.00"))

        topics = ["heavy-topic", "light-topic"]
        _insert_topic_resources(storage, topics)
        metrics_source = _make_metrics_source_with_topics(
            topics,
            bytes_per_topic={"heavy-topic": 900.0, "light-topic": 100.0},
        )

        ta_config = _make_ta_config()  # no overrides
        phase = TopicAttributionPhase(
            ecosystem=ECOSYSTEM,
            tenant_id=TENANT_ID,
            metrics_source=metrics_source,
            config=ta_config,
            metrics_step=timedelta(hours=1),
        )

        with storage.create_unit_of_work() as uow:
            phase.run(uow, TRACKING_DATE)
            uow.commit()

        with storage.create_read_only_unit_of_work() as uow:
            rows = uow.topic_attributions.find_by_date(ECOSYSTEM, TENANT_ID, TRACKING_DATE)

        assert len(rows) == 2
        amounts = {r.topic_name: r.amount for r in rows}
        # heavy-topic should receive ~90%, light-topic ~10%
        assert amounts["heavy-topic"] > amounts["light-topic"]
