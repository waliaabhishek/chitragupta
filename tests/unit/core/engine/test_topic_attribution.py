from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from unittest.mock import MagicMock

import pytest


def _make_config(**overrides):
    from plugins.confluent_cloud.config import TopicAttributionConfig

    defaults = dict(
        enabled=True,
        exclude_topic_patterns=["__consumer_offsets", "_schemas", "_confluent-*"],
        missing_metrics_behavior="even_split",
    )
    defaults.update(overrides)
    return TopicAttributionConfig(**defaults)


def _make_billing_line(
    resource_id: str = "lkc-abc",
    product_type: str = "KAFKA_NETWORK_WRITE",
    total_cost: Decimal = Decimal("10.00"),
    env_id: str = "env-1",
    product_category: str = "KAFKA",
    timestamp: datetime | None = None,
    granularity: str = "daily",
) -> MagicMock:
    line = MagicMock()
    line.resource_id = resource_id
    line.product_type = product_type
    line.total_cost = total_cost
    line.env_id = env_id
    line.product_category = product_category
    line.timestamp = timestamp or datetime(2026, 1, 1, tzinfo=UTC)
    line.granularity = granularity
    return line


def _make_resource(display_name: str, parent_id: str = "lkc-abc") -> MagicMock:
    r = MagicMock()
    r.display_name = display_name
    r.parent_id = parent_id
    return r


_UNSET = object()


def _make_phase(
    config=None,
    metrics_source: object = _UNSET,
):
    from core.engine.topic_attribution import TopicAttributionPhase

    if metrics_source is _UNSET:
        metrics_source = MagicMock()
    cfg = config or _make_config()
    return TopicAttributionPhase(
        ecosystem="eco",
        tenant_id="t1",
        metrics_source=metrics_source,
        config=cfg,
        metrics_step=timedelta(minutes=1),
    )


class TestTopicAttributionPhasePrometheusFailure:
    def test_prometheus_infra_failure_skips_cluster(self) -> None:
        """_fetch_topic_metrics raises → returns None → _attribute_cluster skips cluster, 0 rows."""
        mock_metrics_source = MagicMock()
        mock_metrics_source.query.side_effect = RuntimeError("connection refused")

        phase = _make_phase(metrics_source=mock_metrics_source)

        mock_uow = MagicMock()
        mock_uow.billing.find_by_date.return_value = [
            _make_billing_line(resource_id="lkc-abc", product_type="KAFKA_NETWORK_WRITE"),
        ]
        mock_uow.resources.find_by_parent.return_value = [
            _make_resource("topic-a"),
            _make_resource("topic-b"),
        ]
        mock_uow.topic_attributions.upsert_batch.return_value = 0

        count = phase.run(mock_uow, date(2026, 1, 1))
        assert count == 0
        mock_uow.topic_attributions.upsert_batch.assert_not_called()

    def test_prometheus_infra_failure_still_marks_calculated(self) -> None:
        """Even if cluster skipped, pipeline state is still marked topic_attribution_calculated."""
        mock_metrics_source = MagicMock()
        mock_metrics_source.query.side_effect = RuntimeError("down")

        phase = _make_phase(metrics_source=mock_metrics_source)
        mock_uow = MagicMock()
        mock_uow.billing.find_by_date.return_value = [
            _make_billing_line(resource_id="lkc-abc"),
        ]
        mock_uow.resources.find_by_parent.return_value = [_make_resource("topic-a")]

        phase.run(mock_uow, date(2026, 1, 1))
        mock_uow.pipeline_state.mark_topic_attribution_calculated.assert_called_once_with("eco", "t1", date(2026, 1, 1))


class TestTopicAttributionPhaseNoTopics:
    def test_no_topics_in_resources_skips_cluster(self) -> None:
        """_get_cluster_topics() returns empty frozenset → cluster skipped, 0 rows."""
        phase = _make_phase()
        mock_uow = MagicMock()
        mock_uow.billing.find_by_date.return_value = [
            _make_billing_line(resource_id="lkc-abc"),
        ]
        mock_uow.resources.find_by_parent.return_value = []  # no topics

        count = phase.run(mock_uow, date(2026, 1, 1))
        assert count == 0
        mock_uow.topic_attributions.upsert_batch.assert_not_called()


class TestTopicAttributionPipelineStateFlags:
    def test_run_marks_topic_attribution_calculated(self) -> None:
        """After TopicAttributionPhase.run() → topic_attribution_calculated=True."""
        mock_metrics_source = MagicMock()
        mock_metrics_source.query.return_value = {
            "topic_bytes_in": [MagicMock(labels={"topic": "orders", "kafka_id": "lkc-abc"}, value=100.0)],
        }

        phase = _make_phase(metrics_source=mock_metrics_source)
        mock_uow = MagicMock()
        mock_uow.billing.find_by_date.return_value = [
            _make_billing_line(resource_id="lkc-abc"),
        ]
        mock_uow.resources.find_by_parent.return_value = [_make_resource("orders")]
        mock_uow.topic_attributions.upsert_batch.return_value = 1

        phase.run(mock_uow, date(2026, 1, 1))
        mock_uow.pipeline_state.mark_topic_attribution_calculated.assert_called_once_with("eco", "t1", date(2026, 1, 1))

    def test_mark_needs_recalculation_resets_topic_attribution_calculated(self) -> None:
        """mark_needs_recalculation() → topic_attribution_calculated=False, topic_overlay_gathered stays True."""
        from sqlmodel import Session, SQLModel, create_engine

        from core.storage.backends.sqlmodel.repositories import SQLModelPipelineStateRepository
        from core.storage.backends.sqlmodel.tables import PipelineStateTable

        engine = create_engine("sqlite://", echo=False)
        SQLModel.metadata.create_all(engine)

        with Session(engine) as session:
            # Insert a pipeline state with both flags True
            ps = PipelineStateTable(
                ecosystem="eco",
                tenant_id="t1",
                tracking_date=date(2026, 1, 1),
                billing_gathered=True,
                resources_gathered=True,
                chargeback_calculated=True,
                topic_overlay_gathered=True,
                topic_attribution_calculated=True,
            )
            session.add(ps)
            session.commit()

            repo = SQLModelPipelineStateRepository(session)
            repo.mark_needs_recalculation("eco", "t1", date(2026, 1, 1))
            session.commit()

            from sqlmodel import select

            result = session.exec(
                select(PipelineStateTable).where(
                    PipelineStateTable.ecosystem == "eco",
                    PipelineStateTable.tenant_id == "t1",
                )
            ).first()

            assert result is not None
            assert result.topic_attribution_calculated is False
            assert result.topic_overlay_gathered is True  # unchanged

    def test_pipeline_state_to_domain_maps_new_fields(self) -> None:
        """pipeline_state_to_domain maps topic_overlay_gathered and topic_attribution_calculated."""
        from core.storage.backends.sqlmodel.mappers import pipeline_state_to_domain
        from core.storage.backends.sqlmodel.tables import PipelineStateTable

        table_row = PipelineStateTable(
            ecosystem="eco",
            tenant_id="t1",
            tracking_date=date(2026, 1, 1),
            billing_gathered=True,
            resources_gathered=True,
            chargeback_calculated=False,
            topic_overlay_gathered=True,
            topic_attribution_calculated=False,
        )
        domain = pipeline_state_to_domain(table_row)
        assert domain.topic_overlay_gathered is True
        assert domain.topic_attribution_calculated is False


class TestTopicAttributionZeroImpactDisabled:
    def test_disabled_no_overlay_rows(self) -> None:
        """topic_attribution.enabled=False → no overlay loop, chargeback rows unchanged."""
        from core.engine.orchestrator import ChargebackOrchestrator

        mock_storage = MagicMock()
        mock_uow = MagicMock()
        mock_uow.__enter__ = MagicMock(return_value=mock_uow)
        mock_uow.__exit__ = MagicMock(return_value=False)
        mock_storage.create_unit_of_work.return_value = mock_uow
        mock_storage.create_read_only_unit_of_work.return_value = mock_uow

        # PipelineState with topic overlay pending
        from core.models.pipeline import PipelineState

        pending_state = PipelineState(
            ecosystem="eco",
            tenant_id="t1",
            tracking_date=date(2026, 1, 1),
            billing_gathered=True,
            resources_gathered=True,
            chargeback_calculated=True,
            topic_overlay_gathered=True,
            topic_attribution_calculated=False,
        )
        mock_uow.pipeline_state.find_needing_topic_attribution.return_value = [pending_state]
        mock_uow.pipeline_state.find_needing_calculation.return_value = []
        mock_uow.pipeline_runs.get_active_run.return_value = None

        from core.config.models import TenantConfig

        tenant_config = TenantConfig(
            ecosystem="eco",
            tenant_id="t1",
            lookback_days=30,
            cutoff_days=5,
        )
        orchestrator = ChargebackOrchestrator(
            ecosystem="eco",
            tenant_id="t1",
            tenant_name="test",
            tenant_config=tenant_config,
            storage_backend=mock_storage,
            plugin_bundle=MagicMock(),
            metrics_source=None,
            metrics_step=timedelta(minutes=1),
        )

        # topic_overlay_phase must be None since config has enabled=False
        assert orchestrator._topic_overlay_phase is None


class TestGatherPhaseTopicDiscovery:
    """GIT-1: GatherPhase topic discover block when TA enabled."""

    def test_gather_topic_resources_called_when_enabled(self) -> None:
        """TA enabled + plugin implements TopicDiscoveryPlugin → upsert topics + mark_topic_overlay_gathered."""
        from unittest.mock import MagicMock, patch

        from core.engine.orchestrator import GatherPhase
        from core.plugin.protocols import TopicDiscoveryPlugin

        # Explicit assignment puts the attribute in __dict__, satisfying
        # runtime_checkable Protocol isinstance checks (Python 3.12+ uses
        # inspect.getattr_static which skips __getattr__/MagicMock._mock_children).
        mock_plugin = MagicMock()
        mock_plugin.ecosystem = "eco"
        discovered = MagicMock()
        discovered.resource_id = "lkc-abc:topic:orders"
        mock_plugin.gather_topic_resources = MagicMock(return_value=[discovered])
        mock_plugin.build_shared_context.return_value = None

        assert isinstance(mock_plugin, TopicDiscoveryPlugin)

        mock_ta_config = MagicMock()
        mock_ta_config.enabled = True
        mock_plugin.get_overlay_config = MagicMock(return_value=mock_ta_config)

        mock_bundle = MagicMock()
        mock_bundle.plugin = mock_plugin
        mock_bundle.handlers = {}

        mock_tenant_config = MagicMock()
        mock_tenant_config.lookback_days = 30
        mock_tenant_config.cutoff_days = 5
        mock_tenant_config.zero_gather_deletion_threshold = -1

        phase = GatherPhase(
            ecosystem="eco",
            tenant_id="t1",
            tenant_config=mock_tenant_config,
            bundle=mock_bundle,
        )
        assert phase._topic_attribution_enabled is True

        mock_uow = MagicMock()
        billing_date = date(2026, 1, 1)

        # _gather_billing returns a date so the discovery block is triggered;
        # _detect_deletions patched to avoid find_active_at unpack errors.
        with (
            patch.object(phase, "_gather_billing", return_value={billing_date}),
            patch.object(phase, "_gather_resources_and_identities", return_value=(set(), set())),
            patch.object(phase, "_apply_recalculation_window"),
            patch.object(phase, "_detect_deletions"),
        ):
            phase._run_full(mock_uow)

        mock_plugin.gather_topic_resources.assert_called_once_with("t1", [])
        mock_uow.resources.upsert.assert_called_once_with(discovered)
        mock_uow.pipeline_state.mark_topic_overlay_gathered.assert_called_once_with(
            "eco",
            "t1",
            billing_date,
        )

    def test_gather_topic_resources_not_called_when_disabled(self) -> None:
        """TA disabled → gather_topic_resources never called."""
        from unittest.mock import MagicMock, patch

        from core.engine.orchestrator import GatherPhase

        mock_plugin = MagicMock()
        mock_plugin.build_shared_context.return_value = None

        mock_bundle = MagicMock()
        mock_bundle.plugin = mock_plugin
        mock_bundle.handlers = {}

        mock_ta_config = MagicMock()
        mock_ta_config.enabled = False
        mock_plugin_settings = MagicMock()
        mock_plugin_settings.topic_attribution = mock_ta_config

        mock_tenant_config = MagicMock()
        mock_tenant_config.plugin_settings = mock_plugin_settings
        mock_tenant_config.lookback_days = 30
        mock_tenant_config.cutoff_days = 5
        mock_tenant_config.zero_gather_deletion_threshold = -1

        phase = GatherPhase(
            ecosystem="eco",
            tenant_id="t1",
            tenant_config=mock_tenant_config,
            bundle=mock_bundle,
        )
        assert phase._topic_attribution_enabled is False

        mock_uow = MagicMock()
        with (
            patch.object(phase, "_gather_billing", return_value={date(2026, 1, 1)}),
            patch.object(phase, "_gather_resources_and_identities", return_value=(set(), set())),
            patch.object(phase, "_apply_recalculation_window"),
            patch.object(phase, "_detect_deletions"),
        ):
            phase._run_full(mock_uow)

        mock_plugin.gather_topic_resources.assert_not_called()


class TestChargebackOrchestratorOverlayLoop:
    """GIT-2: ChargebackOrchestrator.run() topic overlay loop when phase is set."""

    def _make_mock_storage(self):
        mock_storage = MagicMock()
        mock_uow = MagicMock()
        mock_uow.__enter__ = MagicMock(return_value=mock_uow)
        mock_uow.__exit__ = MagicMock(return_value=False)
        mock_storage.create_unit_of_work.return_value = mock_uow
        return mock_storage, mock_uow

    def test_overlay_loop_calls_phase_run_for_pending_states(self) -> None:
        """Overlay loop: for each pending state, phase.run() is called and UoW is committed."""
        from core.config.models import TenantConfig
        from core.engine.orchestrator import ChargebackOrchestrator
        from core.models.pipeline import PipelineState

        mock_storage, mock_uow = self._make_mock_storage()
        mock_uow.pipeline_state.find_needing_calculation.return_value = []
        mock_uow.pipeline_runs.get_active_run.return_value = None

        pending = PipelineState(
            ecosystem="eco",
            tenant_id="t1",
            tracking_date=date(2026, 1, 1),
            billing_gathered=True,
            resources_gathered=True,
            chargeback_calculated=True,
            topic_overlay_gathered=True,
            topic_attribution_calculated=False,
        )
        mock_uow.pipeline_state.find_needing_topic_attribution.return_value = [pending]

        tenant_config = TenantConfig(
            ecosystem="eco",
            tenant_id="t1",
            lookback_days=30,
            cutoff_days=5,
        )

        orchestrator = ChargebackOrchestrator(
            tenant_name="test",
            tenant_config=tenant_config,
            storage_backend=mock_storage,
            plugin_bundle=MagicMock(),
        )

        # Stub gather phase so run() doesn't fail early before the overlay loop.
        from core.engine.orchestrator import GatherResult

        mock_gather = MagicMock()
        mock_gather.run.return_value = GatherResult(dates_gathered=0, errors=[])
        orchestrator._gather_phase = mock_gather

        mock_phase = MagicMock()
        mock_phase.run.return_value = 5
        orchestrator._topic_overlay_phase = mock_phase

        orchestrator.run()

        mock_phase.run.assert_called_once_with(mock_uow, date(2026, 1, 1))
        mock_uow.commit.assert_called()

    def test_overlay_loop_skipped_when_phase_is_none(self) -> None:
        """When _topic_overlay_phase is None, find_needing_topic_attribution is never called."""
        from core.config.models import TenantConfig
        from core.engine.orchestrator import ChargebackOrchestrator

        mock_storage, mock_uow = self._make_mock_storage()
        mock_uow.pipeline_state.find_needing_calculation.return_value = []
        mock_uow.pipeline_runs.get_active_run.return_value = None

        tenant_config = TenantConfig(
            ecosystem="eco",
            tenant_id="t1",
            lookback_days=30,
            cutoff_days=5,
        )

        orchestrator = ChargebackOrchestrator(
            tenant_name="test",
            tenant_config=tenant_config,
            storage_backend=mock_storage,
            plugin_bundle=MagicMock(),
        )

        assert orchestrator._topic_overlay_phase is None
        orchestrator.run()

        mock_uow.pipeline_state.find_needing_topic_attribution.assert_not_called()


class TestFetchTopicMetricsWithoutMetricsSource:
    def test_fetch_topic_metrics_without_metrics_source_raises_runtime_error(self) -> None:
        from datetime import UTC, datetime

        phase = _make_phase(metrics_source=None)
        b_start = datetime(2026, 1, 1, tzinfo=UTC)
        b_end = datetime(2026, 1, 2, tzinfo=UTC)

        with pytest.raises(RuntimeError, match="should have been caught at config validation"):
            phase._fetch_topic_metrics("lkc-abc", b_start, b_end)


class TestTopicAttributionPhaseIntegration:
    """Integration tests: TopicAttributionPhase.run() against real SQLite storage."""

    @pytest.fixture
    def storage(self):
        from core.storage.backends.sqlmodel.module import CoreStorageModule
        from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend

        backend = SQLModelBackend("sqlite:///:memory:", CoreStorageModule(), use_migrations=False)
        backend.create_tables()
        return backend

    def test_run_writes_rows_to_topic_attributions(self, storage) -> None:
        """TopicAttributionPhase.run() with real SQLite: even-split rows saved, count returned."""
        from core.engine.topic_attribution import TopicAttributionPhase
        from core.models.billing import CoreBillingLineItem
        from core.models.resource import CoreResource
        from plugins.confluent_cloud.config import TopicAttributionConfig

        tracking_date = date(2026, 1, 1)
        timestamp = datetime(2026, 1, 1, tzinfo=UTC)

        cfg = TopicAttributionConfig(
            enabled=True,
            missing_metrics_behavior="even_split",
            exclude_topic_patterns=["__consumer_offsets"],
        )

        # Seed DB: topic resources + billing line
        with storage.create_unit_of_work() as uow:
            for topic in ("orders", "payments"):
                uow.resources.upsert(
                    CoreResource(
                        ecosystem="eco",
                        tenant_id="t1",
                        resource_id=f"lkc-abc:topic:{topic}",
                        resource_type="topic",
                        display_name=topic,
                        parent_id="lkc-abc",
                    )
                )
            uow.billing.upsert(
                CoreBillingLineItem(
                    ecosystem="eco",
                    tenant_id="t1",
                    timestamp=timestamp,
                    resource_id="lkc-abc",
                    product_category="KAFKA",
                    product_type="KAFKA_NETWORK_WRITE",
                    quantity=Decimal("1"),
                    unit_price=Decimal("10.00"),
                    total_cost=Decimal("10.00"),
                    granularity="daily",
                )
            )
            uow.commit()

        metrics_source = MagicMock()
        metrics_source.query.return_value = {}  # Prometheus healthy but no data for cluster

        phase = TopicAttributionPhase(
            ecosystem="eco",
            tenant_id="t1",
            metrics_source=metrics_source,  # healthy metrics source, empty response → even_split fallback
            config=cfg,
            metrics_step=timedelta(minutes=1),
        )

        with storage.create_unit_of_work() as uow:
            count = phase.run(uow, tracking_date)
            uow.commit()

        assert count == 2  # one row per topic

        # Verify rows are queryable from the DB
        with storage.create_unit_of_work() as uow:
            rows, total = uow.topic_attributions.find_by_filters(
                ecosystem="eco",
                tenant_id="t1",
                limit=100,
                offset=0,
            )

        assert total == 2
        topic_names = {r.topic_name for r in rows}
        assert topic_names == {"orders", "payments"}
        total_amount = sum(r.amount for r in rows)
        assert total_amount == Decimal("10.00")
        assert all(r.attribution_method == "even_split" for r in rows)
