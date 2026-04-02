from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock

import pytest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

ECOSYSTEM = "eco"
TENANT_ID = "t1"
NOW = datetime(2026, 1, 1, tzinfo=UTC)


def _make_config(**overrides: Any) -> Any:
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
) -> MagicMock:
    line = MagicMock()
    line.resource_id = resource_id
    line.product_type = product_type
    line.total_cost = total_cost
    line.env_id = env_id
    line.product_category = product_category
    line.timestamp = NOW
    line.granularity = "daily"
    return line


def _make_phase(
    config: Any = None,
    metrics_source: object = None,
    retry_checker: Any = None,
) -> Any:
    from core.engine.topic_attribution import TopicAttributionPhase

    cfg = config or _make_config()
    ms = metrics_source if metrics_source is not None else MagicMock()
    return TopicAttributionPhase(
        ecosystem=ECOSYSTEM,
        tenant_id=TENANT_ID,
        metrics_source=ms,
        config=cfg,
        metrics_step=timedelta(minutes=1),
        retry_checker=retry_checker,
    )


def _make_uow(
    billing_lines: list[Any] | None = None,
    topics: list[Any] | None = None,
) -> MagicMock:
    mock_uow = MagicMock()
    mock_uow.billing.find_by_date.return_value = billing_lines or []
    topic_mocks = []
    for t in topics or ["topic-a"]:
        r = MagicMock()
        r.display_name = t
        r.parent_id = "lkc-abc"
        topic_mocks.append(r)
    mock_uow.resources.find_by_period.return_value = (topic_mocks, len(topic_mocks))
    mock_uow.topic_attributions.upsert_batch.return_value = 0
    return mock_uow


def _make_retry_checker(new_attempts: int = 1, should_fallback: bool = False) -> MagicMock:
    checker = MagicMock()
    checker.increment_and_check.return_value = (new_attempts, should_fallback)
    return checker


# ---------------------------------------------------------------------------
# AC#1 — Column exists on BillingTable and CCloudBillingTable
# ---------------------------------------------------------------------------


class TestAC1ColumnExists:
    def test_billing_table_has_topic_attribution_attempts(self) -> None:
        from core.storage.backends.sqlmodel.base_tables import BillingTable

        col_names = {c.name for c in BillingTable.__table__.columns}
        assert "topic_attribution_attempts" in col_names

    def test_billing_table_topic_attribution_attempts_default_zero(self) -> None:
        from core.storage.backends.sqlmodel.base_tables import BillingTable

        col = BillingTable.__table__.columns["topic_attribution_attempts"]
        # Field default=0 means SQLModel sets col.default.arg == 0
        default_value = col.default.arg if col.default is not None else None
        assert default_value == 0, f"Expected default=0, got {default_value!r}"

    def test_ccloud_billing_table_has_topic_attribution_attempts(self) -> None:
        from plugins.confluent_cloud.storage.tables import CCloudBillingTable

        col_names = {c.name for c in CCloudBillingTable.__table__.columns}
        assert "topic_attribution_attempts" in col_names


# ---------------------------------------------------------------------------
# AC#2 — TenantConfig has topic_attribution_retry_limit with correct defaults/validation
# ---------------------------------------------------------------------------


class TestAC2ConfigField:
    def test_tenant_config_has_topic_attribution_retry_limit_default_3(self) -> None:
        from core.config.models import TenantConfig

        tc = TenantConfig(ecosystem="x", tenant_id="y")
        assert tc.topic_attribution_retry_limit == 3

    def test_tenant_config_rejects_limit_zero(self) -> None:
        from pydantic import ValidationError

        from core.config.models import TenantConfig

        with pytest.raises(ValidationError):
            TenantConfig(ecosystem="x", tenant_id="y", topic_attribution_retry_limit=0)

    def test_tenant_config_rejects_limit_eleven(self) -> None:
        from pydantic import ValidationError

        from core.config.models import TenantConfig

        with pytest.raises(ValidationError):
            TenantConfig(ecosystem="x", tenant_id="y", topic_attribution_retry_limit=11)

    def test_tenant_config_accepts_limit_10(self) -> None:
        from core.config.models import TenantConfig

        tc = TenantConfig(ecosystem="x", tenant_id="y", topic_attribution_retry_limit=10)
        assert tc.topic_attribution_retry_limit == 10

    def test_tenant_config_accepts_limit_1(self) -> None:
        from core.config.models import TenantConfig

        tc = TenantConfig(ecosystem="x", tenant_id="y", topic_attribution_retry_limit=1)
        assert tc.topic_attribution_retry_limit == 1


# ---------------------------------------------------------------------------
# AC#3 — Counter incremented per billing line on cluster failure
# ---------------------------------------------------------------------------


class TestAC3CounterIncrementedOnFailure:
    def test_increment_topic_attribution_attempts_called_per_line(self) -> None:
        """metrics_source raises → increment_and_check called once per billing line."""
        failing_metrics = MagicMock()
        failing_metrics.query.side_effect = RuntimeError("prometheus down")

        retry_checker = _make_retry_checker(new_attempts=1, should_fallback=False)
        phase = _make_phase(metrics_source=failing_metrics, retry_checker=retry_checker)

        line1 = _make_billing_line(resource_id="lkc-abc", product_type="KAFKA_NETWORK_WRITE")
        line2 = _make_billing_line(resource_id="lkc-abc", product_type="KAFKA_STORAGE")
        mock_uow = _make_uow(billing_lines=[line1, line2])

        phase.run(mock_uow, date(2026, 1, 1))

        assert retry_checker.increment_and_check.call_count == 2
        retry_checker.increment_and_check.assert_any_call(line1)
        retry_checker.increment_and_check.assert_any_call(line2)


# ---------------------------------------------------------------------------
# AC#4 — Below limit: date stays pending, mark_calculated NOT called
# ---------------------------------------------------------------------------


class TestAC4BelowLimitDatePending:
    def test_below_limit_mark_calculated_not_called(self) -> None:
        """limit=3, fail once (attempt=1): mark_topic_attribution_calculated NOT called."""
        failing_metrics = MagicMock()
        failing_metrics.query.side_effect = RuntimeError("down")

        # attempt=1, below limit=3 → should_fallback=False
        retry_checker = _make_retry_checker(new_attempts=1, should_fallback=False)
        phase = _make_phase(metrics_source=failing_metrics, retry_checker=retry_checker)

        line = _make_billing_line()
        mock_uow = _make_uow(billing_lines=[line])

        phase.run(mock_uow, date(2026, 1, 1))

        mock_uow.pipeline_state.mark_topic_attribution_calculated.assert_not_called()

    def test_below_limit_attribute_cluster_returns_none(self) -> None:
        """_attribute_cluster returns None when retry checker says below limit."""
        failing_metrics = MagicMock()
        failing_metrics.query.side_effect = RuntimeError("down")

        retry_checker = _make_retry_checker(new_attempts=1, should_fallback=False)
        phase = _make_phase(metrics_source=failing_metrics, retry_checker=retry_checker)

        mock_uow = MagicMock()
        mock_uow.resources.find_by_period.return_value = ([MagicMock()], 1)

        result = phase._attribute_cluster(mock_uow, "lkc-abc", "env-1", [_make_billing_line()], date(2026, 1, 1))
        assert result is None


# ---------------------------------------------------------------------------
# AC#5 — At limit: sentinel rows produced
# ---------------------------------------------------------------------------


class TestAC5AtLimitSentinelRows:
    def test_at_limit_attribute_cluster_returns_sentinel_rows(self) -> None:
        """increment_and_check returns (3, True) for all lines → sentinels returned."""
        failing_metrics = MagicMock()
        failing_metrics.query.side_effect = RuntimeError("dead cluster")

        # All at limit
        retry_checker = _make_retry_checker(new_attempts=3, should_fallback=True)
        phase = _make_phase(metrics_source=failing_metrics, retry_checker=retry_checker)

        line = _make_billing_line(total_cost=Decimal("25.00"))
        mock_uow = MagicMock()
        mock_uow.resources.find_by_period.return_value = ([MagicMock()], 1)

        result = phase._attribute_cluster(mock_uow, "lkc-abc", "env-1", [line], date(2026, 1, 1))

        assert result is not None
        assert len(result) == 1
        sentinel = result[0]
        assert sentinel.topic_name == "__UNATTRIBUTED__"
        assert sentinel.attribution_method == "ATTRIBUTION_FAILED"
        assert sentinel.amount == Decimal("25.00")

    def test_at_limit_one_sentinel_per_billing_line(self) -> None:
        """One sentinel row per billing line when all at limit."""
        failing_metrics = MagicMock()
        failing_metrics.query.side_effect = RuntimeError("dead")

        retry_checker = _make_retry_checker(new_attempts=3, should_fallback=True)
        phase = _make_phase(metrics_source=failing_metrics, retry_checker=retry_checker)

        lines = [
            _make_billing_line(product_type="KAFKA_NETWORK_WRITE", total_cost=Decimal("10.00")),
            _make_billing_line(product_type="KAFKA_STORAGE", total_cost=Decimal("5.00")),
            _make_billing_line(product_type="KAFKA_NUM_CKUS", total_cost=Decimal("3.00")),
        ]
        mock_uow = MagicMock()
        mock_uow.resources.find_by_period.return_value = ([MagicMock()], 1)

        result = phase._attribute_cluster(mock_uow, "lkc-abc", "env-1", lines, date(2026, 1, 1))

        assert result is not None
        assert len(result) == 3
        for sentinel in result:
            assert sentinel.topic_name == "__UNATTRIBUTED__"
            assert sentinel.attribution_method == "ATTRIBUTION_FAILED"

    def test_at_limit_sentinel_amount_equals_total_cost(self) -> None:
        """Sentinel amount preserves full cost from billing line."""
        failing_metrics = MagicMock()
        failing_metrics.query.side_effect = RuntimeError("dead")

        retry_checker = _make_retry_checker(new_attempts=3, should_fallback=True)
        phase = _make_phase(metrics_source=failing_metrics, retry_checker=retry_checker)

        cost = Decimal("99.50")
        line = _make_billing_line(total_cost=cost)
        mock_uow = MagicMock()
        mock_uow.resources.find_by_period.return_value = ([MagicMock()], 1)

        result = phase._attribute_cluster(mock_uow, "lkc-abc", "env-1", [line], date(2026, 1, 1))

        assert result is not None
        assert result[0].amount == cost


# ---------------------------------------------------------------------------
# AC#6a — All clusters resolved: date IS marked calculated
# ---------------------------------------------------------------------------


class TestAC6aAllClustersResolved:
    def test_cluster_a_success_cluster_b_sentinel_marks_calculated(self) -> None:
        """Cluster A succeeds normally, cluster B at limit (sentinels): date IS marked calculated."""
        mock_metrics_source = MagicMock()

        def _query_side_effect(**kwargs: Any) -> dict:
            resource_id = kwargs.get("resource_id_filter", "")
            if resource_id == "lkc-bbb":
                raise RuntimeError("dead cluster")
            return {}  # lkc-aaa: empty metrics → even_split

        mock_metrics_source.query.side_effect = _query_side_effect

        # Cluster B at limit
        retry_checker = _make_retry_checker(new_attempts=3, should_fallback=True)
        phase = _make_phase(metrics_source=mock_metrics_source, retry_checker=retry_checker)

        line_a = _make_billing_line(resource_id="lkc-aaa", env_id="env-1")
        line_b = _make_billing_line(resource_id="lkc-bbb", env_id="env-1")
        mock_uow = _make_uow(billing_lines=[line_a, line_b])
        mock_uow.topic_attributions.upsert_batch.return_value = 2

        phase.run(mock_uow, date(2026, 1, 1))

        mock_uow.pipeline_state.mark_topic_attribution_calculated.assert_called_once_with(
            ECOSYSTEM, TENANT_ID, date(2026, 1, 1)
        )

    def test_cluster_b_sentinel_rows_are_upserted(self) -> None:
        """Sentinel rows from cluster B at limit are upserted into topic_attributions."""
        mock_metrics_source = MagicMock()

        def _query_side_effect(**kwargs: Any) -> dict:
            if kwargs.get("resource_id_filter") == "lkc-bbb":
                raise RuntimeError("dead")
            return {}

        mock_metrics_source.query.side_effect = _query_side_effect

        retry_checker = _make_retry_checker(new_attempts=3, should_fallback=True)
        phase = _make_phase(metrics_source=mock_metrics_source, retry_checker=retry_checker)

        line_a = _make_billing_line(resource_id="lkc-aaa", env_id="env-1")
        line_b = _make_billing_line(resource_id="lkc-bbb", env_id="env-1")
        mock_uow = _make_uow(billing_lines=[line_a, line_b])

        phase.run(mock_uow, date(2026, 1, 1))

        mock_uow.topic_attributions.upsert_batch.assert_called()


# ---------------------------------------------------------------------------
# AC#6b — Mixed pending: date NOT marked, but successful rows upserted
# ---------------------------------------------------------------------------


class TestAC6bMixedPending:
    def test_cluster_a_success_cluster_b_below_limit_not_marked(self) -> None:
        """Cluster A succeeds, cluster B below limit (returns None): date NOT marked."""
        mock_metrics_source = MagicMock()

        def _query_side_effect(**kwargs: Any) -> dict:
            if kwargs.get("resource_id_filter") == "lkc-bbb":
                raise RuntimeError("down")
            return {}

        mock_metrics_source.query.side_effect = _query_side_effect

        # Cluster B below limit
        retry_checker = _make_retry_checker(new_attempts=1, should_fallback=False)
        phase = _make_phase(metrics_source=mock_metrics_source, retry_checker=retry_checker)

        line_a = _make_billing_line(resource_id="lkc-aaa", env_id="env-1")
        line_b = _make_billing_line(resource_id="lkc-bbb", env_id="env-1")
        mock_uow = _make_uow(billing_lines=[line_a, line_b])

        phase.run(mock_uow, date(2026, 1, 1))

        mock_uow.pipeline_state.mark_topic_attribution_calculated.assert_not_called()

    def test_cluster_a_rows_upserted_despite_cluster_b_pending(self) -> None:
        """Cluster A successful rows are upserted even when cluster B is still pending."""
        mock_metrics_source = MagicMock()

        def _query_side_effect(**kwargs: Any) -> dict:
            if kwargs.get("resource_id_filter") == "lkc-bbb":
                raise RuntimeError("down")
            return {}

        mock_metrics_source.query.side_effect = _query_side_effect

        retry_checker = _make_retry_checker(new_attempts=1, should_fallback=False)
        phase = _make_phase(metrics_source=mock_metrics_source, retry_checker=retry_checker)

        line_a = _make_billing_line(resource_id="lkc-aaa", env_id="env-1")
        line_b = _make_billing_line(resource_id="lkc-bbb", env_id="env-1")
        mock_uow = _make_uow(billing_lines=[line_a, line_b])
        mock_uow.topic_attributions.upsert_batch.return_value = 1

        phase.run(mock_uow, date(2026, 1, 1))

        # upsert_batch called with rows for cluster A only (cluster B is pending → None)
        mock_uow.topic_attributions.upsert_batch.assert_called_once()
        upserted_rows = mock_uow.topic_attributions.upsert_batch.call_args[0][0]
        # All upserted rows must belong to cluster A (lkc-aaa), not cluster B (lkc-bbb)
        assert len(upserted_rows) > 0
        for row in upserted_rows:
            assert row.cluster_resource_id == "lkc-aaa", (
                f"Expected only cluster A rows, got cluster_resource_id={row.cluster_resource_id!r}"
            )


# ---------------------------------------------------------------------------
# AC#7 — Normal success path unaffected
# ---------------------------------------------------------------------------


class TestAC7NormalSuccessUnaffected:
    def test_working_metrics_no_increment_calls(self) -> None:
        """Working metrics → increment_topic_attribution_attempts never called."""
        mock_metrics_source = MagicMock()
        mock_metrics_source.query.return_value = {
            "topic_bytes_in": [MagicMock(labels={"topic": "orders", "kafka_id": "lkc-abc"}, value=100.0)],
        }

        retry_checker = MagicMock()
        phase = _make_phase(metrics_source=mock_metrics_source, retry_checker=retry_checker)

        line = _make_billing_line()
        mock_uow = _make_uow(billing_lines=[line])
        mock_uow.resources.find_by_period.return_value = ([MagicMock(display_name="orders", parent_id="lkc-abc")], 1)
        mock_uow.topic_attributions.upsert_batch.return_value = 1

        phase.run(mock_uow, date(2026, 1, 1))

        retry_checker.increment_and_check.assert_not_called()

    def test_working_metrics_marks_calculated(self) -> None:
        """Working metrics → mark_topic_attribution_calculated IS called."""
        mock_metrics_source = MagicMock()
        mock_metrics_source.query.return_value = {
            "topic_bytes_in": [MagicMock(labels={"topic": "orders", "kafka_id": "lkc-abc"}, value=100.0)],
        }

        phase = _make_phase(metrics_source=mock_metrics_source)

        line = _make_billing_line()
        mock_uow = _make_uow(billing_lines=[line])
        mock_uow.resources.find_by_period.return_value = ([MagicMock(display_name="orders", parent_id="lkc-abc")], 1)
        mock_uow.topic_attributions.upsert_batch.return_value = 1

        phase.run(mock_uow, date(2026, 1, 1))

        mock_uow.pipeline_state.mark_topic_attribution_calculated.assert_called_once_with(
            ECOSYSTEM, TENANT_ID, date(2026, 1, 1)
        )


# ---------------------------------------------------------------------------
# AC#8a — Persistence across runs (mock state)
# ---------------------------------------------------------------------------


class TestAC8aPersistenceAcrossRuns:
    def test_attempt_count_increments_across_two_runs(self) -> None:
        """Fail twice → increment_and_check called twice (simulating 2 separate runs)."""
        failing_metrics = MagicMock()
        failing_metrics.query.side_effect = RuntimeError("down")

        # Stateful mock: returns attempt=1 first call, attempt=2 second call
        _call_count = [0]

        def _increment_and_check(line: Any) -> tuple[int, bool]:
            _call_count[0] += 1
            return (_call_count[0], _call_count[0] >= 3)

        retry_checker = MagicMock()
        retry_checker.increment_and_check.side_effect = _increment_and_check

        phase = _make_phase(metrics_source=failing_metrics, retry_checker=retry_checker)

        line = _make_billing_line()
        mock_uow = _make_uow(billing_lines=[line])

        # Run 1 (attempt=1, below limit)
        phase.run(mock_uow, date(2026, 1, 1))
        assert retry_checker.increment_and_check.call_count == 1

        # Run 2 (attempt=2, still below limit)
        phase.run(mock_uow, date(2026, 1, 1))
        assert retry_checker.increment_and_check.call_count == 2


# ---------------------------------------------------------------------------
# AC#8b — Mixed cluster: both rows upserted, date marked
# ---------------------------------------------------------------------------


class TestAC8bMixedClusterBothUpserted:
    def test_cluster_a_success_cluster_b_at_limit_both_upserted_date_marked(self) -> None:
        """Cluster A (lkc-aaa) succeeds, cluster B (lkc-bbb) at limit: both rows upserted, date marked."""
        mock_metrics_source = MagicMock()

        def _query_side_effect(**kwargs: Any) -> dict:
            if kwargs.get("resource_id_filter") == "lkc-bbb":
                raise RuntimeError("dead")
            return {}

        mock_metrics_source.query.side_effect = _query_side_effect

        retry_checker = _make_retry_checker(new_attempts=3, should_fallback=True)
        phase = _make_phase(metrics_source=mock_metrics_source, retry_checker=retry_checker)

        line_a = _make_billing_line(resource_id="lkc-aaa", env_id="env-1", total_cost=Decimal("50.00"))
        line_b = _make_billing_line(resource_id="lkc-bbb", env_id="env-1", total_cost=Decimal("20.00"))
        mock_uow = _make_uow(billing_lines=[line_a, line_b])
        mock_uow.topic_attributions.upsert_batch.return_value = 2

        phase.run(mock_uow, date(2026, 1, 1))

        mock_uow.topic_attributions.upsert_batch.assert_called_once()
        mock_uow.pipeline_state.mark_topic_attribution_calculated.assert_called_once_with(
            ECOSYSTEM, TENANT_ID, date(2026, 1, 1)
        )


# ---------------------------------------------------------------------------
# AC#11 — retry_checker=None retains TASK-177 behavior (no increment calls)
# ---------------------------------------------------------------------------


class TestRetryCheckerNoneRetainsTask177Behavior:
    def test_no_retry_checker_attribute_cluster_returns_none(self) -> None:
        """Without retry_checker, infra failure → _attribute_cluster returns None."""
        failing_metrics = MagicMock()
        failing_metrics.query.side_effect = RuntimeError("down")

        # No retry_checker
        phase = _make_phase(metrics_source=failing_metrics, retry_checker=None)

        mock_uow = MagicMock()
        mock_uow.resources.find_by_period.return_value = ([MagicMock()], 1)

        result = phase._attribute_cluster(mock_uow, "lkc-abc", "env-1", [_make_billing_line()], date(2026, 1, 1))
        assert result is None

    def test_no_retry_checker_mark_calculated_not_called(self) -> None:
        """Without retry_checker, infra failure → mark_topic_attribution_calculated NOT called."""
        failing_metrics = MagicMock()
        failing_metrics.query.side_effect = RuntimeError("down")

        phase = _make_phase(metrics_source=failing_metrics, retry_checker=None)

        line = _make_billing_line()
        mock_uow = _make_uow(billing_lines=[line])

        phase.run(mock_uow, date(2026, 1, 1))

        mock_uow.pipeline_state.mark_topic_attribution_calculated.assert_not_called()

    def test_no_retry_checker_no_increment_calls(self) -> None:
        """Without retry_checker, no increment_and_check calls on any object."""
        failing_metrics = MagicMock()
        failing_metrics.query.side_effect = RuntimeError("down")

        phase = _make_phase(metrics_source=failing_metrics, retry_checker=None)

        line = _make_billing_line()
        mock_uow = _make_uow(billing_lines=[line])
        # Spy on billing to ensure no increment calls
        mock_uow.billing.increment_topic_attribution_attempts = MagicMock()
        mock_uow.billing.increment_allocation_attempts = MagicMock()

        phase.run(mock_uow, date(2026, 1, 1))

        mock_uow.billing.increment_topic_attribution_attempts.assert_not_called()
        mock_uow.billing.increment_allocation_attempts.assert_not_called()


# ---------------------------------------------------------------------------
# AC#12 — RetryManager backward compat (no increment_fn → uses allocation_attempts)
# ---------------------------------------------------------------------------


class TestRetryManagerBackwardCompat:
    def test_no_increment_fn_calls_increment_allocation_attempts(self) -> None:
        """RetryManager with no increment_fn → calls uow.billing.increment_allocation_attempts."""
        from core.engine.orchestrator import RetryManager

        mock_uow = MagicMock()
        mock_uow.billing.increment_allocation_attempts.return_value = 1
        mock_uow.__enter__ = MagicMock(return_value=mock_uow)
        mock_uow.__exit__ = MagicMock(return_value=False)

        mock_storage = MagicMock()
        mock_storage.create_unit_of_work.return_value = mock_uow

        line = _make_billing_line()

        # No increment_fn → default: increment_allocation_attempts
        manager = RetryManager(storage_backend=mock_storage, limit=3)
        manager.increment_and_check(line)

        mock_uow.billing.increment_allocation_attempts.assert_called_once_with(line)

    def test_no_increment_fn_does_not_call_increment_topic_attribution_attempts(self) -> None:
        """RetryManager with no increment_fn → does NOT call increment_topic_attribution_attempts."""
        from core.engine.orchestrator import RetryManager

        mock_uow = MagicMock()
        mock_uow.billing.increment_allocation_attempts.return_value = 1
        mock_uow.__enter__ = MagicMock(return_value=mock_uow)
        mock_uow.__exit__ = MagicMock(return_value=False)

        mock_storage = MagicMock()
        mock_storage.create_unit_of_work.return_value = mock_uow

        line = _make_billing_line()

        manager = RetryManager(storage_backend=mock_storage, limit=3)
        manager.increment_and_check(line)

        mock_uow.billing.increment_topic_attribution_attempts.assert_not_called()

    def test_with_increment_fn_calls_custom_fn(self) -> None:
        """RetryManager with custom increment_fn → calls the custom function, not allocation_attempts."""
        from core.engine.orchestrator import RetryManager

        mock_uow = MagicMock()
        mock_uow.billing.increment_topic_attribution_attempts.return_value = 2
        mock_uow.__enter__ = MagicMock(return_value=mock_uow)
        mock_uow.__exit__ = MagicMock(return_value=False)

        mock_storage = MagicMock()
        mock_storage.create_unit_of_work.return_value = mock_uow

        line = _make_billing_line()

        manager = RetryManager(
            storage_backend=mock_storage,
            limit=3,
            increment_fn=lambda uow, ln: uow.billing.increment_topic_attribution_attempts(ln),
        )
        manager.increment_and_check(line)

        mock_uow.billing.increment_topic_attribution_attempts.assert_called_once_with(line)
        mock_uow.billing.increment_allocation_attempts.assert_not_called()


# ---------------------------------------------------------------------------
# AC#13 — Counter persistence failure returns None (safe pending)
# ---------------------------------------------------------------------------


class TestCounterPersistenceFailure:
    def test_increment_and_check_raises_returns_none(self) -> None:
        """If increment_and_check raises, _attribute_cluster returns None (safe pending)."""
        failing_metrics = MagicMock()
        failing_metrics.query.side_effect = RuntimeError("prometheus down")

        retry_checker = MagicMock()
        retry_checker.increment_and_check.side_effect = RuntimeError("db connection lost")

        phase = _make_phase(metrics_source=failing_metrics, retry_checker=retry_checker)

        mock_uow = MagicMock()
        mock_uow.resources.find_by_period.return_value = ([MagicMock()], 1)

        result = phase._attribute_cluster(mock_uow, "lkc-abc", "env-1", [_make_billing_line()], date(2026, 1, 1))
        assert result is None

    def test_increment_and_check_raises_does_not_mark_calculated(self) -> None:
        """If increment_and_check raises, mark_topic_attribution_calculated NOT called."""
        failing_metrics = MagicMock()
        failing_metrics.query.side_effect = RuntimeError("down")

        retry_checker = MagicMock()
        retry_checker.increment_and_check.side_effect = RuntimeError("storage failure")

        phase = _make_phase(metrics_source=failing_metrics, retry_checker=retry_checker)

        line = _make_billing_line()
        mock_uow = _make_uow(billing_lines=[line])

        phase.run(mock_uow, date(2026, 1, 1))

        mock_uow.pipeline_state.mark_topic_attribution_calculated.assert_not_called()


# ---------------------------------------------------------------------------
# SQLite integration — GIT-1/2/3
# Real DB path: increment_topic_attribution_attempts persists across UoW commits
# ---------------------------------------------------------------------------


def _make_core_backend(tmp_path: Any) -> Any:
    """SQLModelBackend with CoreStorageModule, tables created (no migrations)."""
    from core.storage.backends.sqlmodel.module import CoreStorageModule
    from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend

    conn = f"sqlite:///{tmp_path / 'test_core.db'}"
    module = CoreStorageModule()
    backend = SQLModelBackend(conn, module, use_migrations=False)
    backend.create_tables()
    return backend


def _make_ccloud_backend(tmp_path: Any) -> Any:
    """SQLModelBackend with CCloudStorageModule, tables created (no migrations)."""
    from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend
    from plugins.confluent_cloud.storage.module import CCloudStorageModule

    conn = f"sqlite:///{tmp_path / 'test_ccloud.db'}"
    module = CCloudStorageModule()
    backend = SQLModelBackend(conn, module, use_migrations=False)
    backend.create_tables()
    return backend


def _insert_core_billing_line(backend: Any) -> Any:
    """Insert a CoreBillingLineItem into the base billing table; return the domain object."""
    from core.models.billing import CoreBillingLineItem

    line = CoreBillingLineItem(
        ecosystem=ECOSYSTEM,
        tenant_id=TENANT_ID,
        timestamp=NOW,
        resource_id="lkc-abc",
        product_category="KAFKA",
        product_type="KAFKA_NETWORK_WRITE",
        quantity=Decimal("1"),
        unit_price=Decimal("10.00"),
        total_cost=Decimal("10.00"),
        granularity="daily",
    )
    with backend.create_unit_of_work() as uow:
        uow.billing.upsert(line)
        uow.commit()
    return line


def _insert_ccloud_billing_line(backend: Any) -> Any:
    """Insert a CCloudBillingLineItem into the ccloud billing table; return the domain object."""
    from plugins.confluent_cloud.models.billing import CCloudBillingLineItem

    line = CCloudBillingLineItem(
        ecosystem="confluent_cloud",
        tenant_id="org-001",
        timestamp=NOW,
        env_id="env-1",
        resource_id="lkc-abc",
        product_category="KAFKA",
        product_type="KAFKA_NETWORK_WRITE",
        quantity=Decimal("1"),
        unit_price=Decimal("10.00"),
        total_cost=Decimal("10.00"),
        currency="USD",
        granularity="daily",
    )
    with backend.create_unit_of_work() as uow:
        uow.billing.upsert(line)
        uow.commit()
    return line


class TestBillingRepositoryIncrementTopicAttribution:
    """GIT-3 — BillingRepository.increment_topic_attribution_attempts with real SQLite."""

    def test_increment_returns_1_on_first_call(self, tmp_path: Any) -> None:
        backend = _make_core_backend(tmp_path)
        line = _insert_core_billing_line(backend)

        with backend.create_unit_of_work() as uow:
            count = uow.billing.increment_topic_attribution_attempts(line)
            uow.commit()

        assert count == 1

    def test_increment_persists_across_separate_uow(self, tmp_path: Any) -> None:
        """Two separate UoW commits → count accumulates to 2 in real DB."""
        backend = _make_core_backend(tmp_path)
        line = _insert_core_billing_line(backend)

        with backend.create_unit_of_work() as uow:
            uow.billing.increment_topic_attribution_attempts(line)
            uow.commit()

        with backend.create_unit_of_work() as uow:
            count = uow.billing.increment_topic_attribution_attempts(line)
            uow.commit()

        assert count == 2

    def test_increment_raises_key_error_for_missing_row(self, tmp_path: Any) -> None:
        """increment_topic_attribution_attempts raises KeyError when row not found."""
        from core.models.billing import CoreBillingLineItem

        backend = _make_core_backend(tmp_path)
        missing_line = CoreBillingLineItem(
            ecosystem=ECOSYSTEM,
            tenant_id=TENANT_ID,
            timestamp=NOW,
            resource_id="lkc-missing",
            product_category="KAFKA",
            product_type="KAFKA_NETWORK_WRITE",
            quantity=Decimal("1"),
            unit_price=Decimal("1.00"),
            total_cost=Decimal("1.00"),
            granularity="daily",
        )

        with backend.create_unit_of_work() as uow, pytest.raises(KeyError):
            uow.billing.increment_topic_attribution_attempts(missing_line)


class TestCCloudBillingRepositoryIncrementTopicAttribution:
    """GIT-2 — CCloudBillingRepository.increment_topic_attribution_attempts with real SQLite."""

    def test_ccloud_increment_returns_1_on_first_call(self, tmp_path: Any) -> None:
        backend = _make_ccloud_backend(tmp_path)
        line = _insert_ccloud_billing_line(backend)

        with backend.create_unit_of_work() as uow:
            count = uow.billing.increment_topic_attribution_attempts(line)
            uow.commit()

        assert count == 1

    def test_ccloud_increment_persists_across_separate_uow(self, tmp_path: Any) -> None:
        """Two separate UoW commits → count accumulates to 2."""
        backend = _make_ccloud_backend(tmp_path)
        line = _insert_ccloud_billing_line(backend)

        with backend.create_unit_of_work() as uow:
            uow.billing.increment_topic_attribution_attempts(line)
            uow.commit()

        with backend.create_unit_of_work() as uow:
            count = uow.billing.increment_topic_attribution_attempts(line)
            uow.commit()

        assert count == 2

    def test_ccloud_increment_raises_key_error_for_missing_row(self, tmp_path: Any) -> None:
        from plugins.confluent_cloud.models.billing import CCloudBillingLineItem

        backend = _make_ccloud_backend(tmp_path)
        missing_line = CCloudBillingLineItem(
            ecosystem="confluent_cloud",
            tenant_id="org-001",
            timestamp=NOW,
            env_id="env-missing",
            resource_id="lkc-missing",
            product_category="KAFKA",
            product_type="KAFKA_NETWORK_WRITE",
            quantity=Decimal("1"),
            unit_price=Decimal("1.00"),
            total_cost=Decimal("1.00"),
            currency="USD",
            granularity="daily",
        )

        with backend.create_unit_of_work() as uow, pytest.raises(KeyError):
            uow.billing.increment_topic_attribution_attempts(missing_line)


class TestTopicAttributionRetryIntegration:
    """GIT-1 — Full integration: TopicAttributionPhase → RetryManager → real DB flush/commit."""

    def test_run_failure_persists_attempt_count_via_retry_manager(self, tmp_path: Any) -> None:
        """Full path: phase.run() → RetryManager.increment_and_check() → BillingRepository → real SQLite commit."""
        from sqlmodel import Session, select

        from core.engine.orchestrator import RetryManager
        from core.storage.backends.sqlmodel.base_tables import BillingTable
        from core.storage.backends.sqlmodel.engine import get_or_create_engine
        from core.storage.backends.sqlmodel.module import CoreStorageModule
        from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend

        conn = f"sqlite:///{tmp_path / 'integration.db'}"
        module = CoreStorageModule()
        backend = SQLModelBackend(conn, module, use_migrations=False)
        backend.create_tables()

        line = _insert_core_billing_line(backend)

        failing_metrics = MagicMock()
        failing_metrics.query.side_effect = RuntimeError("prometheus permanently down")

        retry_manager = RetryManager(
            storage_backend=backend,
            limit=3,
            increment_fn=lambda uow, ln: uow.billing.increment_topic_attribution_attempts(ln),
        )
        phase = _make_phase(metrics_source=failing_metrics, retry_checker=retry_manager)

        resource_mock = MagicMock()
        resource_mock.display_name = "topic-a"
        resource_mock.parent_id = "lkc-abc"
        mock_uow = MagicMock()
        mock_uow.billing.find_by_date.return_value = [line]
        mock_uow.resources.find_by_period.return_value = ([resource_mock], 1)

        phase.run(mock_uow, date(2026, 1, 1))

        engine = get_or_create_engine(conn)
        with Session(engine) as session:
            row = session.exec(
                select(BillingTable).where(
                    BillingTable.ecosystem == ECOSYSTEM,
                    BillingTable.tenant_id == TENANT_ID,
                    BillingTable.resource_id == "lkc-abc",
                )
            ).first()

        assert row is not None
        assert row.topic_attribution_attempts == 1, (
            f"Expected topic_attribution_attempts=1 after one failed run, got {row.topic_attribution_attempts}"
        )

    def test_run_failure_twice_accumulates_count(self, tmp_path: Any) -> None:
        """Two failed runs → topic_attribution_attempts=2 in real DB (AC#8a)."""
        from sqlmodel import Session, select

        from core.engine.orchestrator import RetryManager
        from core.storage.backends.sqlmodel.base_tables import BillingTable
        from core.storage.backends.sqlmodel.engine import get_or_create_engine
        from core.storage.backends.sqlmodel.module import CoreStorageModule
        from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend

        conn = f"sqlite:///{tmp_path / 'integration2.db'}"
        module = CoreStorageModule()
        backend = SQLModelBackend(conn, module, use_migrations=False)
        backend.create_tables()

        line = _insert_core_billing_line(backend)

        failing_metrics = MagicMock()
        failing_metrics.query.side_effect = RuntimeError("dead cluster")

        retry_manager = RetryManager(
            storage_backend=backend,
            limit=3,
            increment_fn=lambda uow, ln: uow.billing.increment_topic_attribution_attempts(ln),
        )
        phase = _make_phase(metrics_source=failing_metrics, retry_checker=retry_manager)

        resource_mock = MagicMock()
        resource_mock.display_name = "topic-a"
        resource_mock.parent_id = "lkc-abc"

        for _ in range(2):
            mock_uow = MagicMock()
            mock_uow.billing.find_by_date.return_value = [line]
            mock_uow.resources.find_by_period.return_value = ([resource_mock], 1)
            phase.run(mock_uow, date(2026, 1, 1))

        engine = get_or_create_engine(conn)
        with Session(engine) as session:
            row = session.exec(
                select(BillingTable).where(
                    BillingTable.ecosystem == ECOSYSTEM,
                    BillingTable.tenant_id == TENANT_ID,
                    BillingTable.resource_id == "lkc-abc",
                )
            ).first()

        assert row is not None
        assert row.topic_attribution_attempts == 2, (
            f"Expected topic_attribution_attempts=2 after two failed runs, got {row.topic_attribution_attempts}"
        )
