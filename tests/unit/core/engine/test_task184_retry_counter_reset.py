from __future__ import annotations

from collections.abc import Iterable
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ECOSYSTEM = "eco"
TENANT_ID = "t1"
NOW = datetime(2026, 1, 15, tzinfo=UTC)
TODAY = NOW.date()  # 2026-01-15


# ---------------------------------------------------------------------------
# Backend helpers
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


def _make_core_billing_line(
    timestamp: datetime = NOW,
    resource_id: str = "lkc-abc",
    product_type: str = "KAFKA_NETWORK_WRITE",
) -> Any:
    from core.models.billing import CoreBillingLineItem

    return CoreBillingLineItem(
        ecosystem=ECOSYSTEM,
        tenant_id=TENANT_ID,
        timestamp=timestamp,
        resource_id=resource_id,
        product_category="KAFKA",
        product_type=product_type,
        quantity=Decimal("1"),
        unit_price=Decimal("10.00"),
        total_cost=Decimal("10.00"),
        granularity="daily",
    )


def _make_ccloud_billing_line(
    timestamp: datetime = NOW,
    resource_id: str = "lkc-abc",
    env_id: str = "env-1",
    product_type: str = "KAFKA_NETWORK_WRITE",
) -> Any:
    from plugins.confluent_cloud.models.billing import CCloudBillingLineItem

    return CCloudBillingLineItem(
        ecosystem="confluent_cloud",
        tenant_id="org-001",
        timestamp=timestamp,
        env_id=env_id,
        resource_id=resource_id,
        product_category="KAFKA",
        product_type=product_type,
        quantity=Decimal("1"),
        unit_price=Decimal("10.00"),
        total_cost=Decimal("10.00"),
        currency="USD",
        granularity="daily",
    )


def _insert_and_exhaust_allocation(backend: Any, line: Any, attempts: int = 3) -> None:
    """Insert billing row then increment allocation_attempts to given count."""
    with backend.create_unit_of_work() as uow:
        uow.billing.upsert(line)
        uow.commit()
    for _ in range(attempts):
        with backend.create_unit_of_work() as uow:
            uow.billing.increment_allocation_attempts(line)
            uow.commit()


def _insert_and_exhaust_topic(backend: Any, line: Any, attempts: int = 3) -> None:
    """Insert billing row then increment topic_attribution_attempts to given count."""
    with backend.create_unit_of_work() as uow:
        uow.billing.upsert(line)
        uow.commit()
    for _ in range(attempts):
        with backend.create_unit_of_work() as uow:
            uow.billing.increment_topic_attribution_attempts(line)
            uow.commit()


# ---------------------------------------------------------------------------
# GatherPhase helpers (mirrors test_orchestrator_phases.py pattern)
# ---------------------------------------------------------------------------


def _make_tenant_config(**overrides: Any) -> Any:
    from core.config.models import TenantConfig

    defaults: dict[str, Any] = {
        "ecosystem": ECOSYSTEM,
        "tenant_id": TENANT_ID,
        "lookback_days": 30,
        "cutoff_days": 5,
    }
    defaults.update(overrides)
    return TenantConfig(**defaults)


class _MockCostInput:
    def gather(self, tenant_id: str, start: datetime, end: datetime, uow: Any) -> Iterable[Any]:
        return []


class _MockPlugin:
    @property
    def ecosystem(self) -> str:
        return ECOSYSTEM

    def initialize(self, config: Any) -> None:
        pass

    def get_service_handlers(self) -> dict[str, Any]:
        return {}

    def get_cost_input(self) -> _MockCostInput:
        return _MockCostInput()

    def get_metrics_source(self) -> None:
        return None

    def get_fallback_allocator(self) -> None:
        return None

    def build_shared_context(self, tenant_id: str) -> None:
        return None

    def close(self) -> None:
        pass


def _make_gather_phase(tenant_config: Any = None) -> Any:
    from core.engine.orchestrator import GatherPhase
    from core.plugin.registry import EcosystemBundle

    tc = tenant_config or _make_tenant_config()
    bundle = EcosystemBundle.build(_MockPlugin())
    return GatherPhase(
        ecosystem=ECOSYSTEM,
        tenant_id=TENANT_ID,
        tenant_config=tc,
        bundle=bundle,
    )


def _make_uow_with_pipeline_state(billing_date: date, chargeback_calculated: bool = True) -> MagicMock:
    """Return a MagicMock UoW whose pipeline_state.get returns a PipelineState."""
    from core.models.pipeline import PipelineState

    uow = MagicMock()
    state = PipelineState(
        ecosystem=ECOSYSTEM,
        tenant_id=TENANT_ID,
        tracking_date=billing_date,
        billing_gathered=True,
        resources_gathered=True,
        chargeback_calculated=chargeback_calculated,
    )
    uow.pipeline_state.get.return_value = state
    return uow


# ===========================================================================
# A. SQLModelBillingRepository — reset_allocation_attempts_by_date
# ===========================================================================


class TestSQLModelBillingRepositoryResetAllocationAttempts:
    """TASK-184 A — reset_allocation_attempts_by_date on real SQLite."""

    def test_reset_allocation_attempts_by_date_sets_to_zero(self, tmp_path: Any) -> None:
        """Rows with allocation_attempts=3 on target date → reset → counter restarts at 1."""
        backend = _make_core_backend(tmp_path)
        line = _make_core_billing_line()
        _insert_and_exhaust_allocation(backend, line, attempts=3)

        with backend.create_unit_of_work() as uow:
            updated = uow.billing.reset_allocation_attempts_by_date(ECOSYSTEM, TENANT_ID, TODAY)
            uow.commit()

        assert updated == 1

        # Column is 0 after reset — next increment returns 1 (not 4)
        with backend.create_unit_of_work() as uow:
            new_count = uow.billing.increment_allocation_attempts(line)
            uow.commit()

        assert new_count == 1

    def test_reset_topic_attribution_attempts_by_date_sets_to_zero(self, tmp_path: Any) -> None:
        """Rows with topic_attribution_attempts=3 on target date → reset → counter restarts at 1."""
        backend = _make_core_backend(tmp_path)
        line = _make_core_billing_line()
        _insert_and_exhaust_topic(backend, line, attempts=3)

        with backend.create_unit_of_work() as uow:
            updated = uow.billing.reset_topic_attribution_attempts_by_date(ECOSYSTEM, TENANT_ID, TODAY)
            uow.commit()

        assert updated == 1

        with backend.create_unit_of_work() as uow:
            new_count = uow.billing.increment_topic_attribution_attempts(line)
            uow.commit()

        assert new_count == 1

    def test_reset_multi_row_date_resets_all_rows(self, tmp_path: Any) -> None:
        """Multiple billing lines on same date all reset to 0 in a single UPDATE call."""
        backend = _make_core_backend(tmp_path)
        line_a = _make_core_billing_line(resource_id="lkc-aaa")
        line_b = _make_core_billing_line(resource_id="lkc-bbb", product_type="KAFKA_CKU")

        _insert_and_exhaust_allocation(backend, line_a, attempts=3)
        _insert_and_exhaust_allocation(backend, line_b, attempts=3)

        with backend.create_unit_of_work() as uow:
            updated = uow.billing.reset_allocation_attempts_by_date(ECOSYSTEM, TENANT_ID, TODAY)
            uow.commit()

        assert updated == 2

        # Both rows now start fresh — next increment returns 1 for each
        with backend.create_unit_of_work() as uow:
            count_a = uow.billing.increment_allocation_attempts(line_a)
            count_b = uow.billing.increment_allocation_attempts(line_b)
            uow.commit()

        assert count_a == 1
        assert count_b == 1

    def test_reset_does_not_affect_adjacent_dates(self, tmp_path: Any) -> None:
        """Reset for date D must not alter rows on D-1 or D+1."""
        backend = _make_core_backend(tmp_path)

        yesterday = datetime(2026, 1, 14, tzinfo=UTC)
        tomorrow = datetime(2026, 1, 16, tzinfo=UTC)

        line_y = _make_core_billing_line(timestamp=yesterday, resource_id="lkc-y")
        line_t = _make_core_billing_line(timestamp=tomorrow, resource_id="lkc-t")

        _insert_and_exhaust_allocation(backend, line_y, attempts=2)
        _insert_and_exhaust_allocation(backend, line_t, attempts=2)

        # Reset TODAY (Jan 15) — no rows on that date, adjacent rows unaffected
        with backend.create_unit_of_work() as uow:
            updated = uow.billing.reset_allocation_attempts_by_date(ECOSYSTEM, TENANT_ID, TODAY)
            uow.commit()

        assert updated == 0

        # Adjacent rows retain their attempt counts — next increment gives 3
        with backend.create_unit_of_work() as uow:
            count_y = uow.billing.increment_allocation_attempts(line_y)
            count_t = uow.billing.increment_allocation_attempts(line_t)
            uow.commit()

        assert count_y == 3
        assert count_t == 3

    def test_reset_nonexistent_date_returns_zero(self, tmp_path: Any) -> None:
        """Resetting a date with no billing rows returns 0 without error."""
        backend = _make_core_backend(tmp_path)

        with backend.create_unit_of_work() as uow:
            updated = uow.billing.reset_allocation_attempts_by_date(ECOSYSTEM, TENANT_ID, date(2025, 1, 1))
            uow.commit()

        assert updated == 0


# ===========================================================================
# B. CCloudBillingRepository — reset_*_by_date
# ===========================================================================

_CCLOUD_ECOSYSTEM = "confluent_cloud"
_CCLOUD_TENANT = "org-001"


class TestCCloudBillingRepositoryResetAttempts:
    """TASK-184 B — reset_*_by_date on CCloudBillingRepository with real SQLite."""

    def test_ccloud_reset_allocation_attempts_by_date_sets_to_zero(self, tmp_path: Any) -> None:
        """CCloud rows with allocation_attempts=3 → reset → counter restarts at 1."""
        backend = _make_ccloud_backend(tmp_path)
        line = _make_ccloud_billing_line()
        _insert_and_exhaust_allocation(backend, line, attempts=3)

        with backend.create_unit_of_work() as uow:
            updated = uow.billing.reset_allocation_attempts_by_date(_CCLOUD_ECOSYSTEM, _CCLOUD_TENANT, TODAY)
            uow.commit()

        assert updated == 1

        with backend.create_unit_of_work() as uow:
            new_count = uow.billing.increment_allocation_attempts(line)
            uow.commit()

        assert new_count == 1

    def test_ccloud_reset_topic_attribution_attempts_by_date_sets_to_zero(self, tmp_path: Any) -> None:
        """CCloud rows with topic_attribution_attempts=3 → reset → counter restarts at 1."""
        backend = _make_ccloud_backend(tmp_path)
        line = _make_ccloud_billing_line()
        _insert_and_exhaust_topic(backend, line, attempts=3)

        with backend.create_unit_of_work() as uow:
            updated = uow.billing.reset_topic_attribution_attempts_by_date(_CCLOUD_ECOSYSTEM, _CCLOUD_TENANT, TODAY)
            uow.commit()

        assert updated == 1

        with backend.create_unit_of_work() as uow:
            new_count = uow.billing.increment_topic_attribution_attempts(line)
            uow.commit()

        assert new_count == 1

    def test_ccloud_reset_multi_row_date_resets_all_rows(self, tmp_path: Any) -> None:
        """Multiple CCloud billing lines on same date all reset in one UPDATE call."""
        backend = _make_ccloud_backend(tmp_path)
        line_a = _make_ccloud_billing_line(resource_id="lkc-aaa")
        line_b = _make_ccloud_billing_line(resource_id="lkc-bbb", product_type="KAFKA_CKU")

        _insert_and_exhaust_allocation(backend, line_a, attempts=3)
        _insert_and_exhaust_allocation(backend, line_b, attempts=3)

        with backend.create_unit_of_work() as uow:
            updated = uow.billing.reset_allocation_attempts_by_date(_CCLOUD_ECOSYSTEM, _CCLOUD_TENANT, TODAY)
            uow.commit()

        assert updated == 2

        # Both rows now start fresh — next increment returns 1 for each
        with backend.create_unit_of_work() as uow:
            count_a = uow.billing.increment_allocation_attempts(line_a)
            count_b = uow.billing.increment_allocation_attempts(line_b)
            uow.commit()

        assert count_a == 1
        assert count_b == 1

    def test_ccloud_reset_does_not_affect_adjacent_dates(self, tmp_path: Any) -> None:
        """CCloud reset for date D does not alter rows on D-1 or D+1."""
        backend = _make_ccloud_backend(tmp_path)

        yesterday = datetime(2026, 1, 14, tzinfo=UTC)
        tomorrow = datetime(2026, 1, 16, tzinfo=UTC)

        line_y = _make_ccloud_billing_line(timestamp=yesterday, resource_id="lkc-y")
        line_t = _make_ccloud_billing_line(timestamp=tomorrow, resource_id="lkc-t")

        _insert_and_exhaust_allocation(backend, line_y, attempts=2)
        _insert_and_exhaust_allocation(backend, line_t, attempts=2)

        with backend.create_unit_of_work() as uow:
            updated = uow.billing.reset_allocation_attempts_by_date(_CCLOUD_ECOSYSTEM, _CCLOUD_TENANT, TODAY)
            uow.commit()

        assert updated == 0

        with backend.create_unit_of_work() as uow:
            count_y = uow.billing.increment_allocation_attempts(line_y)
            count_t = uow.billing.increment_allocation_attempts(line_t)
            uow.commit()

        assert count_y == 3
        assert count_t == 3

    def test_ccloud_reset_nonexistent_date_returns_zero(self, tmp_path: Any) -> None:
        """CCloud: resetting a date with no rows returns 0 without error."""
        backend = _make_ccloud_backend(tmp_path)

        with backend.create_unit_of_work() as uow:
            updated = uow.billing.reset_allocation_attempts_by_date(_CCLOUD_ECOSYSTEM, _CCLOUD_TENANT, date(2025, 1, 1))
            uow.commit()

        assert updated == 0


# ===========================================================================
# C. GatherPhase._apply_recalculation_window — mock UoW behavior tests
# ===========================================================================

_PHASE_NOW = datetime(2026, 2, 22, tzinfo=UTC)
_BILLING_DATE_IN_WINDOW = (_PHASE_NOW - timedelta(days=2)).date()
_BILLING_DATE_OUT_WINDOW = (_PHASE_NOW - timedelta(days=10)).date()


class TestApplyRecalculationWindowResets:
    """TASK-184 C — _apply_recalculation_window calls reset methods on re-queue."""

    def test_recalculation_window_resets_allocation_attempts(self) -> None:
        """reset_allocation_attempts_by_date called when chargeback_calculated=True and within window."""
        tc = _make_tenant_config(cutoff_days=5)
        phase = _make_gather_phase(tenant_config=tc)

        uow = _make_uow_with_pipeline_state(_BILLING_DATE_IN_WINDOW, chargeback_calculated=True)

        phase._apply_recalculation_window(uow, {_BILLING_DATE_IN_WINDOW}, _PHASE_NOW)

        uow.billing.reset_allocation_attempts_by_date.assert_called_once_with(
            ECOSYSTEM, TENANT_ID, _BILLING_DATE_IN_WINDOW
        )

    def test_recalculation_window_resets_topic_attribution_attempts(self) -> None:
        """reset_topic_attribution_attempts_by_date called when chargeback_calculated=True and within window."""
        tc = _make_tenant_config(cutoff_days=5)
        phase = _make_gather_phase(tenant_config=tc)

        uow = _make_uow_with_pipeline_state(_BILLING_DATE_IN_WINDOW, chargeback_calculated=True)

        phase._apply_recalculation_window(uow, {_BILLING_DATE_IN_WINDOW}, _PHASE_NOW)

        uow.billing.reset_topic_attribution_attempts_by_date.assert_called_once_with(
            ECOSYSTEM, TENANT_ID, _BILLING_DATE_IN_WINDOW
        )

    def test_recalculation_window_no_reset_when_not_calculated(self) -> None:
        """Dates with chargeback_calculated=False do not trigger reset calls."""
        tc = _make_tenant_config(cutoff_days=5)
        phase = _make_gather_phase(tenant_config=tc)

        uow = _make_uow_with_pipeline_state(_BILLING_DATE_IN_WINDOW, chargeback_calculated=False)

        phase._apply_recalculation_window(uow, {_BILLING_DATE_IN_WINDOW}, _PHASE_NOW)

        uow.billing.reset_allocation_attempts_by_date.assert_not_called()
        uow.billing.reset_topic_attribution_attempts_by_date.assert_not_called()

    def test_recalculation_window_no_reset_outside_cutoff(self) -> None:
        """Dates outside the recalculation window (>cutoff_days old) are not touched."""
        tc = _make_tenant_config(cutoff_days=5)
        phase = _make_gather_phase(tenant_config=tc)

        # _BILLING_DATE_OUT_WINDOW is 10 days ago — outside 5-day cutoff
        uow = _make_uow_with_pipeline_state(_BILLING_DATE_OUT_WINDOW, chargeback_calculated=True)

        phase._apply_recalculation_window(uow, {_BILLING_DATE_OUT_WINDOW}, _PHASE_NOW)

        uow.billing.reset_allocation_attempts_by_date.assert_not_called()
        uow.billing.reset_topic_attribution_attempts_by_date.assert_not_called()


# ===========================================================================
# D. Integration test — full reset-then-retry cycle
# ===========================================================================


class TestResetThenRetryManagerStartsFresh:
    """TASK-184 D — Integration: exhaust retries, reset, RetryManager starts from 0."""

    def test_reset_then_retry_manager_starts_fresh(self, tmp_path: Any) -> None:
        """Insert billing row, exhaust retries to limit, reset, then increment_and_check returns (1, False)."""
        from core.engine.orchestrator import RetryManager

        backend = _make_core_backend(tmp_path)
        line = _make_core_billing_line()

        with backend.create_unit_of_work() as uow:
            uow.billing.upsert(line)
            uow.commit()

        retry_manager = RetryManager(backend, limit=3)

        # Exhaust the retry budget (limit=3, so 3rd call returns should_fallback=True)
        for _ in range(3):
            retry_manager.increment_and_check(line)

        # Confirm budget is exhausted — 4th call has count=4 >= limit=3
        count_before, should_fallback_before = retry_manager.increment_and_check(line)
        assert count_before == 4
        assert should_fallback_before is True

        # Reset via the new repository method
        with backend.create_unit_of_work() as uow:
            updated = uow.billing.reset_allocation_attempts_by_date(ECOSYSTEM, TENANT_ID, TODAY)
            uow.commit()

        assert updated == 1

        # After reset, counter is 0 — first increment_and_check returns (1, False)
        count_after, should_fallback_after = retry_manager.increment_and_check(line)
        assert count_after == 1
        assert should_fallback_after is False
