from __future__ import annotations

import logging
from collections.abc import Iterable
from datetime import UTC, datetime, timedelta
from datetime import date as date_type
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock

import pytest

from core.engine.allocation import AllocatorRegistry
from core.engine.orchestrator import (
    CalculatePhase,
    ChargebackOrchestrator,
    GatherFailureThresholdError,
    GatherPhase,
    GatherResult,
    RetryManager,
)
from core.models.billing import BillingLineItem, CoreBillingLineItem
from core.models.chargeback import ChargebackRow, CostType
from core.models.identity import CoreIdentity, Identity, IdentityResolution, IdentitySet
from core.models.metrics import MetricQuery
from core.models.pipeline import PipelineState
from core.models.resource import CoreResource, Resource

# ---------- Constants ----------

NOW = datetime(2026, 2, 22, 12, 0, 0, tzinfo=UTC)
TODAY = NOW.date()
ECOSYSTEM = "test-eco"
TENANT_ID = "tenant-1"
TENANT_NAME = "test-tenant"


# ---------- Factories ----------


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


def _make_billing_line(
    product_type: str = "KAFKA_CKU",
    resource_id: str = "cluster-1",
    total_cost: Decimal = Decimal("100.00"),
    timestamp: datetime | None = None,
    granularity: str = "daily",
) -> BillingLineItem:
    return CoreBillingLineItem(
        ecosystem=ECOSYSTEM,
        tenant_id=TENANT_ID,
        timestamp=timestamp or NOW,
        resource_id=resource_id,
        product_category="kafka",
        product_type=product_type,
        quantity=Decimal(1),
        unit_price=total_cost,
        total_cost=total_cost,
        granularity=granularity,
    )


def _make_resource(resource_id: str = "cluster-1", created_at: datetime | None = None) -> Resource:
    return CoreResource(
        ecosystem=ECOSYSTEM,
        tenant_id=TENANT_ID,
        resource_id=resource_id,
        resource_type="kafka_cluster",
        created_at=created_at or NOW - timedelta(days=30),
    )


def _make_identity(identity_id: str = "user-1") -> Identity:
    return CoreIdentity(
        ecosystem=ECOSYSTEM,
        tenant_id=TENANT_ID,
        identity_id=identity_id,
        identity_type="user",
        display_name=f"User {identity_id}",
    )


def _make_metric_query(key: str = "bytes_in") -> MetricQuery:
    return MetricQuery(
        key=key,
        query_expression=f"sum(metric_{key}{{}})",
        label_keys=("cluster_id",),
        resource_label="cluster_id",
    )


# ---------- Minimal mock infrastructure ----------


class MockResourceRepo:
    def __init__(self) -> None:
        self._data: dict[str, Resource] = {}
        self._deletions: list[tuple[str, datetime]] = []

    def upsert(self, resource: Resource) -> Resource:
        self._data[resource.resource_id] = resource
        return resource

    def get(self, ecosystem: str, tenant_id: str, resource_id: str) -> Resource | None:
        return self._data.get(resource_id)

    def find_active_at(
        self, ecosystem: str, tenant_id: str, timestamp: datetime, **kwargs: Any
    ) -> tuple[list[Resource], int]:
        items = [r for r in self._data.values() if r.deleted_at is None]
        return items, len(items)

    def find_by_period(
        self, ecosystem: str, tenant_id: str, start: datetime, end: datetime, **kwargs: Any
    ) -> tuple[list[Resource], int]:
        items = list(self._data.values())
        return items, len(items)

    def mark_deleted(self, ecosystem: str, tenant_id: str, resource_id: str, deleted_at: datetime) -> None:
        self._deletions.append((resource_id, deleted_at))
        if resource_id in self._data:
            self._data[resource_id].deleted_at = deleted_at

    def find_by_type(self, *args: Any) -> list[Resource]:
        return []

    def delete_before(self, *args: Any) -> int:
        return 0


class MockIdentityRepo:
    def __init__(self) -> None:
        self._data: dict[str, Identity] = {}
        self._deletions: list[tuple[str, datetime]] = []

    def upsert(self, identity: Identity) -> Identity:
        self._data[identity.identity_id] = identity
        return identity

    def get(self, ecosystem: str, tenant_id: str, identity_id: str) -> Identity | None:
        return self._data.get(identity_id)

    def find_active_at(
        self, ecosystem: str, tenant_id: str, timestamp: datetime, **kwargs: Any
    ) -> tuple[list[Identity], int]:
        items = [i for i in self._data.values() if i.deleted_at is None]
        return items, len(items)

    def find_by_period(
        self, ecosystem: str, tenant_id: str, start: datetime, end: datetime, **kwargs: Any
    ) -> tuple[list[Identity], int]:
        items = list(self._data.values())
        return items, len(items)

    def mark_deleted(self, ecosystem: str, tenant_id: str, identity_id: str, deleted_at: datetime) -> None:
        self._deletions.append((identity_id, deleted_at))
        if identity_id in self._data:
            self._data[identity_id].deleted_at = deleted_at

    def find_by_type(self, *args: Any) -> list[Identity]:
        return []

    def delete_before(self, *args: Any) -> int:
        return 0


class MockBillingRepo:
    def __init__(self) -> None:
        self._data: list[BillingLineItem] = []
        self._attempts: dict[tuple[str, str, str], int] = {}

    def upsert(self, line: BillingLineItem) -> BillingLineItem:
        self._data.append(line)
        return line

    def find_by_date(self, ecosystem: str, tenant_id: str, target_date: date_type) -> list[BillingLineItem]:
        return [bl for bl in self._data if bl.timestamp.date() == target_date]

    def find_by_range(self, *args: Any) -> list[BillingLineItem]:
        return self._data

    def increment_allocation_attempts(
        self,
        ecosystem: str,
        tenant_id: str,
        timestamp: datetime,
        resource_id: str,
        product_type: str,
    ) -> int:
        key = (resource_id, product_type, str(timestamp))
        self._attempts[key] = self._attempts.get(key, 0) + 1
        return self._attempts[key]

    def delete_before(self, *args: Any) -> int:
        return 0


class MockChargebackRepo:
    def __init__(self) -> None:
        self._data: list[ChargebackRow] = []

    def upsert(self, row: ChargebackRow) -> ChargebackRow:
        self._data.append(row)
        return row

    def upsert_batch(self, rows: list[ChargebackRow]) -> int:
        for row in rows:
            self._data.append(row)
        return len(rows)

    def find_by_date(self, ecosystem: str, tenant_id: str, target_date: date_type) -> list[ChargebackRow]:
        return [r for r in self._data if r.timestamp.date() == target_date]

    def delete_by_date(self, ecosystem: str, tenant_id: str, target_date: date_type) -> int:
        before = len(self._data)
        self._data = [r for r in self._data if r.timestamp.date() != target_date]
        return before - len(self._data)

    def delete_before(self, *args: Any) -> int:
        return 0


class MockPipelineStateRepo:
    def __init__(self) -> None:
        self._data: dict[tuple[str, str, date_type], PipelineState] = {}

    def upsert(self, state: PipelineState) -> PipelineState:
        key = (state.ecosystem, state.tenant_id, state.tracking_date)
        if key not in self._data:
            self._data[key] = state
        return self._data[key]

    def get(self, ecosystem: str, tenant_id: str, tracking_date: date_type) -> PipelineState | None:
        return self._data.get((ecosystem, tenant_id, tracking_date))

    def find_needing_calculation(self, ecosystem: str, tenant_id: str) -> list[PipelineState]:
        return sorted(
            [
                s
                for s in self._data.values()
                if s.ecosystem == ecosystem
                and s.tenant_id == tenant_id
                and s.billing_gathered
                and s.resources_gathered
                and not s.chargeback_calculated
            ],
            key=lambda s: s.tracking_date,
        )

    def find_by_range(self, *args: Any) -> list[PipelineState]:
        return list(self._data.values())

    def mark_billing_gathered(self, ecosystem: str, tenant_id: str, tracking_date: date_type) -> None:
        key = (ecosystem, tenant_id, tracking_date)
        if key in self._data:
            self._data[key].billing_gathered = True

    def mark_resources_gathered(self, ecosystem: str, tenant_id: str, tracking_date: date_type) -> None:
        key = (ecosystem, tenant_id, tracking_date)
        if key in self._data:
            self._data[key].resources_gathered = True

    def mark_needs_recalculation(self, ecosystem: str, tenant_id: str, tracking_date: date_type) -> None:
        key = (ecosystem, tenant_id, tracking_date)
        if key in self._data:
            self._data[key].chargeback_calculated = False

    def mark_chargeback_calculated(self, ecosystem: str, tenant_id: str, tracking_date: date_type) -> None:
        key = (ecosystem, tenant_id, tracking_date)
        if key in self._data:
            self._data[key].chargeback_calculated = True


class MockUnitOfWork:
    def __init__(self) -> None:
        self.resources = MockResourceRepo()
        self.identities = MockIdentityRepo()
        self.billing = MockBillingRepo()
        self.chargebacks = MockChargebackRepo()
        self.pipeline_state = MockPipelineStateRepo()
        self.tags = MagicMock()
        self._committed = False

    def __enter__(self) -> MockUnitOfWork:
        return self

    def __exit__(self, *args: Any) -> None:
        pass

    def commit(self) -> None:
        self._committed = True

    def rollback(self) -> None:
        pass


class MockStorageBackend:
    def __init__(self, uow: MockUnitOfWork | None = None) -> None:
        self._uow = uow or MockUnitOfWork()

    def create_unit_of_work(self) -> MockUnitOfWork:
        return self._uow

    def create_tables(self) -> None:
        pass

    def dispose(self) -> None:
        pass


class MockServiceHandler:
    def __init__(
        self,
        service_type: str = "kafka",
        product_types: list[str] | None = None,
        resources: list[Resource] | None = None,
        identities: list[Identity] | None = None,
        metrics_queries: list[MetricQuery] | None = None,
        allocator: Any = None,
        resolve_fn: Any = None,
    ) -> None:
        self._service_type = service_type
        self._product_types = product_types or ["KAFKA_CKU"]
        self._resources = resources or []
        self._identities = identities or []
        self._metrics_queries = metrics_queries or []
        self._allocator = allocator
        self._resolve_fn = resolve_fn

    @property
    def service_type(self) -> str:
        return self._service_type

    @property
    def handles_product_types(self) -> list[str]:
        return self._product_types

    def gather_resources(self, tenant_id: str, uow: Any, shared_ctx: object | None = None) -> Iterable[Resource]:
        return self._resources

    def gather_identities(self, tenant_id: str, uow: Any) -> Iterable[Identity]:
        return self._identities

    def resolve_identities(
        self,
        tenant_id: str,
        resource_id: str,
        billing_timestamp: datetime,
        billing_duration: timedelta,
        metrics_data: Any,
        uow: Any,
        context: Any = None,
    ) -> IdentityResolution:
        if self._resolve_fn:
            return self._resolve_fn(tenant_id, resource_id, billing_timestamp, billing_duration, metrics_data, uow)
        ra = IdentitySet()
        for i in self._identities:
            ra.add(i)
        return IdentityResolution(
            resource_active=ra,
            metrics_derived=IdentitySet(),
            tenant_period=IdentitySet(),
        )

    def get_metrics_for_product_type(self, product_type: str) -> list[MetricQuery]:
        return self._metrics_queries

    def get_allocator(self, product_type: str) -> Any:
        return self._allocator


class MockCostInput:
    def __init__(self, lines: list[BillingLineItem] | None = None) -> None:
        self._lines = lines or []

    def gather(self, tenant_id: str, start: datetime, end: datetime, uow: Any) -> Iterable[BillingLineItem]:
        return self._lines


class MockPlugin:
    def __init__(
        self,
        handlers: dict[str, MockServiceHandler] | None = None,
        cost_input: MockCostInput | None = None,
    ) -> None:
        self._handlers = handlers or {}
        self._cost_input = cost_input or MockCostInput()
        self._initialized = False

    @property
    def ecosystem(self) -> str:
        return ECOSYSTEM

    def initialize(self, config: dict[str, Any]) -> None:
        self._initialized = True

    def get_service_handlers(self) -> dict[str, MockServiceHandler]:
        return self._handlers

    def get_cost_input(self) -> MockCostInput:
        return self._cost_input

    def get_metrics_source(self) -> None:
        return None

    def get_fallback_allocator(self) -> None:
        return None

    def build_shared_context(self, tenant_id: str) -> None:
        return None

    def close(self) -> None:
        pass


def _make_gather_phase(
    tenant_config: Any | None = None,
    bundle: Any | None = None,
    min_refresh_gap: timedelta = timedelta(hours=1),
) -> GatherPhase:
    from core.plugin.registry import EcosystemBundle

    tc = tenant_config or _make_tenant_config()
    if bundle is None:
        plugin = MockPlugin(handlers={}, cost_input=MockCostInput())
        bundle = EcosystemBundle.build(plugin)
    return GatherPhase(
        ecosystem=ECOSYSTEM,
        tenant_id=TENANT_ID,
        tenant_config=tc,
        bundle=bundle,
        min_refresh_gap=min_refresh_gap,
    )


# =============================================================================
# GatherPhase tests
# =============================================================================


class TestGatherPhaseShouldRefresh:
    def test_should_refresh_returns_false_within_gap(self) -> None:
        """_should_refresh returns False when now - last < min_refresh_gap."""
        phase = _make_gather_phase(min_refresh_gap=timedelta(hours=1))
        last = NOW - timedelta(minutes=30)
        phase._last_resource_gather_at = last

        assert phase._should_refresh(NOW) is False

    def test_should_refresh_returns_true_after_gap_elapsed(self) -> None:
        phase = _make_gather_phase(min_refresh_gap=timedelta(hours=1))
        last = NOW - timedelta(hours=2)
        phase._last_resource_gather_at = last

        assert phase._should_refresh(NOW) is True

    def test_should_refresh_returns_true_when_never_gathered(self) -> None:
        phase = _make_gather_phase(min_refresh_gap=timedelta(hours=1))
        assert phase._last_resource_gather_at is None
        assert phase._should_refresh(NOW) is True

    def test_run_returns_skipped_true_when_throttled(self) -> None:
        """When _should_refresh returns False, run() returns GatherResult.skipped=True."""
        phase = _make_gather_phase(min_refresh_gap=timedelta(hours=1))
        # Set last gather to 10 minutes ago (real time) so it's within the 1h gap
        phase._last_resource_gather_at = datetime.now(UTC) - timedelta(minutes=10)

        uow = MockUnitOfWork()
        result = phase.run(uow)

        assert isinstance(result, GatherResult)
        assert result.skipped is True
        assert result.dates_gathered == 0
        assert result.errors == []


class TestGatherPhaseHandlerException:
    def test_handler_exception_sets_gather_complete_false_skips_deletion(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Handler exception during gather → gather_complete=False → deletion detection skipped."""
        from core.plugin.registry import EcosystemBundle

        class FailingHandler(MockServiceHandler):
            def gather_resources(
                self, tenant_id: str, uow: Any, shared_ctx: object | None = None
            ) -> Iterable[Resource]:
                raise RuntimeError("API unavailable")

        failing_handler = FailingHandler(service_type="kafka", resources=[], identities=[])
        plugin = MockPlugin(handlers={"kafka": failing_handler}, cost_input=MockCostInput())
        bundle = EcosystemBundle.build(plugin)
        phase = _make_gather_phase(
            bundle=bundle,
            min_refresh_gap=timedelta(seconds=0),  # always refresh
        )

        uow = MockUnitOfWork()
        # Pre-populate a resource that would be deleted if deletion ran
        uow.resources.upsert(_make_resource("r-existing"))

        with caplog.at_level(logging.WARNING):
            result = phase.run(uow)

        # gather_complete=False means deletion detection was skipped
        assert uow.resources._deletions == []
        # gather still completes (billing may continue)
        assert isinstance(result, GatherResult)
        assert result.skipped is False
        # Error is captured in result
        assert len(result.errors) > 0

    def test_handler_exception_billing_continues(self) -> None:
        """Even with handler exception, billing gather still runs."""
        from core.plugin.registry import EcosystemBundle

        class FailingHandler(MockServiceHandler):
            def gather_resources(
                self, tenant_id: str, uow: Any, shared_ctx: object | None = None
            ) -> Iterable[Resource]:
                raise RuntimeError("API unavailable")

        billing_ts = NOW - timedelta(days=10)
        billing_line = _make_billing_line(timestamp=billing_ts)
        failing_handler = FailingHandler(service_type="kafka", resources=[], identities=[])
        cost_input = MockCostInput(lines=[billing_line])
        plugin = MockPlugin(handlers={"kafka": failing_handler}, cost_input=cost_input)
        bundle = EcosystemBundle.build(plugin)
        phase = _make_gather_phase(
            bundle=bundle,
            min_refresh_gap=timedelta(seconds=0),
        )

        uow = MockUnitOfWork()
        result = phase.run(uow)

        # Billing should have been gathered despite handler failure
        assert result.dates_gathered >= 1

    def test_identity_with_created_at_is_utc_normalised(self) -> None:
        """Identity with created_at is UTC-normalised before upsert (covers line 199)."""
        from core.plugin.registry import EcosystemBundle

        identity_with_ts = _make_identity("user-1")
        identity_with_ts = CoreIdentity(
            ecosystem=ECOSYSTEM,
            tenant_id=TENANT_ID,
            identity_id="user-1",
            identity_type="user",
            display_name="User 1",
            created_at=NOW - timedelta(days=5),
        )
        handler = MockServiceHandler(
            service_type="kafka",
            resources=[],
            identities=[identity_with_ts],
        )
        plugin = MockPlugin(handlers={"kafka": handler}, cost_input=MockCostInput())
        bundle = EcosystemBundle.build(plugin)
        phase = _make_gather_phase(bundle=bundle, min_refresh_gap=timedelta(seconds=0))

        uow = MockUnitOfWork()
        phase.run(uow)

        stored = uow.identities.get(ECOSYSTEM, TENANT_ID, "user-1")
        assert stored is not None
        assert stored.created_at is not None
        assert stored.created_at.tzinfo is not None  # UTC-aware


class TestGatherPhaseDetectEntityDeletions:
    def test_threshold_neg1_always_skips_deletion(self) -> None:
        """threshold=-1 means never auto-delete even with zero gather."""
        tc = _make_tenant_config(zero_gather_deletion_threshold=-1)
        phase = _make_gather_phase(tenant_config=tc)

        repo = MockResourceRepo()
        repo.upsert(_make_resource("r1"))

        # gathered_ids is empty → would normally trigger deletion check
        phase._detect_entity_deletions(
            repo=repo,
            gathered_ids=set(),
            entity_name="resources",
            id_getter=lambda r: r.resource_id,
            now=NOW,
        )

        assert repo._deletions == []

    def test_threshold_2_skips_first_zero_gather(self) -> None:
        """threshold=2: first zero-gather skips deletion (counter=1 < threshold=2)."""
        tc = _make_tenant_config(zero_gather_deletion_threshold=2)
        phase = _make_gather_phase(tenant_config=tc)

        repo = MockResourceRepo()
        repo.upsert(_make_resource("r1"))

        phase._detect_entity_deletions(
            repo=repo,
            gathered_ids=set(),
            entity_name="resources",
            id_getter=lambda r: r.resource_id,
            now=NOW,
        )

        # First zero-gather: counter=1, threshold=2 → skip
        assert repo._deletions == []
        assert phase._zero_gather_counters["resources"] == 1

    def test_threshold_2_fires_on_second_zero_gather(self) -> None:
        """threshold=2: second consecutive zero-gather fires deletion."""
        tc = _make_tenant_config(zero_gather_deletion_threshold=2)
        phase = _make_gather_phase(tenant_config=tc)

        repo = MockResourceRepo()
        repo.upsert(_make_resource("r1"))

        # First call: counter=1, skip
        phase._detect_entity_deletions(
            repo=repo,
            gathered_ids=set(),
            entity_name="resources",
            id_getter=lambda r: r.resource_id,
            now=NOW,
        )
        assert repo._deletions == []

        # Second call: counter=2 >= threshold=2 → fire deletion
        phase._detect_entity_deletions(
            repo=repo,
            gathered_ids=set(),
            entity_name="resources",
            id_getter=lambda r: r.resource_id,
            now=NOW,
        )
        deleted_ids = [rid for rid, _ in repo._deletions]
        assert "r1" in deleted_ids

    def test_nonzero_gather_resets_counter_and_deletes_missing(self) -> None:
        """Non-zero gather resets counter and deletes IDs not in gathered set."""
        tc = _make_tenant_config(zero_gather_deletion_threshold=2)
        phase = _make_gather_phase(tenant_config=tc)
        phase._zero_gather_counters["resources"] = 1  # simulate prior zero-gather

        repo = MockResourceRepo()
        repo.upsert(_make_resource("r-active"))
        repo.upsert(_make_resource("r-gone"))

        # Gather returns only r-active; r-gone should be deleted
        phase._detect_entity_deletions(
            repo=repo,
            gathered_ids={"r-active"},
            entity_name="resources",
            id_getter=lambda r: r.resource_id,
            now=NOW,
        )

        deleted_ids = [rid for rid, _ in repo._deletions]
        assert "r-gone" in deleted_ids
        assert "r-active" not in deleted_ids
        # Counter reset
        assert phase._zero_gather_counters["resources"] == 0


class TestGatherPhaseApplyRecalculationWindow:
    def test_calculated_date_within_cutoff_gets_reset_and_chargebacks_deleted(self) -> None:
        """Dates within recalculation window that are already calculated get reset and rows deleted."""
        tc = _make_tenant_config(cutoff_days=5)
        phase = _make_gather_phase(tenant_config=tc)

        # billing_date within recalculation window (>= recalc_cutoff)
        recalc_cutoff = (NOW - timedelta(days=5)).date()
        billing_date = (NOW - timedelta(days=2)).date()
        assert billing_date >= recalc_cutoff

        uow = MockUnitOfWork()

        # Pipeline state already calculated
        state = PipelineState(
            ecosystem=ECOSYSTEM,
            tenant_id=TENANT_ID,
            tracking_date=billing_date,
            billing_gathered=True,
            resources_gathered=True,
            chargeback_calculated=True,
        )
        uow.pipeline_state.upsert(state)

        # Add a chargeback row for that date
        cb_ts = datetime(billing_date.year, billing_date.month, billing_date.day, tzinfo=UTC)
        uow.chargebacks._data.append(
            ChargebackRow(
                ecosystem=ECOSYSTEM,
                tenant_id=TENANT_ID,
                timestamp=cb_ts,
                resource_id="cluster-1",
                product_category="kafka",
                product_type="KAFKA_CKU",
                identity_id="user-1",
                cost_type=CostType.USAGE,
                amount=Decimal("100"),
                allocation_method="test",
            )
        )

        phase._apply_recalculation_window(uow, {billing_date}, NOW)

        # State reset to needs recalculation
        state_after = uow.pipeline_state.get(ECOSYSTEM, TENANT_ID, billing_date)
        assert state_after is not None
        assert state_after.chargeback_calculated is False

        # Chargeback rows deleted
        remaining = uow.chargebacks.find_by_date(ECOSYSTEM, TENANT_ID, billing_date)
        assert remaining == []

    def test_date_beyond_cutoff_not_reset(self) -> None:
        """Dates outside the recalculation window are not touched."""
        tc = _make_tenant_config(cutoff_days=5)
        phase = _make_gather_phase(tenant_config=tc)

        # billing_date BEFORE recalc_cutoff → not in recalculation window
        billing_date = (NOW - timedelta(days=10)).date()

        uow = MockUnitOfWork()
        state = PipelineState(
            ecosystem=ECOSYSTEM,
            tenant_id=TENANT_ID,
            tracking_date=billing_date,
            billing_gathered=True,
            resources_gathered=True,
            chargeback_calculated=True,
        )
        uow.pipeline_state.upsert(state)

        phase._apply_recalculation_window(uow, {billing_date}, NOW)

        # State unchanged
        state_after = uow.pipeline_state.get(ECOSYSTEM, TENANT_ID, billing_date)
        assert state_after is not None
        assert state_after.chargeback_calculated is True


# =============================================================================
# RetryManager tests
# =============================================================================


class TestRetryManager:
    def test_increment_and_check_opens_uow_calls_increment_and_commits(self) -> None:
        """increment_and_check opens a UoW, calls increment_allocation_attempts, commits."""
        mock_uow = MagicMock()
        mock_uow.__enter__ = MagicMock(return_value=mock_uow)
        mock_uow.__exit__ = MagicMock(return_value=False)
        mock_uow.billing.increment_allocation_attempts.return_value = 1

        mock_backend = MagicMock()
        mock_backend.create_unit_of_work.return_value = mock_uow

        manager = RetryManager(storage_backend=mock_backend, limit=3)
        line = _make_billing_line()

        manager.increment_and_check(line)

        mock_backend.create_unit_of_work.assert_called_once()
        mock_uow.billing.increment_allocation_attempts.assert_called_once_with(line)
        mock_uow.commit.assert_called_once()

    def test_below_limit_should_fallback_is_false(self) -> None:
        """new_attempts < limit → should_fallback=False."""
        mock_uow = MagicMock()
        mock_uow.__enter__ = MagicMock(return_value=mock_uow)
        mock_uow.__exit__ = MagicMock(return_value=False)
        mock_uow.billing.increment_allocation_attempts.return_value = 2

        mock_backend = MagicMock()
        mock_backend.create_unit_of_work.return_value = mock_uow

        manager = RetryManager(storage_backend=mock_backend, limit=3)
        line = _make_billing_line()

        new_attempts, should_fallback = manager.increment_and_check(line)

        assert new_attempts == 2
        assert should_fallback is False

    def test_at_limit_should_fallback_is_true(self) -> None:
        """new_attempts >= limit → should_fallback=True."""
        mock_uow = MagicMock()
        mock_uow.__enter__ = MagicMock(return_value=mock_uow)
        mock_uow.__exit__ = MagicMock(return_value=False)
        mock_uow.billing.increment_allocation_attempts.return_value = 3

        mock_backend = MagicMock()
        mock_backend.create_unit_of_work.return_value = mock_uow

        manager = RetryManager(storage_backend=mock_backend, limit=3)
        line = _make_billing_line()

        new_attempts, should_fallback = manager.increment_and_check(line)

        assert new_attempts == 3
        assert should_fallback is True

    def test_above_limit_should_fallback_is_true(self) -> None:
        """new_attempts > limit also triggers fallback."""
        mock_uow = MagicMock()
        mock_uow.__enter__ = MagicMock(return_value=mock_uow)
        mock_uow.__exit__ = MagicMock(return_value=False)
        mock_uow.billing.increment_allocation_attempts.return_value = 5

        mock_backend = MagicMock()
        mock_backend.create_unit_of_work.return_value = mock_uow

        manager = RetryManager(storage_backend=mock_backend, limit=3)
        line = _make_billing_line()

        _, should_fallback = manager.increment_and_check(line)
        assert should_fallback is True

    def test_retry_manager_satisfies_retry_checker_protocol(self) -> None:
        """RetryManager has increment_and_check method matching RetryChecker protocol."""
        mock_backend = MagicMock()
        manager = RetryManager(storage_backend=mock_backend, limit=3)

        assert callable(getattr(manager, "increment_and_check", None))


# =============================================================================
# CalculatePhase tests
# =============================================================================


def _make_calculate_phase(
    bundle: Any | None = None,
    retry_checker: Any | None = None,
    metrics_source: Any | None = None,
    allocator_registry: AllocatorRegistry | None = None,
    identity_overrides: dict | None = None,
    allocator_params: dict | None = None,
    metrics_step: timedelta = timedelta(hours=1),
) -> CalculatePhase:
    if bundle is None:
        bundle = MagicMock()
        bundle.product_type_to_handler = {}

    if retry_checker is None:
        retry_checker = MagicMock()
        retry_checker.increment_and_check.return_value = (1, False)

    return CalculatePhase(
        ecosystem=ECOSYSTEM,
        tenant_id=TENANT_ID,
        bundle=bundle,
        retry_checker=retry_checker,
        metrics_source=metrics_source,
        allocator_registry=allocator_registry or AllocatorRegistry(),
        identity_overrides=identity_overrides or {},
        allocator_params=allocator_params or {},
        metrics_step=metrics_step,
    )


class TestCalculatePhasePrefetchMetrics:
    def test_prefetch_deduplicates_query_keys_same_resource_window(self) -> None:
        """Duplicate metric query keys for same resource/window are deduplicated."""
        query_a = _make_metric_query(key="bytes_in")

        mock_handler = MagicMock()
        mock_handler.get_metrics_for_product_type.return_value = [query_a]

        mock_bundle = MagicMock()
        mock_bundle.product_type_to_handler.get.return_value = mock_handler

        mock_metrics = MagicMock()
        mock_metrics.query.return_value = {}

        phase = _make_calculate_phase(
            bundle=mock_bundle,
            metrics_source=mock_metrics,
        )

        # Two billing lines for same resource and same timestamp (same daily window)
        line1 = _make_billing_line(product_type="KAFKA_CKU", resource_id="cluster-1")
        line2 = _make_billing_line(product_type="KAFKA_CKU", resource_id="cluster-1")
        lines = [line1, line2]

        phase._prefetch_metrics(lines, phase._compute_line_window_cache(lines))

        # query should be called once with deduplicated query list (length 1, not 2)
        assert mock_metrics.query.call_count == 1
        called_queries = mock_metrics.query.call_args[0][0]
        assert len(called_queries) == 1
        assert called_queries[0].key == "bytes_in"

    def test_prefetch_uses_configured_metrics_step(self) -> None:
        """_prefetch_metrics passes self._metrics_step to metrics_source.query."""
        query_a = _make_metric_query(key="bytes_in")

        mock_handler = MagicMock()
        mock_handler.get_metrics_for_product_type.return_value = [query_a]

        mock_bundle = MagicMock()
        mock_bundle.product_type_to_handler.get.return_value = mock_handler

        mock_metrics = MagicMock()
        mock_metrics.query.return_value = {}

        custom_step = timedelta(minutes=15)
        phase = _make_calculate_phase(
            bundle=mock_bundle,
            metrics_source=mock_metrics,
            metrics_step=custom_step,
        )

        line = _make_billing_line()
        phase._prefetch_metrics([line], phase._compute_line_window_cache([line]))

        mock_metrics.query.assert_called_once()
        call_kwargs = mock_metrics.query.call_args
        # step kwarg should be custom_step, not timedelta(hours=1)
        assert call_kwargs.kwargs.get("step") == custom_step or (
            len(call_kwargs.args) > 3 and call_kwargs.args[3] == custom_step
        )

    def test_prefetch_different_resources_queries_separately(self) -> None:
        """Lines with different resource_ids result in separate metrics_source.query calls."""
        query_a = _make_metric_query(key="bytes_in")

        mock_handler = MagicMock()
        mock_handler.get_metrics_for_product_type.return_value = [query_a]

        mock_bundle = MagicMock()
        mock_bundle.product_type_to_handler.get.return_value = mock_handler

        mock_metrics = MagicMock()
        mock_metrics.query.return_value = {}

        phase = _make_calculate_phase(
            bundle=mock_bundle,
            metrics_source=mock_metrics,
        )

        line1 = _make_billing_line(resource_id="cluster-1")
        line2 = _make_billing_line(resource_id="cluster-2")
        lines = [line1, line2]

        phase._prefetch_metrics(lines, phase._compute_line_window_cache(lines))

        # Two different resource_ids → two separate query calls
        assert mock_metrics.query.call_count == 2


class TestCalculatePhaseProcessBillingLine:
    def test_unknown_product_type_no_fallback_returns_0_and_writes_nothing(self) -> None:
        """Billing line with unknown product_type and no fallback_allocator → returns 0, writes nothing."""
        mock_bundle = MagicMock()
        # No handler for product type
        mock_bundle.product_type_to_handler.get.return_value = None
        # No fallback allocator configured
        mock_bundle.fallback_allocator = None

        phase = _make_calculate_phase(bundle=mock_bundle)

        line = _make_billing_line(product_type="UNKNOWN_PRODUCT")
        uow = MockUnitOfWork()

        rows = phase._collect_billing_line_rows(
            line=line,
            uow=uow,
            prefetched_metrics={},
            tenant_period_cache={},
            resource_cache={},
            line_window_cache=phase._compute_line_window_cache([line]),
        )

        assert rows == []
        assert len(uow.chargebacks._data) == 0

    def test_allocation_exception_no_fallback_reraises(self) -> None:
        """Allocation exception + should_fallback=False → re-raises original exception."""
        mock_handler = MagicMock()
        mock_handler.service_type = "kafka"
        mock_handler.resolve_identities.side_effect = RuntimeError("allocation blew up")

        mock_bundle = MagicMock()
        mock_bundle.product_type_to_handler.get.return_value = mock_handler

        mock_retry_checker = MagicMock()
        mock_retry_checker.increment_and_check.return_value = (1, False)  # no fallback

        phase = _make_calculate_phase(
            bundle=mock_bundle,
            retry_checker=mock_retry_checker,
        )

        line = _make_billing_line()
        uow = MockUnitOfWork()

        with pytest.raises(RuntimeError, match="allocation blew up"):
            phase._collect_billing_line_rows(
                line=line,
                uow=uow,
                prefetched_metrics={},
                tenant_period_cache={},
                resource_cache={},
                line_window_cache=phase._compute_line_window_cache([line]),
            )

    def test_allocation_exception_with_fallback_writes_unallocated(self) -> None:
        """Allocation exception + should_fallback=True → writes UNALLOCATED row, returns 1."""
        mock_handler = MagicMock()
        mock_handler.service_type = "kafka"
        mock_handler.resolve_identities.side_effect = RuntimeError("persistent failure")

        mock_bundle = MagicMock()
        mock_bundle.product_type_to_handler.get.return_value = mock_handler

        mock_retry_checker = MagicMock()
        mock_retry_checker.increment_and_check.return_value = (3, True)  # fallback=True

        phase = _make_calculate_phase(
            bundle=mock_bundle,
            retry_checker=mock_retry_checker,
        )

        line = _make_billing_line()
        uow = MockUnitOfWork()

        rows = phase._collect_billing_line_rows(
            line=line,
            uow=uow,
            prefetched_metrics={},
            tenant_period_cache={},
            resource_cache={},
            line_window_cache=phase._compute_line_window_cache([line]),
        )

        assert len(rows) == 1
        unalloc_rows = [r for r in rows if r.identity_id == "UNALLOCATED"]
        assert len(unalloc_rows) == 1
        assert unalloc_rows[0].allocation_method == "ALLOCATION_FAILED"

    def test_retry_checker_raises_reraises_original_exception(self) -> None:
        """If retry_checker.increment_and_check raises, original allocation exception re-raised."""
        original_exc = RuntimeError("original allocation error")

        mock_handler = MagicMock()
        mock_handler.service_type = "kafka"
        mock_handler.resolve_identities.side_effect = original_exc

        mock_bundle = MagicMock()
        mock_bundle.product_type_to_handler.get.return_value = mock_handler

        mock_retry_checker = MagicMock()
        mock_retry_checker.increment_and_check.side_effect = Exception("DB connection failed")

        phase = _make_calculate_phase(
            bundle=mock_bundle,
            retry_checker=mock_retry_checker,
        )

        line = _make_billing_line()
        uow = MockUnitOfWork()

        # Original exception re-raised, not the retry checker exception
        with pytest.raises(RuntimeError, match="original allocation error"):
            phase._collect_billing_line_rows(
                line=line,
                uow=uow,
                prefetched_metrics={},
                tenant_period_cache={},
                resource_cache={},
                line_window_cache=phase._compute_line_window_cache([line]),
            )


# =============================================================================
# ChargebackOrchestrator (thin coordinator) tests
# =============================================================================


def _make_orchestrator_with_mocked_phases(
    gather_failure_threshold: int = 3,
) -> tuple[ChargebackOrchestrator, MagicMock, MagicMock, MockStorageBackend]:
    """Build ChargebackOrchestrator, then replace phases with mocks."""
    plugin = MockPlugin(handlers={}, cost_input=MockCostInput())
    storage = MockStorageBackend()
    tc = _make_tenant_config(
        gather_failure_threshold=gather_failure_threshold,
        plugin_settings={},
    )

    orch = ChargebackOrchestrator(TENANT_NAME, tc, plugin, storage)

    mock_gather = MagicMock()
    mock_calculate = MagicMock()

    orch._gather_phase = mock_gather
    orch._calculate_phase = mock_calculate

    return orch, mock_gather, mock_calculate, storage


class TestChargebackOrchestratorDelegation:
    def test_run_delegates_to_gather_phase(self) -> None:
        """run() calls _gather_phase.run() with a UoW."""
        orch, mock_gather, mock_calculate, storage = _make_orchestrator_with_mocked_phases()

        # Gather returns success
        mock_gather.run.return_value = GatherResult(dates_gathered=0, errors=[], skipped=False)

        orch.run()

        mock_gather.run.assert_called_once()

    def test_run_delegates_to_calculate_phase(self) -> None:
        """run() calls _calculate_phase.run() for each pending date."""
        orch, mock_gather, mock_calculate, storage = _make_orchestrator_with_mocked_phases()

        mock_gather.run.return_value = GatherResult(dates_gathered=1, errors=[], skipped=False)

        # Add a pending pipeline state that needs calculation
        uow = storage.create_unit_of_work()
        billing_date = (NOW - timedelta(days=10)).date()
        ps = PipelineState(
            ecosystem=ECOSYSTEM,
            tenant_id=TENANT_ID,
            tracking_date=billing_date,
            billing_gathered=True,
            resources_gathered=True,
            chargeback_calculated=False,
        )
        uow.pipeline_state.upsert(ps)

        mock_calculate.run.return_value = 1

        orch.run()

        mock_calculate.run.assert_called_once()

    def test_gather_exception_increments_consecutive_failures(self) -> None:
        """Gather exception increments _consecutive_gather_failures."""
        orch, mock_gather, mock_calculate, storage = _make_orchestrator_with_mocked_phases(gather_failure_threshold=5)

        mock_gather.run.side_effect = RuntimeError("gather exploded")

        orch.run()

        assert orch._consecutive_gather_failures == 1

    def test_gather_exception_at_threshold_raises_gather_failure_threshold_error(self) -> None:
        """At threshold, run() raises GatherFailureThresholdError."""
        threshold = 3
        orch, mock_gather, mock_calculate, storage = _make_orchestrator_with_mocked_phases(
            gather_failure_threshold=threshold
        )

        mock_gather.run.side_effect = RuntimeError("gather failed")

        # Run threshold-1 times: each increments counter, no raise yet
        for _ in range(threshold - 1):
            orch.run()
        assert orch._consecutive_gather_failures == threshold - 1

        # Final run: meets threshold → raises
        with pytest.raises(GatherFailureThresholdError):
            orch.run()

    def test_gather_success_resets_consecutive_failures_to_zero(self) -> None:
        """Gather success resets _consecutive_gather_failures to 0."""
        orch, mock_gather, mock_calculate, storage = _make_orchestrator_with_mocked_phases(gather_failure_threshold=5)

        # Simulate prior failures
        orch._consecutive_gather_failures = 2

        mock_gather.run.return_value = GatherResult(dates_gathered=0, errors=[], skipped=False)

        orch.run()

        assert orch._consecutive_gather_failures == 0
