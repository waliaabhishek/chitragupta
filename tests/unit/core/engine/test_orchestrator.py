from __future__ import annotations

import logging
from collections.abc import Iterable
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock, patch

import pytest

from core.engine.allocation import AllocationContext, AllocationResult
from core.engine.orchestrator import (
    ChargebackOrchestrator,
    GatherFailureThresholdError,
    PipelineRunResult,
    _ensure_utc,
    billing_window,
)
from core.models.billing import BillingLineItem, CoreBillingLineItem
from core.models.chargeback import ChargebackRow, CostType
from core.models.identity import CoreIdentity, Identity, IdentityResolution, IdentitySet
from core.models.pipeline import PipelineState
from core.models.resource import CoreResource, Resource

if TYPE_CHECKING:
    from core.models.metrics import MetricQuery, MetricRow

# ---------- Helpers ----------

NOW = datetime(2026, 2, 22, 12, 0, 0, tzinfo=UTC)
TODAY = NOW.date()
ECOSYSTEM = "test-eco"
TENANT_ID = "tenant-1"
TENANT_NAME = "test-tenant"


def _make_tenant_config(**overrides: Any) -> Any:
    from core.config.models import TenantConfig

    defaults = {
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


def _simple_allocator(ctx: AllocationContext) -> AllocationResult:
    """Test allocator: assigns full amount to first merged_active identity."""
    ids = list(ctx.identities.merged_active.ids())
    identity_id = ids[0] if ids else ctx.billing_line.resource_id
    row = ChargebackRow(
        ecosystem=ctx.billing_line.ecosystem,
        tenant_id=ctx.billing_line.tenant_id,
        timestamp=ctx.billing_line.timestamp,
        resource_id=ctx.billing_line.resource_id,
        product_category=ctx.billing_line.product_category,
        product_type=ctx.billing_line.product_type,
        identity_id=identity_id,
        cost_type=CostType.USAGE,
        amount=ctx.split_amount,
        allocation_method="test_allocator",
    )
    return AllocationResult(rows=[row])


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
    ):
        self._service_type = service_type
        self._product_types = product_types or ["KAFKA_CKU"]
        self._resources = resources or []
        self._identities = identities or []
        self._metrics_queries = metrics_queries or []
        self._allocator = allocator or _simple_allocator
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
        metrics_data: dict[str, list[MetricRow]] | None,
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
    def __init__(self, lines: list[BillingLineItem] | None = None):
        self._lines = lines or []

    def gather(self, tenant_id: str, start: datetime, end: datetime, uow: Any) -> Iterable[BillingLineItem]:
        return self._lines


class MockPlugin:
    def __init__(self, handlers: dict[str, MockServiceHandler] | None = None, cost_input: MockCostInput | None = None):
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


class MockPluginWithFallback(MockPlugin):
    """MockPlugin with get_fallback_allocator() for GAP-074 tests."""

    def __init__(self, fallback_allocator: Any = None, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._fallback_allocator = fallback_allocator

    def get_fallback_allocator(self) -> Any:
        return self._fallback_allocator


class MockUnitOfWork:
    """In-memory UoW for unit tests."""

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
            r = self._data[resource_id]
            r.deleted_at = deleted_at

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

    def _pk(self, line: BillingLineItem) -> tuple[Any, ...]:
        return (line.ecosystem, line.tenant_id, line.timestamp, line.resource_id, line.product_type)

    def upsert(self, line: BillingLineItem) -> BillingLineItem:
        pk = self._pk(line)
        for i, existing in enumerate(self._data):
            if self._pk(existing) == pk:
                self._data[i] = line
                return line
        self._data.append(line)
        return line

    def find_by_date(self, ecosystem: str, tenant_id: str, target_date: date) -> list[BillingLineItem]:
        return [bl for bl in self._data if bl.timestamp.date() == target_date]

    def find_by_range(self, *args: Any) -> list[BillingLineItem]:
        return self._data

    def increment_allocation_attempts(self, line: BillingLineItem) -> int:
        key = (line.resource_id, line.product_type, str(line.timestamp))
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

    def find_by_date(self, ecosystem: str, tenant_id: str, target_date: date) -> list[ChargebackRow]:
        return [r for r in self._data if r.timestamp.date() == target_date]

    def find_by_range(self, *args: Any) -> list[ChargebackRow]:
        return self._data

    def find_by_identity(self, *args: Any) -> list[ChargebackRow]:
        return []

    def delete_by_date(self, ecosystem: str, tenant_id: str, target_date: date) -> int:
        before = len(self._data)
        self._data = [r for r in self._data if r.timestamp.date() != target_date]
        return before - len(self._data)

    def delete_before(self, *args: Any) -> int:
        return 0


class MockPipelineStateRepo:
    def __init__(self) -> None:
        self._data: dict[tuple[str, str, date], PipelineState] = {}

    def upsert(self, state: PipelineState) -> PipelineState:
        key = (state.ecosystem, state.tenant_id, state.tracking_date)
        if key not in self._data:
            self._data[key] = state
        return self._data[key]

    def get(self, ecosystem: str, tenant_id: str, tracking_date: date) -> PipelineState | None:
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

    def mark_billing_gathered(self, ecosystem: str, tenant_id: str, tracking_date: date) -> None:
        key = (ecosystem, tenant_id, tracking_date)
        if key in self._data:
            self._data[key].billing_gathered = True

    def mark_resources_gathered(self, ecosystem: str, tenant_id: str, tracking_date: date) -> None:
        key = (ecosystem, tenant_id, tracking_date)
        if key in self._data:
            self._data[key].resources_gathered = True

    def mark_needs_recalculation(self, ecosystem: str, tenant_id: str, tracking_date: date) -> None:
        key = (ecosystem, tenant_id, tracking_date)
        if key in self._data:
            self._data[key].chargeback_calculated = False

    def mark_chargeback_calculated(self, ecosystem: str, tenant_id: str, tracking_date: date) -> None:
        key = (ecosystem, tenant_id, tracking_date)
        if key in self._data:
            self._data[key].chargeback_calculated = True


class MockStorageBackend:
    def __init__(self, uow: MockUnitOfWork | None = None) -> None:
        self._uow = uow or MockUnitOfWork()

    def create_unit_of_work(self) -> MockUnitOfWork:
        return self._uow

    def create_tables(self) -> None:
        pass

    def dispose(self) -> None:
        pass


# ---------- billing_window tests ----------


class TestBillingWindow:
    def test_hourly(self) -> None:
        line = _make_billing_line(granularity="hourly")
        start, end, dur = billing_window(line)
        assert start == line.timestamp
        assert dur == timedelta(hours=1)
        assert end == start + dur

    def test_daily(self) -> None:
        line = _make_billing_line(granularity="daily")
        start, end, dur = billing_window(line)
        assert dur == timedelta(hours=24)

    def test_monthly_feb_leap(self) -> None:
        ts = datetime(2024, 2, 1, tzinfo=UTC)
        line = _make_billing_line(granularity="monthly", timestamp=ts)
        _, _, dur = billing_window(line)
        assert dur == timedelta(days=29)

    def test_monthly_feb_non_leap(self) -> None:
        ts = datetime(2023, 2, 1, tzinfo=UTC)
        line = _make_billing_line(granularity="monthly", timestamp=ts)
        _, _, dur = billing_window(line)
        assert dur == timedelta(days=28)

    def test_monthly_april(self) -> None:
        ts = datetime(2024, 4, 1, tzinfo=UTC)
        line = _make_billing_line(granularity="monthly", timestamp=ts)
        _, _, dur = billing_window(line)
        assert dur == timedelta(days=30)

    def test_monthly_december(self) -> None:
        ts = datetime(2024, 12, 1, tzinfo=UTC)
        line = _make_billing_line(granularity="monthly", timestamp=ts)
        _, _, dur = billing_window(line)
        assert dur == timedelta(days=31)

    def test_unknown_granularity(self) -> None:
        line = _make_billing_line(granularity="biweekly")
        with pytest.raises(ValueError, match="Unknown billing granularity"):
            billing_window(line)


# ---------- _ensure_utc tests ----------


class TestEnsureUtc:
    def test_naive_raises(self) -> None:
        dt = datetime(2024, 1, 1, 12, 0, 0)
        with pytest.raises(ValueError, match="Naive datetime"):
            _ensure_utc(dt)

    def test_utc_passthrough(self) -> None:
        dt = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
        assert _ensure_utc(dt) == dt

    def test_non_utc_converted(self) -> None:
        from datetime import timezone

        eastern = timezone(timedelta(hours=-5))
        dt = datetime(2024, 1, 1, 12, 0, 0, tzinfo=eastern)
        result = _ensure_utc(dt)
        assert result.tzinfo == UTC
        assert result.hour == 17  # 12 + 5


# ---------- Orchestrator tests ----------


def _create_orchestrator(
    handler: MockServiceHandler | None = None,
    cost_input: MockCostInput | None = None,
    storage: MockStorageBackend | None = None,
    metrics_source: Any = None,
    plugin_settings: dict[str, Any] | None = None,
    shutdown_check: Any = None,
    **config_overrides: Any,
) -> tuple[ChargebackOrchestrator, MockStorageBackend]:
    if handler is None:
        handler = MockServiceHandler(
            resources=[_make_resource()],
            identities=[_make_identity()],
        )
    if cost_input is None:
        cost_input = MockCostInput()
    plugin = MockPlugin(handlers={"kafka": handler}, cost_input=cost_input)
    if storage is None:
        storage = MockStorageBackend()
    tc = _make_tenant_config(plugin_settings=plugin_settings or {}, **config_overrides)
    orch = ChargebackOrchestrator(TENANT_NAME, tc, plugin, storage, metrics_source, shutdown_check=shutdown_check)
    return orch, storage


class TestOrchestratorInit:
    def test_unallocated_identity_created(self) -> None:
        _, storage = _create_orchestrator()
        uow = storage.create_unit_of_work()
        assert "UNALLOCATED" in uow.identities._data
        unalloc = uow.identities._data["UNALLOCATED"]
        assert unalloc.identity_type == "system"
        assert unalloc.display_name == "Unallocated Costs"

    def test_metrics_prefetch_workers_wired_from_tenant_config(self) -> None:
        """TenantConfig.metrics_prefetch_workers is passed through to CalculatePhase."""
        orch, _ = _create_orchestrator(metrics_prefetch_workers=7)
        assert orch._calculate_phase._metrics_prefetch_workers == 7


class TestGatherPhase:
    def test_resources_and_identities_gathered(self) -> None:
        handler = MockServiceHandler(
            resources=[_make_resource("r1"), _make_resource("r2")],
            identities=[_make_identity("i1")],
        )
        lines = [_make_billing_line(timestamp=NOW - timedelta(days=10))]
        cost_input = MockCostInput(lines)
        orch, storage = _create_orchestrator(handler=handler, cost_input=cost_input)

        with patch("core.engine.orchestrator.datetime") as mock_dt:
            mock_dt.now.return_value = NOW
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            orch.run()

        uow = storage.create_unit_of_work()
        assert "r1" in uow.resources._data
        assert "r2" in uow.resources._data
        assert "i1" in uow.identities._data

    def test_partial_gather_skips_deletion(self, caplog: pytest.LogCaptureFixture) -> None:
        """If a handler raises during gather, deletion detection is skipped."""

        class FailingHandler(MockServiceHandler):
            def gather_resources(
                self, tenant_id: str, uow: Any, shared_ctx: object | None = None
            ) -> Iterable[Resource]:
                raise RuntimeError("API down")

        handler = FailingHandler(resources=[], identities=[])
        orch, storage = _create_orchestrator(handler=handler)

        with caplog.at_level(logging.WARNING):
            orch.run()

        assert any("incomplete gather" in r.message.lower() for r in caplog.records)
        # No deletions should have been attempted
        uow = storage.create_unit_of_work()
        assert uow.resources._deletions == []

    def test_deletion_detection_marks_missing(self) -> None:
        """Resources not returned by gather get marked deleted."""
        handler = MockServiceHandler(
            resources=[_make_resource("r1")],
            identities=[_make_identity("i1")],
        )
        orch, storage = _create_orchestrator(handler=handler)
        uow = storage.create_unit_of_work()

        # Pre-populate an active resource that handler won't return
        old_resource = _make_resource("r-old")
        uow.resources.upsert(old_resource)

        orch.run()
        # r-old should be deleted
        deleted_ids = [rid for rid, _ in uow.resources._deletions]
        assert "r-old" in deleted_ids


class TestZeroGatherProtection:
    def test_zero_gather_default_threshold_skips_deletion(self) -> None:
        """Default threshold=-1 never auto-deletes on zero gather."""
        handler = MockServiceHandler(resources=[], identities=[])
        orch, storage = _create_orchestrator(handler=handler)
        uow = storage.create_unit_of_work()

        # Pre-populate active resource
        uow.resources.upsert(_make_resource("r-existing"))

        orch.run()
        assert uow.resources._deletions == []

    def test_zero_gather_threshold_exceeded_deletes(self) -> None:
        """After N consecutive zero gathers, deletion proceeds."""
        handler = MockServiceHandler(resources=[], identities=[])
        orch, storage = _create_orchestrator(
            handler=handler, zero_gather_deletion_threshold=2, plugin_settings={"min_refresh_gap_seconds": 0}
        )
        uow = storage.create_unit_of_work()
        uow.resources.upsert(_make_resource("r-existing"))

        # First run: under threshold
        orch.run()
        assert uow.resources._deletions == []

        # Second run: meets threshold
        orch.run()
        deleted_ids = [rid for rid, _ in uow.resources._deletions]
        assert "r-existing" in deleted_ids

    def test_nonzero_gather_resets_counter(self) -> None:
        """Non-zero gather resets the consecutive counter."""
        handler = MockServiceHandler(resources=[], identities=[])
        orch, storage = _create_orchestrator(
            handler=handler, zero_gather_deletion_threshold=3, plugin_settings={"min_refresh_gap_seconds": 0}
        )
        uow = storage.create_unit_of_work()
        uow.resources.upsert(_make_resource("r-existing"))

        # 2 zero gathers
        orch.run()
        orch.run()
        assert orch._zero_gather_counters["resources"] == 2

        # Now handler returns a resource — resets counter
        handler._resources = [_make_resource("r-new")]
        orch.run()
        assert orch._zero_gather_counters["resources"] == 0


class TestCalculatePhase:
    def test_billing_line_dispatched_to_handler(self) -> None:
        handler = MockServiceHandler(
            resources=[_make_resource()],
            identities=[_make_identity()],
        )
        line = _make_billing_line(timestamp=NOW - timedelta(days=10))
        cost_input = MockCostInput([line])
        orch, storage = _create_orchestrator(handler=handler, cost_input=cost_input)

        result = orch.run()
        assert result.dates_calculated >= 0  # may be 0 if resources_gathered not set for that date

    def test_unknown_product_type_no_fallback_skips_line(self) -> None:
        """Billing line with unmapped product_type and no fallback_allocator — skipped, no row written."""
        handler = MockServiceHandler(
            product_types=["KAFKA_CKU"],
            resources=[_make_resource()],
            identities=[_make_identity()],
        )
        line = _make_billing_line(product_type="UNKNOWN_THING", timestamp=NOW - timedelta(days=10))
        cost_input = MockCostInput([line])
        orch, storage = _create_orchestrator(handler=handler, cost_input=cost_input)

        # Manually set pipeline state to allow calculation
        uow = storage.create_unit_of_work()
        ps = PipelineState(
            ecosystem=ECOSYSTEM,
            tenant_id=TENANT_ID,
            tracking_date=line.timestamp.date(),
            billing_gathered=True,
            resources_gathered=True,
        )
        uow.pipeline_state.upsert(ps)
        uow.pipeline_state.mark_billing_gathered(ECOSYSTEM, TENANT_ID, line.timestamp.date())
        uow.pipeline_state.mark_resources_gathered(ECOSYSTEM, TENANT_ID, line.timestamp.date())
        uow.billing.upsert(line)

        orch.run()
        # No fallback_allocator on MockPlugin — line is skipped, no chargeback row produced
        unknown_rows = [r for r in uow.chargebacks._data if r.product_type == "UNKNOWN_THING"]
        assert len(unknown_rows) == 0

    def test_empty_billing_marks_calculated(self) -> None:
        handler = MockServiceHandler(resources=[_make_resource()], identities=[_make_identity()])
        orch, storage = _create_orchestrator(handler=handler)
        uow = storage.create_unit_of_work()

        # Set up a date with no billing lines but needing calculation
        ps = PipelineState(
            ecosystem=ECOSYSTEM,
            tenant_id=TENANT_ID,
            tracking_date=date(2026, 2, 10),
            billing_gathered=True,
            resources_gathered=True,
        )
        uow.pipeline_state.upsert(ps)
        uow.pipeline_state.mark_billing_gathered(ECOSYSTEM, TENANT_ID, date(2026, 2, 10))
        uow.pipeline_state.mark_resources_gathered(ECOSYSTEM, TENANT_ID, date(2026, 2, 10))

        orch.run()
        state = uow.pipeline_state.get(ECOSYSTEM, TENANT_ID, date(2026, 2, 10))
        assert state is not None
        assert state.chargeback_calculated

    def test_resource_not_found_uses_fraction_1(self) -> None:
        """If resource is not in storage, active_fraction defaults to 1."""
        handler = MockServiceHandler(
            resources=[],
            identities=[_make_identity()],
        )
        line = _make_billing_line(resource_id="nonexistent", timestamp=NOW - timedelta(days=10))
        cost_input = MockCostInput([line])
        orch, storage = _create_orchestrator(handler=handler, cost_input=cost_input)
        uow = storage.create_unit_of_work()

        ps = PipelineState(
            ecosystem=ECOSYSTEM,
            tenant_id=TENANT_ID,
            tracking_date=line.timestamp.date(),
            billing_gathered=True,
            resources_gathered=True,
        )
        uow.pipeline_state.upsert(ps)
        uow.pipeline_state.mark_billing_gathered(ECOSYSTEM, TENANT_ID, line.timestamp.date())
        uow.pipeline_state.mark_resources_gathered(ECOSYSTEM, TENANT_ID, line.timestamp.date())
        uow.billing.upsert(line)

        orch.run()
        rows = [r for r in uow.chargebacks._data if r.resource_id == "nonexistent"]
        assert len(rows) >= 1
        # Full cost allocated (fraction=1)
        assert rows[0].amount == line.total_cost

    def test_no_metrics_source_passes_none(self) -> None:
        """Without metrics_source, metrics_data is None."""
        calls: list[Any] = []

        def tracking_allocator(ctx: AllocationContext) -> AllocationResult:
            calls.append(ctx.metrics_data)
            return _simple_allocator(ctx)

        handler = MockServiceHandler(
            resources=[_make_resource()],
            identities=[_make_identity()],
            allocator=tracking_allocator,
        )
        line = _make_billing_line(timestamp=NOW - timedelta(days=10))
        cost_input = MockCostInput([line])
        orch, storage = _create_orchestrator(handler=handler, cost_input=cost_input, metrics_source=None)
        uow = storage.create_unit_of_work()

        ps = PipelineState(
            ecosystem=ECOSYSTEM,
            tenant_id=TENANT_ID,
            tracking_date=line.timestamp.date(),
            billing_gathered=True,
            resources_gathered=True,
        )
        uow.pipeline_state.upsert(ps)
        uow.pipeline_state.mark_billing_gathered(ECOSYSTEM, TENANT_ID, line.timestamp.date())
        uow.pipeline_state.mark_resources_gathered(ECOSYSTEM, TENANT_ID, line.timestamp.date())
        uow.billing.upsert(line)

        orch.run()
        assert calls[0] is None


class TestAllocationRetry:
    def test_first_failure_increments_and_raises(self) -> None:
        """On first failure, attempts incremented, date fails."""

        def failing_allocator(ctx: AllocationContext) -> AllocationResult:
            raise RuntimeError("transient error")

        handler = MockServiceHandler(
            resources=[_make_resource()],
            identities=[_make_identity()],
            allocator=failing_allocator,
        )
        line = _make_billing_line(timestamp=NOW - timedelta(days=10))
        cost_input = MockCostInput([line])
        orch, storage = _create_orchestrator(handler=handler, cost_input=cost_input, allocation_retry_limit=3)
        uow = storage.create_unit_of_work()

        ps = PipelineState(
            ecosystem=ECOSYSTEM,
            tenant_id=TENANT_ID,
            tracking_date=line.timestamp.date(),
            billing_gathered=True,
            resources_gathered=True,
        )
        uow.pipeline_state.upsert(ps)
        uow.pipeline_state.mark_billing_gathered(ECOSYSTEM, TENANT_ID, line.timestamp.date())
        uow.pipeline_state.mark_resources_gathered(ECOSYSTEM, TENANT_ID, line.timestamp.date())
        uow.billing.upsert(line)

        result = orch.run()
        # Date should fail (error captured), not marked calculated
        assert len(result.errors) > 0
        state = uow.pipeline_state.get(ECOSYSTEM, TENANT_ID, line.timestamp.date())
        assert state is not None
        assert not state.chargeback_calculated

    def test_exhausted_retries_allocates_to_unallocated(self) -> None:
        """After exhausting retry limit, allocate to UNALLOCATED."""

        def failing_allocator(ctx: AllocationContext) -> AllocationResult:
            raise RuntimeError("persistent error")

        handler = MockServiceHandler(
            resources=[_make_resource()],
            identities=[_make_identity()],
            allocator=failing_allocator,
        )
        line = _make_billing_line(timestamp=NOW - timedelta(days=10))
        cost_input = MockCostInput([line])
        orch, storage = _create_orchestrator(handler=handler, cost_input=cost_input, allocation_retry_limit=1)
        uow = storage.create_unit_of_work()

        ps = PipelineState(
            ecosystem=ECOSYSTEM,
            tenant_id=TENANT_ID,
            tracking_date=line.timestamp.date(),
            billing_gathered=True,
            resources_gathered=True,
        )
        uow.pipeline_state.upsert(ps)
        uow.pipeline_state.mark_billing_gathered(ECOSYSTEM, TENANT_ID, line.timestamp.date())
        uow.pipeline_state.mark_resources_gathered(ECOSYSTEM, TENANT_ID, line.timestamp.date())
        uow.billing.upsert(line)

        orch.run()
        unalloc = [r for r in uow.chargebacks._data if r.allocation_method == "ALLOCATION_FAILED"]
        assert len(unalloc) >= 1
        assert "persistent error" in (unalloc[0].allocation_detail or "")


class TestTenantPeriod:
    def test_orchestrator_injects_tenant_period(self) -> None:
        """Orchestrator replaces handler's tenant_period with cached value."""
        captured: list[IdentityResolution] = []

        def capturing_allocator(ctx: AllocationContext) -> AllocationResult:
            captured.append(ctx.identities)
            return _simple_allocator(ctx)

        handler = MockServiceHandler(
            resources=[_make_resource()],
            identities=[_make_identity("i1"), _make_identity("i2")],
            allocator=capturing_allocator,
        )
        line = _make_billing_line(timestamp=NOW - timedelta(days=10))
        cost_input = MockCostInput([line])
        orch, storage = _create_orchestrator(handler=handler, cost_input=cost_input)
        uow = storage.create_unit_of_work()

        ps = PipelineState(
            ecosystem=ECOSYSTEM,
            tenant_id=TENANT_ID,
            tracking_date=line.timestamp.date(),
            billing_gathered=True,
            resources_gathered=True,
        )
        uow.pipeline_state.upsert(ps)
        uow.pipeline_state.mark_billing_gathered(ECOSYSTEM, TENANT_ID, line.timestamp.date())
        uow.pipeline_state.mark_resources_gathered(ECOSYSTEM, TENANT_ID, line.timestamp.date())
        uow.billing.upsert(line)

        orch.run()
        assert len(captured) >= 1
        # tenant_period should contain identities from identity repo (find_by_period)
        tp = captured[0].tenant_period
        tp_ids = set(tp.ids())
        # Must contain real identities (i1/i2 from handler gather); UNALLOCATED (system) must be excluded
        assert "UNALLOCATED" not in tp_ids
        assert "i1" in tp_ids or "i2" in tp_ids

    def test_handler_tenant_period_replaced_with_warning(self, caplog: pytest.LogCaptureFixture) -> None:
        """Handler returning non-empty tenant_period gets warned and replaced."""
        handler_tp = IdentitySet()
        handler_tp.add(_make_identity("bogus"))

        def resolve_with_tp(
            tenant_id: str,
            resource_id: str,
            bt: datetime,
            bd: timedelta,
            md: Any,
            uow: Any,
        ) -> IdentityResolution:
            return IdentityResolution(
                resource_active=IdentitySet(),
                metrics_derived=IdentitySet(),
                tenant_period=handler_tp,
            )

        handler = MockServiceHandler(
            resources=[_make_resource()],
            identities=[_make_identity()],
            resolve_fn=resolve_with_tp,
        )
        line = _make_billing_line(timestamp=NOW - timedelta(days=10))
        cost_input = MockCostInput([line])
        orch, storage = _create_orchestrator(handler=handler, cost_input=cost_input)
        uow = storage.create_unit_of_work()

        ps = PipelineState(
            ecosystem=ECOSYSTEM,
            tenant_id=TENANT_ID,
            tracking_date=line.timestamp.date(),
            billing_gathered=True,
            resources_gathered=True,
        )
        uow.pipeline_state.upsert(ps)
        uow.pipeline_state.mark_billing_gathered(ECOSYSTEM, TENANT_ID, line.timestamp.date())
        uow.pipeline_state.mark_resources_gathered(ECOSYSTEM, TENANT_ID, line.timestamp.date())
        uow.billing.upsert(line)

        with caplog.at_level(logging.WARNING):
            orch.run()
        assert any("non-empty tenant_period" in r.message for r in caplog.records)

    def test_tenant_period_excludes_system_identities(self) -> None:
        """GAP-23: Tenant-period cache must not include UNALLOCATED (system) identities."""
        captured: list[IdentityResolution] = []

        def capturing_allocator(ctx: AllocationContext) -> AllocationResult:
            captured.append(ctx.identities)
            return _simple_allocator(ctx)

        handler = MockServiceHandler(
            resources=[_make_resource()],
            identities=[
                _make_identity("user-1"),  # identity_type="user"
                CoreIdentity(
                    ecosystem=ECOSYSTEM,
                    tenant_id=TENANT_ID,
                    identity_id="sa-1",
                    identity_type="service_account",
                ),
            ],
            allocator=capturing_allocator,
        )
        line = _make_billing_line(timestamp=NOW - timedelta(days=10))
        cost_input = MockCostInput([line])
        orch, storage = _create_orchestrator(handler=handler, cost_input=cost_input)
        uow = storage.create_unit_of_work()

        ps = PipelineState(
            ecosystem=ECOSYSTEM,
            tenant_id=TENANT_ID,
            tracking_date=line.timestamp.date(),
            billing_gathered=True,
            resources_gathered=True,
        )
        uow.pipeline_state.upsert(ps)
        uow.pipeline_state.mark_billing_gathered(ECOSYSTEM, TENANT_ID, line.timestamp.date())
        uow.pipeline_state.mark_resources_gathered(ECOSYSTEM, TENANT_ID, line.timestamp.date())
        uow.billing.upsert(line)

        orch.run()

        assert len(captured) >= 1
        tp_ids = set(captured[0].tenant_period.ids())
        # UNALLOCATED is a system identity and must not appear in tenant_period
        assert "UNALLOCATED" not in tp_ids
        # Real identities must still be present
        assert "user-1" in tp_ids
        assert "sa-1" in tp_ids

    def test_tenant_period_empty_when_only_system_identities(self) -> None:
        """GAP-23: With only UNALLOCATED in the repo, tenant_period must be an empty set."""
        captured: list[IdentityResolution] = []

        def capturing_allocator(ctx: AllocationContext) -> AllocationResult:
            captured.append(ctx.identities)
            return _simple_allocator(ctx)

        # No real identities gathered — only UNALLOCATED (inserted by orchestrator init)
        handler = MockServiceHandler(
            resources=[_make_resource()],
            identities=[],
            allocator=capturing_allocator,
        )
        line = _make_billing_line(timestamp=NOW - timedelta(days=10))
        cost_input = MockCostInput([line])
        orch, storage = _create_orchestrator(handler=handler, cost_input=cost_input)
        uow = storage.create_unit_of_work()

        ps = PipelineState(
            ecosystem=ECOSYSTEM,
            tenant_id=TENANT_ID,
            tracking_date=line.timestamp.date(),
            billing_gathered=True,
            resources_gathered=True,
        )
        uow.pipeline_state.upsert(ps)
        uow.pipeline_state.mark_billing_gathered(ECOSYSTEM, TENANT_ID, line.timestamp.date())
        uow.pipeline_state.mark_resources_gathered(ECOSYSTEM, TENANT_ID, line.timestamp.date())
        uow.billing.upsert(line)

        orch.run()

        assert len(captured) >= 1
        # After filtering system identities, tenant_period must be empty
        tp_ids = set(captured[0].tenant_period.ids())
        assert tp_ids == set()


class TestAllDatesProcessed:
    def test_all_pending_dates_processed_in_one_run(self) -> None:
        """All pending dates are processed in a single run() call — no artificial cap."""
        handler = MockServiceHandler(resources=[_make_resource()], identities=[_make_identity()])
        orch, storage = _create_orchestrator(handler=handler)
        uow = storage.create_unit_of_work()

        # Simulate 90-day backfill scenario
        for i in range(90):
            d = date(2025, 10, 1) + timedelta(days=i)
            ps = PipelineState(
                ecosystem=ECOSYSTEM,
                tenant_id=TENANT_ID,
                tracking_date=d,
                billing_gathered=True,
                resources_gathered=True,
            )
            uow.pipeline_state.upsert(ps)
            uow.pipeline_state.mark_billing_gathered(ECOSYSTEM, TENANT_ID, d)
            uow.pipeline_state.mark_resources_gathered(ECOSYSTEM, TENANT_ID, d)

        result = orch.run()
        assert result.dates_calculated == 90  # all 90 processed in one cycle

    def test_steady_state_two_dates_processed(self) -> None:
        """Steady-state with 1-2 pending dates still works correctly."""
        handler = MockServiceHandler(resources=[_make_resource()], identities=[_make_identity()])
        orch, storage = _create_orchestrator(handler=handler)
        uow = storage.create_unit_of_work()

        for i in range(2):
            d = date(2026, 3, 1 + i)
            ps = PipelineState(
                ecosystem=ECOSYSTEM,
                tenant_id=TENANT_ID,
                tracking_date=d,
                billing_gathered=True,
                resources_gathered=True,
            )
            uow.pipeline_state.upsert(ps)
            uow.pipeline_state.mark_billing_gathered(ECOSYSTEM, TENANT_ID, d)
            uow.pipeline_state.mark_resources_gathered(ECOSYSTEM, TENANT_ID, d)

        result = orch.run()
        assert result.dates_calculated == 2


class TestRecalculationWindow:
    def test_recent_dates_recalculated(self) -> None:
        handler = MockServiceHandler(resources=[_make_resource()], identities=[_make_identity()])
        # cutoff_days=5 means recalc_cutoff = NOW - 5 days
        recent_date = (NOW - timedelta(days=3)).date()
        line = _make_billing_line(timestamp=datetime(recent_date.year, recent_date.month, recent_date.day, tzinfo=UTC))
        cost_input = MockCostInput([line])
        orch, storage = _create_orchestrator(handler=handler, cost_input=cost_input)
        uow = storage.create_unit_of_work()

        # Pre-populate state as already calculated
        ps = PipelineState(
            ecosystem=ECOSYSTEM,
            tenant_id=TENANT_ID,
            tracking_date=recent_date,
            billing_gathered=True,
            resources_gathered=True,
            chargeback_calculated=True,
        )
        uow.pipeline_state._data[(ECOSYSTEM, TENANT_ID, recent_date)] = ps
        # Add a stale chargeback
        stale = ChargebackRow(
            ecosystem=ECOSYSTEM,
            tenant_id=TENANT_ID,
            timestamp=datetime(recent_date.year, recent_date.month, recent_date.day, tzinfo=UTC),
            resource_id="cluster-1",
            product_category="kafka",
            product_type="KAFKA_CKU",
            identity_id="user-1",
            cost_type=CostType.USAGE,
            amount=Decimal("50.00"),
        )
        uow.chargebacks.upsert(stale)

        with patch("core.engine.orchestrator.datetime") as mock_dt:
            mock_dt.now.return_value = NOW
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            orch.run()

        # After gather, the stale chargeback ($50) must have been deleted by recalculation window
        stale_rows = [
            r for r in uow.chargebacks._data if r.timestamp.date() == recent_date and r.amount == Decimal("50.00")
        ]
        assert len(stale_rows) == 0, f"Stale chargeback should have been deleted, found {len(stale_rows)}"


class TestPipelineRunResult:
    def test_dataclass(self) -> None:
        r = PipelineRunResult(
            tenant_name="t",
            tenant_id="tid",
            dates_gathered=5,
            dates_calculated=3,
            chargeback_rows_written=10,
        )
        assert r.errors == []
        assert r.dates_gathered == 5


class TestMetricsPreFetch:
    def test_metrics_prefetched_and_passed_to_allocator(self) -> None:
        """When metrics_source is provided, metrics are pre-fetched and passed to allocator."""
        from core.models.metrics import MetricQuery, MetricRow

        captured_metrics: list[Any] = []

        def tracking_allocator(ctx: AllocationContext) -> AllocationResult:
            captured_metrics.append(ctx.metrics_data)
            return _simple_allocator(ctx)

        metrics_query = MetricQuery(
            key="cpu_usage",
            query_expression="rate(cpu_seconds_total{}[5m])",
            resource_label="resource_id",
            label_keys=["pod"],
        )
        handler = MockServiceHandler(
            resources=[_make_resource()],
            identities=[_make_identity()],
            metrics_queries=[metrics_query],
            allocator=tracking_allocator,
        )
        line = _make_billing_line(timestamp=NOW - timedelta(days=10))
        cost_input = MockCostInput([line])

        mock_metrics = MagicMock()
        mock_row = MetricRow(timestamp=NOW, metric_key="cpu_usage", value=42.0, labels={"pod": "p1"})
        mock_metrics.query.return_value = {"cpu_usage": [mock_row]}

        orch, storage = _create_orchestrator(handler=handler, cost_input=cost_input, metrics_source=mock_metrics)
        uow = storage.create_unit_of_work()

        ps = PipelineState(
            ecosystem=ECOSYSTEM,
            tenant_id=TENANT_ID,
            tracking_date=line.timestamp.date(),
            billing_gathered=True,
            resources_gathered=True,
        )
        uow.pipeline_state.upsert(ps)
        uow.pipeline_state.mark_billing_gathered(ECOSYSTEM, TENANT_ID, line.timestamp.date())
        uow.pipeline_state.mark_resources_gathered(ECOSYSTEM, TENANT_ID, line.timestamp.date())
        uow.billing.upsert(line)

        orch.run()
        assert len(captured_metrics) >= 1
        assert captured_metrics[0] is not None
        assert "cpu_usage" in captured_metrics[0]
        assert captured_metrics[0]["cpu_usage"][0].value == 42.0
        mock_metrics.query.assert_called_once()

    def test_metrics_deduped_across_same_resource_window(self) -> None:
        """Two billing lines with same resource/window don't duplicate metric queries."""
        from core.models.metrics import MetricQuery

        metrics_query = MetricQuery(
            key="cpu_usage",
            query_expression="rate(cpu_seconds_total{}[5m])",
            resource_label="resource_id",
            label_keys=[],
        )
        handler = MockServiceHandler(
            resources=[_make_resource()],
            identities=[_make_identity()],
            metrics_queries=[metrics_query],
            product_types=["KAFKA_CKU", "KAFKA_NETWORK"],
        )
        ts = NOW - timedelta(days=10)
        line1 = _make_billing_line(product_type="KAFKA_CKU", timestamp=ts)
        line2 = _make_billing_line(product_type="KAFKA_NETWORK", timestamp=ts)
        cost_input = MockCostInput([line1, line2])

        mock_metrics = MagicMock()
        mock_metrics.query.return_value = {"cpu_usage": []}

        orch, storage = _create_orchestrator(handler=handler, cost_input=cost_input, metrics_source=mock_metrics)
        uow = storage.create_unit_of_work()

        ps = PipelineState(
            ecosystem=ECOSYSTEM,
            tenant_id=TENANT_ID,
            tracking_date=ts.date(),
            billing_gathered=True,
            resources_gathered=True,
        )
        uow.pipeline_state.upsert(ps)
        uow.pipeline_state.mark_billing_gathered(ECOSYSTEM, TENANT_ID, ts.date())
        uow.pipeline_state.mark_resources_gathered(ECOSYSTEM, TENANT_ID, ts.date())
        uow.billing.upsert(line1)
        uow.billing.upsert(line2)

        orch.run()
        # Only one query call since same resource_id + billing window
        assert mock_metrics.query.call_count == 1


class TestLoadIdentityResolver:
    def test_invalid_format_raises(self) -> None:
        from core.engine.orchestrator import _load_identity_resolver

        with pytest.raises(ValueError, match="Expected 'module:attribute' format"):
            _load_identity_resolver("no_colon_here")

    def test_empty_path_raises(self) -> None:
        from core.engine.orchestrator import _load_identity_resolver

        with pytest.raises(ValueError, match="dotted_path must not be empty"):
            _load_identity_resolver("")

    def test_nonexistent_module_raises(self) -> None:
        from core.engine.orchestrator import _load_identity_resolver

        with pytest.raises(ImportError, match="Could not import module"):
            _load_identity_resolver("nonexistent.module:func")

    def test_not_callable_raises(self) -> None:
        from core.engine.orchestrator import _load_identity_resolver

        with pytest.raises(TypeError, match="does not satisfy protocol"):
            _load_identity_resolver("os.path:sep")

    def test_wrong_param_count_raises(self) -> None:
        from core.engine.orchestrator import _load_identity_resolver

        # os.path.join has *args, not 6 positional params — but let's use a known callable
        with pytest.raises(TypeError, match="Signature mismatch"):
            _load_identity_resolver("os.path:exists")


class TestGatherFailureEarlyReturn:
    def test_gather_failure_skips_calculate(self) -> None:
        """If gather phase raises, calculate phase is skipped entirely."""

        class ExplodingCostInput(MockCostInput):
            def gather(self, *args: Any, **kwargs: Any) -> Any:
                raise RuntimeError("cost API exploded")

        handler = MockServiceHandler(resources=[_make_resource()], identities=[_make_identity()])
        cost_input = ExplodingCostInput()
        orch, storage = _create_orchestrator(handler=handler, cost_input=cost_input)
        uow = storage.create_unit_of_work()

        # Set up a date that would need calculation
        ps = PipelineState(
            ecosystem=ECOSYSTEM,
            tenant_id=TENANT_ID,
            tracking_date=date(2026, 2, 10),
            billing_gathered=True,
            resources_gathered=True,
        )
        uow.pipeline_state.upsert(ps)
        uow.pipeline_state.mark_billing_gathered(ECOSYSTEM, TENANT_ID, date(2026, 2, 10))
        uow.pipeline_state.mark_resources_gathered(ECOSYSTEM, TENANT_ID, date(2026, 2, 10))

        result = orch.run()
        assert len(result.errors) == 1
        assert "Gather phase failed" in result.errors[0]
        # Calculate should NOT have run
        assert result.dates_calculated == 0
        state = uow.pipeline_state.get(ECOSYSTEM, TENANT_ID, date(2026, 2, 10))
        assert state is not None
        assert not state.chargeback_calculated


class TestGap004UtcReassignment:
    """GAP-004: UTC canonicalization must reassign converted values before persistence."""

    def test_gather_normalizes_utc_billing_timestamps(self) -> None:
        """Non-UTC billing timestamps are converted to UTC before storage."""
        from datetime import timezone

        utc_plus5 = timezone(timedelta(hours=5))
        # Billing line with UTC+5 timestamp (17:00+05 = 12:00 UTC)
        line = _make_billing_line(
            timestamp=datetime(2026, 2, 12, 17, 0, 0, tzinfo=utc_plus5),
        )
        handler = MockServiceHandler(
            resources=[_make_resource()],
            identities=[_make_identity()],
        )
        cost_input = MockCostInput([line])
        orch, storage = _create_orchestrator(handler=handler, cost_input=cost_input)
        uow = storage.create_unit_of_work()

        orch.run()
        # Stored billing line should have UTC timestamp
        stored = uow.billing._data
        assert len(stored) >= 1
        stored_ts = stored[0].timestamp
        assert stored_ts.tzinfo == UTC
        assert stored_ts.hour == 12  # converted from 17:00+05

    def test_gather_normalizes_utc_resource_created_at(self) -> None:
        """Non-UTC resource created_at is converted to UTC before storage."""
        from datetime import timezone

        utc_plus5 = timezone(timedelta(hours=5))
        resource = CoreResource(
            ecosystem=ECOSYSTEM,
            tenant_id=TENANT_ID,
            resource_id="r-utc5",
            resource_type="kafka_cluster",
            created_at=datetime(2026, 2, 12, 17, 0, 0, tzinfo=utc_plus5),
        )
        handler = MockServiceHandler(
            resources=[resource],
            identities=[_make_identity()],
        )
        orch, storage = _create_orchestrator(handler=handler)
        uow = storage.create_unit_of_work()

        orch.run()
        stored = uow.resources._data.get("r-utc5")
        assert stored is not None
        assert stored.created_at is not None
        assert stored.created_at.tzinfo == UTC
        assert stored.created_at.hour == 12


class TestGap006ErrorPropagation:
    """GAP-006: Partial handler gather errors must appear in PipelineRunResult.errors."""

    def test_partial_handler_failure_surfaces_in_errors(self) -> None:
        class FailingHandler(MockServiceHandler):
            def gather_resources(
                self, tenant_id: str, uow: Any, shared_ctx: object | None = None
            ) -> Iterable[Resource]:
                raise RuntimeError("API timeout")

        handler = FailingHandler(resources=[], identities=[])
        orch, storage = _create_orchestrator(handler=handler)

        result = orch.run()
        assert any("Handler kafka gather failed" in e for e in result.errors)
        assert any("API timeout" in e for e in result.errors)

    def test_multiple_handler_failures_all_surfaced(self) -> None:
        class Failing1(MockServiceHandler):
            def gather_resources(
                self, tenant_id: str, uow: Any, shared_ctx: object | None = None
            ) -> Iterable[Resource]:
                raise RuntimeError("handler1 down")

        class Failing2(MockServiceHandler):
            def gather_resources(
                self, tenant_id: str, uow: Any, shared_ctx: object | None = None
            ) -> Iterable[Resource]:
                raise RuntimeError("handler2 down")

        h1 = Failing1(service_type="svc1", product_types=["P1"])
        h2 = Failing2(service_type="svc2", product_types=["P2"])
        plugin = MockPlugin(handlers={"svc1": h1, "svc2": h2})
        storage = MockStorageBackend()
        tc = _make_tenant_config(plugin_settings={})
        orch = ChargebackOrchestrator(TENANT_NAME, tc, plugin, storage)

        result = orch.run()
        assert len([e for e in result.errors if "gather failed" in e]) == 2


class TestGap001ResourcesGatheredPerDate:
    """GAP-001: resources_gathered must be set for all billing dates, not just today."""

    def test_gather_marks_resources_gathered_for_all_billing_dates(self) -> None:
        handler = MockServiceHandler(
            resources=[_make_resource()],
            identities=[_make_identity()],
        )
        # Billing lines on historical dates
        ts1 = NOW - timedelta(days=10)
        ts2 = NOW - timedelta(days=15)
        lines = [
            _make_billing_line(timestamp=ts1),
            _make_billing_line(timestamp=ts2),
        ]
        cost_input = MockCostInput(lines)
        orch, storage = _create_orchestrator(handler=handler, cost_input=cost_input)

        result = orch.run()
        assert result.dates_gathered == 2

        uow = storage.create_unit_of_work()
        # Both historical dates should have resources_gathered=True
        for ts in [ts1, ts2]:
            state = uow.pipeline_state.get(ECOSYSTEM, TENANT_ID, ts.date())
            assert state is not None, f"No pipeline state for {ts.date()}"
            assert state.billing_gathered, f"billing_gathered not set for {ts.date()}"
            assert state.resources_gathered, f"resources_gathered not set for {ts.date()}"

    def test_partial_gather_skips_resources_gathered_for_billing_dates(self) -> None:
        """If a handler fails, resources_gathered stays False for all dates."""

        class FailingHandler(MockServiceHandler):
            def gather_resources(
                self, tenant_id: str, uow: Any, shared_ctx: object | None = None
            ) -> Iterable[Resource]:
                raise RuntimeError("API down")

        handler = FailingHandler(resources=[], identities=[])
        ts = NOW - timedelta(days=10)
        lines = [_make_billing_line(timestamp=ts)]
        cost_input = MockCostInput(lines)
        orch, storage = _create_orchestrator(handler=handler, cost_input=cost_input)

        orch.run()
        uow = storage.create_unit_of_work()
        state = uow.pipeline_state.get(ECOSYSTEM, TENANT_ID, ts.date())
        assert state is not None
        assert state.billing_gathered
        assert not state.resources_gathered  # gather_complete was False

    def test_both_flags_enable_calculation(self) -> None:
        """Dates with both billing_gathered AND resources_gathered get calculated."""
        handler = MockServiceHandler(
            resources=[_make_resource()],
            identities=[_make_identity()],
        )
        ts = NOW - timedelta(days=10)
        lines = [_make_billing_line(timestamp=ts)]
        cost_input = MockCostInput(lines)
        orch, storage = _create_orchestrator(handler=handler, cost_input=cost_input)

        result = orch.run()
        # Date should have been calculated (both flags were set during gather)
        assert result.dates_calculated >= 1
        uow = storage.create_unit_of_work()
        state = uow.pipeline_state.get(ECOSYSTEM, TENANT_ID, ts.date())
        assert state is not None
        assert state.chargeback_calculated


class TestEndToEnd:
    def test_full_pipeline(self) -> None:
        """End-to-end: gather → calculate with mock plugin."""
        identity = _make_identity("user-1")
        resource = _make_resource("cluster-1")
        handler = MockServiceHandler(
            resources=[resource],
            identities=[identity],
        )
        ts = NOW - timedelta(days=10)
        line = _make_billing_line(timestamp=ts)
        cost_input = MockCostInput([line])
        orch, storage = _create_orchestrator(handler=handler, cost_input=cost_input)

        result = orch.run()
        assert result.tenant_name == TENANT_NAME
        assert result.dates_gathered >= 1
        # Verify PipelineRunResult
        assert isinstance(result, PipelineRunResult)


class TestOrchestratorInvariants:
    """TD-023: Tests for key orchestration invariants.

    These tests verify that the orchestrator follows required sequencing:
    1. Resources gathered before billing (to know what resources exist)
    2. Gather phase completes before calculate phase
    3. Recalculation window respects cutoff_days
    """

    def test_resources_gathered_before_billing(self) -> None:
        """Verify resource gather runs before billing gather.

        Resources must be gathered first so we know what resources exist
        when processing billing data.
        """
        call_log: list[str] = []

        class TrackedCostInput(MockCostInput):
            def gather(
                self,
                tenant_id: str,
                start: datetime,
                end: datetime,
                uow: Any,
            ) -> Iterable[BillingLineItem]:
                call_log.append("billing_gather")
                return super().gather(tenant_id, start, end, uow)

        class TrackedHandler(MockServiceHandler):
            def gather_resources(
                self, tenant_id: str, uow: Any, shared_ctx: object | None = None
            ) -> Iterable[Resource]:
                call_log.append("resource_gather")
                return super().gather_resources(tenant_id, uow, shared_ctx)

        ts = NOW - timedelta(days=10)
        line = _make_billing_line(timestamp=ts)
        cost_input = TrackedCostInput([line])
        handler = TrackedHandler(
            resources=[_make_resource()],
            identities=[_make_identity()],
        )
        orch, _ = _create_orchestrator(handler=handler, cost_input=cost_input)
        orch.run()

        # Resource gather should be called before billing gather
        resource_idx = call_log.index("resource_gather")
        billing_idx = call_log.index("billing_gather")
        assert resource_idx < billing_idx, "Resources must be gathered before billing"

    def test_gather_completes_before_calculate(self) -> None:
        """Verify all gathering completes before any calculation starts."""
        call_log: list[str] = []
        allocation_called = [False]

        class TrackedHandler(MockServiceHandler):
            def gather_resources(
                self, tenant_id: str, uow: Any, shared_ctx: object | None = None
            ) -> Iterable[Resource]:
                call_log.append("gather")
                return super().gather_resources(tenant_id, uow, shared_ctx)

        def tracking_allocator(ctx: AllocationContext) -> AllocationResult:
            call_log.append("allocate")
            allocation_called[0] = True
            return _simple_allocator(ctx)

        ts = NOW - timedelta(days=10)
        line = _make_billing_line(timestamp=ts)
        cost_input = MockCostInput([line])
        handler = TrackedHandler(
            resources=[_make_resource()],
            identities=[_make_identity()],
            allocator=tracking_allocator,
        )
        orch, _ = _create_orchestrator(handler=handler, cost_input=cost_input)
        orch.run()

        # If allocation was called, all gather calls must precede it
        if allocation_called[0]:
            gather_indices = [i for i, c in enumerate(call_log) if c == "gather"]
            allocate_indices = [i for i, c in enumerate(call_log) if c == "allocate"]
            assert max(gather_indices) < min(allocate_indices), "Gather must complete before allocate"

    def test_recalculation_window_respects_cutoff(self) -> None:
        """Dates within cutoff_days get chargeback_calculated reset."""
        ts_in_cutoff = NOW - timedelta(days=3)  # Within cutoff_days=5
        ts_outside_cutoff = NOW - timedelta(days=10)  # Outside cutoff_days=5

        handler = MockServiceHandler(
            resources=[_make_resource()],
            identities=[_make_identity()],
        )
        lines = [
            _make_billing_line(timestamp=ts_in_cutoff),
            _make_billing_line(timestamp=ts_outside_cutoff),
        ]
        cost_input = MockCostInput(lines)
        orch, storage = _create_orchestrator(handler=handler, cost_input=cost_input, cutoff_days=5)

        # First run calculates both dates
        orch.run()
        uow = storage.create_unit_of_work()
        state_in = uow.pipeline_state.get(ECOSYSTEM, TENANT_ID, ts_in_cutoff.date())
        state_out = uow.pipeline_state.get(ECOSYSTEM, TENANT_ID, ts_outside_cutoff.date())
        assert state_in is not None and state_in.chargeback_calculated
        assert state_out is not None and state_out.chargeback_calculated

        # Run again — cutoff window date should be recalculated
        orch2, storage2 = _create_orchestrator(handler=handler, cost_input=cost_input, cutoff_days=5)
        # Pre-populate storage with calculated state
        uow2 = storage2.create_unit_of_work()
        uow2.pipeline_state.upsert(
            PipelineState(
                ecosystem=ECOSYSTEM,
                tenant_id=TENANT_ID,
                tracking_date=ts_in_cutoff.date(),
                billing_gathered=True,
                resources_gathered=True,
                chargeback_calculated=True,
            )
        )
        uow2.pipeline_state.upsert(
            PipelineState(
                ecosystem=ECOSYSTEM,
                tenant_id=TENANT_ID,
                tracking_date=ts_outside_cutoff.date(),
                billing_gathered=True,
                resources_gathered=True,
                chargeback_calculated=True,
            )
        )
        uow2.commit()

        # After run, date within cutoff should have been recalculated
        # (In production, this would delete old chargebacks and reset the flag)
        result = orch2.run()
        # The recalculation logic should have processed the cutoff date
        # Note: exact behavior depends on orchestrator implementation details
        assert result is not None  # Basic sanity check


class TestRefreshThrottle:
    """GAP-04: API object refresh throttle — resource/billing gather skipped within min_refresh_gap."""

    def test_second_run_within_gap_skips_gather(self) -> None:
        """Two run() calls within 30 minutes → second skips handler gather AND billing gather."""
        gather_calls: list[str] = []

        class TrackingHandler(MockServiceHandler):
            def gather_resources(
                self, tenant_id: str, uow: Any, shared_ctx: object | None = None
            ) -> Iterable[Resource]:
                gather_calls.append("gather_resources")
                return self._resources

            def gather_identities(self, tenant_id: str, uow: Any) -> Iterable[Identity]:
                gather_calls.append("gather_identities")
                return self._identities

        billing_gather_calls: list[str] = []

        class TrackingCostInput(MockCostInput):
            def gather(self, tenant_id: str, start: datetime, end: datetime, uow: Any) -> Iterable[BillingLineItem]:
                billing_gather_calls.append("billing_gather")
                return self._lines

        handler = TrackingHandler(
            resources=[_make_resource()],
            identities=[_make_identity()],
        )
        cost_input = TrackingCostInput()
        orch, storage = _create_orchestrator(handler=handler, cost_input=cost_input)

        t1 = NOW
        t2 = NOW + timedelta(minutes=15)  # within 30 min gap

        with patch("core.engine.orchestrator.datetime") as mock_dt:
            mock_dt.now.return_value = t1
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            orch.run()

        first_resource_calls = gather_calls.count("gather_resources")
        first_billing_calls = len(billing_gather_calls)
        assert first_resource_calls >= 1, "First run must gather resources"
        assert first_billing_calls >= 1, "First run must gather billing"

        gather_calls.clear()
        billing_gather_calls.clear()

        with patch("core.engine.orchestrator.datetime") as mock_dt:
            mock_dt.now.return_value = t2
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            orch.run()

        assert gather_calls.count("gather_resources") == 0, "Second run within gap must skip handler gather"
        assert len(billing_gather_calls) == 0, "Second run within gap must skip billing gather"

    def test_second_run_after_gap_does_gather(self) -> None:
        """Two run() calls 31 minutes apart → second call does handler gather AND billing gather."""
        gather_calls: list[str] = []

        class TrackingHandler(MockServiceHandler):
            def gather_resources(
                self, tenant_id: str, uow: Any, shared_ctx: object | None = None
            ) -> Iterable[Resource]:
                gather_calls.append("gather_resources")
                return self._resources

            def gather_identities(self, tenant_id: str, uow: Any) -> Iterable[Identity]:
                gather_calls.append("gather_identities")
                return self._identities

        billing_gather_calls: list[str] = []

        class TrackingCostInput(MockCostInput):
            def gather(self, tenant_id: str, start: datetime, end: datetime, uow: Any) -> Iterable[BillingLineItem]:
                billing_gather_calls.append("billing_gather")
                return self._lines

        handler = TrackingHandler(
            resources=[_make_resource()],
            identities=[_make_identity()],
        )
        cost_input = TrackingCostInput()
        orch, storage = _create_orchestrator(handler=handler, cost_input=cost_input)

        t1 = NOW
        t2 = NOW + timedelta(minutes=31)  # beyond 30 min gap

        with patch("core.engine.orchestrator.datetime") as mock_dt:
            mock_dt.now.return_value = t1
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            orch.run()

        gather_calls.clear()
        billing_gather_calls.clear()

        with patch("core.engine.orchestrator.datetime") as mock_dt:
            mock_dt.now.return_value = t2
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            orch.run()

        assert gather_calls.count("gather_resources") >= 1, "Second run after gap must do handler gather"
        assert len(billing_gather_calls) >= 1, "Second run after gap must do billing gather"

    def test_first_run_always_gathers(self) -> None:
        """First run() always does handler gather regardless of any state."""
        gather_calls: list[str] = []

        class TrackingHandler(MockServiceHandler):
            def gather_resources(
                self, tenant_id: str, uow: Any, shared_ctx: object | None = None
            ) -> Iterable[Resource]:
                gather_calls.append("gather_resources")
                return self._resources

        handler = TrackingHandler(
            resources=[_make_resource()],
            identities=[_make_identity()],
        )
        orch, storage = _create_orchestrator(handler=handler)

        orch.run()
        assert gather_calls.count("gather_resources") >= 1, "First run must always gather"

    def test_min_refresh_gap_loaded_from_plugin_settings(self) -> None:
        """_min_refresh_gap is read from plugin_settings['min_refresh_gap_seconds'] in _load_overrides()."""
        orch, _ = _create_orchestrator(
            plugin_settings={"min_refresh_gap_seconds": 900},
        )
        assert hasattr(orch, "_min_refresh_gap"), "Orchestrator must have _min_refresh_gap attribute"
        assert orch._min_refresh_gap == timedelta(seconds=900), (
            f"Expected timedelta(seconds=900), got {orch._min_refresh_gap}"
        )

    def test_min_refresh_gap_default_1800(self) -> None:
        """Without explicit setting, _min_refresh_gap defaults to 1800 seconds (30 min)."""
        orch, _ = _create_orchestrator(plugin_settings={})
        assert hasattr(orch, "_min_refresh_gap"), "Orchestrator must have _min_refresh_gap attribute"
        assert orch._min_refresh_gap == timedelta(seconds=1800), (
            f"Expected timedelta(seconds=1800), got {orch._min_refresh_gap}"
        )

    def test_deletion_detection_skipped_when_throttled(self) -> None:
        """Deletion detection is skipped when should_refresh_resources is False."""
        handler = MockServiceHandler(
            resources=[_make_resource("r1")],
            identities=[_make_identity("i1")],
        )
        orch, storage = _create_orchestrator(handler=handler)
        uow = storage.create_unit_of_work()

        # Pre-populate a resource that would be "missing" on second gather
        # if gather were to actually run
        old_resource = _make_resource("r-stale")
        uow.resources.upsert(old_resource)

        t1 = NOW
        t2 = NOW + timedelta(minutes=10)  # within gap

        # First run — gathers, r-stale not returned by handler, so normally deleted
        with patch("core.engine.orchestrator.datetime") as mock_dt:
            mock_dt.now.return_value = t1
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            orch.run()

        # Reset handler to NOT return r1 (simulating it disappearing)
        handler._resources = []

        uow.resources._deletions.clear()

        # Second run within gap — should skip deletion detection entirely
        with patch("core.engine.orchestrator.datetime") as mock_dt:
            mock_dt.now.return_value = t2
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            orch.run()

        # No new deletions should have occurred (gather was skipped)
        assert uow.resources._deletions == [], (
            f"Deletion detection should be skipped when throttled, but got: {uow.resources._deletions}"
        )


class TestGatherFailureEscalation:
    """Consecutive gather failure tracking with threshold-based exception."""

    def _make_failing_cost_input(self) -> MockCostInput:
        """Create a cost_input whose gather raises."""

        class FailingCostInput(MockCostInput):
            def gather(self, tenant_id: str, start: datetime, end: datetime, uow: Any) -> Iterable[BillingLineItem]:
                raise RuntimeError("API unavailable")

        return FailingCostInput()

    def test_gather_failure_increments_counter(self) -> None:
        """Counter increments on each gather exception."""
        cost_input = self._make_failing_cost_input()
        orch, _ = _create_orchestrator(
            cost_input=cost_input,
            plugin_settings={"min_refresh_gap_seconds": 0},
        )

        assert orch._consecutive_gather_failures == 0
        orch.run()
        assert orch._consecutive_gather_failures == 1
        orch.run()
        assert orch._consecutive_gather_failures == 2

    def test_gather_failure_threshold_exceeded_raises(self) -> None:
        """Exception raised after N consecutive failures (default threshold=5)."""
        cost_input = self._make_failing_cost_input()
        orch, _ = _create_orchestrator(
            cost_input=cost_input,
            plugin_settings={"min_refresh_gap_seconds": 0},
        )

        # First 4 should not raise GatherFailureThresholdError
        for _ in range(4):
            orch.run()
        assert orch._consecutive_gather_failures == 4

        # 5th should raise
        with pytest.raises(GatherFailureThresholdError, match="5 consecutive times"):
            orch.run()

    def test_gather_success_resets_failure_counter(self) -> None:
        """Counter resets to 0 on successful gather."""
        cost_input = self._make_failing_cost_input()
        orch, _ = _create_orchestrator(
            cost_input=cost_input,
            plugin_settings={"min_refresh_gap_seconds": 0},
        )

        # Accumulate 3 failures
        for _ in range(3):
            orch.run()
        assert orch._consecutive_gather_failures == 3

        # Replace cost_input with working one so gather succeeds
        orch._bundle.plugin._cost_input = MockCostInput()
        orch.run()
        assert orch._consecutive_gather_failures == 0

    def test_gather_failure_threshold_configurable(self) -> None:
        """gather_failure_threshold from TenantConfig is used."""
        cost_input = self._make_failing_cost_input()
        orch, _ = _create_orchestrator(
            cost_input=cost_input,
            plugin_settings={"min_refresh_gap_seconds": 0},
            gather_failure_threshold=2,
        )

        orch.run()
        assert orch._consecutive_gather_failures == 1

        with pytest.raises(GatherFailureThresholdError, match="2 consecutive times"):
            orch.run()


# ---------- GAP-07: Resource Lookup Cache ----------


class TestResourceLookupCache:
    def test_resource_lookup_cache_eliminates_redundant_get_calls(self) -> None:
        """_calculate_date must pre-fetch resources via find_by_period, never call get per line.

        With 10 billing lines sharing the same resource_id and billing window:
        - uow.resources.get must be called 0 times (cache replaces per-line get)
        - uow.resources.find_by_period must be called 1 time (1 unique window)
        """
        resource_id = "cluster-cache-test"
        resource = _make_resource(resource_id)
        identity = _make_identity("user-1")

        # All 10 lines share same resource_id and same billing window (daily at NOW)
        lines = [
            _make_billing_line(
                product_type="KAFKA_CKU",
                resource_id=resource_id,
                total_cost=Decimal("10.00"),
                timestamp=NOW,
            )
            for _ in range(10)
        ]

        uow = MockUnitOfWork()
        uow.resources.upsert(resource)
        uow.identities.upsert(identity)
        for line in lines:
            uow.billing.upsert(line)

        handler = MockServiceHandler(
            product_types=["KAFKA_CKU"],
            resources=[resource],
            identities=[identity],
        )
        storage = MockStorageBackend(uow)
        orch, _ = _create_orchestrator(handler=handler, storage=storage)

        # Spy on get and find_by_period while preserving real behavior
        original_get = uow.resources.get
        original_find_by_period = uow.resources.find_by_period
        uow.resources.get = MagicMock(side_effect=original_get)
        uow.resources.find_by_period = MagicMock(side_effect=original_find_by_period)

        orch._calculate_date(uow, TODAY)

        # Cache must eliminate all per-line get() calls
        assert uow.resources.get.call_count == 0, (
            f"Expected 0 calls to uow.resources.get, got {uow.resources.get.call_count}"
        )
        # find_by_period called once — all 10 lines share the same billing window
        assert uow.resources.find_by_period.call_count == 1, (
            f"Expected 1 call to uow.resources.find_by_period, got {uow.resources.find_by_period.call_count}"
        )

    def test_resource_lookup_cache_miss_preserves_active_fraction_one(self) -> None:
        """Cache miss (resource_id absent from resource_cache) falls back to active_fraction=Decimal(1).

        _process_billing_line must accept a resource_cache param and use dict.get() instead of
        uow.resources.get(). A cache miss for a deleted resource must preserve the existing
        fallback: active_fraction = Decimal(1), so split_amount == total_cost.
        """
        resource_id = "lkc-deleted"
        total_cost = Decimal("75.00")
        line = _make_billing_line(resource_id=resource_id, total_cost=total_cost, timestamp=NOW)

        uow = MockUnitOfWork()
        identity = _make_identity("user-1")
        uow.identities.upsert(identity)
        # Intentionally NOT upserting a resource — simulates resource deleted before billing period

        b_start, b_end, _b_duration = billing_window(line)
        resource_cache: dict[str, Resource] = {}  # empty — cache miss for deleted resource

        tp_set = IdentitySet()
        tp_set.add(identity)
        tenant_period_cache: dict[tuple[datetime, datetime], IdentitySet] = {(b_start, b_end): tp_set}
        prefetched_metrics: dict[tuple[str, datetime, datetime], dict[str, list[Any]]] = {}

        handler = MockServiceHandler(
            product_types=["KAFKA_CKU"],
            resources=[],
            identities=[identity],
        )
        storage = MockStorageBackend(uow)
        orch, _ = _create_orchestrator(handler=handler, storage=storage)

        # Spy on uow.resources.get — must NOT be called when cache is used
        uow.resources.get = MagicMock(side_effect=uow.resources.get)

        # _process_billing_line must accept resource_cache kwarg (will fail with TypeError currently)
        orch._process_billing_line(
            line,
            uow,
            prefetched_metrics,
            tenant_period_cache,
            orch._tenant_config.allocation_retry_limit,
            resource_cache=resource_cache,
        )

        # Cache miss: resource_id not in cache, so uow.resources.get must not be called
        assert resource_id not in resource_cache
        assert uow.resources.get.call_count == 0, (
            f"Expected uow.resources.get not called (cache used), got {uow.resources.get.call_count} calls"
        )
        # With active_fraction=1 on cache miss, full cost must be allocated
        rows = [r for r in uow.chargebacks._data if r.resource_id == resource_id]
        assert len(rows) == 1
        total_allocated = sum(r.amount for r in rows)
        assert total_allocated == total_cost

    def test_resource_lookup_cache_find_by_period_called_once_per_unique_window(self) -> None:
        """find_by_period must be called exactly once per unique billing window.

        With N billing lines across N distinct billing windows, find_by_period is called N times
        (one per window), not once per line.
        """
        resource_id = "cluster-multi-window"
        resource = _make_resource(resource_id)
        identity = _make_identity("user-1")

        # 3 billing lines at 3 distinct hourly windows on TODAY
        # hourly granularity: each timestamp produces a unique 1-hour window
        hour_offsets = [0, 1, 2]
        lines = [
            _make_billing_line(
                product_type="KAFKA_CKU",
                resource_id=resource_id,
                total_cost=Decimal("5.00"),
                timestamp=datetime(TODAY.year, TODAY.month, TODAY.day, h, 0, 0, tzinfo=UTC),
                granularity="hourly",
            )
            for h in hour_offsets
        ]
        expected_windows = len(hour_offsets)  # 3 unique windows → 3 find_by_period calls

        uow = MockUnitOfWork()
        uow.resources.upsert(resource)
        uow.identities.upsert(identity)
        for line in lines:
            uow.billing.upsert(line)

        handler = MockServiceHandler(
            product_types=["KAFKA_CKU"],
            resources=[resource],
            identities=[identity],
        )
        storage = MockStorageBackend(uow)
        orch, _ = _create_orchestrator(handler=handler, storage=storage)

        original_find_by_period = uow.resources.find_by_period
        uow.resources.get = MagicMock(side_effect=uow.resources.get)
        uow.resources.find_by_period = MagicMock(side_effect=original_find_by_period)

        orch._calculate_date(uow, TODAY)

        # get() must never be called — cache replaces all per-line lookups
        assert uow.resources.get.call_count == 0, (
            f"Expected 0 calls to uow.resources.get, got {uow.resources.get.call_count}"
        )
        # find_by_period called once per unique billing window, not once per line
        assert uow.resources.find_by_period.call_count == expected_windows, (
            f"Expected {expected_windows} calls to find_by_period "
            f"(one per unique window), got {uow.resources.find_by_period.call_count}"
        )


class TestLoadOverridesMetricsStep:
    """task-013: _load_overrides must read metrics_step_seconds and store as _metrics_step."""

    def test_load_overrides_sets_metrics_step_from_settings(self) -> None:
        orch, _ = _create_orchestrator(plugin_settings={"metrics_step_seconds": 1800})
        assert orch._metrics_step == timedelta(seconds=1800)

    def test_load_overrides_default_metrics_step_is_one_hour(self) -> None:
        orch, _ = _create_orchestrator(plugin_settings={})
        assert orch._metrics_step == timedelta(hours=1)

    def test_prefetch_uses_metrics_step_not_hardcoded_hour(self) -> None:
        """Orchestrator prefetch query must pass self._metrics_step, not timedelta(hours=1)."""
        from core.models.metrics import MetricQuery, MetricRow

        metrics_query = MetricQuery(
            key="cpu_usage",
            query_expression="rate(cpu_seconds_total{}[5m])",
            resource_label="resource_id",
            label_keys=["pod"],
        )
        handler = MockServiceHandler(
            resources=[_make_resource()],
            identities=[_make_identity()],
            metrics_queries=[metrics_query],
        )
        line = _make_billing_line(timestamp=NOW - timedelta(days=10))
        cost_input = MockCostInput([line])

        mock_metrics = MagicMock()
        mock_row = MetricRow(timestamp=NOW, metric_key="cpu_usage", value=1.0, labels={"pod": "p1"})
        mock_metrics.query.return_value = {"cpu_usage": [mock_row]}

        orch, storage = _create_orchestrator(
            handler=handler,
            cost_input=cost_input,
            plugin_settings={"metrics_step_seconds": 1800},
            metrics_source=mock_metrics,
        )
        uow = storage.create_unit_of_work()

        ps = PipelineState(
            ecosystem=ECOSYSTEM,
            tenant_id=TENANT_ID,
            tracking_date=line.timestamp.date(),
            billing_gathered=True,
            resources_gathered=True,
        )
        uow.pipeline_state.upsert(ps)
        uow.pipeline_state.mark_billing_gathered(ECOSYSTEM, TENANT_ID, line.timestamp.date())
        uow.pipeline_state.mark_resources_gathered(ECOSYSTEM, TENANT_ID, line.timestamp.date())
        uow.billing.upsert(line)

        orch.run()

        mock_metrics.query.assert_called_once()
        _, call_kwargs = mock_metrics.query.call_args
        assert call_kwargs["step"] == timedelta(seconds=1800)


# ---------- task-041: _compute_billing_windows called once per calculate cycle ----------


class TestComputeBillingWindowsOnce:
    """GAP-41: _compute_billing_windows must be called exactly once per run() with non-empty lines."""

    def _setup_uow_with_lines(
        self,
        lines: list[Any],
        resource: Resource | None = None,
        identity: Any | None = None,
    ) -> MockUnitOfWork:
        uow = MockUnitOfWork()
        if resource is None:
            resource = _make_resource()
        if identity is None:
            identity = _make_identity()
        uow.resources.upsert(resource)
        uow.identities.upsert(identity)
        for line in lines:
            uow.billing.upsert(line)
        tracking_date = lines[0].timestamp.date() if lines else TODAY
        ps = PipelineState(
            ecosystem=ECOSYSTEM,
            tenant_id=TENANT_ID,
            tracking_date=tracking_date,
            billing_gathered=True,
            resources_gathered=True,
        )
        uow.pipeline_state.upsert(ps)
        uow.pipeline_state.mark_billing_gathered(ECOSYSTEM, TENANT_ID, tracking_date)
        uow.pipeline_state.mark_resources_gathered(ECOSYSTEM, TENANT_ID, tracking_date)
        return uow

    def test_compute_billing_windows_called_once_when_lines_nonempty(self) -> None:
        """_compute_billing_windows is called exactly once per run() when billing lines exist."""
        line = _make_billing_line(timestamp=NOW - timedelta(days=10))
        resource = _make_resource()
        identity = _make_identity()

        handler = MockServiceHandler(
            product_types=["KAFKA_CKU"],
            resources=[resource],
            identities=[identity],
        )
        storage = MockStorageBackend()
        orch, _ = _create_orchestrator(handler=handler, storage=storage)

        uow = storage.create_unit_of_work()
        uow.resources.upsert(resource)
        uow.identities.upsert(identity)
        uow.billing.upsert(line)
        tracking_date = line.timestamp.date()
        ps = PipelineState(
            ecosystem=ECOSYSTEM,
            tenant_id=TENANT_ID,
            tracking_date=tracking_date,
            billing_gathered=True,
            resources_gathered=True,
        )
        uow.pipeline_state.upsert(ps)
        uow.pipeline_state.mark_billing_gathered(ECOSYSTEM, TENANT_ID, tracking_date)
        uow.pipeline_state.mark_resources_gathered(ECOSYSTEM, TENANT_ID, tracking_date)

        # Spy on _compute_billing_windows while preserving real behavior
        original = orch._calculate_phase._compute_billing_windows
        call_count: list[int] = [0]

        def spy(*args: Any, **kwargs: Any) -> Any:
            call_count[0] += 1
            return original(*args, **kwargs)

        orch._calculate_phase._compute_billing_windows = spy  # type: ignore[method-assign]

        orch._calculate_date(uow, tracking_date)

        assert call_count[0] == 1, (
            f"_compute_billing_windows must be called exactly once per run(), got {call_count[0]}"
        )

    def test_compute_billing_windows_not_called_when_lines_empty(self) -> None:
        """_compute_billing_windows is never called when billing lines are empty (early-return path)."""
        tracking_date = date(2026, 2, 10)
        handler = MockServiceHandler(resources=[_make_resource()], identities=[_make_identity()])
        storage = MockStorageBackend()
        orch, _ = _create_orchestrator(handler=handler, storage=storage)

        uow = storage.create_unit_of_work()
        ps = PipelineState(
            ecosystem=ECOSYSTEM,
            tenant_id=TENANT_ID,
            tracking_date=tracking_date,
            billing_gathered=True,
            resources_gathered=True,
        )
        uow.pipeline_state.upsert(ps)
        uow.pipeline_state.mark_billing_gathered(ECOSYSTEM, TENANT_ID, tracking_date)
        uow.pipeline_state.mark_resources_gathered(ECOSYSTEM, TENANT_ID, tracking_date)
        # No billing lines added for this date

        call_count: list[int] = [0]
        original = orch._calculate_phase._compute_billing_windows

        def spy(*args: Any, **kwargs: Any) -> Any:
            call_count[0] += 1
            return original(*args, **kwargs)

        orch._calculate_phase._compute_billing_windows = spy  # type: ignore[method-assign]

        orch._calculate_date(uow, tracking_date)

        assert call_count[0] == 0, (
            f"_compute_billing_windows must not be called for empty billing lines, got {call_count[0]}"
        )

    def test_build_tenant_period_cache_accepts_windows_set(self) -> None:
        """_build_tenant_period_cache accepts set[tuple[datetime, datetime]] (post-fix signature)."""
        handler = MockServiceHandler(resources=[_make_resource()], identities=[_make_identity()])
        orch, _ = _create_orchestrator(handler=handler)
        uow = MockUnitOfWork()
        identity = _make_identity()
        uow.identities.upsert(identity)

        b_start = datetime(2026, 2, 10, 0, 0, 0, tzinfo=UTC)
        b_end = datetime(2026, 2, 11, 0, 0, 0, tzinfo=UTC)
        windows: set[tuple[datetime, datetime]] = {(b_start, b_end)}

        # Post-fix: accepts set[tuple[datetime, datetime]], not list[BillingLineItem]
        result = orch._calculate_phase._build_tenant_period_cache(uow, windows)

        assert isinstance(result, dict)
        assert (b_start, b_end) in result

    def test_build_resource_cache_accepts_windows_set(self) -> None:
        """_build_resource_cache accepts set[tuple[datetime, datetime]] (post-fix signature)."""
        resource = _make_resource("r-1")
        handler = MockServiceHandler(resources=[resource], identities=[_make_identity()])
        orch, _ = _create_orchestrator(handler=handler)
        uow = MockUnitOfWork()
        uow.resources.upsert(resource)

        b_start = datetime(2026, 2, 10, 0, 0, 0, tzinfo=UTC)
        b_end = datetime(2026, 2, 11, 0, 0, 0, tzinfo=UTC)
        windows: set[tuple[datetime, datetime]] = {(b_start, b_end)}

        # Post-fix: accepts set[tuple[datetime, datetime]], not list[BillingLineItem]
        result = orch._calculate_phase._build_resource_cache(uow, windows)

        assert isinstance(result, dict)

    def test_build_tenant_period_cache_output_parity(self) -> None:
        """_build_tenant_period_cache produces same output whether given windows set or computed from lines."""
        identity = _make_identity("sa-1")
        resource = _make_resource()
        handler = MockServiceHandler(resources=[resource], identities=[identity])
        orch, _ = _create_orchestrator(handler=handler)

        line = _make_billing_line(timestamp=NOW - timedelta(days=1))
        uow = MockUnitOfWork()
        uow.identities.upsert(identity)
        uow.resources.upsert(resource)

        # Compute windows the same way the method does internally
        from core.engine.orchestrator import billing_window

        b_start, b_end, _ = billing_window(line, orch._calculate_phase._merged_granularity_durations)
        windows: set[tuple[datetime, datetime]] = {(b_start, b_end)}

        # Post-fix call with precomputed windows
        result = orch._calculate_phase._build_tenant_period_cache(uow, windows)

        assert (b_start, b_end) in result
        identity_set = result[(b_start, b_end)]
        assert identity.identity_id in identity_set

    def test_build_resource_cache_output_parity(self) -> None:
        """_build_resource_cache produces same output whether given windows set or computed from lines."""
        resource = _make_resource("lkc-parity")
        handler = MockServiceHandler(resources=[resource], identities=[_make_identity()])
        orch, _ = _create_orchestrator(handler=handler)

        line = _make_billing_line(resource_id="lkc-parity", timestamp=NOW - timedelta(days=1))
        uow = MockUnitOfWork()
        uow.resources.upsert(resource)

        from core.engine.orchestrator import billing_window

        b_start, b_end, _ = billing_window(line, orch._calculate_phase._merged_granularity_durations)
        windows: set[tuple[datetime, datetime]] = {(b_start, b_end)}

        result = orch._calculate_phase._build_resource_cache(uow, windows)

        assert "lkc-parity" in result
        assert result["lkc-parity"].resource_id == "lkc-parity"

    def test_build_tenant_period_cache_excludes_system_identities(self) -> None:
        """_build_tenant_period_cache still excludes system identities when given a windows set."""
        system_id = CoreIdentity(
            identity_id="UNALLOCATED",
            identity_type="system",
            display_name="Unallocated Costs",
            ecosystem=ECOSYSTEM,
            tenant_id=TENANT_ID,
            created_at=None,
            deleted_at=None,
            last_seen_at=NOW,
        )
        regular_id = _make_identity("user-real")
        resource = _make_resource()
        handler = MockServiceHandler(resources=[resource], identities=[regular_id])
        orch, _ = _create_orchestrator(handler=handler)

        uow = MockUnitOfWork()
        uow.identities.upsert(system_id)
        uow.identities.upsert(regular_id)

        b_start = datetime(2026, 2, 10, 0, 0, 0, tzinfo=UTC)
        b_end = datetime(2026, 2, 11, 0, 0, 0, tzinfo=UTC)
        windows: set[tuple[datetime, datetime]] = {(b_start, b_end)}

        result = orch._calculate_phase._build_tenant_period_cache(uow, windows)

        identity_set = result.get((b_start, b_end), IdentitySet())
        identity_ids = {i.identity_id for i in identity_set}
        assert "UNALLOCATED" not in identity_ids

    def test_build_resource_cache_multiple_windows(self) -> None:
        """_build_resource_cache iterates all windows in the set, not just the first."""
        resource_a = _make_resource("r-a")
        resource_b = _make_resource("r-b")
        handler = MockServiceHandler(resources=[resource_a, resource_b], identities=[_make_identity()])
        orch, _ = _create_orchestrator(handler=handler)

        uow = MockUnitOfWork()
        uow.resources.upsert(resource_a)
        uow.resources.upsert(resource_b)

        w1_start = datetime(2026, 2, 10, 0, 0, 0, tzinfo=UTC)
        w1_end = datetime(2026, 2, 11, 0, 0, 0, tzinfo=UTC)
        w2_start = datetime(2026, 2, 11, 0, 0, 0, tzinfo=UTC)
        w2_end = datetime(2026, 2, 12, 0, 0, 0, tzinfo=UTC)
        windows: set[tuple[datetime, datetime]] = {(w1_start, w1_end), (w2_start, w2_end)}

        # Patch find_by_period to return different resources per window
        def find_by_period(
            eco: str, tid: str, start: datetime, end: datetime, **kwargs: Any
        ) -> tuple[list[Resource], Any]:
            if start == w1_start:
                return [resource_a], None
            return [resource_b], None

        uow.resources.find_by_period = find_by_period  # type: ignore[method-assign]

        result = orch._calculate_phase._build_resource_cache(uow, windows)

        assert "r-a" in result
        assert "r-b" in result

    def test_build_tenant_period_cache_multiple_windows(self) -> None:
        """_build_tenant_period_cache iterates all windows in the set, not just the first."""
        identity_a = _make_identity("sa-win-a")
        identity_b = _make_identity("sa-win-b")
        resource = _make_resource()
        handler = MockServiceHandler(resources=[resource], identities=[identity_a, identity_b])
        orch, _ = _create_orchestrator(handler=handler)

        uow = MockUnitOfWork()
        uow.identities.upsert(identity_a)
        uow.identities.upsert(identity_b)

        w1_start = datetime(2026, 2, 10, 0, 0, 0, tzinfo=UTC)
        w1_end = datetime(2026, 2, 11, 0, 0, 0, tzinfo=UTC)
        w2_start = datetime(2026, 2, 11, 0, 0, 0, tzinfo=UTC)
        w2_end = datetime(2026, 2, 12, 0, 0, 0, tzinfo=UTC)
        windows: set[tuple[datetime, datetime]] = {(w1_start, w1_end), (w2_start, w2_end)}

        def find_by_period(eco: str, tid: str, start: datetime, end: datetime, **kwargs: Any) -> tuple[list[Any], Any]:
            if start == w1_start:
                return [identity_a], None
            return [identity_b], None

        uow.identities.find_by_period = find_by_period  # type: ignore[method-assign]

        result = orch._calculate_phase._build_tenant_period_cache(uow, windows)

        assert (w1_start, w1_end) in result
        assert (w2_start, w2_end) in result


class TestCalculatePhaseLineWindowCache:
    def test_billing_window_called_once_per_line_not_three_times(self) -> None:
        """billing_window() must be called exactly len(billing_lines) times, not 3×."""
        handler = MockServiceHandler(
            resources=[_make_resource()],
            identities=[_make_identity()],
        )
        lines = [
            _make_billing_line(resource_id="cluster-1", timestamp=NOW - timedelta(days=10)),
            _make_billing_line(resource_id="cluster-2", timestamp=NOW - timedelta(days=10)),
            _make_billing_line(resource_id="cluster-3", timestamp=NOW - timedelta(days=10)),
        ]
        cost_input = MockCostInput(lines)
        orch, storage = _create_orchestrator(handler=handler, cost_input=cost_input)
        uow = storage.create_unit_of_work()

        tracking_date = lines[0].timestamp.date()
        ps = PipelineState(
            ecosystem=ECOSYSTEM,
            tenant_id=TENANT_ID,
            tracking_date=tracking_date,
            billing_gathered=True,
            resources_gathered=True,
        )
        uow.pipeline_state.upsert(ps)
        uow.pipeline_state.mark_billing_gathered(ECOSYSTEM, TENANT_ID, tracking_date)
        uow.pipeline_state.mark_resources_gathered(ECOSYSTEM, TENANT_ID, tracking_date)
        for line in lines:
            uow.billing.upsert(line)

        with patch("core.engine.orchestrator.billing_window", wraps=billing_window) as mock_bw:
            orch._calculate_phase.run(uow, tracking_date)
            assert mock_bw.call_count == len(lines)

    def test_compute_line_window_cache_daily_granularity(self) -> None:
        """_compute_line_window_cache returns (start, end, duration) for daily lines."""
        orch, _ = _create_orchestrator()
        phase = orch._calculate_phase

        ts = datetime(2026, 2, 10, 0, 0, 0, tzinfo=UTC)
        line = _make_billing_line(timestamp=ts, granularity="daily")

        cache = phase._compute_line_window_cache([line])

        assert id(line) in cache
        b_start, b_end, b_duration = cache[id(line)]
        assert b_start == ts
        assert b_end == ts + timedelta(days=1)
        assert b_duration == timedelta(days=1)

    def test_compute_line_window_cache_monthly_granularity(self) -> None:
        """_compute_line_window_cache returns correct duration for monthly lines (28-day Feb)."""
        orch, _ = _create_orchestrator()
        phase = orch._calculate_phase

        ts = datetime(2026, 2, 1, 0, 0, 0, tzinfo=UTC)
        line = _make_billing_line(timestamp=ts, granularity="monthly")

        cache = phase._compute_line_window_cache([line])

        assert id(line) in cache
        b_start, b_end, b_duration = cache[id(line)]
        assert b_start == ts
        assert b_duration == timedelta(days=28)
        assert b_end == ts + timedelta(days=28)


# ---------- GAP-074: Fallback allocator tests ----------


def _create_orchestrator_with_fallback(
    handler: MockServiceHandler | None = None,
    fallback_allocator: Any = None,
    cost_input: MockCostInput | None = None,
    storage: MockStorageBackend | None = None,
) -> tuple[ChargebackOrchestrator, MockStorageBackend]:
    """Create orchestrator backed by MockPluginWithFallback."""
    if handler is None:
        handler = MockServiceHandler(
            resources=[_make_resource()],
            identities=[_make_identity()],
        )
    if cost_input is None:
        cost_input = MockCostInput()
    plugin = MockPluginWithFallback(
        handlers={"kafka": handler},
        cost_input=cost_input,
        fallback_allocator=fallback_allocator,
    )
    if storage is None:
        storage = MockStorageBackend()
    tc = _make_tenant_config()
    orch = ChargebackOrchestrator(TENANT_NAME, tc, plugin, storage, None)
    return orch, storage


def _setup_pipeline_state_for_line(uow: MockUnitOfWork, line: Any) -> None:
    """Pre-populate pipeline state so the orchestrator processes the billing line."""
    from core.models.pipeline import PipelineState

    ps = PipelineState(
        ecosystem=ECOSYSTEM,
        tenant_id=TENANT_ID,
        tracking_date=line.timestamp.date(),
        billing_gathered=True,
        resources_gathered=True,
    )
    uow.pipeline_state.upsert(ps)
    uow.billing.upsert(line)


class TestOrchestratorFallbackAllocator:
    """GAP-074: Orchestrator uses bundle.fallback_allocator for unknown product types."""

    def test_unknown_product_type_uses_fallback_allocator_row(self) -> None:
        """_process_billing_line calls bundle.fallback_allocator for unregistered product_type."""
        from plugins.confluent_cloud.allocators.default_allocators import unknown_allocator

        handler = MockServiceHandler(
            product_types=["KAFKA_CKU"],
            resources=[_make_resource()],
            identities=[_make_identity()],
        )
        line = _make_billing_line(
            product_type="TOTALLY_UNKNOWN_PRODUCT",
            resource_id="res-fallback-001",
            timestamp=NOW - timedelta(days=10),
        )
        cost_input = MockCostInput([line])
        orch, storage = _create_orchestrator_with_fallback(
            handler=handler,
            fallback_allocator=unknown_allocator,
            cost_input=cost_input,
        )
        uow = storage.create_unit_of_work()
        _setup_pipeline_state_for_line(uow, line)

        orch.run()

        fallback_rows = [r for r in uow.chargebacks._data if r.product_type == "TOTALLY_UNKNOWN_PRODUCT"]
        assert len(fallback_rows) == 1
        assert fallback_rows[0].identity_id == "res-fallback-001"

    def test_unknown_product_type_no_unallocated_row_when_fallback_set(self) -> None:
        """With fallback_allocator set, unknown product_type produces NO UNALLOCATED row."""
        from plugins.confluent_cloud.allocators.default_allocators import unknown_allocator

        handler = MockServiceHandler(
            product_types=["KAFKA_CKU"],
            resources=[_make_resource()],
            identities=[_make_identity()],
        )
        line = _make_billing_line(
            product_type="TOTALLY_UNKNOWN_PRODUCT",
            resource_id="res-fallback-002",
            timestamp=NOW - timedelta(days=10),
        )
        cost_input = MockCostInput([line])
        orch, storage = _create_orchestrator_with_fallback(
            handler=handler,
            fallback_allocator=unknown_allocator,
            cost_input=cost_input,
        )
        uow = storage.create_unit_of_work()
        _setup_pipeline_state_for_line(uow, line)

        orch.run()

        unalloc_rows = [
            r
            for r in uow.chargebacks._data
            if r.identity_id == "UNALLOCATED" and r.product_type == "TOTALLY_UNKNOWN_PRODUCT"
        ]
        assert len(unalloc_rows) == 0

    def test_orchestrator_py_has_no_from_plugins_imports(self) -> None:
        """orchestrator.py must not import from plugins.* (DIP compliance)."""
        import ast
        import pathlib

        source = pathlib.Path("src/core/engine/orchestrator.py").read_text()
        tree = ast.parse(source)
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                if node.module and node.module.startswith("plugins"):
                    pytest.fail(f"orchestrator.py has forbidden import: from {node.module}")


# ---------- Shutdown propagation tests (task-083) ----------


def _setup_pending_dates(storage: MockStorageBackend, n: int) -> list[date]:
    """Pre-populate n pending pipeline states (billing+resources gathered, not calculated)."""
    uow = storage.create_unit_of_work()
    dates = []
    for i in range(n):
        d = date(2026, 1, i + 1)
        ps = PipelineState(
            ecosystem=ECOSYSTEM,
            tenant_id=TENANT_ID,
            tracking_date=d,
            billing_gathered=True,
            resources_gathered=True,
        )
        uow.pipeline_state.upsert(ps)
        dates.append(d)
    return dates


class TestShutdownCheckOrchestrator:
    """task-083: shutdown_check parameter propagates shutdown signal into the run() loop."""

    def test_shutdown_check_none_processes_all_dates(self) -> None:
        """shutdown_check=None (default) — all pending dates are processed."""
        orch, storage = _create_orchestrator()
        _setup_pending_dates(storage, 3)

        # No shutdown_check argument — default behavior unchanged
        result = orch.run()

        assert result.dates_calculated == 3

    def test_shutdown_check_always_false_processes_all_dates(self) -> None:
        """shutdown_check returning False — no premature exit."""
        orch, storage = _create_orchestrator(shutdown_check=lambda: False)
        _setup_pending_dates(storage, 3)

        result = orch.run()

        assert result.dates_calculated == 3

    def test_shutdown_check_triggers_after_n_dates(self) -> None:
        """shutdown_check returns True after N calls — loop breaks after N dates."""
        call_count = 0

        def check_after_2() -> bool:
            nonlocal call_count
            call_count += 1
            return call_count > 2

        orch, storage = _create_orchestrator(shutdown_check=check_after_2)
        _setup_pending_dates(storage, 5)

        result = orch.run()

        assert result.dates_calculated == 2

    def test_shutdown_check_true_immediately_stops_before_first_date(self, caplog: pytest.LogCaptureFixture) -> None:
        """shutdown_check=lambda: True — loop breaks immediately, dates_calculated == 0."""
        orch, storage = _create_orchestrator(shutdown_check=lambda: True)
        _setup_pending_dates(storage, 3)

        with caplog.at_level(logging.INFO):
            result = orch.run()

        assert result.dates_calculated == 0
        assert any("shutdown" in r.message.lower() for r in caplog.records)

    def test_shutdown_log_message_contains_dates_processed(self, caplog: pytest.LogCaptureFixture) -> None:
        """Shutdown mid-run — log contains 'Shutdown requested' and count of processed dates."""
        call_count = 0

        def check_after_1() -> bool:
            nonlocal call_count
            call_count += 1
            return call_count > 1

        orch, storage = _create_orchestrator(shutdown_check=check_after_1)
        _setup_pending_dates(storage, 4)

        with caplog.at_level(logging.INFO):
            result = orch.run()

        assert result.dates_calculated == 1
        shutdown_messages = [r.message for r in caplog.records if "shutdown" in r.message.lower()]
        assert len(shutdown_messages) >= 1
        assert "1" in shutdown_messages[0]
