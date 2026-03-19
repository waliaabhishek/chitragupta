from __future__ import annotations

from datetime import UTC, datetime, timedelta
from datetime import date as date_type
from decimal import Decimal
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock

from core.engine.allocation import AllocationContext, AllocationResult, AllocatorRegistry
from core.engine.orchestrator import CalculatePhase
from core.models.billing import BillingLineItem, CoreBillingLineItem
from core.models.chargeback import ChargebackRow, CostType
from core.models.identity import CoreIdentity, Identity, IdentityResolution, IdentitySet

if TYPE_CHECKING:
    from core.models.pipeline import PipelineState
    from core.models.resource import Resource

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

NOW = datetime(2026, 3, 1, 0, 0, 0, tzinfo=UTC)
TODAY = NOW.date()
ECOSYSTEM = "test-eco"
TENANT_ID = "tenant-1"


# ---------------------------------------------------------------------------
# Factories
# ---------------------------------------------------------------------------


def _make_billing_line(
    product_type: str = "KAFKA_CKU",
    resource_id: str = "cluster-1",
    total_cost: Decimal = Decimal("100.00"),
    timestamp: datetime | None = None,
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
        granularity="daily",
    )


def _make_chargeback_row(identity_id: str = "user-1", line: BillingLineItem | None = None) -> ChargebackRow:
    bl = line or _make_billing_line()
    return ChargebackRow(
        ecosystem=bl.ecosystem,
        tenant_id=bl.tenant_id,
        timestamp=bl.timestamp,
        resource_id=bl.resource_id,
        product_category=bl.product_category,
        product_type=bl.product_type,
        identity_id=identity_id,
        cost_type=CostType.USAGE,
        amount=bl.total_cost,
        allocation_method="test_allocator",
    )


def _make_identity(identity_id: str = "user-1") -> Identity:
    return CoreIdentity(
        ecosystem=ECOSYSTEM,
        tenant_id=TENANT_ID,
        identity_id=identity_id,
        identity_type="user",
        display_name=f"User {identity_id}",
    )


# ---------------------------------------------------------------------------
# Minimal mock infrastructure
# ---------------------------------------------------------------------------


class MockChargebackRepo:
    def __init__(self) -> None:
        self._data: list[ChargebackRow] = []
        self._upsert_batch_calls: list[list[ChargebackRow]] = []

    def upsert(self, row: ChargebackRow) -> ChargebackRow:
        self._data.append(row)
        return row

    def upsert_batch(self, rows: list[ChargebackRow]) -> int:
        self._upsert_batch_calls.append(list(rows))
        self._data.extend(rows)
        return len(rows)

    def find_by_date(self, ecosystem: str, tenant_id: str, target_date: date_type) -> list[ChargebackRow]:
        return [r for r in self._data if r.timestamp.date() == target_date]

    def find_by_range(self, *args: Any) -> list[ChargebackRow]:
        return self._data

    def find_by_identity(self, *args: Any) -> list[ChargebackRow]:
        return []

    def delete_by_date(self, ecosystem: str, tenant_id: str, target_date: date_type) -> int:
        before = len(self._data)
        self._data = [r for r in self._data if r.timestamp.date() != target_date]
        return before - len(self._data)

    def delete_before(self, *args: Any) -> int:
        return 0


class MockBillingRepo:
    def __init__(self, lines: list[BillingLineItem] | None = None) -> None:
        self._data: list[BillingLineItem] = lines or []
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


class MockResourceRepo:
    def find_active_at(self, *args: Any, **kwargs: Any) -> tuple[list[Resource], int]:
        return [], 0

    def find_by_period(self, *args: Any, **kwargs: Any) -> tuple[list[Resource], int]:
        return [], 0

    def upsert(self, resource: Resource) -> Resource:
        return resource

    def get(self, *args: Any) -> Resource | None:
        return None

    def mark_deleted(self, *args: Any) -> None:
        pass

    def find_by_type(self, *args: Any) -> list[Resource]:
        return []

    def delete_before(self, *args: Any) -> int:
        return 0


class MockIdentityRepo:
    def find_active_at(self, *args: Any, **kwargs: Any) -> tuple[list[Identity], int]:
        return [], 0

    def find_by_period(self, *args: Any, **kwargs: Any) -> tuple[list[Identity], int]:
        return [], 0

    def upsert(self, identity: Identity) -> Identity:
        return identity

    def get(self, *args: Any) -> Identity | None:
        return None

    def mark_deleted(self, *args: Any) -> None:
        pass

    def find_by_type(self, *args: Any) -> list[Identity]:
        return []

    def delete_before(self, *args: Any) -> int:
        return 0


class MockPipelineStateRepo:
    def __init__(self) -> None:
        self._data: dict[tuple[str, str, date_type], PipelineState] = {}

    def upsert(self, state: PipelineState) -> PipelineState:
        key = (state.ecosystem, state.tenant_id, state.tracking_date)
        self._data.setdefault(key, state)
        return self._data[key]

    def get(self, ecosystem: str, tenant_id: str, tracking_date: date_type) -> PipelineState | None:
        return self._data.get((ecosystem, tenant_id, tracking_date))

    def find_needing_calculation(self, ecosystem: str, tenant_id: str) -> list[PipelineState]:
        return []

    def find_by_range(self, *args: Any) -> list[PipelineState]:
        return list(self._data.values())

    def mark_billing_gathered(self, *args: Any) -> None:
        pass

    def mark_resources_gathered(self, *args: Any) -> None:
        pass

    def mark_needs_recalculation(self, *args: Any) -> None:
        pass

    def mark_chargeback_calculated(self, *args: Any) -> None:
        pass


class MockUnitOfWork:
    def __init__(self, billing: MockBillingRepo | None = None) -> None:
        self.resources = MockResourceRepo()
        self.identities = MockIdentityRepo()
        self.billing = billing or MockBillingRepo()
        self.chargebacks = MockChargebackRepo()
        self.pipeline_state = MockPipelineStateRepo()
        self.tags = MagicMock()

    def __enter__(self) -> MockUnitOfWork:
        return self

    def __exit__(self, *args: Any) -> None:
        pass

    def commit(self) -> None:
        pass

    def rollback(self) -> None:
        pass


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_calculate_phase(
    handlers: dict[str, Any] | None = None,
    retry_checker: Any | None = None,
) -> CalculatePhase:
    bundle = MagicMock()
    bundle.product_type_to_handler = handlers or {}
    bundle.fallback_allocator = None

    if retry_checker is None:
        retry_checker = MagicMock()
        retry_checker.increment_and_check.return_value = (1, False)

    return CalculatePhase(
        ecosystem=ECOSYSTEM,
        tenant_id=TENANT_ID,
        bundle=bundle,
        retry_checker=retry_checker,
        metrics_source=None,
        allocator_registry=AllocatorRegistry(),
        identity_overrides={},
        allocator_params={},
        metrics_step=timedelta(hours=1),
    )


# ---------------------------------------------------------------------------
# Tests: _collect_billing_line_rows
# ---------------------------------------------------------------------------


class TestCollectBillingLineRows:
    def test_collect_billing_line_rows_returns_list_no_db_writes(self) -> None:
        """_collect_billing_line_rows returns list[ChargebackRow] — no DB writes during call."""
        rows_to_return = [
            _make_chargeback_row("u1"),
            _make_chargeback_row("u2"),
            _make_chargeback_row("u3"),
        ]

        def multi_row_allocator(ctx: AllocationContext) -> AllocationResult:
            return AllocationResult(rows=rows_to_return)

        handler = MagicMock()
        handler.service_type = "kafka"
        handler.handles_product_types = ["KAFKA_CKU"]
        handler.get_allocator.return_value = multi_row_allocator
        handler.resolve_identities.return_value = IdentityResolution(
            resource_active=IdentitySet(),
            metrics_derived=IdentitySet(),
            tenant_period=IdentitySet(),
        )
        handler.get_metrics_for_product_type.return_value = []

        bundle = MagicMock()
        bundle.product_type_to_handler = {"KAFKA_CKU": handler}
        bundle.fallback_allocator = None

        retry_checker = MagicMock()
        retry_checker.increment_and_check.return_value = (1, False)

        phase = CalculatePhase(
            ecosystem=ECOSYSTEM,
            tenant_id=TENANT_ID,
            bundle=bundle,
            retry_checker=retry_checker,
            metrics_source=None,
            allocator_registry=AllocatorRegistry(),
            identity_overrides={},
            allocator_params={},
            metrics_step=timedelta(hours=1),
        )

        line = _make_billing_line()
        uow = MockUnitOfWork()
        line_window_cache = phase._compute_line_window_cache([line])
        tenant_period_cache = {(k[0], k[1]): IdentitySet() for k in line_window_cache.values()}

        result = phase._collect_billing_line_rows(
            line=line,
            uow=uow,
            prefetched_metrics={},
            failed_metric_keys=frozenset(),
            tenant_period_cache=tenant_period_cache,
            resource_cache={},
            line_window_cache=line_window_cache,
        )

        assert isinstance(result, list)
        assert len(result) == 3
        assert all(isinstance(r, ChargebackRow) for r in result)
        # No DB writes during collection
        assert uow.chargebacks._data == []

    def test_collect_billing_line_rows_no_handler_returns_empty(self) -> None:
        """_collect_billing_line_rows returns [] when no handler and no fallback."""
        phase = _make_calculate_phase(handlers={})

        line = _make_billing_line(product_type="UNKNOWN_TYPE")
        uow = MockUnitOfWork()
        line_window_cache = phase._compute_line_window_cache([line])
        b_start, b_end, _ = line_window_cache[id(line)]
        tenant_period_cache = {(b_start, b_end): IdentitySet()}

        result = phase._collect_billing_line_rows(
            line=line,
            uow=uow,
            prefetched_metrics={},
            failed_metric_keys=frozenset(),
            tenant_period_cache=tenant_period_cache,
            resource_cache={},
            line_window_cache=line_window_cache,
        )

        assert isinstance(result, list)
        assert result == []


# ---------------------------------------------------------------------------
# Tests: CalculatePhase.run() uses upsert_batch
# ---------------------------------------------------------------------------


class TestCalculatePhaseRunBatch:
    def test_run_calls_upsert_batch_once_per_date(self) -> None:
        """CalculatePhase.run() calls upsert_batch once with all rows for the date."""

        def single_row_allocator(ctx: AllocationContext) -> AllocationResult:
            row = ChargebackRow(
                ecosystem=ctx.billing_line.ecosystem,
                tenant_id=ctx.billing_line.tenant_id,
                timestamp=ctx.billing_line.timestamp,
                resource_id=ctx.billing_line.resource_id,
                product_category=ctx.billing_line.product_category,
                product_type=ctx.billing_line.product_type,
                identity_id="user-1",
                cost_type=CostType.USAGE,
                amount=ctx.split_amount,
                allocation_method="test_allocator",
            )
            return AllocationResult(rows=[row])

        handler = MagicMock()
        handler.service_type = "kafka"
        handler.handles_product_types = ["KAFKA_CKU"]
        handler.get_allocator.return_value = single_row_allocator
        handler.resolve_identities.return_value = IdentityResolution(
            resource_active=IdentitySet(),
            metrics_derived=IdentitySet(),
            tenant_period=IdentitySet(),
        )
        handler.get_metrics_for_product_type.return_value = []

        bundle = MagicMock()
        bundle.product_type_to_handler = {"KAFKA_CKU": handler}
        bundle.fallback_allocator = None

        retry_checker = MagicMock()
        retry_checker.increment_and_check.return_value = (1, False)

        phase = CalculatePhase(
            ecosystem=ECOSYSTEM,
            tenant_id=TENANT_ID,
            bundle=bundle,
            retry_checker=retry_checker,
            metrics_source=None,
            allocator_registry=AllocatorRegistry(),
            identity_overrides={},
            allocator_params={},
            metrics_step=timedelta(hours=1),
        )

        # 3 billing lines → 3 chargeback rows (1 per line)
        lines = [
            _make_billing_line(product_type="KAFKA_CKU", resource_id="c1"),
            _make_billing_line(product_type="KAFKA_CKU", resource_id="c2"),
            _make_billing_line(product_type="KAFKA_CKU", resource_id="c3"),
        ]
        billing_repo = MockBillingRepo(lines=lines)
        uow = MockUnitOfWork(billing=billing_repo)

        total = phase.run(uow, TODAY)

        assert total == 3
        # upsert_batch called exactly once
        assert len(uow.chargebacks._upsert_batch_calls) == 1
        batch = uow.chargebacks._upsert_batch_calls[0]
        assert len(batch) == 3

    def test_run_empty_billing_lines_returns_zero_no_batch(self) -> None:
        """run() with no billing lines returns 0 and never calls upsert_batch."""
        phase = _make_calculate_phase(handlers={})
        uow = MockUnitOfWork(billing=MockBillingRepo(lines=[]))

        total = phase.run(uow, TODAY)

        assert total == 0
        assert uow.chargebacks._upsert_batch_calls == []
