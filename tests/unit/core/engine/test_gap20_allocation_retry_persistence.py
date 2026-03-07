from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock

import pytest

from core.engine.orchestrator import ChargebackOrchestrator, billing_window
from core.models.billing import BillingLineItem, CoreBillingLineItem
from core.models.identity import CoreIdentity, Identity, IdentityResolution, IdentitySet
from core.models.resource import CoreResource, Resource

if TYPE_CHECKING:
    from core.engine.allocation import AllocationContext, AllocationResult
    from core.models.chargeback import ChargebackRow
    from core.models.pipeline import PipelineState

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

NOW = datetime(2026, 2, 22, 12, 0, 0, tzinfo=UTC)
ECOSYSTEM = "test-eco"
TENANT_ID = "tenant-1"
TENANT_NAME = "test-tenant"

# ---------------------------------------------------------------------------
# Helpers (replicated from test_orchestrator to keep tests self-contained)
# ---------------------------------------------------------------------------


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


def _make_resource(resource_id: str = "cluster-1") -> Resource:
    return CoreResource(
        ecosystem=ECOSYSTEM,
        tenant_id=TENANT_ID,
        resource_id=resource_id,
        resource_type="kafka_cluster",
        created_at=NOW - timedelta(days=30),
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
# Mock infrastructure
# ---------------------------------------------------------------------------


class MockBillingRepo:
    """In-memory billing repo with a shared attempt counter (simulates persistent DB)."""

    def __init__(self) -> None:
        self._data: list[BillingLineItem] = []
        self._attempts: dict[tuple[str, str, str], int] = {}

    def upsert(self, line: BillingLineItem) -> BillingLineItem:
        self._data.append(line)
        return line

    def find_by_date(self, *args: Any) -> list[BillingLineItem]:
        return self._data

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

    def find_by_date(self, *args: Any) -> list[ChargebackRow]:
        return self._data

    def find_by_range(self, *args: Any) -> list[ChargebackRow]:
        return self._data

    def find_by_identity(self, *args: Any) -> list[ChargebackRow]:
        return []

    def delete_by_date(self, *args: Any) -> int:
        return 0

    def delete_before(self, *args: Any) -> int:
        return 0


class MockResourceRepo:
    def __init__(self) -> None:
        self._data: dict[str, Resource] = {}
        self._deletions: list[tuple[str, datetime]] = []

    def upsert(self, resource: Resource) -> Resource:
        self._data[resource.resource_id] = resource
        return resource

    def get(self, *args: Any) -> Resource | None:
        return None

    def find_active_at(self, *args: Any) -> tuple[list[Resource], int]:
        items = list(self._data.values())
        return items, len(items)

    def find_by_period(self, *args: Any) -> tuple[list[Resource], int]:
        items = list(self._data.values())
        return items, len(items)

    def mark_deleted(self, *args: Any) -> None:
        pass

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

    def get(self, *args: Any) -> Identity | None:
        return None

    def find_active_at(self, *args: Any) -> tuple[list[Identity], int]:
        items = list(self._data.values())
        return items, len(items)

    def find_by_period(self, *args: Any) -> tuple[list[Identity], int]:
        items = list(self._data.values())
        return items, len(items)

    def mark_deleted(self, *args: Any) -> None:
        pass

    def find_by_type(self, *args: Any) -> list[Identity]:
        return []

    def delete_before(self, *args: Any) -> int:
        return 0


class MockPipelineStateRepo:
    def __init__(self) -> None:
        self._data: dict[tuple[str, str, Any], PipelineState] = {}

    def upsert(self, state: PipelineState) -> PipelineState:
        key = (state.ecosystem, state.tenant_id, state.tracking_date)
        if key not in self._data:
            self._data[key] = state
        return self._data[key]

    def get(self, *args: Any) -> PipelineState | None:
        return None

    def find_needing_calculation(self, *args: Any) -> list[PipelineState]:
        return []

    def find_by_range(self, *args: Any) -> list[PipelineState]:
        return []

    def mark_billing_gathered(self, *args: Any) -> None:
        pass

    def mark_resources_gathered(self, *args: Any) -> None:
        pass

    def mark_chargeback_calculated(self, *args: Any) -> None:
        pass

    def mark_needs_recalculation(self, *args: Any) -> None:
        pass


class MockUnitOfWork:
    """Lightweight in-memory UoW; billing repo can be shared for persistence simulation."""

    def __init__(self, shared_billing: MockBillingRepo | None = None) -> None:
        self.resources = MockResourceRepo()
        self.identities = MockIdentityRepo()
        self.billing = shared_billing if shared_billing is not None else MockBillingRepo()
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
        allocator: Any = None,
        resources: list[Resource] | None = None,
        identities: list[Identity] | None = None,
    ) -> None:
        self._allocator = allocator
        self._resources = resources or [_make_resource()]
        self._identities = identities or [_make_identity()]

    @property
    def service_type(self) -> str:
        return "kafka"

    @property
    def handles_product_types(self) -> list[str]:
        return ["KAFKA_CKU"]

    def gather_resources(self, tenant_id: str, uow: Any, shared_ctx: object | None = None) -> list[Resource]:
        return self._resources

    def gather_identities(self, tenant_id: str, uow: Any) -> list[Identity]:
        return self._identities

    def resolve_identities(
        self,
        tenant_id: str,
        resource_id: str,
        billing_timestamp: datetime,
        billing_duration: timedelta,
        metrics_data: Any,
        uow: Any,
    ) -> IdentityResolution:
        ra = IdentitySet()
        for i in self._identities:
            ra.add(i)
        return IdentityResolution(
            resource_active=ra,
            metrics_derived=IdentitySet(),
            tenant_period=IdentitySet(),
        )

    def get_metrics_for_product_type(self, product_type: str) -> list[Any]:
        return []

    def get_allocator(self, product_type: str) -> Any:
        return self._allocator


class MockPlugin:
    def __init__(self, handler: MockServiceHandler, cost_input: Any = None) -> None:
        self._handler = handler
        self._cost_input = cost_input or _MockCostInput()
        self._initialized = False

    @property
    def ecosystem(self) -> str:
        return ECOSYSTEM

    def initialize(self, config: dict[str, Any]) -> None:
        self._initialized = True

    def get_service_handlers(self) -> dict[str, MockServiceHandler]:
        return {"kafka": self._handler}

    def get_cost_input(self) -> Any:
        return self._cost_input

    def get_metrics_source(self) -> None:
        return None

    def build_shared_context(self, tenant_id: str) -> None:
        return None

    def close(self) -> None:
        pass


class _MockCostInput:
    def gather(self, *args: Any) -> list[BillingLineItem]:
        return []


# ---------------------------------------------------------------------------
# Orchestrator factory
# ---------------------------------------------------------------------------


def _create_orchestrator_with_failing_allocator(
    allocator: Any,
    allocation_retry_limit: int = 3,
) -> tuple[ChargebackOrchestrator, MockStorageBackend]:
    """Create an orchestrator whose KAFKA_CKU allocator always fails."""
    handler = MockServiceHandler(allocator=allocator)
    plugin = MockPlugin(handler=handler)
    storage = MockStorageBackend()
    tc = _make_tenant_config(
        plugin_settings={},
        allocation_retry_limit=allocation_retry_limit,
    )
    orch = ChargebackOrchestrator(TENANT_NAME, tc, plugin, storage, None)
    return orch, storage


def _tenant_period_cache_for(line: BillingLineItem) -> dict[tuple[datetime, datetime], IdentitySet]:
    """Build the minimal tenant_period_cache required by _process_billing_line."""
    b_start, b_end, _ = billing_window(line)
    return {(b_start, b_end): IdentitySet()}


# ---------------------------------------------------------------------------
# Test 1: Attempt counter persists across transaction rollback
# ---------------------------------------------------------------------------


class TestAttemptCounterPersistsAcrossRollback:
    def test_attempt_counter_written_to_separate_uow_not_main(self) -> None:
        """
        With the fix: the attempt increment must go into a SEPARATE UoW that
        commits independently. The main UoW (which rolls back on exception) must
        NOT be used for the counter increment.

        With buggy code: increment happens on main_uow; retry_uow._attempts stays
        empty → assertion fails → red state confirmed.
        """

        def failing_allocator(ctx: AllocationContext) -> AllocationResult:
            raise RuntimeError("transient failure")

        orch, storage = _create_orchestrator_with_failing_allocator(failing_allocator, allocation_retry_limit=3)
        main_uow = storage.create_unit_of_work()

        # Separate "committed" UoW — simulates a DB transaction that survives rollback.
        # The fix must use storage.create_unit_of_work() to obtain this for the counter.
        persistent_billing = MockBillingRepo()  # shared persistent state
        retry_uow = MockUnitOfWork(shared_billing=persistent_billing)
        storage.create_unit_of_work = MagicMock(return_value=retry_uow)

        line = _make_billing_line()
        cache = _tenant_period_cache_for(line)
        key = (line.resource_id, line.product_type, str(line.timestamp))

        # First call — should raise, counter should be persisted in retry_uow
        with pytest.raises(RuntimeError, match="transient failure"):
            orch._process_billing_line(line, main_uow, {}, cache, 3, {})

        # FIX behavior: attempt is recorded in the SEPARATE retry_uow
        assert retry_uow.billing._attempts.get(key, 0) == 1, (
            "Attempt counter must be persisted in the separate retry UoW, not lost with main UoW rollback"
        )
        # FIX behavior: main_uow must NOT have had increment_allocation_attempts called on it
        assert main_uow.billing._attempts.get(key, 0) == 0, (
            "Main UoW billing must not record the attempt counter (it would roll back on exception)"
        )

    def test_attempt_counter_accumulates_across_multiple_failures(self) -> None:
        """
        The second call to _process_billing_line must see attempt=2 (not 1 again),
        proving the counter survives between runs via the separate committed UoW.

        With buggy code: counter always goes to main_uow which never really persists
        separately, and retry_uow._attempts stays 0 for both calls → assertion fails.
        """

        def failing_allocator(ctx: AllocationContext) -> AllocationResult:
            raise RuntimeError("transient failure")

        orch, storage = _create_orchestrator_with_failing_allocator(failing_allocator, allocation_retry_limit=10)
        main_uow = storage.create_unit_of_work()

        # Shared persistent billing repo (simulates committed DB state)
        persistent_billing = MockBillingRepo()
        retry_uow = MockUnitOfWork(shared_billing=persistent_billing)
        storage.create_unit_of_work = MagicMock(return_value=retry_uow)

        line = _make_billing_line()
        cache = _tenant_period_cache_for(line)
        key = (line.resource_id, line.product_type, str(line.timestamp))

        # First failure
        with pytest.raises(RuntimeError):
            orch._process_billing_line(line, main_uow, {}, cache, 10, {})

        assert retry_uow.billing._attempts.get(key, 0) == 1

        # Second failure — counter must accumulate (not reset to 1)
        with pytest.raises(RuntimeError):
            orch._process_billing_line(line, main_uow, {}, cache, 10, {})

        assert retry_uow.billing._attempts.get(key, 0) == 2, (
            "Attempt counter must accumulate to 2 on second failure, not reset"
        )


# ---------------------------------------------------------------------------
# Test 2: Exhausted retries allocates to UNALLOCATED after persistence
# ---------------------------------------------------------------------------


class TestExhaustedRetriesAllocatesToUnallocated:
    def test_unallocated_row_written_after_retry_limit_reached(self) -> None:
        """
        After retry_limit exhaustion, _process_billing_line must:
        1. Persist the final attempt in the separate retry UoW
        2. Write a chargeback row to UNALLOCATED (not raise)

        With buggy code: attempt counter is in main_uow (not retry_uow), so
        new_attempts never accumulates correctly across calls → limit is never
        reached reliably, and retry_uow assertions fail → red state confirmed.
        """

        def failing_allocator(ctx: AllocationContext) -> AllocationResult:
            raise RuntimeError("persistent error")

        retry_limit = 3
        orch, storage = _create_orchestrator_with_failing_allocator(
            failing_allocator, allocation_retry_limit=retry_limit
        )
        main_uow = storage.create_unit_of_work()

        persistent_billing = MockBillingRepo()
        retry_uow = MockUnitOfWork(shared_billing=persistent_billing)
        storage.create_unit_of_work = MagicMock(return_value=retry_uow)

        line = _make_billing_line()
        cache = _tenant_period_cache_for(line)
        key = (line.resource_id, line.product_type, str(line.timestamp))

        # Run 1: attempt=1 < 3 → raises
        with pytest.raises(RuntimeError, match="persistent error"):
            orch._process_billing_line(line, main_uow, {}, cache, retry_limit, {})

        assert retry_uow.billing._attempts.get(key, 0) == 1, "Attempt 1 must be persisted"

        # Run 2: attempt=2 < 3 → raises
        with pytest.raises(RuntimeError, match="persistent error"):
            orch._process_billing_line(line, main_uow, {}, cache, retry_limit, {})

        assert retry_uow.billing._attempts.get(key, 0) == 2, "Attempt 2 must be persisted"

        # Run 3: attempt=3 >= 3 → must NOT raise; must write UNALLOCATED row
        rows_written = orch._process_billing_line(line, main_uow, {}, cache, retry_limit, {})

        assert retry_uow.billing._attempts.get(key, 0) == 3, "Attempt 3 must be persisted"
        assert rows_written == 1, "Must write exactly one UNALLOCATED chargeback row"

        unallocated_rows = [r for r in main_uow.chargebacks._data if r.allocation_method == "ALLOCATION_FAILED"]
        assert len(unallocated_rows) == 1, "Exactly one ALLOCATION_FAILED chargeback row must be written"
        assert unallocated_rows[0].identity_id == "UNALLOCATED"


# ---------------------------------------------------------------------------
# Test 3: Retry UoW failure does not mask the original allocator exception
# ---------------------------------------------------------------------------


class TestRetryUowFailureDoesNotMaskOriginalException:
    def test_original_allocator_exception_propagates_when_retry_uow_fails(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """
        If create_unit_of_work() raises while trying to persist the retry counter,
        the ORIGINAL allocator exception must still propagate — not the storage error.
        A warning must be logged about the retry counter persistence failure.

        With buggy code: create_unit_of_work() is never called in the except block,
        so the mock raising there has no effect. The warning is never logged.
        → assertion on warning message fails → red state confirmed.
        """

        original_error = RuntimeError("allocation logic failed")

        def failing_allocator(ctx: AllocationContext) -> AllocationResult:
            raise original_error

        orch, storage = _create_orchestrator_with_failing_allocator(failing_allocator, allocation_retry_limit=5)
        main_uow = storage.create_unit_of_work()

        # Simulate DB connection lost when trying to persist retry counter
        storage.create_unit_of_work = MagicMock(side_effect=RuntimeError("db connection lost"))

        line = _make_billing_line()
        cache = _tenant_period_cache_for(line)

        with caplog.at_level(logging.WARNING), pytest.raises(RuntimeError, match="allocation logic failed"):
            orch._process_billing_line(line, main_uow, {}, cache, 5, {})

        # The warning about retry counter persistence failure must have been logged
        warning_messages = [r.message for r in caplog.records if r.levelno >= logging.WARNING]
        assert any(
            "retry" in msg.lower() or "persist" in msg.lower() or "counter" in msg.lower() for msg in warning_messages
        ), f"Expected a warning about retry counter persistence failure, got: {warning_messages}"

    def test_retry_uow_exception_does_not_replace_original_exception_type(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """
        The exception that propagates must be the original allocator exception,
        not a storage exception from the retry UoW attempt.
        A warning must be logged about retry counter persistence failure.

        With buggy code: create_unit_of_work is never called in the except block,
        so the mock's side_effect never fires. No warning about retry persistence
        is logged → assertion on warning fails → red state confirmed.
        """

        class AllocatorError(Exception):
            """Distinct exception class to verify correct propagation."""

        def failing_allocator(ctx: AllocationContext) -> AllocationResult:
            raise AllocatorError("root cause")

        orch, storage = _create_orchestrator_with_failing_allocator(failing_allocator, allocation_retry_limit=5)
        main_uow = storage.create_unit_of_work()

        storage.create_unit_of_work = MagicMock(side_effect=RuntimeError("storage boom"))

        line = _make_billing_line()
        cache = _tenant_period_cache_for(line)

        with caplog.at_level(logging.WARNING), pytest.raises(AllocatorError, match="root cause"):
            orch._process_billing_line(line, main_uow, {}, cache, 5, {})

        # With the fix, create_unit_of_work is called for the retry counter and raises,
        # which triggers the warning. With buggy code, it's never called → no warning.
        warning_messages = [r.message for r in caplog.records if r.levelno >= logging.WARNING]
        assert any(
            "retry" in msg.lower() or "persist" in msg.lower() or "counter" in msg.lower() for msg in warning_messages
        ), f"Expected a warning about retry counter persistence failure, got: {warning_messages}"
