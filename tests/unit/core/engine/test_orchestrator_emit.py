from __future__ import annotations

from collections.abc import Iterable
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock, patch

from core.engine.allocation import AllocationContext, AllocationResult
from core.engine.orchestrator import ChargebackOrchestrator
from core.models.billing import BillingLineItem, CoreBillingLineItem
from core.models.chargeback import ChargebackRow, CostType
from core.models.identity import CoreIdentity, Identity, IdentityResolution, IdentitySet
from core.models.resource import CoreResource, Resource

# ---------- constants ----------

NOW = datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC)
TODAY = NOW.date()
ECOSYSTEM = "test-eco"
TENANT_ID = "tenant-1"
TENANT_NAME = "test-tenant"

# ---------- minimal helpers (mirror test_orchestrator.py style) ----------


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
    *,
    timestamp: datetime | None = None,
    total_cost: Decimal = Decimal("100.00"),
) -> BillingLineItem:
    return CoreBillingLineItem(
        ecosystem=ECOSYSTEM,
        tenant_id=TENANT_ID,
        timestamp=timestamp or NOW,
        resource_id="cluster-1",
        product_category="kafka",
        product_type="KAFKA_CKU",
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


if TYPE_CHECKING:
    from core.models.pipeline import PipelineState


def _simple_allocator(ctx: AllocationContext) -> AllocationResult:
    ids = list(ctx.identities.merged_active.ids())
    identity_id = ids[0] if ids else (ctx.billing_line.resource_id or "UNALLOCATED")
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
        resources: list[Resource] | None = None,
        identities: list[Identity] | None = None,
    ) -> None:
        self._resources = resources or []
        self._identities = identities or []

    @property
    def service_type(self) -> str:
        return "kafka"

    @property
    def handles_product_types(self) -> list[str]:
        return ["KAFKA_CKU"]

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
        metrics_data: dict[str, Any] | None,
        uow: Any,
        context: Any = None,
    ) -> IdentityResolution:
        ra = IdentitySet()
        for i in self._identities:
            ra.add(i)
        return IdentityResolution(resource_active=ra, metrics_derived=IdentitySet(), tenant_period=IdentitySet())

    def get_metrics_for_product_type(self, product_type: str) -> list[Any]:
        return []

    def get_allocator(self, product_type: str) -> Any:
        return _simple_allocator


class MockCostInput:
    def __init__(self, lines: list[BillingLineItem] | None = None) -> None:
        self._lines = lines or []

    def gather(self, tenant_id: str, start: datetime, end: datetime, uow: Any) -> Iterable[BillingLineItem]:
        return self._lines


class MockPlugin:
    def __init__(
        self,
        handler: MockServiceHandler | None = None,
        cost_input: MockCostInput | None = None,
    ) -> None:
        h = handler or MockServiceHandler(resources=[_make_resource()], identities=[_make_identity()])
        self._handlers = {"kafka": h}
        self._cost_input = cost_input or MockCostInput()

    @property
    def ecosystem(self) -> str:
        return ECOSYSTEM

    def initialize(self, config: dict[str, Any]) -> None:
        pass

    def get_service_handlers(self) -> dict[str, Any]:
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

    def aggregate_by_dimensions(self, *args: Any, **kwargs: Any) -> list[Any]:
        return []

    def find_dimensions(self, *args: Any, **kwargs: Any) -> tuple[list[Any], int]:
        return [], 0

    def find_tags_for_dimension(self, *args: Any) -> list[Any]:
        return []


class MockUnitOfWork:
    def __init__(self) -> None:

        self.resources = _MockResourceRepo()
        self.identities = _MockIdentityRepo()
        self.billing = _MockBillingRepo()
        self.chargebacks = MockChargebackRepo()
        self.pipeline_state = _MockPipelineStateRepo()
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


class _MockResourceRepo:
    def __init__(self) -> None:
        self._data: dict[str, Resource] = {}
        self._deletions: list[tuple[str, datetime]] = []

    def upsert(self, r: Resource) -> Resource:
        self._data[r.resource_id] = r
        return r

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

    def find_by_type(self, *args: Any) -> list[Resource]:
        return []

    def delete_before(self, *args: Any) -> int:
        return 0


class _MockIdentityRepo:
    def __init__(self) -> None:
        self._data: dict[str, Identity] = {}
        self._deletions: list[tuple[str, datetime]] = []

    def upsert(self, i: Identity) -> Identity:
        self._data[i.identity_id] = i
        return i

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

    def find_by_type(self, *args: Any) -> list[Identity]:
        return []

    def delete_before(self, *args: Any) -> int:
        return 0


class _MockBillingRepo:
    def __init__(self) -> None:
        self._data: list[BillingLineItem] = []
        self._attempts: dict[tuple[str, str, str], int] = {}

    def upsert(self, line: BillingLineItem) -> BillingLineItem:
        self._data.append(line)
        return line

    def find_by_date(self, ecosystem: str, tenant_id: str, target_date: date) -> list[BillingLineItem]:
        return [bl for bl in self._data if bl.timestamp.date() == target_date]

    def find_by_range(self, *args: Any) -> list[BillingLineItem]:
        return self._data

    def increment_allocation_attempts(
        self, ecosystem: str, tenant_id: str, timestamp: datetime, resource_id: str, product_type: str
    ) -> int:
        key = (resource_id, product_type, str(timestamp))
        self._attempts[key] = self._attempts.get(key, 0) + 1
        return self._attempts[key]

    def delete_before(self, *args: Any) -> int:
        return 0


class _MockPipelineStateRepo:
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


# ---------- helpers ----------


def _create_orchestrator_with_emitters(
    plugin_settings_dict: dict[str, Any] | None = None,
    billing_lines: list[BillingLineItem] | None = None,
) -> tuple[ChargebackOrchestrator, MockStorageBackend]:
    cost_input = MockCostInput(lines=billing_lines or [])
    plugin = MockPlugin(cost_input=cost_input)
    storage = MockStorageBackend()
    tc = _make_tenant_config(plugin_settings=plugin_settings_dict or {})
    orch = ChargebackOrchestrator(TENANT_NAME, tc, plugin, storage, None)
    return orch, storage


# ---------- tests ----------


class TestOrchestratorEmitPhaseWired:
    def setup_method(self) -> None:
        from core.emitters import registry

        registry._REGISTRY.clear()

    def teardown_method(self) -> None:
        from core.emitters import registry

        registry._REGISTRY.clear()

    def _register_csv(self) -> None:
        from core.emitters.registry import register
        from emitters.csv_emitter import make_csv_emitter

        register("csv", make_csv_emitter)

    def test_emit_phase_run_called_after_each_calculate(self, tmp_path: object) -> None:
        """EmitPhase.run() is called once per calculated date."""
        import tempfile

        self._register_csv()

        with tempfile.TemporaryDirectory() as output_dir:
            settings = {"emitters": [{"type": "csv", "aggregation": "daily", "params": {"output_dir": output_dir}}]}
            billing_ts = NOW - timedelta(days=10)
            lines = [_make_billing_line(timestamp=billing_ts)]
            orch, storage = _create_orchestrator_with_emitters(
                plugin_settings_dict=settings,
                billing_lines=lines,
            )

            # Patch EmitPhase.run to track calls
            from core.engine import orchestrator as orch_module

            original_run = orch_module.EmitPhase.run
            call_dates: list[Any] = []

            def tracking_run(self: Any, tracking_date: Any) -> Any:
                call_dates.append(tracking_date)
                return original_run(self, tracking_date)

            with patch.object(orch_module.EmitPhase, "run", tracking_run):
                orch.run()

            # At least one date should have triggered EmitPhase.run
            assert len(call_dates) >= 1

    def test_emit_failure_does_not_prevent_subsequent_date_processing(self) -> None:
        """Emit errors are captured in result.errors but pipeline continues."""
        self._register_csv()

        import tempfile

        with tempfile.TemporaryDirectory() as output_dir:
            settings = {"emitters": [{"type": "csv", "aggregation": "daily", "params": {"output_dir": output_dir}}]}
            billing_ts = NOW - timedelta(days=10)
            lines = [_make_billing_line(timestamp=billing_ts)]
            orch, storage = _create_orchestrator_with_emitters(
                plugin_settings_dict=settings,
                billing_lines=lines,
            )

            from core.engine import orchestrator as orch_module
            from core.engine.orchestrator import EmitResult

            # Make EmitPhase.run always return an error
            def failing_run(self: Any, tracking_date: Any) -> EmitResult:
                return EmitResult(dates_attempted=1, errors=["emitter exploded"])

            with patch.object(orch_module.EmitPhase, "run", failing_run):
                result = orch.run()

            # Errors collected but pipeline did not crash
            assert any("emitter" in e.lower() for e in result.errors)

    def test_orchestrator_with_csv_emitter_no_error_on_clean_run(self) -> None:
        """No errors when emitter succeeds."""
        self._register_csv()

        import tempfile

        with tempfile.TemporaryDirectory() as output_dir:
            settings = {"emitters": [{"type": "csv", "aggregation": "daily", "params": {"output_dir": output_dir}}]}
            billing_ts = NOW - timedelta(days=10)
            lines = [_make_billing_line(timestamp=billing_ts)]
            orch, storage = _create_orchestrator_with_emitters(
                plugin_settings_dict=settings,
                billing_lines=lines,
            )
            result = orch.run()
            # Emit errors should be empty for a clean run
            emit_errors = [e for e in result.errors if "emitter" in e.lower()]
            assert emit_errors == []
