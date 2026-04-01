from __future__ import annotations

from collections import defaultdict
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

ECOSYSTEM = "test-eco"
TENANT_ID = "tenant-1"


# ---------- helpers ----------


def _make_cb_row(dt: date) -> Any:
    from core.models.chargeback import ChargebackRow, CostType

    return ChargebackRow(
        ecosystem=ECOSYSTEM,
        tenant_id=TENANT_ID,
        timestamp=datetime(dt.year, dt.month, dt.day, 0, 0, 0, tzinfo=UTC),
        resource_id="cluster-1",
        product_category="kafka",
        product_type="KAFKA_CKU",
        identity_id="user-1",
        cost_type=CostType.USAGE,
        amount=Decimal("10.00"),
        allocation_method="even",
    )


class MockEmissionRepo:
    """In-memory emission repo; shared across UoW instances via MockStorageBackendForRunner."""

    def __init__(self) -> None:
        self._records: list[Any] = []
        self._emitted: set[date] = set()
        self._failed: set[date] = set()

    def upsert(self, record: Any) -> None:
        self._records.append(record)
        if record.status == "emitted":
            self._emitted.add(record.date)
        elif record.status == "failed":
            self._failed.add(record.date)

    def get_emitted_dates(self, ecosystem: str, tenant_id: str, emitter_name: str, pipeline: str) -> set[date]:
        return self._emitted.copy()

    def get_failed_dates(self, ecosystem: str, tenant_id: str, emitter_name: str, pipeline: str) -> set[date]:
        return self._failed.copy()


class MockChargebackRepoForRunner:
    """In-memory chargeback repo for EmitterRunner tests."""

    def __init__(self, dates: list[date]) -> None:
        self._dates = dates
        self._rows: dict[date, list[Any]] = defaultdict(list)
        for d in dates:
            self._rows[d].append(_make_cb_row(d))

    def get_distinct_dates(self, ecosystem: str, tenant_id: str) -> list[date]:
        return sorted(self._dates)

    def find_by_date(self, ecosystem: str, tenant_id: str, target_date: date) -> list[Any]:
        return self._rows.get(target_date, [])

    def find_aggregated_for_emit(
        self,
        ecosystem: str,
        tenant_id: str,
        start: date,
        end: date,
        granularity: str,
    ) -> list[Any]:
        # Return one aggregated row for the range start date
        return [_make_cb_row(start)]


class MockUnitOfWorkForRunner:
    def __init__(self, dates: list[date], emission_repo: MockEmissionRepo) -> None:
        self.chargebacks = MockChargebackRepoForRunner(dates)
        self.emissions = emission_repo
        self._committed = False

    def __enter__(self) -> MockUnitOfWorkForRunner:
        return self

    def __exit__(self, *args: Any) -> None:
        pass

    def commit(self) -> None:
        self._committed = True


class MockStorageBackendForRunner:
    """Returns fresh UoW each call, sharing the emission repo instance."""

    def __init__(self, dates: list[date], emission_repo: MockEmissionRepo | None = None) -> None:
        self._emission_repo = emission_repo or MockEmissionRepo()
        self._dates = dates

    def create_unit_of_work(self) -> MockUnitOfWorkForRunner:
        return MockUnitOfWorkForRunner(self._dates, self._emission_repo)


def _make_spec(
    name: str = "mock-emitter",
    aggregation: str | None = None,
    lookback_days: int | None = None,
) -> Any:
    from core.config.models import EmitterSpec

    return EmitterSpec(type=name, name=name, aggregation=aggregation, lookback_days=lookback_days)


def _make_runner(
    storage: Any,
    specs: list[Any],
    granularity: str = "daily",
) -> Any:
    """Build an EmitterRunner wired to chargeback mocks."""
    from core.emitters.runner import EmitterRunner
    from core.emitters.sources import ChargebackDateSource, ChargebackRowFetcher, RegistryEmitterBuilder

    return EmitterRunner(
        ecosystem=ECOSYSTEM,
        storage_backend=storage,
        emitter_specs=specs,
        date_source=ChargebackDateSource(storage),
        row_fetcher=ChargebackRowFetcher(storage),
        emitter_builder=RegistryEmitterBuilder(),
        pipeline="chargeback",
        chargeback_granularity=granularity,
    )


def _clear_registry() -> None:
    from core.emitters import registry

    registry._REGISTRY.clear()


# ---------- Case 2: Pending inference — all dates emitted ----------


class TestEmitterRunnerPendingInference:
    def setup_method(self) -> None:
        _clear_registry()

    def teardown_method(self) -> None:
        _clear_registry()

    def test_all_pending_dates_emitted(self) -> None:
        from core.emitters.registry import register

        dates = [date(2025, 1, 1), date(2025, 1, 2), date(2025, 1, 3)]
        call_log: list[date] = []

        def _fake_emitter(tenant_id: str, dt: date, rows: Any) -> None:
            call_log.append(dt)

        register("mock-emitter", lambda **_: _fake_emitter)

        emission_repo = MockEmissionRepo()
        storage = MockStorageBackendForRunner(dates, emission_repo)
        runner = _make_runner(storage, [_make_spec()])
        runner.run(TENANT_ID)

        assert sorted(call_log) == dates

    def test_emission_records_written_with_emitted_status(self) -> None:
        from core.emitters.registry import register

        dates = [date(2025, 2, 1), date(2025, 2, 2)]

        def _fake_emitter(tenant_id: str, dt: date, rows: Any) -> None:
            pass

        register("mock-emitter", lambda **_: _fake_emitter)

        emission_repo = MockEmissionRepo()
        storage = MockStorageBackendForRunner(dates, emission_repo)
        runner = _make_runner(storage, [_make_spec()])
        runner.run(TENANT_ID)

        assert len(emission_repo._records) == 2
        assert all(r.status == "emitted" for r in emission_repo._records)

    def test_three_dates_produce_three_emission_records(self) -> None:
        from core.emitters.registry import register

        dates = [date(2025, 3, 1), date(2025, 3, 2), date(2025, 3, 3)]

        register("mock-emitter", lambda **_: lambda *a, **kw: None)

        emission_repo = MockEmissionRepo()
        storage = MockStorageBackendForRunner(dates, emission_repo)
        runner = _make_runner(storage, [_make_spec()])
        runner.run(TENANT_ID)

        assert len(emission_repo._records) == 3


# ---------- Case 3: Already-emitted dates skipped ----------


class TestEmitterRunnerSkipsEmittedDates:
    def setup_method(self) -> None:
        _clear_registry()

    def teardown_method(self) -> None:
        _clear_registry()

    def test_emitted_date_not_re_emitted(self) -> None:
        from core.emitters.registry import register

        dates = [date(2025, 1, 1), date(2025, 1, 2), date(2025, 1, 3)]
        call_log: list[date] = []

        def _fake_emitter(tenant_id: str, dt: date, rows: Any) -> None:
            call_log.append(dt)

        register("mock-emitter", lambda **_: _fake_emitter)

        emission_repo = MockEmissionRepo()
        emission_repo._emitted.add(date(2025, 1, 1))  # pre-seed as already emitted

        storage = MockStorageBackendForRunner(dates, emission_repo)
        runner = _make_runner(storage, [_make_spec()])
        runner.run(TENANT_ID)

        assert date(2025, 1, 1) not in call_log
        assert len(call_log) == 2
        assert set(call_log) == {date(2025, 1, 2), date(2025, 1, 3)}

    def test_all_emitted_produces_no_calls(self) -> None:
        from core.emitters.registry import register

        dates = [date(2025, 1, 1), date(2025, 1, 2)]
        call_log: list[date] = []

        def _fake_emitter(tenant_id: str, dt: date, rows: Any) -> None:
            call_log.append(dt)

        register("mock-emitter", lambda **_: _fake_emitter)

        emission_repo = MockEmissionRepo()
        emission_repo._emitted.update(dates)  # all already emitted

        storage = MockStorageBackendForRunner(dates, emission_repo)
        runner = _make_runner(storage, [_make_spec()])
        runner.run(TENANT_ID)

        assert call_log == []


# ---------- Case 4: Failed emission recorded ----------


class TestEmitterRunnerFailedEmission:
    def setup_method(self) -> None:
        _clear_registry()

    def teardown_method(self) -> None:
        _clear_registry()

    def test_failed_emitter_records_status_failed(self) -> None:
        from core.emitters.registry import register

        def _raising_emitter(tenant_id: str, dt: date, rows: Any) -> None:
            raise RuntimeError("disk full")

        register("mock-emitter", lambda **_: _raising_emitter)

        dates = [date(2025, 1, 5)]
        emission_repo = MockEmissionRepo()
        storage = MockStorageBackendForRunner(dates, emission_repo)
        runner = _make_runner(storage, [_make_spec()])

        runner.run(TENANT_ID)

        failed = [r for r in emission_repo._records if r.status == "failed"]
        assert len(failed) == 1
        assert failed[0].date == date(2025, 1, 5)

    def test_failed_emission_does_not_propagate_exception(self) -> None:
        from core.emitters.registry import register

        def _raising_emitter(tenant_id: str, dt: date, rows: Any) -> None:
            raise ValueError("connection refused")

        register("mock-emitter", lambda **_: _raising_emitter)

        dates = [date(2025, 1, 5)]
        storage = MockStorageBackendForRunner(dates)
        runner = _make_runner(storage, [_make_spec()])

        # Must not raise
        runner.run(TENANT_ID)


# ---------- Case 5: Crash recovery / idempotent re-emit ----------


class TestEmitterRunnerCrashRecovery:
    def setup_method(self) -> None:
        _clear_registry()

    def teardown_method(self) -> None:
        _clear_registry()

    def test_date_re_emitted_if_persist_outcomes_skipped(self) -> None:
        from core.emitters.registry import register

        call_log: list[date] = []

        def _fake_emitter(tenant_id: str, dt: date, rows: Any) -> None:
            call_log.append(dt)

        register("mock-emitter", lambda **_: _fake_emitter)

        dates = [date(2025, 1, 15)]
        emission_repo = MockEmissionRepo()
        storage = MockStorageBackendForRunner(dates, emission_repo)
        runner = _make_runner(storage, [_make_spec()])

        # First run: patch _persist_outcomes to simulate crash before persisting
        with patch.object(runner, "_persist_outcomes"):
            runner.run(TENANT_ID)

        # No records written — date still pending
        assert len(emission_repo._records) == 0
        assert date(2025, 1, 15) in call_log

        # Second run (no crash): emitter called again because date still pending
        runner.run(TENANT_ID)

        assert call_log.count(date(2025, 1, 15)) == 2


# ---------- Case 6: lookback_days bounds ----------


class TestEmitterRunnerLookbackDays:
    def setup_method(self) -> None:
        _clear_registry()

    def teardown_method(self) -> None:
        _clear_registry()

    def test_only_dates_within_lookback_emitted(self) -> None:
        from core.emitters.registry import register

        call_log: list[date] = []

        def _fake_emitter(tenant_id: str, dt: date, rows: Any) -> None:
            call_log.append(dt)

        register("mock-emitter", lambda **_: _fake_emitter)

        today = date.today()
        recent_date = today - timedelta(days=10)
        old_date = today - timedelta(days=60)
        dates = [old_date, recent_date]

        storage = MockStorageBackendForRunner(dates)
        spec = _make_spec(lookback_days=30)
        runner = _make_runner(storage, [spec])
        runner.run(TENANT_ID)

        assert recent_date in call_log
        assert old_date not in call_log

    def test_lookback_none_emits_all_dates(self) -> None:
        from core.emitters.registry import register

        call_log: list[date] = []

        def _fake_emitter(tenant_id: str, dt: date, rows: Any) -> None:
            call_log.append(dt)

        register("mock-emitter", lambda **_: _fake_emitter)

        today = date.today()
        old_date = today - timedelta(days=60)
        recent_date = today - timedelta(days=5)
        dates = [old_date, recent_date]

        storage = MockStorageBackendForRunner(dates)
        spec = _make_spec(lookback_days=None)
        runner = _make_runner(storage, [spec])
        runner.run(TENANT_ID)

        assert set(call_log) == {old_date, recent_date}

    def test_boundary_date_included(self) -> None:
        from core.emitters.registry import register

        call_log: list[date] = []

        def _fake_emitter(tenant_id: str, dt: date, rows: Any) -> None:
            call_log.append(dt)

        register("mock-emitter", lambda **_: _fake_emitter)

        today = date.today()
        boundary_date = today - timedelta(days=29)  # within 30 days
        outside_date = today - timedelta(days=31)  # outside 30 days
        dates = [outside_date, boundary_date]

        storage = MockStorageBackendForRunner(dates)
        spec = _make_spec(lookback_days=30)
        runner = _make_runner(storage, [spec])
        runner.run(TENANT_ID)

        assert boundary_date in call_log
        assert outside_date not in call_log


# ---------- Case 8: Monthly aggregation — single emit call ----------


class TestEmitterRunnerMonthlyAggregation:
    def setup_method(self) -> None:
        _clear_registry()

    def teardown_method(self) -> None:
        _clear_registry()

    def test_monthly_emitter_called_once_with_month_start(self) -> None:
        from core.emitters.registry import register

        call_args: list[tuple[date, Any]] = []

        def _fake_emitter(tenant_id: str, dt: date, rows: Any) -> None:
            call_args.append((dt, rows))

        register("mock-emitter", lambda **_: _fake_emitter)

        dates = [date(2025, 1, 5), date(2025, 1, 10), date(2025, 1, 20)]
        emission_repo = MockEmissionRepo()
        storage = MockStorageBackendForRunner(dates, emission_repo)

        spec = _make_spec(aggregation="monthly")
        runner = _make_runner(storage, [spec])
        runner.run(TENANT_ID)

        # Emitter called exactly ONCE with the month start date (2025-01-01)
        assert len(call_args) == 1
        assert call_args[0][0] == date(2025, 1, 1)

    def test_monthly_aggregation_emission_records_for_each_chargeback_date(self) -> None:
        from core.emitters.registry import register

        def _fake_emitter(tenant_id: str, dt: date, rows: Any) -> None:
            pass

        register("mock-emitter", lambda **_: _fake_emitter)

        dates = [date(2025, 1, 5), date(2025, 1, 10), date(2025, 1, 20)]
        emission_repo = MockEmissionRepo()
        storage = MockStorageBackendForRunner(dates, emission_repo)

        spec = _make_spec(aggregation="monthly")
        runner = _make_runner(storage, [spec])
        runner.run(TENANT_ID)

        emitted_dates = {r.date for r in emission_repo._records if r.status == "emitted"}
        assert date(2025, 1, 5) in emitted_dates
        assert date(2025, 1, 10) in emitted_dates
        assert date(2025, 1, 20) in emitted_dates

    def test_monthly_aggregation_three_records_for_three_dates(self) -> None:
        from core.emitters.registry import register

        register("mock-emitter", lambda **_: lambda *a: None)

        dates = [date(2025, 1, 5), date(2025, 1, 10), date(2025, 1, 20)]
        emission_repo = MockEmissionRepo()
        storage = MockStorageBackendForRunner(dates, emission_repo)

        spec = _make_spec(aggregation="monthly")
        runner = _make_runner(storage, [spec])
        runner.run(TENANT_ID)

        assert len(emission_repo._records) == 3


# ---------- Case 10: Granularity validation at init ----------


class TestEmitterRunnerGranularityValidation:
    def test_finer_aggregation_than_chargeback_granularity_raises(self) -> None:
        from core.config.models import EmitterSpec
        from core.emitters.runner import EmitterRunner

        spec = EmitterSpec(type="csv", name="csv", aggregation="daily")

        with pytest.raises(ValueError):
            EmitterRunner(
                ecosystem=ECOSYSTEM,
                storage_backend=MagicMock(),
                emitter_specs=[spec],
                date_source=MagicMock(),
                row_fetcher=MagicMock(),
                emitter_builder=MagicMock(),
                pipeline="chargeback",
                chargeback_granularity="monthly",
            )

    def test_hourly_aggregation_finer_than_daily_raises(self) -> None:
        from core.config.models import EmitterSpec
        from core.emitters.runner import EmitterRunner

        spec = EmitterSpec(type="csv", name="csv", aggregation="hourly")

        with pytest.raises(ValueError):
            EmitterRunner(
                ecosystem=ECOSYSTEM,
                storage_backend=MagicMock(),
                emitter_specs=[spec],
                date_source=MagicMock(),
                row_fetcher=MagicMock(),
                emitter_builder=MagicMock(),
                pipeline="chargeback",
                chargeback_granularity="daily",
            )

    def test_same_granularity_does_not_raise(self) -> None:
        from core.config.models import EmitterSpec
        from core.emitters.runner import EmitterRunner

        spec = EmitterSpec(type="csv", name="csv", aggregation="daily")

        # Should not raise
        EmitterRunner(
            ECOSYSTEM,
            MagicMock(),
            [spec],
            MagicMock(),
            MagicMock(),
            MagicMock(),
            "chargeback",
            "daily",
        )

    def test_coarser_aggregation_does_not_raise(self) -> None:
        from core.config.models import EmitterSpec
        from core.emitters.runner import EmitterRunner

        spec = EmitterSpec(type="csv", name="csv", aggregation="monthly")

        # monthly > daily: valid
        EmitterRunner(
            ECOSYSTEM,
            MagicMock(),
            [spec],
            MagicMock(),
            MagicMock(),
            MagicMock(),
            "chargeback",
            "daily",
        )

    def test_none_aggregation_does_not_raise(self) -> None:
        from core.config.models import EmitterSpec
        from core.emitters.runner import EmitterRunner

        spec = EmitterSpec(type="csv", name="csv", aggregation=None)

        # None means no aggregation constraint
        EmitterRunner(
            ECOSYSTEM,
            MagicMock(),
            [spec],
            MagicMock(),
            MagicMock(),
            MagicMock(),
            "chargeback",
            "monthly",
        )


# ---------- Case 13: Tenant isolation ----------


class TestEmitterRunnerTenantIsolation:
    def setup_method(self) -> None:
        _clear_registry()

    def teardown_method(self) -> None:
        _clear_registry()

    def test_tenant_a_emitted_tenant_b_failed_independent(self) -> None:
        from core.config.models import EmitterSpec
        from core.emitters.registry import register

        tenant_a = "tenant-a"
        tenant_b = "tenant-b"

        def _tenant_a_emitter(tenant_id: str, dt: date, rows: Any) -> None:
            pass  # succeeds

        def _tenant_b_emitter(tenant_id: str, dt: date, rows: Any) -> None:
            raise RuntimeError("tenant-b emitter failed")

        register("emitter-a", lambda **_: _tenant_a_emitter)
        register("emitter-b", lambda **_: _tenant_b_emitter)

        dates = [date(2025, 1, 10)]
        repo_a = MockEmissionRepo()
        repo_b = MockEmissionRepo()
        storage_a = MockStorageBackendForRunner(dates, repo_a)
        storage_b = MockStorageBackendForRunner(dates, repo_b)

        spec_a = EmitterSpec(type="emitter-a", name="emitter-a")
        spec_b = EmitterSpec(type="emitter-b", name="emitter-b")

        runner_a = _make_runner(storage_a, [spec_a])
        runner_b = _make_runner(storage_b, [spec_b])

        runner_a.run(tenant_a)
        runner_b.run(tenant_b)

        a_emitted = [r for r in repo_a._records if r.status == "emitted"]
        assert len(a_emitted) >= 1

        b_failed = [r for r in repo_b._records if r.status == "failed"]
        assert len(b_failed) >= 1

    def test_tenant_b_failure_does_not_affect_tenant_a_records(self) -> None:
        from core.config.models import EmitterSpec
        from core.emitters.registry import register

        register("ok-emitter", lambda **_: lambda *a: None)
        register("bad-emitter", lambda **_: lambda *a: (_ for _ in ()).throw(RuntimeError("fail")))

        dates = [date(2025, 6, 1)]
        repo_a = MockEmissionRepo()
        storage_a = MockStorageBackendForRunner(dates, repo_a)
        spec_a = EmitterSpec(type="ok-emitter", name="ok-emitter")
        runner_a = _make_runner(storage_a, [spec_a])
        runner_a.run("tenant-x")

        # Tenant A should have emitted records
        assert len(repo_a._records) > 0
        assert all(r.status == "emitted" for r in repo_a._records)


# ---------- Case 1: Pipeline no longer calls emitters ----------


class TestPipelineNoLongerCallsEmitters:
    def setup_method(self) -> None:
        _clear_registry()

    def teardown_method(self) -> None:
        _clear_registry()

    def test_orchestrator_run_does_not_invoke_emitter(self) -> None:
        """After decoupling, orchestrator.run() must not call any emitter."""
        from collections.abc import Iterable
        from datetime import timedelta

        from core.config.models import TenantConfig
        from core.emitters.registry import register
        from core.engine.allocation import AllocationContext, AllocationResult
        from core.engine.orchestrator import ChargebackOrchestrator
        from core.models.billing import BillingLineItem, CoreBillingLineItem
        from core.models.chargeback import ChargebackRow, CostType
        from core.models.identity import CoreIdentity, Identity, IdentityResolution, IdentitySet
        from core.models.resource import CoreResource, Resource

        now = datetime(2025, 1, 15, 12, 0, 0, tzinfo=UTC)
        emitter_called: list[date] = []

        def _tracking_emitter(tenant_id: str, dt: date, rows: Any) -> None:
            emitter_called.append(dt)

        register("tracking-emitter", lambda **_: _tracking_emitter)

        class _Handler:
            @property
            def service_type(self) -> str:
                return "kafka"

            @property
            def handles_product_types(self) -> list[str]:
                return ["KAFKA_CKU"]

            def gather_resources(self, tenant_id: str, uow: Any, shared_ctx: Any = None) -> Iterable[Resource]:
                return [
                    CoreResource(
                        ecosystem=ECOSYSTEM,
                        tenant_id=TENANT_ID,
                        resource_id="cluster-1",
                        resource_type="kafka_cluster",
                        created_at=now - timedelta(days=30),
                    )
                ]

            def gather_identities(self, tenant_id: str, uow: Any) -> Iterable[Identity]:
                return [
                    CoreIdentity(
                        ecosystem=ECOSYSTEM,
                        tenant_id=TENANT_ID,
                        identity_id="user-1",
                        identity_type="user",
                        display_name="User 1",
                    )
                ]

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
                ra = IdentitySet()
                ra.add(
                    CoreIdentity(
                        ecosystem=ECOSYSTEM,
                        tenant_id=TENANT_ID,
                        identity_id="user-1",
                        identity_type="user",
                        display_name="User 1",
                    )
                )
                return IdentityResolution(
                    resource_active=ra,
                    metrics_derived=IdentitySet(),
                    tenant_period=IdentitySet(),
                )

            def get_metrics_for_product_type(self, product_type: str) -> list[Any]:
                return []

            def get_allocator(self, product_type: str) -> Any:
                def _alloc(ctx: AllocationContext) -> AllocationResult:
                    return AllocationResult(
                        rows=[
                            ChargebackRow(
                                ecosystem=ctx.billing_line.ecosystem,
                                tenant_id=ctx.billing_line.tenant_id,
                                timestamp=ctx.billing_line.timestamp,
                                resource_id=ctx.billing_line.resource_id,
                                product_category=ctx.billing_line.product_category,
                                product_type=ctx.billing_line.product_type,
                                identity_id="user-1",
                                cost_type=CostType.USAGE,
                                amount=ctx.split_amount,
                                allocation_method="even",
                            )
                        ]
                    )

                return _alloc

        class _CostInput:
            def gather(self, tenant_id: str, start: datetime, end: datetime, uow: Any) -> Iterable[BillingLineItem]:
                return [
                    CoreBillingLineItem(
                        ecosystem=ECOSYSTEM,
                        tenant_id=TENANT_ID,
                        timestamp=now - timedelta(days=5),
                        resource_id="cluster-1",
                        product_category="kafka",
                        product_type="KAFKA_CKU",
                        quantity=Decimal("1"),
                        unit_price=Decimal("100.00"),
                        total_cost=Decimal("100.00"),
                        granularity="daily",
                    )
                ]

        class _Plugin:
            @property
            def ecosystem(self) -> str:
                return ECOSYSTEM

            def initialize(self, config: dict[str, Any]) -> None:
                pass

            def get_service_handlers(self) -> dict[str, Any]:
                return {"kafka": _Handler()}

            def get_cost_input(self) -> Any:
                return _CostInput()

            def get_metrics_source(self) -> None:
                return None

            def get_fallback_allocator(self) -> None:
                return None

            def build_shared_context(self, tenant_id: str) -> None:
                return None

            def close(self) -> None:
                pass

        from core.storage.backends.sqlmodel.module import CoreStorageModule
        from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend

        storage = SQLModelBackend("sqlite:///:memory:", CoreStorageModule(), use_migrations=False)
        storage.create_tables()

        tc = TenantConfig(
            ecosystem=ECOSYSTEM,
            tenant_id=TENANT_ID,
            lookback_days=30,
            cutoff_days=5,
            plugin_settings={
                "emitters": [{"type": "tracking-emitter"}],
            },
        )
        orch = ChargebackOrchestrator("test-tenant", tc, _Plugin(), storage, None)
        orch.run()

        # After decoupling, emitter must NOT be called during orchestrator.run()
        assert emitter_called == [], (
            f"Emitter was called during orchestrator.run() for dates: {emitter_called}. "
            "EmitPhase must be removed from the pipeline loop."
        )


# ---------- helpers for GIT-001 through GIT-007 ----------


class _MockChargebackRepoControlled(MockChargebackRepoForRunner):
    """Allows overriding find_aggregated_for_emit return value."""

    def __init__(self, dates: list[date], aggregated_rows: list[Any] | None = None) -> None:
        super().__init__(dates)
        self._aggregated_rows = aggregated_rows

    def find_aggregated_for_emit(
        self,
        ecosystem: str,
        tenant_id: str,
        start: date,
        end: date,
        granularity: str,
    ) -> list[Any]:
        if self._aggregated_rows is not None:
            return self._aggregated_rows
        return super().find_aggregated_for_emit(ecosystem, tenant_id, start, end, granularity)


class _MockStorageControlled:
    """Storage that lets tests inject a custom chargeback repo."""

    def __init__(self, cb_repo: Any, emission_repo: MockEmissionRepo | None = None) -> None:
        self._cb_repo = cb_repo
        self._emission_repo = emission_repo or MockEmissionRepo()

    def create_unit_of_work(self) -> Any:
        cb_repo = self._cb_repo
        emission_repo = self._emission_repo

        class _UoW:
            def __init__(self) -> None:
                self.chargebacks = cb_repo
                self.emissions = emission_repo
                self._committed = False

            def __enter__(self) -> Any:
                return self

            def __exit__(self, *args: Any) -> None:
                pass

            def commit(self) -> None:
                self._committed = True

        return _UoW()


# ---------- GIT-001: ExpositionEmitter dispatch ----------


class TestExpositionEmitterDispatch:
    def setup_method(self) -> None:
        _clear_registry()

    def teardown_method(self) -> None:
        _clear_registry()

    def test_exposition_emitter_load_and_get_consumed_called(self) -> None:
        """GIT-001: ExpositionEmitter.load() and get_consumed() are called; EmissionRecord written."""
        from core.emitters.registry import register

        dates = [date(2025, 3, 1), date(2025, 3, 2)]
        load_calls: list[Any] = []
        consumed_dates = {date(2025, 3, 1)}  # only first date consumed

        class _FakeExposition:
            def load(self, tenant_id: str, manifest: Any, rows: Any) -> None:
                load_calls.append((tenant_id, manifest, rows))

            def get_consumed(self, tenant_id: str) -> set[date]:
                return consumed_dates

        exposition_instance = _FakeExposition()
        register("expo-emitter", lambda **_: exposition_instance)

        emission_repo = MockEmissionRepo()
        storage = MockStorageBackendForRunner(dates, emission_repo)
        runner = _make_runner(storage, [_make_spec(name="expo-emitter")])
        runner.run(TENANT_ID)

        assert len(load_calls) == 1
        _tenant, manifest, _rows = load_calls[0]
        assert _tenant == TENANT_ID
        assert set(manifest.pending_dates) == set(dates)

        # date(2025,3,1) consumed → EMITTED; date(2025,3,2) not → SKIPPED (not persisted)
        emitted_records = [r for r in emission_repo._records if r.status == "emitted"]
        assert any(r.date == date(2025, 3, 1) for r in emitted_records)


# ---------- GIT-002: LifecycleEmitter dispatch (non-monthly) ----------


class TestLifecycleEmitterDispatchNonMonthly:
    def setup_method(self) -> None:
        _clear_registry()

    def teardown_method(self) -> None:
        _clear_registry()

    def test_lifecycle_open_emit_close_called(self) -> None:
        """GIT-002: open→emit→close sequence invoked for non-monthly LifecycleEmitter."""
        from core.emitters.models import EmitOutcome, EmitResult
        from core.emitters.registry import register

        dates = [date(2025, 4, 1), date(2025, 4, 2)]
        open_calls: list[str] = []
        emit_calls: list[date] = []
        close_calls: list[str] = []

        class _FakeLifecycle:
            def open(self, tenant_id: str, manifest: Any) -> None:
                open_calls.append(tenant_id)

            def emit(self, tenant_id: str, dt: date, rows: Any) -> None:
                emit_calls.append(dt)

            def close(self, tenant_id: str) -> EmitResult:
                close_calls.append(tenant_id)
                return EmitResult(outcomes={d: EmitOutcome.EMITTED for d in dates})

        lifecycle_instance = _FakeLifecycle()
        register("lifecycle-emitter", lambda **_: lifecycle_instance)

        emission_repo = MockEmissionRepo()
        storage = MockStorageBackendForRunner(dates, emission_repo)
        runner = _make_runner(storage, [_make_spec(name="lifecycle-emitter")])
        runner.run(TENANT_ID)

        assert open_calls == [TENANT_ID]
        assert sorted(emit_calls) == sorted(dates)
        assert close_calls == [TENANT_ID]

        emitted = {r.date for r in emission_repo._records if r.status == "emitted"}
        assert emitted == set(dates)


# ---------- GIT-003: Monthly aggregation with no rows → SKIPPED ----------


class TestMonthlyNoRowsSkipped:
    def setup_method(self) -> None:
        _clear_registry()

    def teardown_method(self) -> None:
        _clear_registry()

    def test_monthly_no_rows_yields_skipped(self) -> None:
        """GIT-003: find_aggregated_for_emit returns empty → all dates in month get SKIPPED."""
        from core.emitters.registry import register

        dates = [date(2025, 5, 1), date(2025, 5, 15)]
        call_log: list[date] = []

        def _fake_emitter(tenant_id: str, dt: date, rows: Any) -> None:
            call_log.append(dt)

        register("monthly-emitter", lambda **_: _fake_emitter)

        cb_repo = _MockChargebackRepoControlled(dates, aggregated_rows=[])  # no rows
        emission_repo = MockEmissionRepo()
        storage = _MockStorageControlled(cb_repo, emission_repo)
        runner = _make_runner(storage, [_make_spec(name="monthly-emitter", aggregation="monthly")])
        runner.run(TENANT_ID)

        assert call_log == [], "Emitter should not be called when no rows"
        assert emission_repo._records == [], "No EmissionRecord for SKIPPED outcomes"


# ---------- GIT-004: Monthly ExpositionEmitter and LifecycleEmitter ----------


class TestMonthlySpecialEmitters:
    def setup_method(self) -> None:
        _clear_registry()

    def teardown_method(self) -> None:
        _clear_registry()

    def test_monthly_exposition_emitter_load_called_with_month_rows(self) -> None:
        """GIT-004a: monthly ExpositionEmitter receives load() with month rows."""
        from core.emitters.registry import register

        dates = [date(2025, 6, 5), date(2025, 6, 20)]
        month_start = date(2025, 6, 1)
        month_row = _make_cb_row(month_start)
        load_calls: list[Any] = []

        class _FakeMonthlyExposition:
            def load(self, tenant_id: str, manifest: Any, rows: Any) -> None:
                # Call the provider to exercise the _month_provider closure (runner.py:133)
                fetched = rows(tenant_id, month_start)
                load_calls.append((manifest, fetched))

            def get_consumed(self, tenant_id: str) -> set[date]:
                return {month_start}

        expo_instance = _FakeMonthlyExposition()
        register("monthly-expo", lambda **_: expo_instance)

        cb_repo = _MockChargebackRepoControlled(dates, aggregated_rows=[month_row])
        emission_repo = MockEmissionRepo()
        storage = _MockStorageControlled(cb_repo, emission_repo)
        runner = _make_runner(storage, [_make_spec(name="monthly-expo", aggregation="monthly")])
        runner.run(TENANT_ID)

        assert len(load_calls) == 1
        manifest, fetched_rows = load_calls[0]
        assert month_start in manifest.pending_dates
        assert fetched_rows == [month_row]

    def test_monthly_lifecycle_emitter_open_emit_close_sequence(self) -> None:
        """GIT-004b: monthly LifecycleEmitter receives open→emit(month_start, rows)→close."""
        from core.emitters.models import EmitOutcome, EmitResult
        from core.emitters.registry import register

        dates = [date(2025, 7, 10), date(2025, 7, 25)]
        month_start = date(2025, 7, 1)
        month_row = _make_cb_row(month_start)
        open_calls: list[str] = []
        emit_calls: list[tuple[str, date, Any]] = []
        close_calls: list[str] = []

        class _FakeMonthlyLifecycle:
            def open(self, tenant_id: str, manifest: Any) -> None:
                open_calls.append(tenant_id)

            def emit(self, tenant_id: str, dt: date, rows: Any) -> None:
                emit_calls.append((tenant_id, dt, rows))

            def close(self, tenant_id: str) -> EmitResult:
                close_calls.append(tenant_id)
                return EmitResult(outcomes={month_start: EmitOutcome.EMITTED})

        lifecycle_instance = _FakeMonthlyLifecycle()
        register("monthly-lifecycle", lambda **_: lifecycle_instance)

        cb_repo = _MockChargebackRepoControlled(dates, aggregated_rows=[month_row])
        emission_repo = MockEmissionRepo()
        storage = _MockStorageControlled(cb_repo, emission_repo)
        runner = _make_runner(storage, [_make_spec(name="monthly-lifecycle", aggregation="monthly")])
        runner.run(TENANT_ID)

        assert open_calls == [TENANT_ID]
        assert len(emit_calls) == 1
        _tid, emit_date, _rows = emit_calls[0]
        assert emit_date == month_start
        assert close_calls == [TENANT_ID]

    def test_monthly_lifecycle_emitter_failure(self) -> None:
        """GIT-009: monthly LifecycleEmitter raises → FAILED for all dates in month."""
        from core.emitters.registry import register

        dates = [date(2025, 11, 3), date(2025, 11, 17)]
        month_row = _make_cb_row(date(2025, 11, 1))

        class _BrokenLifecycle:
            def open(self, tenant_id: str, manifest: Any) -> None:
                raise RuntimeError("lifecycle open exploded")

            def emit(self, tenant_id: str, dt: date, rows: Any) -> None:  # pragma: no cover
                pass

            def close(self, tenant_id: str) -> Any:  # pragma: no cover
                pass

        lifecycle_instance = _BrokenLifecycle()
        register("monthly-lifecycle-fail", lambda **_: lifecycle_instance)

        cb_repo = _MockChargebackRepoControlled(dates, aggregated_rows=[month_row])
        emission_repo = MockEmissionRepo()
        storage = _MockStorageControlled(cb_repo, emission_repo)
        runner = _make_runner(storage, [_make_spec(name="monthly-lifecycle-fail", aggregation="monthly")])
        # Must not raise
        runner.run(TENANT_ID)

        failed = {r.date for r in emission_repo._records if r.status == "failed"}
        assert failed == set(dates), "All dates in failed month must be recorded as FAILED"


# ---------- GIT-005: Monthly plain-emitter failure ----------


class TestMonthlyPlainEmitterFailure:
    def setup_method(self) -> None:
        _clear_registry()

    def teardown_method(self) -> None:
        _clear_registry()

    def test_monthly_emitter_failure_records_failed_for_all_dates(self) -> None:
        """GIT-005: plain monthly emitter raising → FAILED for all dates in month."""
        from core.emitters.registry import register

        dates = [date(2025, 8, 5), date(2025, 8, 20)]
        month_row = _make_cb_row(date(2025, 8, 1))

        def _failing_emitter(tenant_id: str, dt: date, rows: Any) -> None:
            raise RuntimeError("monthly exploded")

        register("monthly-fail", lambda **_: _failing_emitter)

        cb_repo = _MockChargebackRepoControlled(dates, aggregated_rows=[month_row])
        emission_repo = MockEmissionRepo()
        storage = _MockStorageControlled(cb_repo, emission_repo)
        runner = _make_runner(storage, [_make_spec(name="monthly-fail", aggregation="monthly")])
        # Must not raise
        runner.run(TENANT_ID)

        failed = {r.date for r in emission_repo._records if r.status == "failed"}
        assert failed == set(dates), "All dates in failed month should be recorded as failed"


# ---------- GIT-006: _fetch_rows with aggregation=None uses find_by_date ----------


class TestFetchRowsNoAggregation:
    def setup_method(self) -> None:
        _clear_registry()

    def teardown_method(self) -> None:
        _clear_registry()

    def test_fetch_rows_no_aggregation_calls_find_by_date(self) -> None:
        """GIT-006: spec.aggregation=None → find_by_date, not find_aggregated_for_emit."""
        from core.emitters.registry import register

        dates = [date(2025, 9, 1)]
        find_by_date_calls: list[date] = []
        find_aggregated_calls: list[Any] = []

        class _TrackingCbRepo(MockChargebackRepoForRunner):
            def find_by_date(self, ecosystem: str, tenant_id: str, target_date: date) -> list[Any]:
                find_by_date_calls.append(target_date)
                return super().find_by_date(ecosystem, tenant_id, target_date)

            def find_aggregated_for_emit(self, *args: Any, **kwargs: Any) -> list[Any]:
                find_aggregated_calls.append(args)
                return super().find_aggregated_for_emit(*args, **kwargs)

        call_log: list[date] = []

        def _fake_emitter(tenant_id: str, dt: date, rows: Any) -> None:
            call_log.append(dt)

        register("plain-emitter", lambda **_: _fake_emitter)

        cb_repo = _TrackingCbRepo(dates)
        emission_repo = MockEmissionRepo()
        storage = _MockStorageControlled(cb_repo, emission_repo)
        runner = _make_runner(storage, [_make_spec(name="plain-emitter", aggregation=None)])
        runner.run(TENANT_ID)

        assert date(2025, 9, 1) in find_by_date_calls
        assert find_aggregated_calls == [], "find_aggregated_for_emit must not be called for aggregation=None"


# ---------- GIT-007: LifecycleDriver emit-failure path ----------


class TestLifecycleDriverEmitFailure:
    def test_emit_failure_close_still_called(self) -> None:
        """GIT-007: open() succeeds, emit() raises, close() still called and result returned."""
        from core.emitters.drivers import LifecycleDriver
        from core.emitters.models import EmitManifest, EmitOutcome, EmitResult

        dates = [date(2025, 10, 1), date(2025, 10, 2)]
        open_calls: list[str] = []
        emit_calls: list[date] = []
        close_calls: list[str] = []

        class _FaultyLifecycle:
            def open(self, tenant_id: str, manifest: Any) -> None:
                open_calls.append(tenant_id)

            def emit(self, tenant_id: str, dt: date, rows: Any) -> None:
                emit_calls.append(dt)
                raise RuntimeError("emit exploded")

            def close(self, tenant_id: str) -> EmitResult:
                close_calls.append(tenant_id)
                # Return outcomes for dates that were attempted (even if failed)
                return EmitResult(outcomes={d: EmitOutcome.FAILED for d in dates})

        manifest = EmitManifest(pending_dates=dates, total_rows_estimate=2, is_reemission=False)

        def _row_provider(tid: str, dt: date) -> list[Any]:
            return [_make_cb_row(dt)]

        driver = LifecycleDriver(_FaultyLifecycle())
        outcomes = driver.run(TENANT_ID, manifest, _row_provider)

        assert open_calls == [TENANT_ID], "open() must be called"
        assert len(emit_calls) == 2, "emit() must be attempted for each date"
        assert close_calls == [TENANT_ID], "close() must be called even after emit() failure"
        assert all(v == EmitOutcome.FAILED for v in outcomes.values())


# ---------- GIT-010: _fetch_rows with aggregation="daily" ----------


class TestFetchRowsDailyAggregation:
    def setup_method(self) -> None:
        _clear_registry()

    def teardown_method(self) -> None:
        _clear_registry()

    def test_fetch_rows_daily_aggregation_calls_find_aggregated(self) -> None:
        """GIT-010: spec.aggregation='daily' → find_aggregated_for_emit(eco, tid, dt, dt, 'daily')."""
        from core.emitters.registry import register

        target_date = date(2025, 12, 5)
        find_aggregated_calls: list[tuple[Any, ...]] = []

        class _TrackingCbRepo(MockChargebackRepoForRunner):
            def find_aggregated_for_emit(
                self,
                ecosystem: str,
                tenant_id: str,
                start: date,
                end: date,
                aggregation: str,
            ) -> list[Any]:
                find_aggregated_calls.append((ecosystem, tenant_id, start, end, aggregation))
                return super().find_aggregated_for_emit(ecosystem, tenant_id, start, end, aggregation)

        call_log: list[date] = []

        def _fake_emitter(tenant_id: str, dt: date, rows: Any) -> None:
            call_log.append(dt)

        register("daily-agg-emitter", lambda **_: _fake_emitter)

        cb_repo = _TrackingCbRepo([target_date])
        emission_repo = MockEmissionRepo()
        storage = _MockStorageControlled(cb_repo, emission_repo)
        runner = _make_runner(storage, [_make_spec(name="daily-agg-emitter", aggregation="daily")])
        runner.run(TENANT_ID)

        assert len(find_aggregated_calls) == 1, "find_aggregated_for_emit must be called once"
        eco, tid, start, end, agg = find_aggregated_calls[0]
        assert eco == ECOSYSTEM
        assert tid == TENANT_ID
        assert start == target_date
        assert end == target_date
        assert agg == "daily"
