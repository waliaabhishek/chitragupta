from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock

ECOSYSTEM = "test-eco"
TENANT_ID = "tenant-1"


def _make_row(dt: date | None = None) -> Any:
    from core.models.chargeback import ChargebackRow, CostType

    ts = datetime(2025, 1, 15, 0, 0, 0, tzinfo=UTC) if dt is None else datetime(dt.year, dt.month, dt.day, tzinfo=UTC)
    return ChargebackRow(
        ecosystem=ECOSYSTEM,
        tenant_id=TENANT_ID,
        timestamp=ts,
        resource_id="cluster-1",
        product_category="kafka",
        product_type="KAFKA_CKU",
        identity_id="user-1",
        cost_type=CostType.USAGE,
        amount=Decimal("10.00"),
        allocation_method="even",
    )


class TestPerDateDriver:
    def test_emitter_called_once_per_date_with_rows(self) -> None:
        from core.emitters.drivers import PerDateDriver
        from core.emitters.models import EmitManifest, EmitOutcome

        dates = [date(2025, 1, 1), date(2025, 1, 2)]
        emitter = MagicMock()
        driver = PerDateDriver(emitter)
        manifest = EmitManifest(pending_dates=dates, total_rows_estimate=2, is_reemission=False)

        outcomes = driver.run(TENANT_ID, manifest, MagicMock(return_value=[_make_row()]))

        assert emitter.call_count == 2
        assert outcomes[date(2025, 1, 1)] == EmitOutcome.EMITTED
        assert outcomes[date(2025, 1, 2)] == EmitOutcome.EMITTED

    def test_empty_rows_yields_skipped(self) -> None:
        from core.emitters.drivers import PerDateDriver
        from core.emitters.models import EmitManifest, EmitOutcome

        emitter = MagicMock()
        driver = PerDateDriver(emitter)
        manifest = EmitManifest(pending_dates=[date(2025, 1, 1)], total_rows_estimate=0, is_reemission=False)

        outcomes = driver.run(TENANT_ID, manifest, MagicMock(return_value=[]))

        emitter.assert_not_called()
        assert outcomes[date(2025, 1, 1)] == EmitOutcome.SKIPPED

    def test_emitter_raises_records_failed_no_reraise(self) -> None:
        from core.emitters.drivers import PerDateDriver
        from core.emitters.models import EmitManifest, EmitOutcome

        emitter = MagicMock(side_effect=RuntimeError("network error"))
        driver = PerDateDriver(emitter)
        manifest = EmitManifest(pending_dates=[date(2025, 1, 1)], total_rows_estimate=1, is_reemission=False)

        outcomes = driver.run(TENANT_ID, manifest, MagicMock(return_value=[_make_row()]))

        assert outcomes[date(2025, 1, 1)] == EmitOutcome.FAILED

    def test_mixed_success_and_failure(self) -> None:
        from core.emitters.drivers import PerDateDriver
        from core.emitters.models import EmitManifest, EmitOutcome

        call_count = 0

        def _emitter(tenant_id: str, dt: date, rows: Any) -> None:
            nonlocal call_count
            call_count += 1
            if dt == date(2025, 1, 2):
                raise RuntimeError("fail")

        driver = PerDateDriver(_emitter)
        dates = [date(2025, 1, 1), date(2025, 1, 2)]
        manifest = EmitManifest(pending_dates=dates, total_rows_estimate=2, is_reemission=False)

        outcomes = driver.run(TENANT_ID, manifest, MagicMock(return_value=[_make_row()]))

        assert outcomes[date(2025, 1, 1)] == EmitOutcome.EMITTED
        assert outcomes[date(2025, 1, 2)] == EmitOutcome.FAILED


class TestLifecycleDriver:
    def _make_lifecycle_emitter(self, outcomes_map: dict | None = None) -> Any:
        from core.emitters.models import EmitResult

        class _FakeLifecycle:
            def __init__(self) -> None:
                self.open_called = False
                self.emit_calls: list[date] = []
                self.close_called = False
                self._outcomes = outcomes_map or {}

            def open(self, tenant_id: str, manifest: object) -> None:
                self.open_called = True

            def emit(self, tenant_id: str, dt: date, rows: object) -> None:
                self.emit_calls.append(dt)

            def close(self, tenant_id: str) -> EmitResult:
                self.close_called = True
                return EmitResult(outcomes=self._outcomes)

        return _FakeLifecycle()

    def test_open_emit_close_called_in_order(self) -> None:
        from core.emitters.drivers import LifecycleDriver
        from core.emitters.models import EmitManifest, EmitOutcome

        dates = [date(2025, 1, 1), date(2025, 1, 2)]
        outcomes_map = {d: EmitOutcome.EMITTED for d in dates}
        lc = self._make_lifecycle_emitter(outcomes_map)
        driver = LifecycleDriver(lc)
        manifest = EmitManifest(pending_dates=dates, total_rows_estimate=2, is_reemission=False)

        result = driver.run(TENANT_ID, manifest, MagicMock(return_value=[_make_row()]))

        assert lc.open_called
        assert len(lc.emit_calls) == 2
        assert lc.close_called
        assert result[date(2025, 1, 1)] == EmitOutcome.EMITTED
        assert result[date(2025, 1, 2)] == EmitOutcome.EMITTED

    def test_open_failure_returns_all_failed(self) -> None:
        from core.emitters.drivers import LifecycleDriver
        from core.emitters.models import EmitManifest, EmitOutcome, EmitResult

        class _FailingOpen:
            def open(self, tenant_id: str, manifest: object) -> None:
                raise RuntimeError("open failed")

            def emit(self, *a: object) -> None: ...

            def close(self, *a: object) -> EmitResult:
                return EmitResult()

        driver = LifecycleDriver(_FailingOpen())
        manifest = EmitManifest(pending_dates=[date(2025, 1, 1)], total_rows_estimate=1, is_reemission=False)

        result = driver.run(TENANT_ID, manifest, MagicMock(return_value=[_make_row()]))

        assert result[date(2025, 1, 1)] == EmitOutcome.FAILED

    def test_empty_rows_not_passed_to_emit(self) -> None:
        from core.emitters.drivers import LifecycleDriver
        from core.emitters.models import EmitManifest, EmitResult

        class _TrackingLifecycle:
            def __init__(self) -> None:
                self.emit_calls: list[date] = []

            def open(self, *a: object) -> None: ...

            def emit(self, tenant_id: str, dt: date, rows: object) -> None:
                self.emit_calls.append(dt)

            def close(self, *a: object) -> EmitResult:
                return EmitResult(outcomes={})

        lc = _TrackingLifecycle()
        driver = LifecycleDriver(lc)
        manifest = EmitManifest(pending_dates=[date(2025, 1, 1)], total_rows_estimate=0, is_reemission=False)

        driver.run(TENANT_ID, manifest, MagicMock(return_value=[]))

        assert lc.emit_calls == []

    def test_close_failure_returns_all_failed(self) -> None:
        from core.emitters.drivers import LifecycleDriver
        from core.emitters.models import EmitManifest, EmitOutcome

        class _FailingClose:
            def open(self, *a: object) -> None: ...

            def emit(self, *a: object) -> None: ...

            def close(self, *a: object) -> object:
                raise RuntimeError("close failed")

        driver = LifecycleDriver(_FailingClose())
        manifest = EmitManifest(pending_dates=[date(2025, 1, 1)], total_rows_estimate=1, is_reemission=False)

        result = driver.run(TENANT_ID, manifest, MagicMock(return_value=[_make_row()]))

        assert result[date(2025, 1, 1)] == EmitOutcome.FAILED
