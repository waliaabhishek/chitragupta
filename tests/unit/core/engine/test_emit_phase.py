from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock

import pytest

# ---------- helpers ----------

ECOSYSTEM = "test-eco"
TENANT_ID = "tenant-1"


def _make_chargeback_row(
    *,
    timestamp: datetime | None = None,
    amount: Decimal = Decimal("10.00"),
    identity_id: str = "user-1",
    resource_id: str = "cluster-1",
) -> Any:
    from core.models.chargeback import ChargebackRow, CostType

    return ChargebackRow(
        ecosystem=ECOSYSTEM,
        tenant_id=TENANT_ID,
        timestamp=timestamp or datetime(2024, 1, 15, 0, 0, 0, tzinfo=UTC),
        resource_id=resource_id,
        product_category="kafka",
        product_type="KAFKA_CKU",
        identity_id=identity_id,
        cost_type=CostType.USAGE,
        amount=amount,
        allocation_method="even",
        allocation_detail="even_split",
    )


def _make_storage_backend(rows: list[Any] | None = None) -> MagicMock:
    """Create a mock storage backend that returns rows for find_by_date and find_by_range."""
    backend = MagicMock()
    uow = MagicMock()
    uow.__enter__ = MagicMock(return_value=uow)
    uow.__exit__ = MagicMock(return_value=False)
    uow.chargebacks.find_by_date.return_value = rows or []
    uow.chargebacks.find_by_range.return_value = rows or []
    backend.create_unit_of_work.return_value = uow
    return backend


# ---------- _load_emitters ----------


class TestLoadEmitters:
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

    def test_empty_specs_returns_empty_list(self) -> None:
        from core.engine.orchestrator import _load_emitters

        result = _load_emitters([], "daily")
        assert result == []

    def test_single_csv_spec_returns_one_entry(self) -> None:
        from core.config.models import EmitterSpec
        from core.engine.orchestrator import _EmitterEntry, _load_emitters

        self._register_csv()
        specs = [EmitterSpec(type="csv", params={"output_dir": "/tmp"})]
        result = _load_emitters(specs, "daily")
        assert len(result) == 1
        assert isinstance(result[0], _EmitterEntry)

    def test_hourly_aggregation_finer_than_daily_raises(self) -> None:
        from core.config.models import EmitterSpec
        from core.engine.orchestrator import _load_emitters

        self._register_csv()
        specs = [EmitterSpec(type="csv", aggregation="hourly", params={"output_dir": "/tmp"})]
        with pytest.raises(ValueError, match="finer"):
            _load_emitters(specs, "daily")

    def test_monthly_aggregation_coarser_than_daily_succeeds(self) -> None:
        from core.config.models import EmitterSpec
        from core.engine.orchestrator import _EmitterEntry, _load_emitters

        self._register_csv()
        specs = [EmitterSpec(type="csv", aggregation="monthly", params={"output_dir": "/tmp"})]
        result = _load_emitters(specs, "daily")
        assert len(result) == 1
        assert isinstance(result[0], _EmitterEntry)
        assert result[0].aggregation == "monthly"

    def test_unknown_type_raises_value_error(self) -> None:
        from core.config.models import EmitterSpec
        from core.engine.orchestrator import _load_emitters

        specs = [EmitterSpec(type="nonexistent_emitter_xyz", params={})]
        with pytest.raises(ValueError):
            _load_emitters(specs, "daily")

    def test_same_granularity_aggregation_succeeds(self) -> None:
        from core.config.models import EmitterSpec
        from core.engine.orchestrator import _load_emitters

        self._register_csv()
        specs = [EmitterSpec(type="csv", aggregation="daily", params={"output_dir": "/tmp"})]
        result = _load_emitters(specs, "daily")
        assert len(result) == 1


# ---------- EmitPhase ----------


class TestEmitPhaseEmptyEntries:
    def test_run_with_no_emitters_returns_zero_dates_no_errors(self) -> None:
        from core.engine.orchestrator import EmitPhase, EmitResult

        backend = _make_storage_backend()
        phase = EmitPhase(
            ecosystem=ECOSYSTEM,
            tenant_id=TENANT_ID,
            storage_backend=backend,
            emitter_entries=[],
            chargeback_granularity="daily",
        )
        result = phase.run(date(2024, 1, 15))
        assert result == EmitResult(dates_attempted=0, errors=[])
        backend.create_unit_of_work.assert_not_called()

    def test_run_with_no_emitters_does_not_hit_storage(self) -> None:
        from core.engine.orchestrator import EmitPhase

        backend = _make_storage_backend()
        phase = EmitPhase(
            ecosystem=ECOSYSTEM,
            tenant_id=TENANT_ID,
            storage_backend=backend,
            emitter_entries=[],
            chargeback_granularity="daily",
        )
        phase.run(date(2024, 1, 15))
        backend.create_unit_of_work.assert_not_called()


class TestEmitPhaseNoRows:
    def setup_method(self) -> None:
        from core.emitters import registry

        registry._REGISTRY.clear()

    def teardown_method(self) -> None:
        from core.emitters import registry

        registry._REGISTRY.clear()

    def test_run_with_no_rows_returns_zero_dates_attempted(self) -> None:
        from core.engine.orchestrator import EmitPhase, _EmitterEntry

        backend = _make_storage_backend(rows=[])  # no rows
        emitter = MagicMock()
        entry = _EmitterEntry(emitter=emitter, aggregation=None)

        phase = EmitPhase(
            ecosystem=ECOSYSTEM,
            tenant_id=TENANT_ID,
            storage_backend=backend,
            emitter_entries=[entry],
            chargeback_granularity="daily",
        )
        result = phase.run(date(2024, 1, 15))
        assert result.dates_attempted == 0
        assert result.errors == []
        emitter.assert_not_called()


class TestEmitPhaseEmitterRaisesError:
    def test_runtime_error_captured_not_reraised(self) -> None:
        from core.engine.orchestrator import EmitPhase, _EmitterEntry

        rows = [_make_chargeback_row()]
        backend = _make_storage_backend(rows=rows)

        failing_emitter = MagicMock(side_effect=RuntimeError("disk full"))
        entry = _EmitterEntry(emitter=failing_emitter, aggregation=None)

        phase = EmitPhase(
            ecosystem=ECOSYSTEM,
            tenant_id=TENANT_ID,
            storage_backend=backend,
            emitter_entries=[entry],
            chargeback_granularity="daily",
        )
        result = phase.run(date(2024, 1, 15))
        assert result.dates_attempted == 1
        assert len(result.errors) == 1
        assert "disk full" in result.errors[0]

    def test_emit_error_does_not_reraise(self) -> None:
        from core.engine.orchestrator import EmitPhase, _EmitterEntry

        rows = [_make_chargeback_row()]
        backend = _make_storage_backend(rows=rows)

        entry = _EmitterEntry(emitter=MagicMock(side_effect=RuntimeError("boom")), aggregation=None)
        phase = EmitPhase(
            ecosystem=ECOSYSTEM,
            tenant_id=TENANT_ID,
            storage_backend=backend,
            emitter_entries=[entry],
            chargeback_granularity="daily",
        )
        # Must not raise
        result = phase.run(date(2024, 1, 15))
        assert result.dates_attempted == 1
        assert len(result.errors) == 1


# ---------- _aggregate_rows ----------


class TestAggregateRowsDaily:
    def test_24_hourly_rows_same_identity_aggregated_to_1(self) -> None:
        from core.engine.orchestrator import _aggregate_rows

        rows = [
            _make_chargeback_row(
                timestamp=datetime(2024, 1, 15, hour, 0, 0, tzinfo=UTC),
                amount=Decimal("1.00"),
            )
            for hour in range(24)
        ]
        result = _aggregate_rows(rows, "daily")
        assert len(result) == 1
        assert result[0].amount == Decimal("24.00")
        assert result[0].timestamp == datetime(2024, 1, 15, 0, 0, 0, tzinfo=UTC)

    def test_aggregated_timestamp_is_midnight_utc(self) -> None:
        from core.engine.orchestrator import _aggregate_rows

        rows = [_make_chargeback_row(timestamp=datetime(2024, 1, 15, 14, 30, 0, tzinfo=UTC))]
        result = _aggregate_rows(rows, "daily")
        assert result[0].timestamp == datetime(2024, 1, 15, 0, 0, 0, tzinfo=UTC)


class TestAggregateRowsMonthly:
    def test_daily_january_rows_aggregated_to_1_per_group(self) -> None:
        from core.engine.orchestrator import _aggregate_rows

        # 31 daily rows for January, same identity/product/resource
        rows = [
            _make_chargeback_row(
                timestamp=datetime(2024, 1, day, 0, 0, 0, tzinfo=UTC),
                amount=Decimal("1.00"),
            )
            for day in range(1, 32)
        ]
        result = _aggregate_rows(rows, "monthly")
        assert len(result) == 1
        assert result[0].amount == Decimal("31.00")
        assert result[0].timestamp == datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC)

    def test_monthly_timestamp_is_first_of_month(self) -> None:
        from core.engine.orchestrator import _aggregate_rows

        rows = [
            _make_chargeback_row(timestamp=datetime(2024, 1, 15, 0, 0, 0, tzinfo=UTC)),
            _make_chargeback_row(timestamp=datetime(2024, 1, 20, 0, 0, 0, tzinfo=UTC)),
        ]
        result = _aggregate_rows(rows, "monthly")
        assert result[0].timestamp == datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC)

    def test_multiple_identities_produce_separate_rows(self) -> None:
        from core.engine.orchestrator import _aggregate_rows

        rows = [
            _make_chargeback_row(identity_id="user-1", amount=Decimal("5.00")),
            _make_chargeback_row(identity_id="user-2", amount=Decimal("7.00")),
        ]
        result = _aggregate_rows(rows, "monthly")
        assert len(result) == 2
        amounts = {r.identity_id: r.amount for r in result}
        assert amounts["user-1"] == Decimal("5.00")
        assert amounts["user-2"] == Decimal("7.00")


class TestAggregateRowsDropsAllocationDetail:
    def test_allocation_detail_set_to_none(self) -> None:
        from core.engine.orchestrator import _aggregate_rows

        rows = [_make_chargeback_row()]
        result = _aggregate_rows(rows, "daily")
        assert result[0].allocation_detail is None

    def test_allocation_detail_none_even_when_original_set(self) -> None:
        from core.engine.orchestrator import _aggregate_rows

        row = _make_chargeback_row()
        row.allocation_detail = "even_split"
        result = _aggregate_rows([row], "daily")
        assert result[0].allocation_detail is None


# ---------- EmitPhase._fetch_rows ----------


class TestEmitPhaseFetchRowsMonthly:
    def test_fetch_rows_monthly_queries_find_by_range(self) -> None:
        from core.engine.orchestrator import EmitPhase, _EmitterEntry

        rows = [_make_chargeback_row()]
        backend = _make_storage_backend(rows=rows)
        entry = _EmitterEntry(emitter=MagicMock(), aggregation="monthly")

        phase = EmitPhase(
            ecosystem=ECOSYSTEM,
            tenant_id=TENANT_ID,
            storage_backend=backend,
            emitter_entries=[entry],
            chargeback_granularity="daily",
        )
        fetched_rows, emit_date = phase._fetch_rows(date(2024, 1, 15), "monthly")
        assert fetched_rows == rows
        assert emit_date == date(2024, 1, 1)
        uow = backend.create_unit_of_work.return_value.__enter__.return_value
        uow.chargebacks.find_by_range.assert_called_once()

    def test_fetch_rows_monthly_emit_date_is_month_start(self) -> None:
        from core.engine.orchestrator import EmitPhase

        backend = _make_storage_backend(rows=[])
        phase = EmitPhase(
            ecosystem=ECOSYSTEM,
            tenant_id=TENANT_ID,
            storage_backend=backend,
            emitter_entries=[],
            chargeback_granularity="daily",
        )
        _, emit_date = phase._fetch_rows(date(2024, 3, 20), "monthly")
        assert emit_date == date(2024, 3, 1)

    def test_fetch_rows_daily_uses_find_by_date(self) -> None:
        from core.engine.orchestrator import EmitPhase

        backend = _make_storage_backend(rows=[])
        phase = EmitPhase(
            ecosystem=ECOSYSTEM,
            tenant_id=TENANT_ID,
            storage_backend=backend,
            emitter_entries=[],
            chargeback_granularity="daily",
        )
        tracking_date = date(2024, 1, 15)
        _, emit_date = phase._fetch_rows(tracking_date, None)
        assert emit_date == tracking_date
        uow = backend.create_unit_of_work.return_value.__enter__.return_value
        uow.chargebacks.find_by_date.assert_called_once()
