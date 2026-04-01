"""Integration tests: EmitterRunner → real SQLite storage → emitter chain."""

from __future__ import annotations

from collections.abc import Generator
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

import pytest

from core.storage.backends.sqlmodel.module import CoreStorageModule
from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend

ECOSYSTEM = "integration-eco"
TENANT_ID = "integration-tenant"
NOW = datetime(2026, 1, 15, 12, 0, 0, tzinfo=UTC)


@pytest.fixture
def storage() -> Generator[SQLModelBackend]:
    backend = SQLModelBackend("sqlite:///:memory:", CoreStorageModule(), use_migrations=False)
    backend.create_tables()
    yield backend
    backend.dispose()


def _insert_chargeback(storage: SQLModelBackend, dt: date, amount: Decimal = Decimal("10.00")) -> None:
    from core.models.chargeback import ChargebackRow, CostType

    row = ChargebackRow(
        ecosystem=ECOSYSTEM,
        tenant_id=TENANT_ID,
        timestamp=datetime(dt.year, dt.month, dt.day, 0, 0, 0, tzinfo=UTC),
        resource_id="cluster-1",
        product_category="kafka",
        product_type="KAFKA_CKU",
        identity_id="user-1",
        cost_type=CostType.USAGE,
        amount=amount,
        allocation_method="even",
    )
    with storage.create_unit_of_work() as uow:
        uow.chargebacks.upsert(row)
        uow.commit()


def _clear_registry() -> None:
    from core.emitters import registry

    registry._REGISTRY.clear()


class TestEmitterRunnerIntegration:
    def setup_method(self) -> None:
        _clear_registry()

    def teardown_method(self) -> None:
        _clear_registry()

    def test_runner_emits_pending_dates_and_writes_emission_records(self, storage: SQLModelBackend) -> None:
        """Full chain: chargebacks in DB → EmitterRunner.run() → EmissionRecord rows written."""
        from core.config.models import EmitterSpec
        from core.emitters.registry import register
        from core.emitters.runner import EmitterRunner

        dates = [date(2025, 1, 1), date(2025, 1, 2), date(2025, 1, 3)]
        for d in dates:
            _insert_chargeback(storage, d)

        call_log: list[date] = []

        def _fake_emitter(tenant_id: str, dt: date, rows: Any) -> None:
            call_log.append(dt)

        register("test-emitter", lambda **_: _fake_emitter)

        spec = EmitterSpec(type="test-emitter", name="test-emitter")
        from core.emitters.sources import ChargebackDateSource, ChargebackRowFetcher, RegistryEmitterBuilder

        runner = EmitterRunner(
            ecosystem=ECOSYSTEM,
            storage_backend=storage,
            emitter_specs=[spec],
            date_source=ChargebackDateSource(storage),
            row_fetcher=ChargebackRowFetcher(storage),
            emitter_builder=RegistryEmitterBuilder(),
            pipeline="chargeback",
            chargeback_granularity="daily",
        )
        runner.run(TENANT_ID)

        # All 3 dates were emitted
        assert sorted(call_log) == dates

        # EmissionRecord rows written to DB
        with storage.create_unit_of_work() as uow:
            emitted = uow.emissions.get_emitted_dates(ECOSYSTEM, TENANT_ID, "test-emitter", "chargeback")

        assert set(dates) == emitted

    def test_runner_skips_already_emitted_dates(self, storage: SQLModelBackend) -> None:
        """Second run skips dates already recorded as emitted."""
        from core.config.models import EmitterSpec
        from core.emitters.models import EmissionRecord
        from core.emitters.registry import register
        from core.emitters.runner import EmitterRunner

        dates = [date(2025, 2, 1), date(2025, 2, 2)]
        for d in dates:
            _insert_chargeback(storage, d)

        # Pre-seed: 2025-02-01 already emitted
        with storage.create_unit_of_work() as uow:
            uow.emissions.upsert(
                EmissionRecord(ECOSYSTEM, TENANT_ID, "test-emitter", "chargeback", date(2025, 2, 1), "emitted")
            )
            uow.commit()

        call_log: list[date] = []

        def _fake_emitter(tenant_id: str, dt: date, rows: Any) -> None:
            call_log.append(dt)

        register("test-emitter", lambda **_: _fake_emitter)

        spec = EmitterSpec(type="test-emitter", name="test-emitter")
        from core.emitters.sources import ChargebackDateSource, ChargebackRowFetcher, RegistryEmitterBuilder

        runner = EmitterRunner(
            ECOSYSTEM,
            storage,
            [spec],
            ChargebackDateSource(storage),
            ChargebackRowFetcher(storage),
            RegistryEmitterBuilder(),
            "chargeback",
            "daily",
        )
        runner.run(TENANT_ID)

        # Only 2025-02-02 emitted; 2025-02-01 skipped
        assert call_log == [date(2025, 2, 2)]

    def test_runner_records_failed_emission_in_db(self, storage: SQLModelBackend) -> None:
        """Emitter that raises → EmissionRecord with status='failed' persisted."""
        from core.config.models import EmitterSpec
        from core.emitters.registry import register
        from core.emitters.runner import EmitterRunner

        _insert_chargeback(storage, date(2025, 3, 10))

        def _failing_emitter(tenant_id: str, dt: date, rows: Any) -> None:
            raise RuntimeError("external sink unavailable")

        register("failing-emitter", lambda **_: _failing_emitter)

        spec = EmitterSpec(type="failing-emitter", name="failing-emitter")
        from core.emitters.sources import ChargebackDateSource, ChargebackRowFetcher, RegistryEmitterBuilder

        runner = EmitterRunner(
            ECOSYSTEM,
            storage,
            [spec],
            ChargebackDateSource(storage),
            ChargebackRowFetcher(storage),
            RegistryEmitterBuilder(),
            "chargeback",
            "daily",
        )
        runner.run(TENANT_ID)  # must not raise

        with storage.create_unit_of_work() as uow:
            failed = uow.emissions.get_failed_dates(ECOSYSTEM, TENANT_ID, "failing-emitter", "chargeback")

        assert date(2025, 3, 10) in failed

    def test_runner_idempotent_on_re_run(self, storage: SQLModelBackend) -> None:
        """Running twice: second run is a no-op (all dates already emitted)."""
        from core.config.models import EmitterSpec
        from core.emitters.registry import register
        from core.emitters.runner import EmitterRunner

        _insert_chargeback(storage, date(2025, 4, 5))

        call_count = 0

        def _counting_emitter(tenant_id: str, dt: date, rows: Any) -> None:
            nonlocal call_count
            call_count += 1

        register("counting-emitter", lambda **_: _counting_emitter)

        spec = EmitterSpec(type="counting-emitter", name="counting-emitter")
        from core.emitters.sources import ChargebackDateSource, ChargebackRowFetcher, RegistryEmitterBuilder

        runner = EmitterRunner(
            ECOSYSTEM,
            storage,
            [spec],
            ChargebackDateSource(storage),
            ChargebackRowFetcher(storage),
            RegistryEmitterBuilder(),
            "chargeback",
            "daily",
        )

        runner.run(TENANT_ID)  # first run: emits once
        runner.run(TENANT_ID)  # second run: should be a no-op

        assert call_count == 1
