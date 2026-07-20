from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any
from unittest.mock import patch

import pytest
from sqlmodel import Session, select

from core.preview.evidence import PreviewEvidenceScope
from core.storage.backends.sqlmodel.engine import _engine_lock, _engines, get_or_create_engine
from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend
from plugins.confluent_cloud.models.billing import CCloudBillingLineItem
from plugins.confluent_cloud.storage.module import CCloudStorageModule


@pytest.fixture(autouse=True)
def clean_engine_cache() -> Any:
    with _engine_lock:
        for engine in _engines.values():
            engine.dispose()
        _engines.clear()
    yield
    with _engine_lock:
        for engine in _engines.values():
            engine.dispose()
        _engines.clear()


def _origin(*, tenant_id: str = "org-1", day: int = 1) -> CCloudBillingLineItem:
    return CCloudBillingLineItem(
        ecosystem="confluent_cloud",
        tenant_id=tenant_id,
        timestamp=datetime(2026, 7, day, tzinfo=UTC),
        env_id="env-1",
        resource_id="lkc-1",
        product_category="KAFKA",
        product_type="KAFKA_STORAGE",
        quantity=Decimal("5.000"),
        unit_price=Decimal("2.00"),
        total_cost=Decimal("8.00"),
        currency="USD",
        granularity="daily",
    )


def _capture(*, status: str = "complete", reason: str | None = None) -> Any:
    from core.storage.interface import (
        AllocationLineageCapture,
        AllocationLineageFact,
        AllocationTargetKind,
        LineageCaptureReason,
        LineageCaptureStatus,
    )

    facts = (
        AllocationLineageFact(
            portion_ordinal=0,
            target_kind=AllocationTargetKind.IDENTITY,
            target_id="sa-1",
            allocated_cost=Decimal("6.00"),
            allocated_quantity=Decimal("3.750"),
            allocation_ratio=Decimal("0.75"),
            method_id="usage_ratio",
            method_version="v1",
            method_details_json='{"allocation_detail":"usage","metadata":{},"target_kind":"identity"}',
        ),
        AllocationLineageFact(
            portion_ordinal=1,
            target_kind=AllocationTargetKind.UNALLOCATED,
            target_id=None,
            allocated_cost=Decimal("2.00"),
            allocated_quantity=Decimal("1.250"),
            allocation_ratio=Decimal("0.25"),
            method_id="usage_ratio",
            method_version="v1",
            method_details_json='{"allocation_detail":"no_identity","metadata":{},"target_kind":"unallocated"}',
        ),
    )
    return AllocationLineageCapture(
        origin_timestamp=datetime(2026, 7, 1, tzinfo=UTC),
        origin_env_id="env-1",
        origin_resource_id="lkc-1",
        origin_product_type="KAFKA_STORAGE",
        origin_product_category="KAFKA",
        status=LineageCaptureStatus(status),
        reason=None if reason is None else LineageCaptureReason(reason),
        facts=facts if status == "complete" else (),
    )


def _run(
    *,
    tenant_id: str = "org-1",
    tracking_date: date = date(2026, 7, 1),
    calculation_id: str = "calculation-1",
    captures: tuple[Any, ...] | None = None,
) -> Any:
    from core.storage.interface import AllocationLineageRunCapture

    return AllocationLineageRunCapture(
        ecosystem="confluent_cloud",
        tenant_id=tenant_id,
        tracking_date=tracking_date,
        calculation_id=calculation_id,
        captures=(_capture(),) if captures is None else captures,
    )


def test_repository_round_trips_run_and_every_fact_through_preview_reader(tmp_path: Any) -> None:
    connection_string = f"sqlite:///{tmp_path / 'lineage.db'}"
    backend = SQLModelBackend(connection_string, CCloudStorageModule(), use_migrations=False)
    backend.create_tables()
    completed_at = datetime(2026, 7, 3, 4, 5, tzinfo=UTC)
    with backend.create_unit_of_work() as uow:
        uow.billing.upsert(_origin())
        uow.chargebacks.replace_calculation_lineage(  # type: ignore[attr-defined]
            _run(), calculation_completed_at=completed_at
        )
        uow.commit()

    scope = PreviewEvidenceScope(
        ecosystem="confluent_cloud",
        tenant_id="org-1",
        start=datetime(2026, 7, 1, tzinfo=UTC),
        end=datetime(2026, 7, 2, tzinfo=UTC),
    )
    with backend.create_read_only_unit_of_work() as uow:
        rows = tuple(uow.chargebacks.iter_preview_allocations(scope, ("calculation-1",)))  # type: ignore[attr-defined]

    assert len(rows) == 2
    assert [row.calculation_id for row in rows] == ["calculation-1", "calculation-1"]
    assert [row.portion_ordinal for row in rows] == [0, 1]
    assert [row.target_kind for row in rows] == ["identity", "unallocated"]
    assert [row.target_id for row in rows] == ["sa-1", None]
    assert [row.allocated_cost for row in rows] == [Decimal("6.00"), Decimal("2.00")]
    assert [row.allocated_quantity for row in rows] == [Decimal("3.750"), Decimal("1.250")]
    assert [row.allocation_ratio for row in rows] == [Decimal("0.75"), Decimal("0.25")]
    assert [row.method_id for row in rows] == ["usage_ratio", "usage_ratio"]
    assert [row.method_version for row in rows] == ["v1", "v1"]
    assert rows[0].method_details_json.startswith('{"allocation_detail"')
    assert rows[0].origin_total_cost == Decimal("8.00")
    assert rows[0].origin_quantity == Decimal("5.000")
    assert rows[0].origin_unit_price == Decimal("2.00")
    assert rows[0].origin_currency == "USD"
    assert rows[0].origin_granularity == "daily"
    backend.dispose()


def test_repository_atomically_replaces_only_the_owned_tenant_date_and_calculation(tmp_path: Any) -> None:
    connection_string = f"sqlite:///{tmp_path / 'lineage-isolation.db'}"
    backend = SQLModelBackend(connection_string, CCloudStorageModule(), use_migrations=False)
    backend.create_tables()
    completed_at = datetime(2026, 7, 3, tzinfo=UTC)
    with backend.create_unit_of_work() as uow:
        uow.billing.upsert(_origin())
        uow.chargebacks.replace_calculation_lineage(  # type: ignore[attr-defined]
            _run(), calculation_completed_at=completed_at
        )
        uow.chargebacks.replace_calculation_lineage(  # type: ignore[attr-defined]
            _run(tenant_id="org-2", captures=()), calculation_completed_at=completed_at
        )
        uow.chargebacks.replace_calculation_lineage(  # type: ignore[attr-defined]
            _run(tracking_date=date(2026, 7, 2), captures=()), calculation_completed_at=completed_at
        )
        uow.chargebacks.replace_calculation_lineage(  # type: ignore[attr-defined]
            _run(calculation_id="calculation-2", captures=()),
            calculation_completed_at=completed_at + timedelta(minutes=1),
        )
        uow.commit()

    from plugins.confluent_cloud.storage.tables import (
        CCloudAllocationLineagePortionTable,
        CCloudAllocationLineageRunTable,
    )

    engine = get_or_create_engine(connection_string)
    with Session(engine) as session:
        runs = list(session.exec(select(CCloudAllocationLineageRunTable)).all())
        portions = list(session.exec(select(CCloudAllocationLineagePortionTable)).all())

    assert sorted((row.tenant_id, row.tracking_date.isoformat(), row.calculation_id) for row in runs) == [
        ("org-1", "2026-07-01", "calculation-2"),
        ("org-1", "2026-07-02", "calculation-1"),
        ("org-2", "2026-07-01", "calculation-1"),
    ]
    assert portions == []
    backend.dispose()


def test_failed_mid_replacement_rolls_back_deletes_and_partial_new_rows(tmp_path: Any) -> None:
    connection_string = f"sqlite:///{tmp_path / 'lineage-rollback.db'}"
    backend = SQLModelBackend(connection_string, CCloudStorageModule(), use_migrations=False)
    backend.create_tables()
    completed_at = datetime(2026, 7, 3, tzinfo=UTC)
    with backend.create_unit_of_work() as uow:
        uow.billing.upsert(_origin())
        uow.chargebacks.replace_calculation_lineage(_run(), calculation_completed_at=completed_at)  # type: ignore[attr-defined]
        uow.commit()

    with pytest.raises(RuntimeError, match="after flush"), backend.create_unit_of_work() as uow:
        session = uow.chargebacks._session  # type: ignore[attr-defined]
        real_flush = session.flush

        def fail_after_flush() -> None:
            real_flush()
            raise RuntimeError("after flush")

        with patch.object(session, "flush", side_effect=fail_after_flush):
            uow.chargebacks.replace_calculation_lineage(  # type: ignore[attr-defined]
                _run(calculation_id="replacement", captures=()),
                calculation_completed_at=completed_at + timedelta(minutes=1),
            )

    from plugins.confluent_cloud.storage.tables import (
        CCloudAllocationLineagePortionTable,
        CCloudAllocationLineageRunTable,
    )

    engine = get_or_create_engine(connection_string)
    with Session(engine) as session:
        runs = list(session.exec(select(CCloudAllocationLineageRunTable)).all())
        portions = list(session.exec(select(CCloudAllocationLineagePortionTable)).all())
    assert [run.calculation_id for run in runs] == ["calculation-1"]
    assert [portion.portion_ordinal for portion in portions] == [0, 1]
    backend.dispose()


def test_repository_persists_invalid_status_and_safe_reason_without_public_exception_text(tmp_path: Any) -> None:
    connection_string = f"sqlite:///{tmp_path / 'lineage-invalid.db'}"
    backend = SQLModelBackend(connection_string, CCloudStorageModule(), use_migrations=False)
    backend.create_tables()
    completed_at = datetime(2026, 7, 3, tzinfo=UTC)
    with backend.create_unit_of_work() as uow:
        uow.chargebacks.replace_calculation_lineage(  # type: ignore[attr-defined]
            _run(captures=(_capture(status="invalid", reason="invalid_metadata"),)),
            calculation_completed_at=completed_at,
        )
        uow.commit()

    from plugins.confluent_cloud.storage.tables import CCloudAllocationLineageRunTable

    engine = get_or_create_engine(connection_string)
    with Session(engine) as session:
        row = session.exec(select(CCloudAllocationLineageRunTable)).one()
    assert row.capture_status == "invalid"
    assert row.capture_reason == "invalid_metadata"
    assert row.portion_count == 0
    backend.dispose()


def test_optional_lineage_capability_is_exposed_only_by_supported_confluent_repository(tmp_path: Any) -> None:
    from core.storage.interface import AllocationLineageRepository
    from tests.unit.core.engine.test_batch_chargeback_write import MockChargebackRepo

    connection_string = f"sqlite:///{tmp_path / 'lineage-capability.db'}"
    backend = SQLModelBackend(connection_string, CCloudStorageModule(), use_migrations=False)
    backend.create_tables()
    with backend.create_unit_of_work() as uow:
        assert isinstance(uow.chargebacks, AllocationLineageRepository)
    assert not isinstance(MockChargebackRepo(), AllocationLineageRepository)
    backend.dispose()
