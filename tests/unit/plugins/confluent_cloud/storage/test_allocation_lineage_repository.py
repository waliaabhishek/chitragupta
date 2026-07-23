from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any
from unittest.mock import patch

import pytest
from sqlalchemy import text
from sqlmodel import Session, select

from core.preview.evidence import (
    AllocationLineageRunStatus,
    AllocationLineageUnavailableReason,
    AllocationLineageUnavailableRun,
    PreviewAllocationEvidenceDecodeError,
    PreviewEvidenceScope,
)
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


def _scope() -> PreviewEvidenceScope:
    return PreviewEvidenceScope(
        ecosystem="confluent_cloud",
        tenant_id="org-1",
        start=datetime(2026, 7, 1, tzinfo=UTC),
        end=datetime(2026, 7, 2, tzinfo=UTC),
    )


def _set_persisted_run_codec(
    backend: SQLModelBackend,
    *,
    status: str | None,
    reason: str | None,
    portion_count: int | None = None,
) -> None:
    engine = get_or_create_engine(backend._connection_string)
    if status is not None:
        assignments = "capture_status = :status, capture_reason = :reason"
        values: dict[str, object] = {"status": status, "reason": reason}
        if portion_count is not None:
            assignments += ", portion_count = :portion_count"
            values["portion_count"] = portion_count
        with engine.begin() as connection:
            connection.execute(
                text(f"UPDATE ccloud_allocation_lineage_runs SET {assignments}"),  # noqa: S608
                values,
            )
        return

    with engine.connect() as connection:
        connection.exec_driver_sql("PRAGMA foreign_keys=OFF")
        connection.commit()
        with connection.begin():
            connection.exec_driver_sql(
                """CREATE TABLE ccloud_allocation_lineage_runs_nullable (
                    ecosystem VARCHAR NOT NULL,
                    tenant_id VARCHAR NOT NULL,
                    tracking_date DATE NOT NULL,
                    calculation_id VARCHAR NOT NULL,
                    calculation_completed_at DATETIME NOT NULL,
                    capture_status VARCHAR,
                    capture_reason VARCHAR,
                    portion_count INTEGER NOT NULL,
                    PRIMARY KEY (ecosystem, tenant_id, tracking_date)
                )"""
            )
            connection.exec_driver_sql(
                """INSERT INTO ccloud_allocation_lineage_runs_nullable
                    SELECT ecosystem, tenant_id, tracking_date, calculation_id,
                           calculation_completed_at, NULL, capture_reason, portion_count
                    FROM ccloud_allocation_lineage_runs"""
            )
            connection.exec_driver_sql("DROP TABLE ccloud_allocation_lineage_runs")
            connection.exec_driver_sql(
                "ALTER TABLE ccloud_allocation_lineage_runs_nullable RENAME TO ccloud_allocation_lineage_runs"
            )
        connection.exec_driver_sql("PRAGMA foreign_keys=ON")
        connection.commit()


def test_repository_round_trips_run_and_every_fact_through_preview_reader(tmp_path: Any) -> None:
    connection_string = f"sqlite:///{tmp_path / 'lineage.db'}"
    backend = SQLModelBackend(
        connection_string,
        CCloudStorageModule(),
        use_migrations=False,
        focus_preview_enabled=True,
    )
    backend.create_tables()
    completed_at = datetime(2026, 7, 3, 4, 5, tzinfo=UTC)
    with backend.create_unit_of_work() as uow:
        uow.billing.upsert(_origin())
        uow.commit()
    with backend.create_preview_evidence_unit_of_work() as uow:
        uow.allocation_lineage.replace_calculation_lineage(_run(), calculation_completed_at=completed_at)
        uow.commit()

    scope = PreviewEvidenceScope(
        ecosystem="confluent_cloud",
        tenant_id="org-1",
        start=datetime(2026, 7, 1, tzinfo=UTC),
        end=datetime(2026, 7, 2, tzinfo=UTC),
    )
    with backend.create_preview_generation_read_unit_of_work() as uow:
        rows = tuple(uow.allocation_evidence.iter_preview_allocations(scope, ("calculation-1",)))

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


@pytest.mark.parametrize(
    ("status", "reason"),
    [
        ("complete", None),
        ("unavailable", "capture_failed"),
        ("unavailable", "persistence_failed"),
    ],
)
def test_preview_run_reader_accepts_only_closed_available_and_unavailable_codecs(
    tmp_path: Any,
    status: str,
    reason: str | None,
) -> None:
    backend = SQLModelBackend(
        f"sqlite:///{tmp_path / f'lineage-valid-{status}-{reason}.db'}",
        CCloudStorageModule(),
        use_migrations=False,
        focus_preview_enabled=True,
    )
    backend.create_tables()
    completed_at = datetime(2026, 7, 3, 4, 5, tzinfo=UTC)
    with backend.create_preview_evidence_unit_of_work() as uow:
        if status == "complete":
            uow.allocation_lineage.replace_calculation_lineage(
                _run(),
                calculation_completed_at=completed_at,
            )
        else:
            assert reason is not None
            uow.allocation_lineage.mark_calculation_lineage_unavailable(
                AllocationLineageUnavailableRun(
                    ecosystem="confluent_cloud",
                    tenant_id="org-1",
                    tracking_date=date(2026, 7, 1),
                    calculation_id="calculation-1",
                    calculation_completed_at=completed_at,
                    status=AllocationLineageRunStatus.UNAVAILABLE,
                    reason=AllocationLineageUnavailableReason(reason),
                )
            )
        uow.commit()

    with backend.create_preview_generation_read_unit_of_work() as uow:
        runs = tuple(uow.allocation_evidence.iter_preview_allocation_runs(_scope(), ("calculation-1",)))

    assert len(runs) == 1
    assert runs[0].capture_status == status
    assert runs[0].capture_reason == reason
    assert runs[0].portion_count == (2 if status == "complete" else 0)
    backend.dispose()


@pytest.mark.parametrize(
    ("status", "reason"),
    [
        ("unknown", None),
        (None, None),
        ("invalid", "invalid_metadata"),
        ("complete", "capture_failed"),
        ("unavailable", None),
        ("unavailable", "unknown_reason"),
        ("unavailable", "invalid_metadata"),
    ],
    ids=[
        "unknown-status",
        "null-status",
        "legacy-invalid-status",
        "complete-with-reason",
        "unavailable-without-reason",
        "unavailable-with-unknown-reason",
        "unavailable-with-legacy-reason",
    ],
)
def test_preview_run_reader_rejects_corrupt_persisted_status_reason_codec(
    tmp_path: Any,
    status: str | None,
    reason: str | None,
) -> None:
    backend = SQLModelBackend(
        f"sqlite:///{tmp_path / f'lineage-corrupt-{status}-{reason}.db'}",
        CCloudStorageModule(),
        use_migrations=False,
        focus_preview_enabled=True,
    )
    backend.create_tables()
    with backend.create_preview_evidence_unit_of_work() as uow:
        uow.allocation_lineage.replace_calculation_lineage(
            _run(),
            calculation_completed_at=datetime(2026, 7, 3, 4, 5, tzinfo=UTC),
        )
        uow.commit()
    _set_persisted_run_codec(
        backend,
        status=status,
        reason=reason,
        portion_count=0 if status == "unavailable" else None,
    )

    with (
        pytest.raises(PreviewAllocationEvidenceDecodeError),
        backend.create_preview_generation_read_unit_of_work() as uow,
    ):
        tuple(uow.allocation_evidence.iter_preview_allocation_runs(_scope(), ("calculation-1",)))
    backend.dispose()


def test_repository_atomically_replaces_only_the_owned_tenant_date_and_calculation(tmp_path: Any) -> None:
    connection_string = f"sqlite:///{tmp_path / 'lineage-isolation.db'}"
    backend = SQLModelBackend(
        connection_string,
        CCloudStorageModule(),
        use_migrations=False,
        focus_preview_enabled=True,
    )
    backend.create_tables()
    completed_at = datetime(2026, 7, 3, tzinfo=UTC)
    with backend.create_unit_of_work() as uow:
        uow.billing.upsert(_origin())
        uow.commit()
    with backend.create_preview_evidence_unit_of_work() as uow:
        uow.allocation_lineage.replace_calculation_lineage(_run(), calculation_completed_at=completed_at)
        uow.allocation_lineage.replace_calculation_lineage(
            _run(tenant_id="org-2", captures=()), calculation_completed_at=completed_at
        )
        uow.allocation_lineage.replace_calculation_lineage(
            _run(tracking_date=date(2026, 7, 2), captures=()), calculation_completed_at=completed_at
        )
        uow.allocation_lineage.replace_calculation_lineage(
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
    backend = SQLModelBackend(
        connection_string,
        CCloudStorageModule(),
        use_migrations=False,
        focus_preview_enabled=True,
    )
    backend.create_tables()
    completed_at = datetime(2026, 7, 3, tzinfo=UTC)
    with backend.create_unit_of_work() as uow:
        uow.billing.upsert(_origin())
        uow.commit()
    with backend.create_preview_evidence_unit_of_work() as uow:
        uow.allocation_lineage.replace_calculation_lineage(_run(), calculation_completed_at=completed_at)
        uow.commit()

    with pytest.raises(RuntimeError, match="after flush"), backend.create_preview_evidence_unit_of_work() as uow:
        session = uow.allocation_lineage._lineage._session  # type: ignore[attr-defined]
        real_flush = session.flush

        def fail_after_flush() -> None:
            real_flush()
            raise RuntimeError("after flush")

        with patch.object(session, "flush", side_effect=fail_after_flush):
            uow.allocation_lineage.replace_calculation_lineage(
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
    backend = SQLModelBackend(
        connection_string,
        CCloudStorageModule(),
        use_migrations=False,
        focus_preview_enabled=True,
    )
    backend.create_tables()
    completed_at = datetime(2026, 7, 3, tzinfo=UTC)
    with backend.create_preview_evidence_unit_of_work() as uow:
        uow.allocation_lineage.replace_calculation_lineage(
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
    backend = SQLModelBackend(
        connection_string,
        CCloudStorageModule(),
        use_migrations=False,
        focus_preview_enabled=True,
    )
    backend.create_tables()
    with backend.create_unit_of_work() as uow:
        assert isinstance(uow.chargebacks, AllocationLineageRepository)
    assert not isinstance(MockChargebackRepo(), AllocationLineageRepository)
    backend.dispose()
