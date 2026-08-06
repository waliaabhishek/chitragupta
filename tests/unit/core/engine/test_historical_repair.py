from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest

from core.engine.orchestrator import ChargebackOrchestrator
from core.preview.evidence_capture import NativeSourceGatherResult, NativeSourceWindow
from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend
from plugins.confluent_cloud.models.billing import CCloudBillingLineItem
from plugins.confluent_cloud.source_capture import CCloudNativeSourceEvidenceCapture
from plugins.confluent_cloud.storage.module import CCloudStorageModule

ECOSYSTEM = "confluent_cloud"
TENANT_ID = "org-1"
TRACKING_DATE = date(2026, 7, 1)
START = datetime(2026, 7, 1, tzinfo=UTC)
END = START + timedelta(days=1)


def _line(total_cost: str) -> CCloudBillingLineItem:
    return CCloudBillingLineItem(
        ecosystem=ECOSYSTEM,
        tenant_id=TENANT_ID,
        timestamp=START,
        env_id="env-1",
        resource_id="lkc-1",
        product_category="KAFKA",
        product_type="KAFKA_STORAGE",
        quantity=Decimal("1"),
        unit_price=Decimal(total_cost),
        total_cost=Decimal(total_cost),
    )


class _HistoricalCostInput:
    def __init__(self, line: CCloudBillingLineItem) -> None:
        self._result = NativeSourceGatherResult(
            billing_lines=(line,),
            capture=CCloudNativeSourceEvidenceCapture(
                ecosystem=ECOSYSTEM,
                tenant_id=TENANT_ID,
                refresh_start=START,
                refresh_end=END,
                windows=(NativeSourceWindow(start=START, end=END),),
                records=(),
            ),
            capture_failure=None,
        )

    def gather_with_native_source_evidence(
        self,
        tenant_id: str,
        start: datetime,
        end: datetime,
    ) -> NativeSourceGatherResult:
        assert (tenant_id, start, end) == (TENANT_ID, START, END)
        return self._result


def _orchestrator(storage_backend: Any, line: CCloudBillingLineItem) -> ChargebackOrchestrator:
    orchestrator = object.__new__(ChargebackOrchestrator)
    orchestrator._gather_phase = SimpleNamespace(  # noqa: SLF001
        _bundle=SimpleNamespace(plugin=SimpleNamespace(get_cost_input=lambda: _HistoricalCostInput(line)))
    )
    orchestrator._ecosystem = ECOSYSTEM  # noqa: SLF001
    orchestrator._tenant_id = TENANT_ID  # noqa: SLF001
    orchestrator._storage_backend = storage_backend  # noqa: SLF001
    orchestrator._calculate_phase = MagicMock()  # noqa: SLF001
    return orchestrator


class _MissingWriterUnitOfWork:
    billing = object()

    def __enter__(self) -> _MissingWriterUnitOfWork:
        return self

    def __exit__(self, *_args: object) -> None:
        return None


class _MissingWriterBackend:
    def create_unit_of_work(self) -> _MissingWriterUnitOfWork:
        return _MissingWriterUnitOfWork()


def test_historical_repair_rejects_storage_without_exact_date_replacement_writer() -> None:
    orchestrator = _orchestrator(_MissingWriterBackend(), _line("8"))

    with pytest.raises(RuntimeError, match="historical repair billing replacement is unavailable"):
        orchestrator.repair_historical_date(TRACKING_DATE)

    orchestrator._calculate_phase.run_with_lineage_capture.assert_not_called()  # noqa: SLF001


def test_historical_repair_rolls_back_billing_replacement_when_calculation_fails(tmp_path: Any) -> None:
    backend = SQLModelBackend(
        f"sqlite:///{tmp_path / 'repair-rollback.db'}",
        CCloudStorageModule(),
        use_migrations=False,
    )
    backend.create_tables()
    original = _line("3")
    with backend.create_unit_of_work() as uow:
        uow.billing.upsert(original)
        uow.commit()

    orchestrator = _orchestrator(backend, _line("8"))
    orchestrator._calculate_phase.run_with_lineage_capture.side_effect = RuntimeError("calculation failed")  # noqa: SLF001

    with pytest.raises(RuntimeError, match="calculation failed"):
        orchestrator.repair_historical_date(TRACKING_DATE)

    with backend.create_read_only_unit_of_work() as uow:
        retained = uow.billing.find_by_date(ECOSYSTEM, TENANT_ID, TRACKING_DATE)
    assert retained == [original]
    backend.dispose()
