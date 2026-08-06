from __future__ import annotations

from datetime import date, datetime
from typing import TYPE_CHECKING

from core.engine.orchestrator import ChargebackOrchestrator, GatherResult
from core.preview.evidence_capture import NativeSourceWindow
from plugins.confluent_cloud.source_capture import CCloudNativeSourceEvidenceCapture

if TYPE_CHECKING:
    from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend
    from plugins.confluent_cloud.models.billing import CCloudCostSourceRecord


def gather_billing_with_source_evidence(
    orchestrator: ChargebackOrchestrator,
    backend: SQLModelBackend,
    now: datetime,
) -> frozenset[date]:
    """Gather billing through the durable source-attempt lifecycle."""
    plan = orchestrator._gather_phase.plan_refresh(now)
    source_state = orchestrator._prepare_preview_source_state(plan)
    with backend.create_unit_of_work() as uow:
        billing = orchestrator._gather_phase._gather_billing(
            uow,
            plan,
            source_attempt=source_state,
        )
        uow.commit()
    orchestrator._persist_preview_source_capture(
        GatherResult(
            dates_gathered=len(billing.dates),
            errors=[],
            source_disposition=billing.source_disposition,
            source_refresh_token=billing.source_refresh_token,
            source_attempt_sequence=billing.source_attempt_sequence,
            source_capture=billing.source_capture,
            source_failure=billing.source_failure,
        )
    )
    return billing.dates


def calculate_with_lineage(
    orchestrator: ChargebackOrchestrator,
    backend: SQLModelBackend,
    tracking_date: date,
) -> int:
    """Calculate and persist the Preview lineage sidecar after generic commit."""
    with backend.create_unit_of_work() as uow:
        result = orchestrator._calculate_phase.run_with_lineage_capture(uow, tracking_date)
        uow.commit()
    orchestrator._persist_preview_lineage(result)
    return result.rows_written


def persist_source_capture(
    backend: SQLModelBackend,
    *,
    ecosystem: str,
    tenant_id: str,
    records: list[CCloudCostSourceRecord],
    captured_at: datetime,
) -> None:
    """Persist native source rows together with their completed readiness attempt."""
    refresh_start = min(record.collection_window_start for record in records)
    refresh_end = max(record.collection_window_end for record in records)
    with backend.create_preview_evidence_unit_of_work() as uow:
        attempt = uow.source_readiness.begin_attempt(
            ecosystem,
            tenant_id,
            f"fixture:{tenant_id}:{refresh_start.isoformat()}:{refresh_end.isoformat()}",
            refresh_start,
            refresh_end,
            captured_at,
        )
        CCloudNativeSourceEvidenceCapture(
            ecosystem=ecosystem,
            tenant_id=tenant_id,
            refresh_start=refresh_start,
            refresh_end=refresh_end,
            windows=(NativeSourceWindow(refresh_start, refresh_end),),
            records=tuple(records),
        ).persist(
            uow.source_windows,
            uow.source_readiness,
            attempt_sequence=attempt.attempt_sequence,
            captured_at=captured_at,
        )
        uow.commit()
