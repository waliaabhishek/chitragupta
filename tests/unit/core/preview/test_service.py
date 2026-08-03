from __future__ import annotations

import csv
import io
import json
import threading
from collections.abc import Callable, Iterator
from concurrent.futures import Future
from dataclasses import replace
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import pytest
from sqlalchemy import text

from core.config.models import StorageConfig, TenantConfig
from core.models.chargeback import AllocationDetail, ChargebackRow, CostType
from core.models.identity import CoreIdentity
from core.models.pipeline import PipelineState
from core.models.resource import CoreResource, ResourceStatus
from core.storage.backends.sqlmodel.engine import get_or_create_engine
from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend
from plugins.confluent_cloud.models.billing import CCloudBillingLineItem, CCloudCostSourceRecord
from plugins.confluent_cloud.storage.module import CCloudStorageModule
from tests.integration.core.api.backend_provider import FixedTenantBackendProvider
from tests.unit.core.preview.conftest import preview_module
from tests.unit.core.preview.test_revision_mapping import assert_public_known_gaps


class ControlledExecutor:
    """Complete PreviewExecutor fake with caller-owned execution and shutdown."""

    def __init__(self) -> None:
        self.pending: list[tuple[Callable[[], None], Future[None]]] = []
        self.shutdown_calls: list[tuple[bool, bool]] = []

    def submit(self, task: Callable[[], None]) -> Future[None]:
        future: Future[None] = Future()
        self.pending.append((task, future))
        return future

    def run_all(self) -> None:
        while self.pending:
            task, future = self.pending.pop(0)
            if future.set_running_or_notify_cancel():
                try:
                    task()
                except BaseException as exc:
                    future.set_exception(exc)
                    raise
                else:
                    future.set_result(None)

    def shutdown(self, wait: bool = True, *, cancel_futures: bool = False) -> None:
        self.shutdown_calls.append((wait, cancel_futures))


def _tenant_config(connection_string: str) -> TenantConfig:
    return TenantConfig.model_validate(
        {
            "ecosystem": "confluent_cloud",
            "tenant_id": "tenant-1",
            "storage": StorageConfig(connection_string=connection_string),
            "focus_preview": {
                "commercial_profile": "direct_payg",
                "billing_currency": "USD",
                "effective_start_date": "2020-01-01",
                "effective_end_date": "2030-01-01",
            },
        }
    )


def _source(**overrides: object) -> CCloudCostSourceRecord:
    values: dict[str, object] = {
        "ecosystem": "confluent_cloud",
        "tenant_id": "tenant-1",
        "source_record_id": "provider:cost-1",
        "identity_scheme": "provider_cost_id",
        "provider_cost_id": "cost-1",
        "source_period_start": datetime(2026, 7, 1, tzinfo=UTC),
        "source_period_end": datetime(2026, 7, 2, tzinfo=UTC),
        "collection_window_start": datetime(2026, 6, 30, tzinfo=UTC),
        "collection_window_end": datetime(2026, 7, 3, tzinfo=UTC),
        "evidence_scope_start": datetime(2026, 7, 1, tzinfo=UTC),
        "evidence_scope_end": datetime(2026, 7, 2, tzinfo=UTC),
        "allocation_timestamp": datetime(2026, 7, 1, tzinfo=UTC),
        "retention_timestamp": datetime(2026, 7, 1, tzinfo=UTC),
        "granularity": "DAILY",
        "product": "KAFKA",
        "line_type": "KAFKA_STORAGE",
        "amount": Decimal("8"),
        "original_amount": Decimal("10"),
        "discount_amount": Decimal("2"),
        "price": Decimal("2"),
        "quantity": Decimal("5"),
        "unit": "GB",
        "description": "Kafka storage usage",
        "network_access_type": "PUBLIC_INTERNET",
        "resource_id": "lkc-1",
        "resource_name": "Orders",
        "environment_id": "env-1",
        "tier_dimensions": {"upper_bound": "100", "lower_bound": "0"},
        "malformed": False,
        "diagnostics": (),
        "raw_payload": {"id": "cost-1"},
    }
    values.update(overrides)
    values.setdefault("billing_timestamp", values["allocation_timestamp"])
    values.setdefault("billing_env_id", values["environment_id"] or "")
    values.setdefault("billing_resource_id", values["resource_id"] or "unresolved_billing_0")
    values.setdefault("billing_product_type", values["line_type"])
    values.setdefault("billing_product_category", values["product"])
    return CCloudCostSourceRecord(**values)  # type: ignore[arg-type]


def _aggregate(**overrides: object) -> CCloudBillingLineItem:
    values: dict[str, object] = {
        "ecosystem": "confluent_cloud",
        "tenant_id": "tenant-1",
        "timestamp": datetime(2026, 7, 1, tzinfo=UTC),
        "env_id": "env-1",
        "resource_id": "lkc-1",
        "product_category": "KAFKA",
        "product_type": "KAFKA_STORAGE",
        "quantity": Decimal("5"),
        "unit_price": Decimal("2"),
        "total_cost": Decimal("8"),
        "currency": "USD",
        "granularity": "daily",
        "metadata": {},
    }
    values.update(overrides)
    return CCloudBillingLineItem(**values)  # type: ignore[arg-type]


def _allocation(**overrides: object) -> ChargebackRow:
    values: dict[str, object] = {
        "ecosystem": "confluent_cloud",
        "tenant_id": "tenant-1",
        "timestamp": datetime(2026, 7, 1, tzinfo=UTC),
        "resource_id": "lkc-1",
        "product_category": "KAFKA",
        "product_type": "KAFKA_STORAGE",
        "identity_id": "sa-1",
        "cost_type": CostType.USAGE,
        "amount": Decimal("8"),
        "allocation_method": "direct",
        "allocation_detail": None,
        "tags": {},
        "metadata": {"env_id": "env-1"},
    }
    values.update(overrides)
    return ChargebackRow(**values)  # type: ignore[arg-type]


def _fallback_evidence(
    *,
    product: str,
    line_type: str,
    resource_id: str = "lkc-1",
    resource_name: str = "Orders",
) -> tuple[CCloudCostSourceRecord, CCloudBillingLineItem, ChargebackRow]:
    source = _source(
        product=product,
        line_type=line_type,
        description="Ordinary provider usage",
        resource_id=resource_id,
        resource_name=resource_name,
        billing_resource_id=resource_id,
        billing_product_type=line_type,
        billing_product_category=product,
    )
    aggregate = _aggregate(
        resource_id=resource_id,
        product_category=product,
        product_type=line_type,
    )
    allocation = _allocation(
        resource_id=resource_id,
        product_category=product,
        product_type=line_type,
        identity_id=resource_id,
        cost_type=CostType.SHARED,
        allocation_method="unknown",
        allocation_detail=AllocationDetail.USING_UNKNOWN_ALLOCATOR,
    )
    return source, aggregate, allocation


def _context_resource(
    resource_id: str,
    resource_type: str,
    *,
    parent_id: str | None = "env-1",
    metadata: dict[str, object] | None = None,
) -> CoreResource:
    return CoreResource(
        ecosystem="confluent_cloud",
        tenant_id="tenant-1",
        resource_id=resource_id,
        resource_type=resource_type,
        display_name=f"Provider {resource_id}",
        parent_id=parent_id,
        status=ResourceStatus.ACTIVE,
        metadata=({"provider_cloud": "AWS", "provider_region": "us-east-1"} if metadata is None else metadata),
    )


def _replace_preview_lineage(
    backend: SQLModelBackend,
    *,
    aggregate: CCloudBillingLineItem,
    allocation: ChargebackRow,
    pipeline_state: PipelineState,
) -> None:
    from core.engine.allocation_lineage import build_allocation_lineage_capture
    from core.storage.interface import AllocationLineageRunCapture

    assert pipeline_state.calculation_id is not None
    completed_at = pipeline_state.calculation_completed_at or datetime(2026, 7, 3, 2, tzinfo=UTC)
    run = AllocationLineageRunCapture(
        ecosystem="confluent_cloud",
        tenant_id="tenant-1",
        tracking_date=aggregate.timestamp.date(),
        calculation_id=pipeline_state.calculation_id,
        captures=(build_allocation_lineage_capture(origin=aggregate, rows=(allocation,)),),
    )
    with backend.create_preview_evidence_unit_of_work() as uow:
        uow.allocation_lineage.replace_calculation_lineage(
            run,
            calculation_completed_at=completed_at,
        )
        uow.commit()


def _replace_compatibility_only_lineage(
    backend: SQLModelBackend,
    *,
    aggregate: CCloudBillingLineItem,
    allocation: ChargebackRow,
    pipeline_state: PipelineState,
) -> None:
    from core.engine.allocation_lineage import build_allocation_lineage_capture
    from core.storage.interface import AllocationLineageRunCapture

    assert pipeline_state.calculation_id is not None
    completed_at = pipeline_state.calculation_completed_at or datetime(2026, 7, 3, 2, tzinfo=UTC)
    run = AllocationLineageRunCapture(
        ecosystem="confluent_cloud",
        tenant_id="tenant-1",
        tracking_date=aggregate.timestamp.date(),
        calculation_id=pipeline_state.calculation_id,
        captures=(build_allocation_lineage_capture(origin=aggregate, rows=(allocation,)),),
    )
    with backend.create_unit_of_work() as uow:
        uow.chargebacks.replace_calculation_lineage(  # type: ignore[attr-defined]
            run,
            calculation_completed_at=completed_at,
        )
        uow.commit()


def _seed(
    backend: SQLModelBackend,
    *,
    source: CCloudCostSourceRecord | None = None,
    synthesize_default_source: bool = True,
    compatibility_only_lineage: bool = False,
    aggregate: CCloudBillingLineItem | None = None,
    allocation: ChargebackRow | None = None,
    state: PipelineState | None = None,
    include_resource: bool = True,
    include_environment: bool = True,
    include_identity: bool = True,
) -> None:
    pipeline_state = state or PipelineState(
        ecosystem="confluent_cloud",
        tenant_id="tenant-1",
        tracking_date=date(2026, 7, 1),
        billing_gathered=True,
        resources_gathered=True,
        chargeback_calculated=True,
        calculation_id="calculation-1",
        calculation_completed_at=datetime(2026, 7, 3, 2, tzinfo=UTC),
        calculation_run_id=None,
    )
    with backend.create_unit_of_work() as uow:
        uow.resources.upsert(
            CoreResource(
                ecosystem="confluent_cloud",
                tenant_id="tenant-1",
                resource_id="11111111-2222-4333-8444-555555555555",
                resource_type="organization",
                display_name="Provider billing organization",
                status=ResourceStatus.ACTIVE,
                metadata={"organization_binding_state": "bound"},
            )
        )
        if include_environment:
            uow.resources.upsert(
                CoreResource(
                    ecosystem="confluent_cloud",
                    tenant_id="tenant-1",
                    resource_id="env-1",
                    resource_type="environment",
                    display_name="Production",
                    status=ResourceStatus.ACTIVE,
                )
            )
        if include_resource:
            uow.resources.upsert(
                CoreResource(
                    ecosystem="confluent_cloud",
                    tenant_id="tenant-1",
                    resource_id="lkc-1",
                    resource_type="kafka_cluster",
                    display_name="Orders",
                    parent_id="env-1",
                    status=ResourceStatus.ACTIVE,
                    created_at=datetime(2026, 1, 1, tzinfo=UTC),
                    metadata={
                        "cloud": "aws",
                        "region": "us-east-1",
                        "provider_cloud": "AWS",
                        "provider_region": "us-east-1",
                    },
                )
            )
        if include_identity:
            uow.identities.upsert(
                CoreIdentity(
                    ecosystem="confluent_cloud",
                    tenant_id="tenant-1",
                    identity_id="sa-1",
                    identity_type="service_account",
                    display_name="Orders service",
                    created_at=datetime(2026, 1, 1, tzinfo=UTC),
                )
            )
        if aggregate is not None:
            uow.billing.upsert(aggregate)
        if allocation is not None:
            uow.chargebacks.upsert_batch([allocation])
        uow.pipeline_state.upsert(pipeline_state)
        uow.commit()
    from core.preview.organization_authority import OrganizationAuthorityFinalStatus

    with backend.create_preview_evidence_unit_of_work() as evidence_uow:
        organization_attempt = evidence_uow.organization_authority.begin(
            "confluent_cloud",
            "tenant-1",
            datetime(2026, 7, 3, tzinfo=UTC),
        )
        evidence_uow.organization_authority.finalize(
            organization_attempt.attempt_sequence,
            OrganizationAuthorityFinalStatus.AVAILABLE,
            completed_at=datetime(2026, 7, 3, 0, 0, 1, tzinfo=UTC),
            organization_id="11111111-2222-4333-8444-555555555555",
            reason=None,
        )
        lineage_source = source
        if lineage_source is None and synthesize_default_source and aggregate is not None and allocation is not None:
            lineage_source = _source()
        if lineage_source is not None:
            from core.preview.evidence_capture import NativeSourceWindow
            from plugins.confluent_cloud.source_capture import CCloudNativeSourceEvidenceCapture

            refresh_start = datetime(2026, 6, 30, tzinfo=UTC)
            refresh_end = datetime(2026, 7, 3, tzinfo=UTC)
            source_attempt = evidence_uow.source_readiness.begin_attempt(
                "confluent_cloud",
                "tenant-1",
                "fixture-source-attempt",
                refresh_start,
                refresh_end,
                datetime(2026, 7, 3, tzinfo=UTC),
            )
            CCloudNativeSourceEvidenceCapture(
                ecosystem="confluent_cloud",
                tenant_id="tenant-1",
                refresh_start=refresh_start,
                refresh_end=refresh_end,
                windows=(NativeSourceWindow(refresh_start, refresh_end),),
                records=(lineage_source,),
            ).persist(
                evidence_uow.source_windows,
                evidence_uow.source_readiness,
                attempt_sequence=source_attempt.attempt_sequence,
                captured_at=datetime(2026, 7, 3, 0, 0, 1, tzinfo=UTC),
            )
        evidence_uow.commit()
    if aggregate is not None and allocation is not None and pipeline_state.calculation_id:
        if compatibility_only_lineage:
            _replace_compatibility_only_lineage(
                backend,
                aggregate=aggregate,
                allocation=allocation,
                pipeline_state=pipeline_state,
            )
        elif lineage_source is not None:
            _replace_preview_lineage(
                backend,
                aggregate=aggregate,
                allocation=allocation,
                pipeline_state=pipeline_state,
            )


def _replace_source_capture(
    backend: SQLModelBackend,
    sources: list[CCloudCostSourceRecord],
) -> None:
    from core.preview.evidence_capture import NativeSourceWindow
    from plugins.confluent_cloud.source_capture import CCloudNativeSourceEvidenceCapture

    refresh_start = min(source.collection_window_start for source in sources)
    refresh_end = max(source.collection_window_end for source in sources)
    with backend.create_preview_evidence_unit_of_work() as uow:
        attempt = uow.source_readiness.begin_attempt(
            "confluent_cloud",
            "tenant-1",
            f"fixture-replacement:{refresh_start.isoformat()}:{refresh_end.isoformat()}",
            refresh_start,
            refresh_end,
            datetime(2026, 7, 3, tzinfo=UTC),
        )
        CCloudNativeSourceEvidenceCapture(
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
            refresh_start=refresh_start,
            refresh_end=refresh_end,
            windows=(NativeSourceWindow(refresh_start, refresh_end),),
            records=tuple(sources),
        ).persist(
            uow.source_windows,
            uow.source_readiness,
            attempt_sequence=attempt.attempt_sequence,
            captured_at=datetime(2026, 7, 3, 0, 0, 1, tzinfo=UTC),
        )
        uow.commit()


def _corrupt_persisted_reconciliation_value(
    backend: SQLModelBackend,
    *,
    source_overrides: dict[str, object],
    aggregate_overrides: dict[str, object],
    allocation_overrides: dict[str, object],
) -> None:
    if source_overrides:
        field, value = next(iter(source_overrides.items()))
        statements = {
            "amount": text("UPDATE ccloud_cost_source_records SET amount = :value"),
            "original_amount": text("UPDATE ccloud_cost_source_records SET original_amount = :value"),
            "price": text("UPDATE ccloud_cost_source_records SET price = :value"),
        }
    elif aggregate_overrides:
        field, value = next(iter(aggregate_overrides.items()))
        statements = {
            "quantity": text("UPDATE ccloud_billing SET quantity = :value"),
            "unit_price": text("UPDATE ccloud_billing SET unit_price = :value"),
            "total_cost": text("UPDATE ccloud_billing SET total_cost = :value"),
        }
    else:
        field = "allocated_cost"
        value = allocation_overrides["amount"]
        assert isinstance(value, Decimal) and value.is_finite() and value >= 0
        statements = {
            "allocated_cost": text(
                "UPDATE ccloud_preview_source_allocation_lineage_portions SET allocated_cost = :value"
            )
        }
    statement = statements.get(field)
    assert statement is not None
    engine = get_or_create_engine(backend._connection_string)
    with engine.begin() as connection:
        connection.execute(
            statement,
            {"value": str(value)},
        )


def _runtime(tmp_path: Path, backend: SQLModelBackend, executor: ControlledExecutor) -> object:
    artifacts = preview_module("artifacts")
    service = preview_module("service")
    return service.PreviewRuntime(
        artifact_store=artifacts.LocalPreviewArtifactStore(tmp_path / "artifacts"),
        backend_provider=FixedTenantBackendProvider({"production": backend}),
        max_workers=1,
        clock=lambda: datetime(2026, 7, 4, tzinfo=UTC),
        request_id_factory=lambda: "request-1",
        executor=executor,
    )


def _submit(runtime: object, backend: SQLModelBackend) -> object:
    return runtime.submit(  # type: ignore[attr-defined,no-any-return]
        tenant_name="production",
        tenant_config=_tenant_config(backend._connection_string),
        backend=backend,
        start_date=date(2026, 7, 1),
        end_date=date(2026, 7, 2),
        grain="daily",
        column_profile="full",
        effective_columns=preview_module("mapping").FOCUS_1_4_FULL_PROFILE_COLUMNS,
    )


def _ready_request(tmp_path: Path) -> tuple[object, object, SQLModelBackend, ControlledExecutor]:
    backend = SQLModelBackend(
        f"sqlite:///{tmp_path / 'preview.db'}",
        CCloudStorageModule(),
        use_migrations=False,
        focus_preview_enabled=True,
    )
    backend.create_tables()
    _seed(backend, source=_source(), aggregate=_aggregate(), allocation=_allocation())
    executor = ControlledExecutor()
    runtime = _runtime(tmp_path, backend, executor)
    queued = _submit(runtime, backend)
    executor.run_all()
    ready = runtime.get_request(  # type: ignore[attr-defined]
        backend=backend,
        request_id=queued.request_id,
        ecosystem="confluent_cloud",
        tenant_id="tenant-1",
    )
    return runtime, ready, backend, executor


def test_request_specific_reconcile_handles_missing_metadata_by_type_not_message(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backend = SQLModelBackend(
        f"sqlite:///{tmp_path / 'preview.db'}",
        CCloudStorageModule(),
        use_migrations=False,
        focus_preview_enabled=True,
    )
    backend.create_tables()
    executor = ControlledExecutor()
    runtime = _runtime(tmp_path, backend, executor)
    persistence = preview_module("persistence")

    def missing_metadata(
        _repository: object,
        *,
        request_id: str,
        ecosystem: str,
        tenant_id: str,
        now: datetime,
    ) -> None:
        del request_id, ecosystem, tenant_id, now
        raise persistence.PreviewEffectiveColumnsMetadataMissingError("diagnostic wording changed")

    monkeypatch.setattr(
        persistence.SQLModelPreviewRequestRepository,
        "expire_ready_request",
        missing_metadata,
    )
    try:
        runtime.reconcile_expiry(
            backend=backend,
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
            request_id="request-1",
        )
    finally:
        runtime.close()
        backend.dispose()


def test_request_specific_reconcile_does_not_special_case_old_message_on_value_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backend = SQLModelBackend(
        f"sqlite:///{tmp_path / 'preview.db'}",
        CCloudStorageModule(),
        use_migrations=False,
        focus_preview_enabled=True,
    )
    backend.create_tables()
    executor = ControlledExecutor()
    runtime = _runtime(tmp_path, backend, executor)
    persistence = preview_module("persistence")
    service = preview_module("service")

    def unrelated_value_error(
        _repository: object,
        *,
        request_id: str,
        ecosystem: str,
        tenant_id: str,
        now: datetime,
    ) -> None:
        del request_id, ecosystem, tenant_id, now
        raise ValueError("preview effective columns metadata is required")

    monkeypatch.setattr(
        persistence.SQLModelPreviewRequestRepository,
        "expire_ready_request",
        unrelated_value_error,
    )
    try:
        with pytest.raises(service.PreviewRecoveryUnavailable):
            runtime.reconcile_expiry(
                backend=backend,
                ecosystem="confluent_cloud",
                tenant_id="tenant-1",
                request_id="request-1",
            )
    finally:
        runtime.close()
        backend.dispose()


def test_controlled_runtime_commits_queued_before_running_and_reaches_ready(tmp_path: Path) -> None:
    backend = SQLModelBackend(
        f"sqlite:///{tmp_path / 'preview.db'}",
        CCloudStorageModule(),
        use_migrations=False,
        focus_preview_enabled=True,
    )
    backend.create_tables()
    _seed(backend, source=_source(), aggregate=_aggregate(), allocation=_allocation())
    executor = ControlledExecutor()
    runtime = _runtime(tmp_path, backend, executor)
    try:
        queued = _submit(runtime, backend)
        persisted_queued = runtime.get_request(
            backend=backend,
            request_id=queued.request_id,
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
        )
        assert queued.status.value == "queued"
        assert persisted_queued.status.value == "queued"
        assert len(executor.pending) == 1

        executor.run_all()
        ready = runtime.get_request(
            backend=backend,
            request_id=queued.request_id,
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
        )
        assert ready.status.value == "ready"
        assert ready.diagnostic is None
        assert ready.source_snapshot.source_through == datetime(2026, 7, 3, tzinfo=UTC)
        assert ready.source_snapshot.calculation_timestamp == datetime(2026, 7, 3, 2, tzinfo=UTC)
        assert ready.package.manifest.name == "manifest.json"
        assert [file.name for file in ready.package.files] == ["cost-and-usage.csv", "focus-metadata.json"]
    finally:
        runtime.close()
        backend.dispose()
    assert executor.shutdown_calls == []


@pytest.mark.parametrize("wait", [False, True])
def test_runtime_shutdown_terminalizes_or_drains_persisted_work_and_stops_heartbeat(
    tmp_path: Path,
    wait: bool,
) -> None:
    backend = SQLModelBackend(
        f"sqlite:///{tmp_path / 'shutdown.db'}",
        CCloudStorageModule(),
        use_migrations=False,
        focus_preview_enabled=True,
    )
    backend.create_tables()
    _seed(backend, source=_source(), aggregate=_aggregate(), allocation=_allocation())
    executor = ControlledExecutor()
    artifacts = preview_module("artifacts")
    service = preview_module("service")
    identifiers = iter(("request-running", "request-waiting"))
    runtime = service.PreviewRuntime(
        artifact_store=artifacts.LocalPreviewArtifactStore(tmp_path / "artifacts"),
        backend_provider=FixedTenantBackendProvider({"production": backend}),
        max_workers=1,
        clock=lambda: datetime(2026, 7, 4, tzinfo=UTC),
        request_id_factory=lambda: next(identifiers),
        executor=executor,
    )
    close_thread: threading.Thread | None = None
    try:
        first = _submit(runtime, backend)
        second = _submit(runtime, backend)
        assert len(executor.pending) == 1
        if wait:
            close_thread = threading.Thread(target=lambda: runtime.close(wait=True))
            close_thread.start()
            close_thread.join(timeout=0.1)
            assert close_thread.is_alive()
            assert not runtime._heartbeat_stop.is_set()  # noqa: SLF001
        else:
            runtime.close(wait=False)
            waiting = runtime.get_request(
                backend=backend,
                request_id=second.request_id,
                ecosystem="confluent_cloud",
                tenant_id="tenant-1",
            )
            assert waiting.status.value == "failed"
            assert waiting.diagnostic.code == "preview_generation_interrupted"
            assert set(runtime._lease_targets) == {first.request_id}  # noqa: SLF001
            assert not runtime._heartbeat_stop.is_set()  # noqa: SLF001

        executor.run_all()
        if close_thread is not None:
            close_thread.join(timeout=5)
            assert not close_thread.is_alive()
            drained = runtime.get_request(
                backend=backend,
                request_id=second.request_id,
                ecosystem="confluent_cloud",
                tenant_id="tenant-1",
            )
            assert drained.status.value == "ready"
        assert runtime._heartbeat_stop.wait(timeout=5)  # noqa: SLF001
        assert runtime._lease_targets == {}  # noqa: SLF001
        assert executor.shutdown_calls == []
    finally:
        executor.run_all()
        runtime._heartbeat_stop.set()  # noqa: SLF001
        heartbeat = runtime._heartbeat_thread  # noqa: SLF001
        if heartbeat is not None:
            heartbeat.join(timeout=5)
        if close_thread is not None:
            close_thread.join(timeout=5)
        backend.dispose()


def test_daily_full_package_maps_provider_financial_account_sku_and_invoice_evidence(tmp_path: Path) -> None:
    runtime, ready, backend, _executor = _ready_request(tmp_path)
    mapping = preview_module("mapping")
    try:
        csv_bytes = runtime.read_file_bytes(ready, "cost-and-usage.csv")
        manifest_bytes = runtime.read_manifest_bytes(ready)
        rows = list(csv.DictReader(io.StringIO(csv_bytes.decode("utf-8"), newline="")))
        assert list(rows[0]) == [*mapping.FOCUS_1_4_FULL_COLUMNS, *mapping.CUSTOM_EVIDENCE_COLUMNS]
        assert len(mapping.FOCUS_1_4_FULL_COLUMNS) == 65
        assert len(mapping.CUSTOM_EVIDENCE_COLUMNS) == 12
        assert rows[0]["BilledCost"] == "8"
        assert rows[0]["EffectiveCost"] == "8"
        assert rows[0]["ListCost"] == "10"
        assert rows[0]["ContractedCost"] == "10"
        assert rows[0]["BillingAccountId"] == "11111111-2222-4333-8444-555555555555"
        assert rows[0]["BillingAccountId"] != "tenant-1"
        assert rows[0]["BillingAccountName"] == "Provider billing organization"
        assert rows[0]["BillingAccountType"] == "Organization"
        assert rows[0]["BillingCurrency"] == "USD"
        assert rows[0]["BillingPeriodStart"] == "2026-07-01T00:00:00Z"
        assert rows[0]["BillingPeriodEnd"] == "2026-08-01T00:00:00Z"
        assert rows[0]["ChargeClass"] == ""
        assert rows[0]["ChargeCategory"] == "Usage"
        assert rows[0]["ChargeFrequency"] == "Usage-Based"
        assert rows[0]["ConsumedQuantity"] == rows[0]["PricingQuantity"] == "5"
        assert rows[0]["ConsumedUnit"] == rows[0]["PricingUnit"] == "GB"
        assert rows[0]["ContractedUnitPrice"] == rows[0]["ListUnitPrice"] == "2"
        assert rows[0]["HostProviderName"] == "AWS"
        assert rows[0]["RegionId"] == "us-east-1"
        assert rows[0]["RegionName"] == ""
        assert rows[0]["InvoiceId"] == ""
        assert rows[0]["InvoiceDetailId"] == ""
        assert rows[0]["InvoiceIssuerName"] == "Confluent Cloud"
        assert rows[0]["ServiceProviderName"] == rows[0]["InvoiceIssuerName"]
        assert rows[0]["ListUnitPrice"] == "2"
        assert rows[0]["PricingCategory"] == "Standard"
        assert rows[0]["PricingCurrency"] == "USD"
        assert rows[0]["PricingCurrencyContractedUnitPrice"] == rows[0]["PricingCurrencyListUnitPrice"] == "2"
        assert rows[0]["PricingCurrencyEffectiveCost"] == "8"
        assert rows[0]["PricingCurrencyListUnitPrice"] == "2"
        assert rows[0]["PricingQuantity"] == "5"
        assert rows[0]["PricingUnit"] == "GB"
        assert rows[0]["ServiceCategory"] == "Integration"
        assert rows[0]["ServiceName"] == "Confluent Cloud Apache Kafka"
        assert rows[0]["ServiceSubcategory"] == "Messaging"
        assert rows[0]["SkuId"].startswith("chitragupta:confluent-cloud:sku:v1:")
        assert rows[0]["SkuPriceId"].startswith("chitragupta:confluent-cloud:sku-price:v1:")
        assert rows[0]["SkuMeter"] == "GB"
        assert json.loads(rows[0]["SkuPriceDetails"])
        sku_components = json.loads(rows[0]["x_ChitraguptaSkuComponents"])
        assert sku_components["schema_version"] == "v1"
        assert sku_components["sku"] == {"line_type": "KAFKA_STORAGE", "product": "KAFKA"}
        assert sku_components["sku_price"]["cloud"] == "AWS"
        assert sku_components["sku_price"]["region"] == "us-east-1"
        assert rows[0]["SubAccountId"] == "env-1"
        assert rows[0]["SubAccountName"] == "Production"
        assert rows[0]["SubAccountType"] == "Environment"
        assert rows[0]["x_ChitraguptaBillingScopeId"]
        assert rows[0]["x_ChitraguptaBillingScopeId"] not in {"tenant-1", rows[0]["BillingAccountId"]}
        assert rows[0]["x_ConfluentTierDimensions"] == '{"lower_bound":"0","upper_bound":"100"}'
        assert csv_bytes.endswith(b"\n")
        assert b"\r\n" not in csv_bytes

        manifest = json.loads(manifest_bytes)
        assert manifest["schema_version"] == "chitragupta.preview-manifest.v1"
        assert manifest["target_focus_version"] == "1.4"
        assert manifest["conformance_status"] == "non_conforming"
        assert manifest["mapping_profile_version"] == "focus-1.4-preview-v1"
        assert_public_known_gaps(manifest)
        gap_codes = {gap["code"] for gap in manifest["known_gaps"]}
        assert "provider_billing_currency_field_unavailable" not in gap_codes
        assert "task_254_04_applicability_and_provider_mapping_pending" not in gap_codes
        assert "billing_account_and_issuer_mapping_pending" not in gap_codes
        assert "provider_host_display_name_unavailable" in gap_codes
        assert "provider_region_display_name_unavailable" in gap_codes
        assert "derived_sku_identity_not_provider_authoritative" in gap_codes
        assert manifest["source_snapshot"]["source_through"] == "2026-07-03T00:00:00Z"
        assert "ContractedUnitPrice" not in manifest["profile_not_applicable_columns"]
        assert "PricingCurrencyContractedUnitPrice" not in manifest["profile_not_applicable_columns"]
        assert manifest["reconciliation"] == {
            "source_cost": "8",
            "allocated_cost": "8",
            "difference": "0",
            "source_quantity": "5",
            "allocated_quantity": "5",
            "quantity_difference": "0",
        }
    finally:
        runtime.close()
        backend.dispose()


@pytest.mark.parametrize(
    ("native_product", "expected_category", "expected_name", "expected_subcategory"),
    [
        ("KAFKA", "Integration", "Confluent Cloud Apache Kafka", "Messaging"),
        ("Provider Product / β", "Other", "Provider Product / β", "Other (Other)"),
    ],
)
def test_fallback_package_preserves_native_evidence_allocation_lineage_and_manifest_contract(
    tmp_path: Path,
    native_product: str,
    expected_category: str,
    expected_name: str,
    expected_subcategory: str,
) -> None:
    backend = SQLModelBackend(
        f"sqlite:///{tmp_path / 'preview.db'}",
        CCloudStorageModule(),
        use_migrations=False,
        focus_preview_enabled=True,
    )
    backend.create_tables()
    native_line_type = "Future Usage / β"
    source, aggregate, allocation = _fallback_evidence(
        product=native_product,
        line_type=native_line_type,
    )
    _seed(backend, source=source, aggregate=aggregate, allocation=allocation)
    executor = ControlledExecutor()
    runtime = _runtime(tmp_path, backend, executor)
    try:
        queued = _submit(runtime, backend)
        executor.run_all()
        ready = runtime.get_request(
            backend=backend,
            request_id=queued.request_id,
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
        )

        assert ready.status.value == "ready", ready.diagnostic
        rows = list(
            csv.DictReader(io.StringIO(runtime.read_file_bytes(ready, "cost-and-usage.csv").decode(), newline=""))
        )
        assert len(rows) == 1
        row = rows[0]
        assert row["ChargeCategory"] == "Usage"
        assert row["ChargeFrequency"] == "Usage-Based"
        assert row["ServiceCategory"] == expected_category
        assert row["ServiceName"] == expected_name
        assert row["ServiceSubcategory"] == expected_subcategory
        assert row["x_ConfluentProduct"] == native_product
        assert row["x_ConfluentLineType"] == native_line_type
        assert row["AllocatedMethodId"] == "unknown"
        assert row["x_ChitraguptaAllocationMethodVersion"] == "v1"
        assert json.loads(row["AllocatedMethodDetails"]) == {
            "allocation_detail": "using_unknown_allocator",
            "metadata": {"env_id": "env-1"},
            "target_kind": "resource",
        }
        assert row["AllocatedResourceId"] == "lkc-1"
        assert row["BilledCost"] == row["EffectiveCost"] == "8"
        assert row["ConsumedQuantity"] == row["PricingQuantity"] == "5"

        manifest = json.loads(runtime.read_manifest_bytes(ready))
        assert set(manifest) == {
            "column_profile",
            "conformance_status",
            "effective_columns",
            "end_date",
            "evidence_coverage",
            "files",
            "generated_at",
            "grain",
            "known_gaps",
            "lifecycle",
            "mapping_profile_version",
            "month",
            "monthly_status",
            "package_type",
            "profile_not_applicable_columns",
            "reconciliation",
            "request_id",
            "schema_version",
            "source_snapshot",
            "start_date",
            "target_focus_version",
            "tenant_name",
            "validation",
        }
        assert set(manifest["validation"]) == {
            "artifact_integrity",
            "mapping_errors",
            "mapping_profile_version",
            "rows",
            "source_records",
            "status",
        }
        assert_public_known_gaps(manifest)
        assert manifest["target_focus_version"] == "1.4"
        assert manifest["conformance_status"] == "non_conforming"
        assert manifest["mapping_profile_version"] == "focus-1.4-preview-v1"
        assert manifest["validation"]["source_records"] == manifest["validation"]["rows"] == 1
        assert manifest["reconciliation"] == {
            "source_cost": "8",
            "allocated_cost": "8",
            "difference": "0",
            "source_quantity": "5",
            "allocated_quantity": "5",
            "quantity_difference": "0",
        }
        assert all("fallback" not in key.casefold() for key in manifest)
    finally:
        runtime.close()
        backend.dispose()


def test_unknown_product_connector_uses_parent_provider_authority_and_preserves_origin(
    tmp_path: Path,
) -> None:
    backend = SQLModelBackend(
        f"sqlite:///{tmp_path / 'preview.db'}",
        CCloudStorageModule(),
        use_migrations=False,
        focus_preview_enabled=True,
    )
    backend.create_tables()
    source, aggregate, allocation = _fallback_evidence(
        product="Provider Connector Product",
        line_type="FUTURE_CONNECT_METER",
        resource_id="lcc-1",
        resource_name="Orders sink",
    )
    _seed(
        backend,
        source=source,
        aggregate=aggregate,
        allocation=allocation,
        include_resource=False,
    )
    with backend.create_unit_of_work() as uow:
        uow.resources.upsert(
            _context_resource(
                "lkc-parent",
                "kafka_cluster",
                metadata={"provider_cloud": "GCP", "provider_region": "us-west2"},
            )
        )
        uow.resources.upsert(
            _context_resource(
                "lcc-1",
                "connector",
                parent_id="lkc-parent",
                metadata={
                    "env_id": "env-1",
                    "provider_cloud": "WRONG",
                    "provider_region": "wrong-region",
                },
            )
        )
        uow.commit()
    executor = ControlledExecutor()
    runtime = _runtime(tmp_path, backend, executor)
    try:
        queued = _submit(runtime, backend)
        executor.run_all()
        ready = runtime.get_request(
            backend=backend,
            request_id=queued.request_id,
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
        )

        assert ready.status.value == "ready", ready.diagnostic
        [row] = list(
            csv.DictReader(io.StringIO(runtime.read_file_bytes(ready, "cost-and-usage.csv").decode(), newline=""))
        )
        assert row["ResourceId"] == "lcc-1"
        assert row["ResourceName"] == "Provider lcc-1"
        assert row["ResourceType"] == "connector"
        assert row["HostProviderName"] == "GCP"
        assert row["RegionId"] == "us-west2"
        assert row["AllocatedResourceId"] == "lcc-1"
    finally:
        runtime.close()
        backend.dispose()


def test_unknown_product_connector_without_parent_fails_all_or_nothing(
    tmp_path: Path,
) -> None:
    backend = SQLModelBackend(
        f"sqlite:///{tmp_path / 'preview.db'}",
        CCloudStorageModule(),
        use_migrations=False,
        focus_preview_enabled=True,
    )
    backend.create_tables()
    source, aggregate, allocation = _fallback_evidence(
        product="Provider Connector Product",
        line_type="FUTURE_CONNECT_METER",
        resource_id="lcc-1",
    )
    _seed(
        backend,
        source=source,
        aggregate=aggregate,
        allocation=allocation,
        include_resource=False,
    )
    with backend.create_unit_of_work() as uow:
        uow.resources.upsert(
            _context_resource(
                "lcc-1",
                "connector",
                parent_id="lkc-missing",
                metadata={"provider_cloud": "AWS", "provider_region": "us-east-1"},
            )
        )
        uow.commit()
    executor = ControlledExecutor()
    runtime = _runtime(tmp_path, backend, executor)
    try:
        queued = _submit(runtime, backend)
        executor.run_all()
        failed = runtime.get_request(
            backend=backend,
            request_id=queued.request_id,
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
        )

        assert failed.status.value == "failed"
        assert failed.diagnostic.code == "preview_provider_context_incomplete"
        assert failed.package is None
        assert failed.storage_key is None
    finally:
        runtime.close()
        backend.dispose()


@pytest.mark.parametrize(
    ("origin", "authority", "expected_provider"),
    [
        (
            _context_resource(
                "lksqlc-1",
                "ksqldb_cluster",
                metadata={
                    "kafka_cluster_id": "lkc-authority",
                    "provider_cloud": "WRONG",
                    "provider_region": "wrong-region",
                },
            ),
            _context_resource(
                "lkc-authority",
                "kafka_cluster",
                metadata={"provider_cloud": "GCP", "provider_region": "us-west2"},
            ),
            ("GCP", "us-west2"),
        ),
        (
            _context_resource(
                "lfstmt-1",
                "flink_statement",
                metadata={
                    "compute_pool_id": "lfcp-authority",
                    "provider_cloud": "WRONG",
                    "provider_region": "wrong-region",
                },
            ),
            _context_resource(
                "lfcp-authority",
                "flink_compute_pool",
                metadata={"provider_cloud": "AZURE", "provider_region": "westus2"},
            ),
            ("AZURE", "westus2"),
        ),
        (
            _context_resource(
                "lfcp-1",
                "flink_compute_pool",
                metadata={"provider_cloud": "AWS", "provider_region": "us-east-2"},
            ),
            None,
            ("AWS", "us-east-2"),
        ),
        (
            _context_resource(
                "lksqlc-missing",
                "ksqldb_cluster",
                metadata={"kafka_cluster_id": "missing"},
            ),
            None,
            None,
        ),
        (
            _context_resource(
                "lfstmt-missing",
                "flink_statement",
                metadata={"compute_pool_id": "missing"},
            ),
            None,
            None,
        ),
    ],
    ids=(
        "ksqldb-kafka-authority",
        "flink-statement-pool-authority",
        "flink-pool-self-authority",
        "ksqldb-missing-kafka-reference",
        "flink-statement-missing-pool-reference",
    ),
)
def test_unknown_product_fallback_uses_relationship_aware_provider_authority(
    tmp_path: Path,
    origin: CoreResource,
    authority: CoreResource | None,
    expected_provider: tuple[str, str] | None,
) -> None:
    backend = SQLModelBackend(
        f"sqlite:///{tmp_path / 'preview.db'}",
        CCloudStorageModule(),
        use_migrations=False,
        focus_preview_enabled=True,
    )
    backend.create_tables()
    source, aggregate, allocation = _fallback_evidence(
        product="Provider Product",
        line_type="FUTURE_PROVIDER_METER",
        resource_id=origin.resource_id,
        resource_name=f"Provider {origin.resource_id}",
    )
    _seed(
        backend,
        source=source,
        aggregate=aggregate,
        allocation=allocation,
        include_resource=False,
    )
    with backend.create_unit_of_work() as uow:
        uow.resources.upsert(origin)
        if authority is not None:
            uow.resources.upsert(authority)
        uow.commit()
    executor = ControlledExecutor()
    runtime = _runtime(tmp_path, backend, executor)
    try:
        queued = _submit(runtime, backend)
        executor.run_all()
        result = runtime.get_request(
            backend=backend,
            request_id=queued.request_id,
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
        )

        if expected_provider is None:
            assert result.status.value == "failed"
            assert result.diagnostic.code == "preview_provider_context_incomplete"
            assert result.package is None
            assert result.storage_key is None
        else:
            assert result.status.value == "ready", result.diagnostic
            [row] = list(
                csv.DictReader(
                    io.StringIO(
                        runtime.read_file_bytes(result, "cost-and-usage.csv").decode(),
                        newline="",
                    )
                )
            )
            assert row["ResourceId"] == origin.resource_id
            assert row["ResourceName"] == origin.display_name
            assert row["ResourceType"] == origin.resource_type
            assert (row["HostProviderName"], row["RegionId"]) == expected_provider
    finally:
        runtime.close()
        backend.dispose()


def test_mapping_profile_partitions_every_column_exactly_once() -> None:
    mapping = preview_module("mapping")
    all_columns = (*mapping.FOCUS_1_4_FULL_COLUMNS, *mapping.CUSTOM_EVIDENCE_COLUMNS)
    classifications = [
        *mapping.MAPPED_COLUMNS,
        *(column for gap in mapping.KNOWN_GAPS for column in gap.columns),
        *mapping.PROFILE_NOT_APPLICABLE_COLUMNS,
    ]

    assert len(classifications) == len(set(classifications))
    assert set(classifications) == set(all_columns)
    assert "HostProviderName" in classifications
    assert "ServiceCategory" in classifications


def test_tableflow_synthetic_topic_cannot_supply_provider_authority(tmp_path: Path) -> None:
    backend = SQLModelBackend(
        f"sqlite:///{tmp_path / 'preview.db'}",
        CCloudStorageModule(),
        use_migrations=False,
        focus_preview_enabled=True,
    )
    backend.create_tables()
    source = _source(
        product="TABLEFLOW",
        line_type="TABLEFLOW_DATA_PROCESSED",
        resource_id="lkc-1:topic:orders",
        resource_name="orders",
        description="Tableflow data processed",
    )
    aggregate = _aggregate(
        product_category="TABLEFLOW",
        product_type="TABLEFLOW_DATA_PROCESSED",
        resource_id="lkc-1:topic:orders",
    )
    allocation = _allocation(
        product_category="TABLEFLOW",
        product_type="TABLEFLOW_DATA_PROCESSED",
        resource_id="lkc-1:topic:orders",
    )
    _seed(backend, source=source, aggregate=aggregate, allocation=allocation)
    with backend.create_unit_of_work() as uow:
        uow.resources.upsert(
            CoreResource(
                ecosystem="confluent_cloud",
                tenant_id="tenant-1",
                resource_id="lkc-1:topic:orders",
                resource_type="topic",
                display_name="orders",
                parent_id="lkc-1",
                status=ResourceStatus.ACTIVE,
                metadata={"provider_cloud": "AWS", "provider_region": "us-east-1"},
            )
        )
        uow.commit()
    executor = ControlledExecutor()
    runtime = _runtime(tmp_path, backend, executor)
    try:
        queued = _submit(runtime, backend)
        executor.run_all()
        failed = runtime.get_request(
            backend=backend,
            request_id=queued.request_id,
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
        )

        assert failed.status.value == "failed"
        assert failed.diagnostic.code == "preview_provider_context_incomplete"
        assert failed.diagnostic.message == (
            "Authoritative provider resource context is unavailable for one or more source records."
        )
        assert failed.diagnostic.retryable is False
        assert failed.package is None
        assert failed.storage_key is None
    finally:
        runtime.close()
        backend.dispose()


def test_concrete_resource_type_mismatch_uses_provider_context_diagnostic(tmp_path: Path) -> None:
    backend = SQLModelBackend(
        f"sqlite:///{tmp_path / 'preview.db'}",
        CCloudStorageModule(),
        use_migrations=False,
        focus_preview_enabled=True,
    )
    backend.create_tables()
    _seed(backend, source=_source(), aggregate=_aggregate(), allocation=_allocation())
    with backend.create_unit_of_work() as uow:
        uow.resources.upsert(
            CoreResource(
                ecosystem="confluent_cloud",
                tenant_id="tenant-1",
                resource_id="lkc-1",
                resource_type="connector",
                display_name="Wrong concrete type",
                parent_id="lkc-parent",
                status=ResourceStatus.ACTIVE,
                metadata={"provider_cloud": "AWS", "provider_region": "us-east-1"},
            )
        )
        uow.commit()
    executor = ControlledExecutor()
    runtime = _runtime(tmp_path, backend, executor)
    try:
        queued = _submit(runtime, backend)
        executor.run_all()
        failed = runtime.get_request(
            backend=backend,
            request_id=queued.request_id,
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
        )

        assert failed.status.value == "failed"
        assert failed.diagnostic.code == "preview_provider_context_incomplete"
        assert failed.diagnostic.message == (
            "Authoritative provider resource context is unavailable for one or more source records."
        )
        assert failed.diagnostic.retryable is False
        assert failed.package is None
    finally:
        runtime.close()
        backend.dispose()


@pytest.mark.parametrize(
    ("native_product", "description", "resource_id", "resources"),
    [
        ("KAFKA", "Refund Kafka usage", "lkc-missing", ()),
        ("KAFKA", "Refund Kafka usage", "lkc-1", (_context_resource("lkc-1", "connector"),)),
        ("CLUSTER_LINK", "Refund cluster linking usage", "lkc-missing", ()),
        (
            "CLUSTER_LINK",
            "Refund cluster linking usage",
            "lkc-1",
            (_context_resource("lkc-1", "connector"),),
        ),
        ("USM", "Refund USM usage", "lkc-missing", ()),
        ("USM", "Refund USM usage", "lkc-1", (_context_resource("lkc-1", "schema_registry"),)),
        ("STREAM_GOVERNANCE", "Refund governance usage", "lsrc-missing", ()),
        (
            "STREAM_GOVERNANCE",
            "Refund governance usage",
            "lsrc-1",
            (_context_resource("lsrc-1", "kafka_cluster"),),
        ),
        ("CONNECT", "Refund Connect usage", "lcc-missing", ()),
        ("CONNECT", "Refund Connect usage", "lcc-1", (_context_resource("lcc-1", "kafka_cluster"),)),
        (
            "CONNECT",
            "Refund Connect usage",
            "lcc-1",
            (_context_resource("lcc-1", "connector", parent_id=None, metadata={"env_id": "env-1"}),),
        ),
        (
            "CONNECT",
            "Refund Connect usage",
            "lcc-1",
            (_context_resource("lcc-1", "connector", parent_id="missing", metadata={"env_id": "env-1"}),),
        ),
        (
            "CONNECT",
            "Refund Connect usage",
            "lcc-1",
            (
                _context_resource("lcc-1", "connector", parent_id="parent", metadata={"env_id": "env-1"}),
                _context_resource("parent", "schema_registry"),
            ),
        ),
        ("KSQL", "Refund ksqlDB usage", "lksqlc-missing", ()),
        ("KSQL", "Refund ksqlDB usage", "lksqlc-1", (_context_resource("lksqlc-1", "connector"),)),
        (
            "KSQL",
            "Refund ksqlDB usage",
            "lksqlc-1",
            (_context_resource("lksqlc-1", "ksqldb_cluster", metadata={}),),
        ),
        (
            "KSQL",
            "Refund ksqlDB usage",
            "lksqlc-1",
            (_context_resource("lksqlc-1", "ksqldb_cluster", metadata={"kafka_cluster_id": "missing"}),),
        ),
        (
            "KSQL",
            "Refund ksqlDB usage",
            "lksqlc-1",
            (
                _context_resource("lksqlc-1", "ksqldb_cluster", metadata={"kafka_cluster_id": "reference"}),
                _context_resource("reference", "connector"),
            ),
        ),
        ("FLINK", "Refund Flink pool usage", "lfcp-missing", ()),
        ("FLINK", "Refund Flink pool usage", "lfcp-1", (_context_resource("lfcp-1", "kafka_cluster"),)),
        ("FLINK", "Refund Flink statement usage", "lfstmt-missing", ()),
        (
            "FLINK",
            "Refund Flink statement usage",
            "lfstmt-1",
            (_context_resource("lfstmt-1", "connector"),),
        ),
        (
            "FLINK",
            "Refund Flink statement usage",
            "lfstmt-1",
            (_context_resource("lfstmt-1", "flink_statement", metadata={}),),
        ),
        (
            "FLINK",
            "Refund Flink statement usage",
            "lfstmt-1",
            (_context_resource("lfstmt-1", "flink_statement", metadata={"compute_pool_id": "missing"}),),
        ),
        (
            "FLINK",
            "Refund Flink statement usage",
            "lfstmt-1",
            (
                _context_resource("lfstmt-1", "flink_statement", metadata={"compute_pool_id": "reference"}),
                _context_resource("reference", "kafka_cluster"),
            ),
        ),
    ],
    ids=(
        "kafka-missing-source",
        "kafka-wrong-source",
        "cluster-link-missing-source",
        "cluster-link-wrong-source",
        "usm-missing-source",
        "usm-wrong-source",
        "schema-registry-missing-source",
        "schema-registry-wrong-source",
        "connect-missing-source",
        "connect-wrong-source",
        "connect-missing-parent-id",
        "connect-missing-parent",
        "connect-wrong-parent",
        "ksqldb-missing-source",
        "ksqldb-wrong-source",
        "ksqldb-missing-reference-id",
        "ksqldb-missing-reference",
        "ksqldb-wrong-reference",
        "flink-pool-missing-source",
        "flink-pool-wrong-source",
        "flink-statement-missing-source",
        "flink-statement-wrong-source",
        "flink-statement-missing-reference-id",
        "flink-statement-missing-reference",
        "flink-statement-wrong-reference",
    ),
)
def test_promo_refund_reaches_provider_context_failures_after_v4_lineage(
    tmp_path: Path,
    native_product: str,
    description: str,
    resource_id: str,
    resources: tuple[CoreResource, ...],
) -> None:
    backend = SQLModelBackend(
        f"sqlite:///{tmp_path / 'preview.db'}",
        CCloudStorageModule(),
        use_migrations=False,
        focus_preview_enabled=True,
    )
    backend.create_tables()
    source = _source(
        product=native_product,
        line_type="PROMO_CREDIT",
        description=description,
        amount=Decimal("-8"),
        original_amount=Decimal("-10"),
        discount_amount=Decimal("-2"),
        price=Decimal("-2"),
        resource_id=resource_id,
        resource_name=f"Provider {resource_id}",
    )
    aggregate = _aggregate(
        product_category=native_product,
        product_type="PROMO_CREDIT",
        resource_id=resource_id,
        unit_price=Decimal("-2"),
        total_cost=Decimal("-8"),
    )
    allocation = _allocation(
        product_category=native_product,
        product_type="PROMO_CREDIT",
        resource_id=resource_id,
        amount=Decimal("-8"),
    )
    _seed(
        backend,
        source=source,
        aggregate=aggregate,
        allocation=allocation,
        include_resource=False,
    )
    with backend.create_unit_of_work() as uow:
        for resource in resources:
            uow.resources.upsert(resource)
        uow.commit()
    executor = ControlledExecutor()
    runtime = _runtime(tmp_path, backend, executor)
    try:
        queued = _submit(runtime, backend)
        executor.run_all()
        failed = runtime.get_request(
            backend=backend,
            request_id=queued.request_id,
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
        )

        assert failed.status.value == "failed"
        assert failed.diagnostic.code == "preview_provider_context_incomplete"
        assert failed.diagnostic.message == (
            "Authoritative provider resource context is unavailable for one or more source records."
        )
        assert failed.diagnostic.retryable is False
        assert failed.source_snapshot is None
        assert failed.package is None
        assert failed.storage_key is None
    finally:
        runtime.close()
        backend.dispose()


def test_lineage_correlations_are_sorted_unique_and_capped(tmp_path: Path) -> None:
    eligibility = __import__("core.preview.eligibility", fromlist=["public_source_correlation_id"])
    backend = SQLModelBackend(
        f"sqlite:///{tmp_path / 'preview.db'}",
        CCloudStorageModule(),
        use_migrations=False,
        focus_preview_enabled=True,
    )
    backend.create_tables()
    _seed(
        backend,
        aggregate=_aggregate(product_type="KAFKA_STREAMS"),
    )
    sources = [
        _source(
            source_record_id=f"provider:source-{index:02}",
            provider_cost_id=f"source-{index:02}",
            line_type="KAFKA_STREAMS",
            description="Kafka Streams usage",
            raw_payload={"id": f"source-{index:02}"},
        )
        for index in range(25)
    ]
    _replace_source_capture(backend, sources)
    expected = tuple(
        sorted(
            eligibility.public_source_correlation_id(
                ecosystem="confluent_cloud",
                tenant_id="tenant-1",
                source_record_id=source.source_record_id,
            )
            for source in sources
        )[:20]
    )
    executor = ControlledExecutor()
    runtime = _runtime(tmp_path, backend, executor)
    try:
        queued = _submit(runtime, backend)
        executor.run_all()
        failed = runtime.get_request(
            backend=backend,
            request_id=queued.request_id,
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
        )

        assert failed.diagnostic.code == "preview_source_reconciliation_failed"
        assert failed.diagnostic.source_correlation_ids == expected
        assert failed.source_snapshot is None
        assert failed.storage_key is None
        assert failed.package is None
    finally:
        runtime.close()
        backend.dispose()


@pytest.mark.parametrize("environment_state", ["missing", "wrong_id", "wrong_type", "cross_tenant"])
def test_persisted_environment_must_match_current_tenant_environment_resource(
    tmp_path: Path,
    environment_state: str,
) -> None:
    backend = SQLModelBackend(
        f"sqlite:///{tmp_path / 'preview.db'}",
        CCloudStorageModule(),
        use_migrations=False,
        focus_preview_enabled=True,
    )
    backend.create_tables()
    _seed(
        backend,
        source=_source(),
        aggregate=_aggregate(),
        allocation=_allocation(),
        include_environment=False,
    )
    if environment_state != "missing":
        with backend.create_unit_of_work() as uow:
            uow.resources.upsert(
                CoreResource(
                    ecosystem="confluent_cloud",
                    tenant_id=("tenant-2" if environment_state == "cross_tenant" else "tenant-1"),
                    resource_id=("env-other" if environment_state == "wrong_id" else "env-1"),
                    resource_type=("environment" if environment_state == "cross_tenant" else "connector"),
                    display_name="Invalid environment",
                    status=ResourceStatus.ACTIVE,
                )
            )
            uow.commit()
    executor = ControlledExecutor()
    runtime = _runtime(tmp_path, backend, executor)
    try:
        queued = _submit(runtime, backend)
        executor.run_all()
        failed = runtime.get_request(
            backend=backend,
            request_id=queued.request_id,
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
        )

        assert failed.status.value == "failed"
        assert failed.diagnostic.code == "preview_provider_context_incomplete"
        assert failed.diagnostic.message == (
            "Authoritative provider resource context is unavailable for one or more source records."
        )
        assert failed.diagnostic.retryable is False
        assert failed.source_snapshot is None
        assert failed.package is None
        assert failed.storage_key is None
    finally:
        runtime.close()
        backend.dispose()


def test_equivalent_requests_have_distinct_ids_but_identical_csv(tmp_path: Path) -> None:
    backend = SQLModelBackend(
        f"sqlite:///{tmp_path / 'preview.db'}",
        CCloudStorageModule(),
        use_migrations=False,
        focus_preview_enabled=True,
    )
    backend.create_tables()
    _seed(backend, source=_source(), aggregate=_aggregate(), allocation=_allocation())
    executor = ControlledExecutor()
    ids = iter(("request-1", "request-2"))
    artifacts = preview_module("artifacts")
    service = preview_module("service")
    runtime = service.PreviewRuntime(
        artifact_store=artifacts.LocalPreviewArtifactStore(tmp_path / "artifacts"),
        backend_provider=FixedTenantBackendProvider({"production": backend}),
        max_workers=1,
        clock=lambda: datetime(2026, 7, 4, tzinfo=UTC),
        request_id_factory=lambda: next(ids),
        executor=executor,
    )
    try:
        first = _submit(runtime, backend)
        second = _submit(runtime, backend)
        executor.run_all()
        first_ready = runtime.get_request(
            backend=backend,
            request_id=first.request_id,
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
        )
        second_ready = runtime.get_request(
            backend=backend,
            request_id=second.request_id,
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
        )
        assert first_ready.request_id != second_ready.request_id
        assert runtime.read_file_bytes(first_ready, "cost-and-usage.csv") == runtime.read_file_bytes(
            second_ready, "cost-and-usage.csv"
        )
        first_csv = runtime.read_file_bytes(first_ready, "cost-and-usage.csv")
        assert first_ready.package.files[0].sha256 == __import__("hashlib").sha256(first_csv).hexdigest()
        first_manifest = json.loads(runtime.read_manifest_bytes(first_ready))
        assert first_manifest["files"][0]["sha256"] == first_ready.package.files[0].sha256
    finally:
        runtime.close()
        backend.dispose()


@pytest.mark.parametrize(
    ("source_overrides", "aggregate_overrides", "allocation_overrides"),
    [
        ({"amount": Decimal("-8")}, {"total_cost": Decimal("-8")}, {"amount": Decimal("-8")}),
        ({"amount": Decimal("0")}, {"total_cost": Decimal("0")}, {"amount": Decimal("0")}),
        ({"amount": Decimal("-1")}, {}, {}),
        ({"price": Decimal("0")}, {"unit_price": Decimal("0")}, {}),
        ({"price": Decimal("-1")}, {}, {}),
        ({"quantity": Decimal("0")}, {"quantity": Decimal("0")}, {}),
        ({"quantity": Decimal("-1")}, {}, {}),
        ({"discount_amount": Decimal("-2")}, {}, {}),
        ({"original_amount": Decimal("0")}, {}, {}),
        ({"original_amount": Decimal("-1")}, {}, {}),
        ({"amount": Decimal("Infinity")}, {}, {}),
        ({"original_amount": Decimal("NaN")}, {}, {}),
        ({"discount_amount": Decimal("Infinity")}, {}, {}),
        ({"price": Decimal("Infinity")}, {}, {}),
        ({"quantity": Decimal("NaN")}, {}, {}),
        ({"line_type": "PROMO_CREDIT"}, {"product_type": "PROMO_CREDIT"}, {}),
        ({"line_type": "SUPPORT"}, {"product_type": "SUPPORT"}, {}),
        ({"product": ""}, {"product_category": ""}, {}),
        ({"description": ""}, {}, {}),
        ({"amount": Decimal("NaN")}, {"total_cost": Decimal("NaN")}, {"amount": Decimal("NaN")}),
        ({}, {"total_cost": Decimal("0")}, {}),
        ({}, {"total_cost": Decimal("-1")}, {}),
        ({}, {"quantity": Decimal("0")}, {}),
        ({}, {"quantity": Decimal("-1")}, {}),
        ({}, {"unit_price": Decimal("0")}, {}),
        ({}, {"unit_price": Decimal("-1")}, {}),
        ({}, {"total_cost": Decimal("Infinity")}, {}),
        ({}, {"quantity": Decimal("NaN")}, {}),
        ({}, {"unit_price": Decimal("Infinity")}, {}),
        ({}, {}, {"amount": Decimal("0")}),
        ({}, {}, {"amount": Decimal("-1")}),
        ({}, {}, {"amount": Decimal("NaN")}),
        ({}, {}, {"amount": Decimal("-Infinity")}),
    ],
)
def test_v4_profile_rejects_invalid_or_unsupported_financial_evidence(
    tmp_path: Path,
    source_overrides: dict[str, object],
    aggregate_overrides: dict[str, object],
    allocation_overrides: dict[str, object],
) -> None:
    backend = SQLModelBackend(
        f"sqlite:///{tmp_path / 'preview.db'}",
        CCloudStorageModule(),
        use_migrations=False,
        focus_preview_enabled=True,
    )
    backend.create_tables()
    allocation_value = allocation_overrides.get("amount")
    finite_allocation_corruption = (
        isinstance(allocation_value, Decimal) and allocation_value.is_finite() and allocation_value >= 0
    )
    if finite_allocation_corruption:
        _seed(
            backend,
            source=_source(**source_overrides),
            aggregate=_aggregate(**aggregate_overrides),
            allocation=_allocation(),
        )
        _corrupt_persisted_reconciliation_value(
            backend,
            source_overrides={},
            aggregate_overrides={},
            allocation_overrides=allocation_overrides,
        )
    else:
        _seed(
            backend,
            source=_source(**source_overrides),
            aggregate=_aggregate(**aggregate_overrides),
            allocation=_allocation(**allocation_overrides),
            compatibility_only_lineage=not (
                not source_overrides and not allocation_overrides and set(aggregate_overrides) == {"unit_price"}
            ),
        )
    executor = ControlledExecutor()
    runtime = _runtime(tmp_path, backend, executor)
    try:
        request = _submit(runtime, backend)
        executor.run_all()
        failed = runtime.get_request(
            backend=backend,
            request_id=request.request_id,
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
        )
        if not source_overrides and not allocation_overrides and set(aggregate_overrides) == {"unit_price"}:
            assert failed.status.value == "ready"
            assert failed.diagnostic is None
            assert failed.package is not None
            return
        assert failed.status.value == "failed"
        if source_overrides.get("line_type") == "PROMO_CREDIT":
            expected_code = "preview_source_economics_unsupported"
        elif source_overrides.get("line_type") == "SUPPORT":
            expected_code = "preview_charge_classification_ambiguous"
        elif source_overrides.get("product") == "" or source_overrides.get("description") == "":
            expected_code = "preview_source_record_incomplete"
        elif source_overrides:
            expected_code = "preview_source_economics_unsupported"
        elif allocation_overrides:
            assert isinstance(allocation_value, Decimal)
            expected_code = (
                "preview_source_reconciliation_failed"
                if allocation_value.is_finite() and allocation_value >= 0
                else "preview_allocation_lineage_incomplete"
            )
        else:
            expected_code = "preview_source_reconciliation_failed"
        assert failed.diagnostic.code == expected_code
        if source_overrides.get("line_type") == "PROMO_CREDIT":
            assert failed.diagnostic.message == (
                "One or more source records have unsupported monetary or quantity values."
            )
        elif source_overrides.get("line_type") == "SUPPORT":
            assert failed.diagnostic.message == (
                "One or more credit, refund, adjustment, or correction-like records cannot be classified "
                "authoritatively."
            )
        assert failed.source_snapshot is None
        assert failed.package is None
        assert failed.storage_key is None
    finally:
        runtime.close()
        backend.dispose()


def test_fractional_economics_use_canonical_non_exponent_output(tmp_path: Path) -> None:
    backend = SQLModelBackend(
        f"sqlite:///{tmp_path / 'preview.db'}",
        CCloudStorageModule(),
        use_migrations=False,
        focus_preview_enabled=True,
    )
    backend.create_tables()
    _seed(
        backend,
        source=_source(
            amount=Decimal("8.1250"),
            original_amount=Decimal("10.2500"),
            discount_amount=Decimal("2.1250"),
            price=Decimal("2.0500"),
            quantity=Decimal("5.0000"),
        ),
        aggregate=_aggregate(
            total_cost=Decimal("8.1250"),
            unit_price=Decimal("2.0500"),
            quantity=Decimal("5.0000"),
        ),
        allocation=_allocation(amount=Decimal("8.1250")),
    )
    executor = ControlledExecutor()
    runtime = _runtime(tmp_path, backend, executor)
    try:
        queued = _submit(runtime, backend)
        executor.run_all()
        ready = runtime.get_request(
            backend=backend,
            request_id=queued.request_id,
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
        )
        row = next(csv.DictReader(io.StringIO(runtime.read_file_bytes(ready, "cost-and-usage.csv").decode())))
        assert row["BilledCost"] == "8.125"
        assert row["EffectiveCost"] == "8.125"
        assert "E" not in row["BilledCost"].upper()
    finally:
        runtime.close()
        backend.dispose()


@pytest.mark.parametrize(
    "semantic_value",
    [
        "support plan",
        "promotional usage",
        "credit applied",
        "customer refund",
        "adjustment for July",
        "corrected usage",
        "reversal",
        "rebate",
        "prior-period true-up",
        "prior period charge",
        "true up",
    ],
)
@pytest.mark.parametrize("semantic_field", ["product", "description"])
def test_positive_tracer_rejects_credit_refund_adjustment_and_correction_semantics(
    tmp_path: Path,
    semantic_value: str,
    semantic_field: str,
) -> None:
    backend = SQLModelBackend(
        f"sqlite:///{tmp_path / 'preview.db'}",
        CCloudStorageModule(),
        use_migrations=False,
        focus_preview_enabled=True,
    )
    backend.create_tables()
    source_overrides = {semantic_field: semantic_value}
    origin_overrides = {"product_category": semantic_value} if semantic_field == "product" else {}
    _seed(
        backend,
        source=_source(**source_overrides),
        aggregate=_aggregate(**origin_overrides),
        allocation=_allocation(**origin_overrides),
    )
    executor = ControlledExecutor()
    runtime = _runtime(tmp_path, backend, executor)
    try:
        request = _submit(runtime, backend)
        executor.run_all()
        failed = runtime.get_request(
            backend=backend,
            request_id=request.request_id,
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
        )
        if semantic_field == "description" and semantic_value == "customer refund":
            assert failed.diagnostic.code == "preview_source_economics_unsupported"
            assert failed.diagnostic.message == (
                "One or more source records have unsupported monetary or quantity values."
            )
        else:
            assert failed.diagnostic.code == "preview_charge_classification_ambiguous"
        assert failed.package is None
    finally:
        runtime.close()
        backend.dispose()


@pytest.mark.parametrize(
    "source_overrides",
    [
        {
            "product": "SUPPORT_CLOUD_BUSINESS",
            "line_type": "SUPPORT",
            "description": "Support subscription",
            "resource_id": None,
            "resource_name": None,
            "environment_id": None,
        },
        {
            "line_type": "PROMO_CREDIT",
            "description": "Promotional allowance",
            "amount": Decimal("-5"),
            "original_amount": Decimal("-5"),
            "discount_amount": Decimal("0"),
            "price": None,
            "quantity": None,
            "unit": None,
            "resource_id": None,
            "resource_name": None,
            "environment_id": None,
        },
    ],
    ids=("support", "promotional-credit"),
)
def test_valid_organization_wide_semantics_reach_coverage_in_v4(
    tmp_path: Path,
    source_overrides: dict[str, object],
) -> None:
    backend = SQLModelBackend(
        f"sqlite:///{tmp_path / 'preview.db'}",
        CCloudStorageModule(),
        use_migrations=False,
        focus_preview_enabled=True,
    )
    backend.create_tables()
    _seed(backend, source=_source(**source_overrides))
    executor = ControlledExecutor()
    runtime = _runtime(tmp_path, backend, executor)
    try:
        request = _submit(runtime, backend)
        executor.run_all()
        failed = runtime.get_request(
            backend=backend,
            request_id=request.request_id,
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
        )

        assert failed.status.value == "failed"
        assert failed.diagnostic.code == "preview_source_coverage_incomplete"
        assert failed.diagnostic.message == (
            "Persisted source evidence does not completely cover the calculated Preview scope."
        )
        assert failed.diagnostic.retryable is False
        assert failed.source_snapshot is None
        assert failed.storage_key is None
        assert failed.package is None
    finally:
        runtime.close()
        backend.dispose()


@pytest.mark.parametrize(
    ("source_overrides", "expected_code", "expected_message"),
    [
        (
            {
                "product": "SUPPORT_CLOUD_BUSINESS",
                "line_type": "SUPPORT",
                "description": "Support subscription",
                "resource_id": None,
                "resource_name": None,
                "environment_id": None,
            },
            "preview_source_coverage_incomplete",
            "Persisted source evidence does not completely cover the calculated Preview scope.",
        ),
        (
            {
                "product": "TABLEFLOW",
                "line_type": "TABLEFLOW_DATA_PROCESSED",
                "description": "Tableflow data processed",
                "resource_id": "tableflow-source-1",
            },
            "preview_provider_context_incomplete",
            "Authoritative provider resource context is unavailable for one or more source records.",
        ),
    ],
    ids=("organization-wide", "unsupported-provider-context"),
)
def test_early_semantic_boundaries_skip_coverage_candidates_context_and_artifacts(
    tmp_path: Path,
    source_overrides: dict[str, object],
    expected_code: str,
    expected_message: str,
) -> None:
    backend = SQLModelBackend(
        f"sqlite:///{tmp_path / 'preview.db'}",
        CCloudStorageModule(),
        use_migrations=False,
        focus_preview_enabled=True,
    )
    backend.create_tables()
    _seed(backend, source=_source(**source_overrides))
    executor = ControlledExecutor()
    runtime = _runtime(tmp_path, backend, executor)
    try:
        request = _submit(runtime, backend)
        executor.run_all()
        failed = runtime.get_request(
            backend=backend,
            request_id=request.request_id,
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
        )

        assert failed.status.value == "failed"
        assert failed.diagnostic.code == expected_code
        assert failed.diagnostic.message == expected_message
        assert failed.diagnostic.retryable is False
        assert len(failed.diagnostic.source_correlation_ids) == 1
        assert failed.diagnostic.source_correlation_ids[0].startswith("src:v1:")
        assert failed.source_snapshot is None
        assert failed.storage_key is None
        assert failed.package is None
    finally:
        runtime.close()
        backend.dispose()


@pytest.mark.parametrize(
    ("include_source_issue", "expected_code"),
    [
        (True, "preview_source_record_malformed"),
        (False, "preview_provider_context_incomplete"),
    ],
    ids=("source-issue-before-organization-wide", "organization-wide-before-tableflow"),
)
def test_mixed_stream_precedence_stops_before_coverage(
    tmp_path: Path,
    include_source_issue: bool,
    expected_code: str,
) -> None:
    backend = SQLModelBackend(
        f"sqlite:///{tmp_path / 'preview.db'}",
        CCloudStorageModule(),
        use_migrations=False,
        focus_preview_enabled=True,
    )
    backend.create_tables()
    _seed(backend)
    sources = [
        _source(
            source_record_id="provider:support",
            provider_cost_id="support",
            product="SUPPORT_CLOUD_BUSINESS",
            line_type="SUPPORT",
            description="Support subscription",
            resource_id=None,
            resource_name=None,
            environment_id=None,
            raw_payload={"id": "support"},
        ),
        _source(
            source_record_id="provider:tableflow",
            provider_cost_id="tableflow",
            product="TABLEFLOW",
            line_type="TABLEFLOW_DATA_PROCESSED",
            description="Tableflow data processed",
            resource_id="tableflow-source-1",
            raw_payload={"id": "tableflow"},
        ),
    ]
    if include_source_issue:
        sources.insert(
            0,
            _source(
                source_record_id="provider:malformed",
                provider_cost_id="malformed",
                malformed=True,
                raw_payload={"id": "malformed"},
            ),
        )
    _replace_source_capture(backend, sources)
    executor = ControlledExecutor()
    runtime = _runtime(tmp_path, backend, executor)

    try:
        request = _submit(runtime, backend)
        executor.run_all()
        failed = runtime.get_request(
            backend=backend,
            request_id=request.request_id,
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
        )

        assert failed.status.value == "failed"
        assert failed.diagnostic.code == expected_code
        assert failed.source_snapshot is None
        assert failed.package is None
    finally:
        runtime.close()
        backend.dispose()


@pytest.mark.parametrize(
    ("product", "description"),
    [("prior", "period"), ("true", "up")],
)
def test_unknown_native_product_is_ambiguous_even_when_split_fields_do_not_form_a_phrase(
    tmp_path: Path,
    product: str,
    description: str,
) -> None:
    backend = SQLModelBackend(
        f"sqlite:///{tmp_path / 'preview.db'}",
        CCloudStorageModule(),
        use_migrations=False,
        focus_preview_enabled=True,
    )
    backend.create_tables()
    _seed(
        backend,
        source=_source(product=product, description=description),
        aggregate=_aggregate(product_category=product),
        allocation=_allocation(product_category=product),
    )
    executor = ControlledExecutor()
    runtime = _runtime(tmp_path, backend, executor)
    try:
        request = _submit(runtime, backend)
        executor.run_all()
        failed = runtime.get_request(
            backend=backend,
            request_id=request.request_id,
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
        )
        assert failed.status.value == "failed"
        assert failed.diagnostic.code == "preview_charge_classification_ambiguous"
        assert failed.diagnostic.message == (
            "One or more credit, refund, adjustment, or correction-like records cannot be classified authoritatively."
        )
        assert failed.package is None
    finally:
        runtime.close()
        backend.dispose()


@pytest.mark.parametrize(
    ("source_overrides", "aggregate_overrides", "allocation_overrides"),
    [
        ({"amount": Decimal("7")}, {}, {}),
        ({"original_amount": Decimal("11")}, {}, {}),
        ({"price": Decimal("3")}, {}, {}),
        ({}, {"quantity": Decimal("4")}, {}),
        ({}, {"unit_price": Decimal("3")}, {}),
        ({}, {"total_cost": Decimal("7")}, {}),
        ({}, {}, {"amount": Decimal("7")}),
    ],
)
def test_positive_tracer_rejects_every_exact_arithmetic_mismatch(
    tmp_path: Path,
    source_overrides: dict[str, object],
    aggregate_overrides: dict[str, object],
    allocation_overrides: dict[str, object],
) -> None:
    backend = SQLModelBackend(
        f"sqlite:///{tmp_path / 'preview.db'}",
        CCloudStorageModule(),
        use_migrations=False,
        focus_preview_enabled=True,
    )
    backend.create_tables()
    _seed(
        backend,
        source=_source(),
        aggregate=_aggregate(),
        allocation=_allocation(),
    )
    if not (not source_overrides and not allocation_overrides and set(aggregate_overrides) == {"unit_price"}):
        _corrupt_persisted_reconciliation_value(
            backend,
            source_overrides=source_overrides,
            aggregate_overrides=aggregate_overrides,
            allocation_overrides=allocation_overrides,
        )
    executor = ControlledExecutor()
    runtime = _runtime(tmp_path, backend, executor)
    try:
        request = _submit(runtime, backend)
        executor.run_all()
        failed = runtime.get_request(
            backend=backend,
            request_id=request.request_id,
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
        )
        if not source_overrides and not allocation_overrides and set(aggregate_overrides) == {"unit_price"}:
            assert failed.status.value == "ready"
            assert failed.diagnostic is None
            assert failed.package is not None
            return
        assert failed.status.value == "failed"
        assert failed.diagnostic.code == "preview_source_reconciliation_failed"
        assert len(failed.diagnostic.source_correlation_ids) == 1
        assert failed.diagnostic.source_correlation_ids[0].startswith("src:v1:")
        assert failed.package is None
    finally:
        runtime.close()
        backend.dispose()


def test_overlapping_but_not_contained_source_fails_tracer_scope(tmp_path: Path) -> None:
    backend = SQLModelBackend(
        f"sqlite:///{tmp_path / 'preview.db'}",
        CCloudStorageModule(),
        use_migrations=False,
        focus_preview_enabled=True,
    )
    backend.create_tables()
    source = _source(
        source_period_start=datetime(2026, 6, 30, tzinfo=UTC),
        evidence_scope_start=datetime(2026, 6, 30, tzinfo=UTC),
        allocation_timestamp=datetime(2026, 6, 30, tzinfo=UTC),
        retention_timestamp=datetime(2026, 6, 30, tzinfo=UTC),
    )
    _seed(
        backend,
        source=source,
        aggregate=_aggregate(),
        allocation=_allocation(),
        compatibility_only_lineage=True,
    )
    executor = ControlledExecutor()
    runtime = _runtime(tmp_path, backend, executor)
    try:
        request = _submit(runtime, backend)
        executor.run_all()
        failed = runtime.get_request(
            backend=backend,
            request_id=request.request_id,
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
        )
        assert failed.diagnostic.code == "preview_source_scope_unsupported"
    finally:
        runtime.close()
        backend.dispose()


def test_malformed_undated_overlapping_evidence_blocks_even_with_epoch_allocation(tmp_path: Path) -> None:
    backend = SQLModelBackend(
        f"sqlite:///{tmp_path / 'preview.db'}",
        CCloudStorageModule(),
        use_migrations=False,
        focus_preview_enabled=True,
    )
    backend.create_tables()
    _seed(backend, source=_source(), aggregate=_aggregate(), allocation=_allocation())
    malformed = _source(
        source_record_id="composite:v1:malformed",
        identity_scheme="composite_v1",
        provider_cost_id=None,
        source_period_start=None,
        source_period_end=None,
        allocation_timestamp=datetime(1970, 1, 1, tzinfo=UTC),
        retention_timestamp=datetime(2026, 7, 2, tzinfo=UTC),
        malformed=True,
        diagnostics=("missing_required:start_date",),
    )
    _replace_source_capture(backend, [_source(), malformed])
    executor = ControlledExecutor()
    runtime = _runtime(tmp_path, backend, executor)
    try:
        request = _submit(runtime, backend)
        executor.run_all()
        failed = runtime.get_request(
            backend=backend,
            request_id=request.request_id,
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
        )
        assert failed.status.value == "failed"
        assert failed.diagnostic.code == "preview_source_record_malformed"
        assert failed.source_snapshot is None
    finally:
        runtime.close()
        backend.dispose()


@pytest.mark.parametrize(
    "source",
    [
        _source(malformed=True),
        _source(diagnostics=("provider_warning",)),
        _source(source_period_start=None, source_period_end=None),
    ],
)
def test_single_malformed_or_incomplete_source_has_snapshot_diagnostic(
    tmp_path: Path,
    source: CCloudCostSourceRecord,
) -> None:
    backend = SQLModelBackend(
        f"sqlite:///{tmp_path / 'preview.db'}",
        CCloudStorageModule(),
        use_migrations=False,
        focus_preview_enabled=True,
    )
    backend.create_tables()
    if source.source_period_start is None:
        source = replace(
            source,
            allocation_timestamp=datetime(1970, 1, 1, tzinfo=UTC),
            billing_timestamp=datetime(1970, 1, 1, tzinfo=UTC),
            retention_timestamp=source.evidence_scope_end,
        )
    _seed(
        backend,
        source=source,
        aggregate=_aggregate(),
        allocation=_allocation(),
        compatibility_only_lineage=True,
    )
    executor = ControlledExecutor()
    runtime = _runtime(tmp_path, backend, executor)
    try:
        queued = _submit(runtime, backend)
        executor.run_all()
        failed = runtime.get_request(
            backend=backend,
            request_id=queued.request_id,
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
        )
        assert failed.status.value == "failed"
        expected_code = (
            "preview_source_record_malformed"
            if source.malformed or source.diagnostics
            else "preview_source_scope_unsupported"
        )
        assert failed.diagnostic.code == expected_code
        assert len(failed.diagnostic.source_correlation_ids) == 1
        assert failed.diagnostic.source_correlation_ids[0].startswith("src:v1:")
        assert failed.source_snapshot is None
        assert failed.package is None
    finally:
        runtime.close()
        backend.dispose()


@pytest.mark.parametrize("candidate_kind", ["source", "aggregate", "allocation"])
@pytest.mark.parametrize("candidate_count", [0, 2])
def test_every_bounded_evidence_seam_rejects_zero_or_multiple_candidates(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    candidate_kind: str,
    candidate_count: int,
) -> None:
    from plugins.confluent_cloud.storage import repositories

    backend = SQLModelBackend(
        f"sqlite:///{tmp_path / 'preview.db'}",
        CCloudStorageModule(),
        use_migrations=False,
        focus_preview_enabled=True,
    )
    backend.create_tables()
    _seed(backend, source=_source(), aggregate=_aggregate(), allocation=_allocation())
    if candidate_kind == "source":
        original = repositories.CCloudBillingRepository.iter_preview_sources

        def source_candidates(repository: object, scope: object) -> Iterator[object]:
            candidates = list(original(repository, scope))
            return iter(()) if candidate_count == 0 else iter((candidates[0], candidates[0]))

        monkeypatch.setattr(
            repositories.CCloudBillingRepository,
            "iter_preview_sources",
            source_candidates,
        )
    elif candidate_kind == "aggregate":
        original = repositories.CCloudBillingRepository.iter_preview_aggregates

        def aggregate_candidates(repository: object, scope: object) -> Iterator[object]:
            candidates = list(original(repository, scope))
            return iter(()) if candidate_count == 0 else iter((candidates[0], candidates[0]))

        monkeypatch.setattr(
            repositories.CCloudBillingRepository,
            "iter_preview_aggregates",
            aggregate_candidates,
        )
    else:
        original = repositories.CCloudChargebackRepository.iter_preview_allocations

        def allocation_candidates(
            repository: object, scope: object, calculation_ids: tuple[str, ...]
        ) -> Iterator[object]:
            candidates = list(original(repository, scope, calculation_ids))
            return iter(()) if candidate_count == 0 else iter((candidates[0], candidates[0]))

        monkeypatch.setattr(
            repositories.CCloudChargebackRepository,
            "iter_preview_allocations",
            allocation_candidates,
        )

    executor = ControlledExecutor()
    runtime = _runtime(tmp_path, backend, executor)
    try:
        queued = _submit(runtime, backend)
        executor.run_all()
        failed = runtime.get_request(
            backend=backend,
            request_id=queued.request_id,
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
        )
        assert failed.status.value == "failed"
        expected_code = (
            "preview_mapping_scope_unsupported"
            if candidate_kind == "source" and candidate_count == 2
            else "preview_allocation_lineage_incomplete"
            if candidate_kind == "allocation"
            else "preview_source_coverage_incomplete"
        )
        assert failed.diagnostic.code == expected_code
        if candidate_kind != "source" or candidate_count != 0:
            assert len(failed.diagnostic.source_correlation_ids) >= 1
            assert all(value.startswith("src:v1:") for value in failed.diagnostic.source_correlation_ids)
        assert failed.source_snapshot is None
        assert failed.package is None
    finally:
        runtime.close()
        backend.dispose()


@pytest.mark.parametrize(
    ("candidate_kind", "field", "value"),
    [
        ("aggregate", "timestamp", datetime(2026, 7, 1, 1, tzinfo=UTC)),
        ("aggregate", "environment_id", "env-other"),
        ("aggregate", "resource_id", "lkc-other"),
        ("aggregate", "native_product", "CONNECT"),
        ("aggregate", "native_line_type", "CONNECT_CAPACITY"),
        ("allocation", "timestamp", datetime(2026, 7, 1, 1, tzinfo=UTC)),
        ("allocation", "environment_id", "env-other"),
        ("allocation", "resource_id", "lkc-other"),
        ("allocation", "native_product", "CONNECT"),
        ("allocation", "native_line_type", "CONNECT_CAPACITY"),
    ],
)
def test_complete_origin_tuple_mismatch_is_rejected_by_runtime_reconciliation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    candidate_kind: str,
    field: str,
    value: object,
) -> None:
    from plugins.confluent_cloud.storage import repositories

    backend = SQLModelBackend(
        f"sqlite:///{tmp_path / 'preview.db'}",
        CCloudStorageModule(),
        use_migrations=False,
        focus_preview_enabled=True,
    )
    backend.create_tables()
    _seed(backend, source=_source(), aggregate=_aggregate(), allocation=_allocation())
    if candidate_kind == "aggregate":
        original = repositories.CCloudBillingRepository.iter_preview_aggregates

        def mismatched(repository: object, scope: object) -> Iterator[object]:
            candidates = list(original(repository, scope))
            return iter((replace(candidates[0], **{field: value}),))

        monkeypatch.setattr(repositories.CCloudBillingRepository, "iter_preview_aggregates", mismatched)
    else:
        original = repositories.CCloudChargebackRepository.iter_preview_allocations

        def mismatched(repository: object, scope: object, calculation_ids: tuple[str, ...]) -> Iterator[object]:
            candidates = list(original(repository, scope, calculation_ids))
            return iter((replace(candidates[0], **{field: value}),))

        monkeypatch.setattr(repositories.CCloudChargebackRepository, "iter_preview_allocations", mismatched)
    executor = ControlledExecutor()
    runtime = _runtime(tmp_path, backend, executor)
    try:
        queued = _submit(runtime, backend)
        executor.run_all()
        failed = runtime.get_request(
            backend=backend,
            request_id=queued.request_id,
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
        )
        assert failed.status.value == "failed"
        assert failed.diagnostic.code == (
            "preview_source_coverage_incomplete"
            if candidate_kind == "aggregate"
            else "preview_allocation_lineage_incomplete"
        )
        assert failed.package is None
    finally:
        runtime.close()
        backend.dispose()


@pytest.mark.parametrize(
    ("allocation", "include_resource", "include_identity", "expected_code", "expected_message"),
    [
        (
            _allocation(identity_id="UNALLOCATED"),
            True,
            True,
            None,
            None,
        ),
        (
            _allocation(),
            False,
            True,
            "preview_provider_context_incomplete",
            "Authoritative provider resource context is unavailable for one or more source records.",
        ),
        (
            _allocation(),
            True,
            False,
            None,
            None,
        ),
    ],
)
def test_allocation_target_resource_and_identity_gap_scenarios(
    tmp_path: Path,
    allocation: ChargebackRow,
    include_resource: bool,
    include_identity: bool,
    expected_code: str | None,
    expected_message: str | None,
) -> None:
    backend = SQLModelBackend(
        f"sqlite:///{tmp_path / 'preview.db'}",
        CCloudStorageModule(),
        use_migrations=False,
        focus_preview_enabled=True,
    )
    backend.create_tables()
    _seed(
        backend,
        source=_source(),
        aggregate=_aggregate(),
        allocation=allocation,
        include_resource=include_resource,
        include_identity=include_identity,
    )
    executor = ControlledExecutor()
    runtime = _runtime(tmp_path, backend, executor)
    try:
        queued = _submit(runtime, backend)
        executor.run_all()
        failed = runtime.get_request(
            backend=backend,
            request_id=queued.request_id,
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
        )
        if expected_code is None:
            assert failed.status.value == "ready"
            assert failed.diagnostic is None
            assert failed.package is not None
        else:
            assert failed.status.value == "failed"
            assert failed.diagnostic.code == expected_code
            assert failed.diagnostic.message == expected_message
            assert len(failed.diagnostic.source_correlation_ids) == 1
            assert failed.diagnostic.source_correlation_ids[0].startswith("src:v1:")
            assert failed.package is None
    finally:
        runtime.close()
        backend.dispose()


@pytest.mark.parametrize(
    ("window_start", "window_end"),
    [
        (datetime(2026, 7, 3), datetime(2026, 7, 4)),
        (datetime(2026, 7, 4, tzinfo=UTC), datetime(2026, 7, 3, tzinfo=UTC)),
    ],
)
def test_invalid_or_naive_collection_window_never_reports_freshness(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    window_start: datetime,
    window_end: datetime,
) -> None:
    from plugins.confluent_cloud.storage import repositories

    backend = SQLModelBackend(
        f"sqlite:///{tmp_path / 'preview.db'}",
        CCloudStorageModule(),
        use_migrations=False,
        focus_preview_enabled=True,
    )
    backend.create_tables()
    _seed(backend, source=_source(), aggregate=_aggregate(), allocation=_allocation())
    original = repositories.CCloudBillingRepository.iter_preview_sources

    def invalid(repository: object, scope: object) -> Iterator[object]:
        candidates = list(original(repository, scope))
        return iter(
            (
                replace(
                    candidates[0],
                    collection_window_start=window_start,
                    collection_window_end=window_end,
                ),
            )
        )

    monkeypatch.setattr(repositories.CCloudBillingRepository, "iter_preview_sources", invalid)
    executor = ControlledExecutor()
    runtime = _runtime(tmp_path, backend, executor)
    try:
        queued = _submit(runtime, backend)
        executor.run_all()
        failed = runtime.get_request(
            backend=backend,
            request_id=queued.request_id,
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
        )
        assert failed.status.value == "failed"
        assert failed.diagnostic.code == "preview_source_evidence_unavailable"
        assert failed.source_snapshot is None
    finally:
        runtime.close()
        backend.dispose()


def test_source_through_tracks_persisted_replacement_window_not_request_end(tmp_path: Path) -> None:
    backend = SQLModelBackend(
        f"sqlite:///{tmp_path / 'preview.db'}",
        CCloudStorageModule(),
        use_migrations=False,
        focus_preview_enabled=True,
    )
    backend.create_tables()
    _seed(backend, source=_source(), aggregate=_aggregate(), allocation=_allocation())
    with backend.create_unit_of_work() as uow:
        uow.pipeline_state.upsert(
            PipelineState(
                ecosystem="confluent_cloud",
                tenant_id="tenant-1",
                tracking_date=date(2026, 7, 2),
                billing_gathered=True,
                resources_gathered=True,
                chargeback_calculated=True,
                calculation_id="calculation-2",
                calculation_completed_at=datetime(2026, 7, 3, 3, tzinfo=UTC),
            )
        )
        uow.commit()
    executor = ControlledExecutor()
    ids = iter(("request-1", "request-2", "request-3"))
    artifacts = preview_module("artifacts")
    service = preview_module("service")
    runtime = service.PreviewRuntime(
        artifact_store=artifacts.LocalPreviewArtifactStore(tmp_path / "artifacts"),
        backend_provider=FixedTenantBackendProvider({"production": backend}),
        max_workers=1,
        clock=lambda: datetime(2026, 7, 5, tzinfo=UTC),
        request_id_factory=lambda: next(ids),
        executor=executor,
    )

    def submit(end_date: date) -> object:
        return runtime.submit(
            tenant_name="production",
            tenant_config=_tenant_config(backend._connection_string),
            backend=backend,
            start_date=date(2026, 7, 1),
            end_date=end_date,
            grain="daily",
            column_profile="full",
            effective_columns=preview_module("mapping").FOCUS_1_4_FULL_PROFILE_COLUMNS,
        )

    try:
        first = submit(date(2026, 7, 2))
        executor.run_all()
        first_ready = runtime.get_request(
            backend=backend,
            request_id=first.request_id,
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
        )
        assert first_ready.source_snapshot.source_through == datetime(2026, 7, 3, tzinfo=UTC)

        _replace_source_capture(
            backend,
            [
                _source(
                    collection_window_start=datetime(2026, 6, 29, tzinfo=UTC),
                    collection_window_end=datetime(2026, 7, 4, tzinfo=UTC),
                )
            ],
        )

        replaced_window = submit(date(2026, 7, 2))
        changed_request_end = submit(date(2026, 7, 3))
        executor.run_all()
        replacement_ready = runtime.get_request(
            backend=backend,
            request_id=replaced_window.request_id,
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
        )
        expanded_ready = runtime.get_request(
            backend=backend,
            request_id=changed_request_end.request_id,
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
        )
        assert replacement_ready.status.value == "ready"
        assert expanded_ready.status.value == "failed"
        assert expanded_ready.diagnostic.code == "preview_allocation_lineage_incomplete"
        assert replacement_ready.source_snapshot.source_through == datetime(2026, 7, 4, tzinfo=UTC)
        assert expanded_ready.source_snapshot is None
    finally:
        runtime.close()
        backend.dispose()
