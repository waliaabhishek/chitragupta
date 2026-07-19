from __future__ import annotations

import csv
import io
import json
from collections.abc import Callable
from concurrent.futures import Future
from dataclasses import replace
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from core.config.models import StorageConfig, TenantConfig
from core.models.chargeback import ChargebackRow, CostType
from core.models.identity import CoreIdentity
from core.models.pipeline import PipelineState
from core.models.resource import CoreResource, ResourceStatus
from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend
from plugins.confluent_cloud.models.billing import CCloudBillingLineItem, CCloudCostSourceRecord
from plugins.confluent_cloud.storage.module import CCloudStorageModule
from tests.unit.core.preview.conftest import preview_module


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
    return TenantConfig(
        ecosystem="confluent_cloud",
        tenant_id="tenant-1",
        storage=StorageConfig(connection_string=connection_string),
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


def _seed(
    backend: SQLModelBackend,
    *,
    source: CCloudCostSourceRecord | None = None,
    aggregate: CCloudBillingLineItem | None = None,
    allocation: ChargebackRow | None = None,
    state: PipelineState | None = None,
    include_resource: bool = True,
    include_identity: bool = True,
) -> None:
    with backend.create_unit_of_work() as uow:
        if include_resource:
            uow.resources.upsert(
                CoreResource(
                    ecosystem="confluent_cloud",
                    tenant_id="tenant-1",
                    resource_id="lkc-1",
                    resource_type="kafka_cluster",
                    display_name="Orders",
                    status=ResourceStatus.ACTIVE,
                    created_at=datetime(2026, 1, 1, tzinfo=UTC),
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
        if source is not None:
            uow.billing.replace_source_window(
                "confluent_cloud",
                "tenant-1",
                datetime(2026, 6, 30, tzinfo=UTC),
                datetime(2026, 7, 3, tzinfo=UTC),
                [source],
            )
        if aggregate is not None:
            uow.billing.upsert(aggregate)
        if allocation is not None:
            uow.chargebacks.upsert_batch([allocation])
        uow.pipeline_state.upsert(
            state
            or PipelineState(
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
        )
        uow.commit()


def _runtime(tmp_path: Path, backend: SQLModelBackend, executor: ControlledExecutor) -> object:
    artifacts = preview_module("artifacts")
    service = preview_module("service")
    return service.PreviewRuntime(
        artifact_store=artifacts.LocalPreviewArtifactStore(tmp_path / "artifacts"),
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
    )


def _ready_request(tmp_path: Path) -> tuple[object, object, SQLModelBackend, ControlledExecutor]:
    backend = SQLModelBackend(
        f"sqlite:///{tmp_path / 'preview.db'}",
        CCloudStorageModule(),
        use_migrations=False,
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


def test_controlled_runtime_commits_queued_before_running_and_reaches_ready(tmp_path: Path) -> None:
    backend = SQLModelBackend(
        f"sqlite:///{tmp_path / 'preview.db'}",
        CCloudStorageModule(),
        use_migrations=False,
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
        assert [file.name for file in ready.package.files] == ["cost-and-usage.csv"]
    finally:
        runtime.close()
        backend.dispose()
    assert executor.shutdown_calls == []


def test_daily_full_package_has_exact_headers_null_authority_fields_and_manifest_gaps(tmp_path: Path) -> None:
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
        assert rows[0]["BillingCurrency"] == ""
        assert rows[0]["BillingPeriodStart"] == ""
        assert rows[0]["BillingPeriodEnd"] == ""
        assert rows[0]["ChargeClass"] == ""
        assert rows[0]["x_ConfluentTierDimensions"] == '{"lower_bound":"0","upper_bound":"100"}'
        assert csv_bytes.endswith(b"\n")
        assert b"\r\n" not in csv_bytes

        manifest = json.loads(manifest_bytes)
        assert manifest["schema_version"] == "chitragupta.preview-manifest.v1"
        assert manifest["target_focus_version"] == "1.4"
        assert manifest["conformance_status"] == "non_conforming"
        assert manifest["mapping_profile_version"] == "focus-1.4-daily-full-tracer-v1"
        assert [gap["code"] for gap in manifest["known_gaps"]] == [gap.code for gap in mapping.KNOWN_GAPS]
        assert manifest["known_gaps"] == [
            {
                "code": gap.code,
                "description": gap.description,
                "owner_task": gap.owner_task,
                "columns": list(gap.columns),
            }
            for gap in mapping.KNOWN_GAPS
        ]
        assert {gap["owner_task"] for gap in manifest["known_gaps"]} == {
            "TASK-254.03",
            "TASK-254.04",
            "TASK-254.05",
        }
        assert manifest["source_snapshot"]["source_through"] == "2026-07-03T00:00:00Z"
        assert manifest["reconciliation"] == {
            "source_cost": "8",
            "allocated_cost": "8",
            "difference": "0",
        }
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


def test_equivalent_requests_have_distinct_ids_but_identical_csv(tmp_path: Path) -> None:
    backend = SQLModelBackend(
        f"sqlite:///{tmp_path / 'preview.db'}",
        CCloudStorageModule(),
        use_migrations=False,
    )
    backend.create_tables()
    _seed(backend, source=_source(), aggregate=_aggregate(), allocation=_allocation())
    executor = ControlledExecutor()
    ids = iter(("request-1", "request-2"))
    artifacts = preview_module("artifacts")
    service = preview_module("service")
    runtime = service.PreviewRuntime(
        artifact_store=artifacts.LocalPreviewArtifactStore(tmp_path / "artifacts"),
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
def test_positive_tracer_rejects_non_positive_non_finite_or_unsupported_evidence(
    tmp_path: Path,
    source_overrides: dict[str, object],
    aggregate_overrides: dict[str, object],
    allocation_overrides: dict[str, object],
) -> None:
    backend = SQLModelBackend(
        f"sqlite:///{tmp_path / 'preview.db'}",
        CCloudStorageModule(),
        use_migrations=False,
    )
    backend.create_tables()
    _seed(
        backend,
        source=_source(**source_overrides),
        aggregate=_aggregate(**aggregate_overrides),
        allocation=_allocation(**allocation_overrides),
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
        assert failed.diagnostic.code == "daily_full_tracer_scope_unsupported"
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
        assert failed.diagnostic.code == "daily_full_tracer_scope_unsupported"
        assert failed.package is None
    finally:
        runtime.close()
        backend.dispose()


@pytest.mark.parametrize(
    ("product", "description"),
    [("prior", "period"), ("true", "up")],
)
def test_rejected_multiword_semantics_do_not_match_across_independent_fields(
    tmp_path: Path,
    product: str,
    description: str,
) -> None:
    backend = SQLModelBackend(
        f"sqlite:///{tmp_path / 'preview.db'}",
        CCloudStorageModule(),
        use_migrations=False,
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
        ready = runtime.get_request(
            backend=backend,
            request_id=request.request_id,
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
        )
        assert ready.status.value == "ready"
        assert ready.diagnostic is None
        assert ready.package is not None
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
    )
    backend.create_tables()
    _seed(
        backend,
        source=_source(**source_overrides),
        aggregate=_aggregate(**aggregate_overrides),
        allocation=_allocation(**allocation_overrides),
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
        assert failed.diagnostic.code == "preview_reconciliation_failed"
        assert failed.package is None
    finally:
        runtime.close()
        backend.dispose()


def test_overlapping_but_not_contained_source_fails_tracer_scope(tmp_path: Path) -> None:
    backend = SQLModelBackend(
        f"sqlite:///{tmp_path / 'preview.db'}",
        CCloudStorageModule(),
        use_migrations=False,
    )
    backend.create_tables()
    source = _source(
        source_period_start=datetime(2026, 6, 30, tzinfo=UTC),
        evidence_scope_start=datetime(2026, 6, 30, tzinfo=UTC),
        allocation_timestamp=datetime(2026, 6, 30, tzinfo=UTC),
        retention_timestamp=datetime(2026, 6, 30, tzinfo=UTC),
    )
    _seed(backend, source=source, aggregate=_aggregate(), allocation=_allocation())
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
        assert failed.diagnostic.code == "daily_full_tracer_scope_unsupported"
    finally:
        runtime.close()
        backend.dispose()


def test_malformed_undated_overlapping_evidence_blocks_even_with_epoch_allocation(tmp_path: Path) -> None:
    backend = SQLModelBackend(
        f"sqlite:///{tmp_path / 'preview.db'}",
        CCloudStorageModule(),
        use_migrations=False,
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
    with backend.create_unit_of_work() as uow:
        uow.billing.replace_source_window(
            "confluent_cloud",
            "tenant-1",
            datetime(2026, 6, 30, tzinfo=UTC),
            datetime(2026, 7, 3, tzinfo=UTC),
            [_source(), malformed],
        )
        uow.commit()
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
        assert failed.diagnostic.code == "daily_full_tracer_scope_unsupported"
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
    )
    backend.create_tables()
    if source.source_period_start is None:
        source = replace(
            source,
            allocation_timestamp=datetime(1970, 1, 1, tzinfo=UTC),
            retention_timestamp=source.evidence_scope_end,
        )
    _seed(backend, source=source, aggregate=_aggregate(), allocation=_allocation())
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
        assert failed.diagnostic.code == "preview_source_snapshot_incomplete"
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
    )
    backend.create_tables()
    _seed(backend, source=_source(), aggregate=_aggregate(), allocation=_allocation())
    if candidate_kind == "source":
        original = repositories.CCloudBillingRepository.find_preview_source_candidates

        def source_candidates(repository: object, scope: object) -> tuple[object, ...]:
            candidates = original(repository, scope)
            return () if candidate_count == 0 else (candidates[0], candidates[0])

        monkeypatch.setattr(
            repositories.CCloudBillingRepository,
            "find_preview_source_candidates",
            source_candidates,
        )
    elif candidate_kind == "aggregate":
        original = repositories.CCloudBillingRepository.find_preview_aggregate_candidates

        def aggregate_candidates(repository: object, scope: object, source: object) -> tuple[object, ...]:
            candidates = original(repository, scope, source)
            return () if candidate_count == 0 else (candidates[0], candidates[0])

        monkeypatch.setattr(
            repositories.CCloudBillingRepository,
            "find_preview_aggregate_candidates",
            aggregate_candidates,
        )
    else:
        original = repositories.CCloudChargebackRepository.find_preview_allocation_candidates

        def allocation_candidates(repository: object, scope: object, source: object) -> tuple[object, ...]:
            candidates = original(repository, scope, source)
            return () if candidate_count == 0 else (candidates[0], candidates[0])

        monkeypatch.setattr(
            repositories.CCloudChargebackRepository,
            "find_preview_allocation_candidates",
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
        assert failed.diagnostic.code == "daily_full_tracer_scope_unsupported"
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
    )
    backend.create_tables()
    _seed(backend, source=_source(), aggregate=_aggregate(), allocation=_allocation())
    if candidate_kind == "aggregate":
        original = repositories.CCloudBillingRepository.find_preview_aggregate_candidates

        def mismatched(repository: object, scope: object, source: object) -> tuple[object, ...]:
            candidates = original(repository, scope, source)
            return (replace(candidates[0], **{field: value}),)

        monkeypatch.setattr(repositories.CCloudBillingRepository, "find_preview_aggregate_candidates", mismatched)
    else:
        original = repositories.CCloudChargebackRepository.find_preview_allocation_candidates

        def mismatched(repository: object, scope: object, source: object) -> tuple[object, ...]:
            candidates = original(repository, scope, source)
            return (replace(candidates[0], **{field: value}),)

        monkeypatch.setattr(repositories.CCloudChargebackRepository, "find_preview_allocation_candidates", mismatched)
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
        assert failed.diagnostic.code == "preview_reconciliation_failed"
        assert failed.package is None
    finally:
        runtime.close()
        backend.dispose()


@pytest.mark.parametrize(
    ("allocation", "include_resource", "include_identity", "expected_code"),
    [
        (_allocation(identity_id="UNALLOCATED"), True, True, "daily_full_tracer_scope_unsupported"),
        (_allocation(), False, True, "preview_source_snapshot_incomplete"),
        (_allocation(), True, False, "preview_source_snapshot_incomplete"),
    ],
)
def test_allocation_target_resource_and_identity_gap_scenarios(
    tmp_path: Path,
    allocation: ChargebackRow,
    include_resource: bool,
    include_identity: bool,
    expected_code: str,
) -> None:
    backend = SQLModelBackend(
        f"sqlite:///{tmp_path / 'preview.db'}",
        CCloudStorageModule(),
        use_migrations=False,
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
        assert failed.status.value == "failed"
        assert failed.diagnostic.code == expected_code
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
    )
    backend.create_tables()
    _seed(backend, source=_source(), aggregate=_aggregate(), allocation=_allocation())
    original = repositories.CCloudBillingRepository.find_preview_source_candidates

    def invalid(repository: object, scope: object) -> tuple[object, ...]:
        candidates = original(repository, scope)
        return (
            replace(
                candidates[0],
                collection_window_start=window_start,
                collection_window_end=window_end,
            ),
        )

    monkeypatch.setattr(repositories.CCloudBillingRepository, "find_preview_source_candidates", invalid)
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
        assert failed.diagnostic.code == "preview_source_snapshot_incomplete"
        assert failed.source_snapshot is None
    finally:
        runtime.close()
        backend.dispose()


def test_source_through_tracks_persisted_replacement_window_not_request_end(tmp_path: Path) -> None:
    backend = SQLModelBackend(
        f"sqlite:///{tmp_path / 'preview.db'}",
        CCloudStorageModule(),
        use_migrations=False,
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

        with backend.create_unit_of_work() as uow:
            uow.billing.replace_source_window(
                "confluent_cloud",
                "tenant-1",
                datetime(2026, 6, 29, tzinfo=UTC),
                datetime(2026, 7, 4, tzinfo=UTC),
                [
                    _source(
                        collection_window_start=datetime(2026, 6, 29, tzinfo=UTC),
                        collection_window_end=datetime(2026, 7, 4, tzinfo=UTC),
                    )
                ],
            )
            uow.commit()

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
        assert expanded_ready.status.value == "ready"
        assert replacement_ready.source_snapshot.source_through == datetime(2026, 7, 4, tzinfo=UTC)
        assert expanded_ready.source_snapshot.source_through == replacement_ready.source_snapshot.source_through
    finally:
        runtime.close()
        backend.dispose()
