from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import ROUND_DOWN, ROUND_UP, Decimal, localcontext
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
from sqlalchemy import text
from sqlmodel import Session

from core.engine.allocation import AllocatorRegistry
from core.engine.orchestrator import CalculatePhase
from core.models.chargeback import ChargebackRow, CostType
from core.models.pipeline import PipelineState
from core.plugin.registry import EcosystemBundle
from core.storage.backends.sqlmodel.engine import get_or_create_engine
from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend
from plugins.confluent_cloud.plugin import ConfluentCloudPlugin
from plugins.confluent_cloud.storage.module import CCloudStorageModule
from tests.unit.core.preview.conftest import preview_module
from tests.unit.core.preview.test_service import (
    ControlledExecutor,
    _aggregate,
    _context_resource,
    _runtime,
    _seed,
    _source,
    _submit,
    _tenant_config,
)


def _backend(tmp_path: Path, name: str = "preview.db") -> SQLModelBackend:
    backend = SQLModelBackend(
        f"sqlite:///{tmp_path / name}",
        CCloudStorageModule(),
        use_migrations=False,
    )
    backend.create_tables()
    return backend


def _persist_legacy_source(backend: SQLModelBackend) -> None:
    from plugins.confluent_cloud.storage.repositories import _source_to_table

    engine = get_or_create_engine(backend._connection_string)
    with Session(engine) as session:
        session.add(
            _source_to_table(
                _source(
                    billing_timestamp=None,
                    billing_env_id=None,
                    billing_resource_id=None,
                    billing_product_type=None,
                    billing_product_category=None,
                )
            )
        )
        session.commit()


def _associated_source(**overrides: object) -> Any:
    values: dict[str, object] = {
        "billing_timestamp": datetime(2026, 7, 1, tzinfo=UTC),
        "billing_env_id": "env-1",
        "billing_resource_id": "lkc-1",
        "billing_product_type": "KAFKA_STORAGE",
        "billing_product_category": "KAFKA",
    }
    values.update(overrides)
    return _source(**values)


def _run_failure(
    tmp_path: Path,
    backend: SQLModelBackend,
    *,
    end_date: date = date(2026, 7, 2),
) -> Any:
    executor = ControlledExecutor()
    runtime = _runtime(tmp_path, backend, executor)
    try:
        queued = (
            _submit(runtime, backend)
            if end_date == date(2026, 7, 2)
            else runtime.submit(
                tenant_name="production",
                tenant_config=_tenant_config(backend._connection_string),
                backend=backend,
                start_date=date(2026, 7, 1),
                end_date=end_date,
                grain="daily",
                column_profile="full",
                effective_columns=preview_module("mapping").FOCUS_1_4_FULL_PROFILE_COLUMNS,
            )
        )
        executor.run_all()
        failed = runtime.get_request(
            backend=backend,
            request_id=queued.request_id,
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
        )
        assert failed.status.value == "failed"
        assert failed.source_snapshot is None
        assert failed.package is None
        assert failed.storage_key is None
        return failed
    finally:
        runtime.close()


def _seed_two_day_origins(
    backend: SQLModelBackend,
    *,
    first_currency: str = "USD",
    first_source_cost: Decimal = Decimal("8"),
    first_aggregate_cost: Decimal = Decimal("8"),
    second_source_cost: Decimal = Decimal("4"),
    second_aggregate_cost: Decimal = Decimal("4"),
    first_allocated_cost: Decimal = Decimal("8"),
    second_method_version: str = "v1",
) -> None:
    from core.engine.allocation_lineage import build_allocation_lineage_capture
    from core.storage.interface import AllocationLineageRunCapture

    _seed(backend)
    first_source = _associated_source(
        amount=first_source_cost,
        original_amount=first_source_cost,
        discount_amount=Decimal(0),
        price=first_source_cost,
        quantity=Decimal(1),
    )
    second_source = _associated_source(
        source_record_id="provider:cost-2",
        provider_cost_id="cost-2",
        source_period_start=datetime(2026, 7, 2, tzinfo=UTC),
        source_period_end=datetime(2026, 7, 3, tzinfo=UTC),
        evidence_scope_start=datetime(2026, 7, 2, tzinfo=UTC),
        evidence_scope_end=datetime(2026, 7, 3, tzinfo=UTC),
        allocation_timestamp=datetime(2026, 7, 2, tzinfo=UTC),
        retention_timestamp=datetime(2026, 7, 2, tzinfo=UTC),
        amount=second_source_cost,
        original_amount=second_source_cost,
        discount_amount=Decimal(0),
        price=second_source_cost,
        quantity=Decimal(1),
        resource_id="lkc-2",
        resource_name="Payments",
        billing_timestamp=datetime(2026, 7, 2, tzinfo=UTC),
        billing_resource_id="lkc-2",
        raw_payload={"id": "cost-2"},
    )
    first_aggregate = _aggregate(
        currency=first_currency,
        quantity=Decimal(1),
        unit_price=first_aggregate_cost,
        total_cost=first_aggregate_cost,
    )
    second_aggregate = _aggregate(
        timestamp=datetime(2026, 7, 2, tzinfo=UTC),
        resource_id="lkc-2",
        quantity=Decimal(1),
        unit_price=second_aggregate_cost,
        total_cost=second_aggregate_cost,
    )
    first_row = ChargebackRow(
        ecosystem="confluent_cloud",
        tenant_id="tenant-1",
        timestamp=first_aggregate.timestamp,
        resource_id=first_aggregate.resource_id,
        product_category=first_aggregate.product_category,
        product_type=first_aggregate.product_type,
        identity_id="sa-1",
        cost_type=CostType.USAGE,
        amount=first_allocated_cost,
        allocation_method="direct",
        allocation_detail="direct",
        metadata={"env_id": "env-1"},
    )
    second_row = ChargebackRow(
        ecosystem="confluent_cloud",
        tenant_id="tenant-1",
        timestamp=second_aggregate.timestamp,
        resource_id=second_aggregate.resource_id,
        product_category=second_aggregate.product_category,
        product_type=second_aggregate.product_type,
        identity_id="sa-1",
        cost_type=CostType.USAGE,
        amount=second_aggregate.total_cost,
        allocation_method="direct",
        allocation_detail="direct",
        metadata={"env_id": "env-1"},
    )
    completed_at = datetime(2026, 7, 3, 2, tzinfo=UTC)
    with backend.create_unit_of_work() as uow:
        uow.resources.upsert(
            replace(
                _context_resource("lkc-2", "kafka_cluster"),
                created_at=datetime(2026, 1, 1, tzinfo=UTC),
            )
        )
        uow.billing.replace_source_window(
            "confluent_cloud",
            "tenant-1",
            datetime(2026, 6, 30, tzinfo=UTC),
            datetime(2026, 7, 4, tzinfo=UTC),
            [first_source, second_source],
        )
        uow.billing.upsert(first_aggregate)
        uow.billing.upsert(second_aggregate)
        for tracking_date, calculation_id, aggregate, row in (
            (date(2026, 7, 1), "calculation-1", first_aggregate, first_row),
            (date(2026, 7, 2), "calculation-2", second_aggregate, second_row),
        ):
            uow.chargebacks.replace_calculation_lineage(  # type: ignore[attr-defined]
                AllocationLineageRunCapture(
                    ecosystem="confluent_cloud",
                    tenant_id="tenant-1",
                    tracking_date=tracking_date,
                    calculation_id=calculation_id,
                    captures=(build_allocation_lineage_capture(origin=aggregate, rows=(row,)),),
                ),
                calculation_completed_at=completed_at,
            )
            uow.pipeline_state.upsert(
                PipelineState(
                    ecosystem="confluent_cloud",
                    tenant_id="tenant-1",
                    tracking_date=tracking_date,
                    billing_gathered=True,
                    resources_gathered=True,
                    chargeback_calculated=True,
                    calculation_id=calculation_id,
                    calculation_completed_at=completed_at,
                    calculation_run_id=None,
                )
            )
        uow.commit()
    if second_method_version != "v1":
        engine = get_or_create_engine(backend._connection_string)
        with engine.begin() as connection:
            connection.execute(
                text(
                    "UPDATE ccloud_allocation_lineage_portions "
                    "SET method_version = :version WHERE origin_resource_id = 'lkc-2'"
                ),
                {"version": second_method_version},
            )


_STREAM_PERMUTATIONS = (
    (False, False, False, False),
    (True, False, False, False),
    (False, True, False, False),
    (False, False, True, False),
    (False, False, False, True),
    (True, True, True, True),
)


def _run_permuted_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    backend: SQLModelBackend,
    permutation: tuple[bool, bool, bool, bool],
) -> Any:
    from plugins.confluent_cloud.storage.repositories import (
        CCloudBillingRepository,
        CCloudChargebackRepository,
    )

    reverse_sources, reverse_aggregates, reverse_runs, reverse_portions = permutation
    original_sources = CCloudBillingRepository.iter_preview_sources
    original_aggregates = CCloudBillingRepository.iter_preview_aggregates
    original_runs = CCloudChargebackRepository.iter_preview_allocation_runs
    original_portions = CCloudChargebackRepository.iter_preview_allocations

    def ordered_sources(self: Any, scope: Any) -> Any:
        rows = tuple(original_sources(self, scope))
        return iter(reversed(rows) if reverse_sources else rows)

    def ordered_aggregates(self: Any, scope: Any) -> Any:
        rows = tuple(original_aggregates(self, scope))
        return iter(reversed(rows) if reverse_aggregates else rows)

    def ordered_runs(self: Any, scope: Any, calculation_ids: tuple[str, ...]) -> Any:
        rows = tuple(original_runs(self, scope, calculation_ids))
        return iter(reversed(rows) if reverse_runs else rows)

    def ordered_portions(self: Any, scope: Any, calculation_ids: tuple[str, ...]) -> Any:
        rows = tuple(original_portions(self, scope, calculation_ids))
        return iter(reversed(rows) if reverse_portions else rows)

    with monkeypatch.context() as patcher:
        patcher.setattr(CCloudBillingRepository, "iter_preview_sources", ordered_sources)
        patcher.setattr(CCloudBillingRepository, "iter_preview_aggregates", ordered_aggregates)
        patcher.setattr(CCloudChargebackRepository, "iter_preview_allocation_runs", ordered_runs)
        patcher.setattr(CCloudChargebackRepository, "iter_preview_allocations", ordered_portions)
        return _run_failure(tmp_path, backend, end_date=date(2026, 7, 3))


def _run_permuted_success(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    backend: SQLModelBackend,
    permutation: tuple[bool, bool, bool, bool],
) -> dict[str, object]:
    from plugins.confluent_cloud.storage.repositories import (
        CCloudBillingRepository,
        CCloudChargebackRepository,
    )

    reverse_sources, reverse_aggregates, reverse_runs, reverse_portions = permutation
    original_sources = CCloudBillingRepository.iter_preview_sources
    original_aggregates = CCloudBillingRepository.iter_preview_aggregates
    original_runs = CCloudChargebackRepository.iter_preview_allocation_runs
    original_portions = CCloudChargebackRepository.iter_preview_allocations

    def ordered_sources(self: Any, scope: Any) -> Any:
        rows = tuple(original_sources(self, scope))
        return iter(reversed(rows) if reverse_sources else rows)

    def ordered_aggregates(self: Any, scope: Any) -> Any:
        rows = tuple(original_aggregates(self, scope))
        return iter(reversed(rows) if reverse_aggregates else rows)

    def ordered_runs(self: Any, scope: Any, calculation_ids: tuple[str, ...]) -> Any:
        rows = tuple(original_runs(self, scope, calculation_ids))
        return iter(reversed(rows) if reverse_runs else rows)

    def ordered_portions(self: Any, scope: Any, calculation_ids: tuple[str, ...]) -> Any:
        rows = tuple(original_portions(self, scope, calculation_ids))
        return iter(reversed(rows) if reverse_portions else rows)

    executor = ControlledExecutor()
    runtime = _runtime(tmp_path, backend, executor)
    try:
        with monkeypatch.context() as patcher:
            patcher.setattr(CCloudBillingRepository, "iter_preview_sources", ordered_sources)
            patcher.setattr(CCloudBillingRepository, "iter_preview_aggregates", ordered_aggregates)
            patcher.setattr(CCloudChargebackRepository, "iter_preview_allocation_runs", ordered_runs)
            patcher.setattr(CCloudChargebackRepository, "iter_preview_allocations", ordered_portions)
            queued = runtime.submit(
                tenant_name="production",
                tenant_config=_tenant_config(backend._connection_string),
                backend=backend,
                start_date=date(2026, 7, 1),
                end_date=date(2026, 7, 3),
                grain="daily",
                column_profile="full",
                effective_columns=preview_module("mapping").FOCUS_1_4_FULL_PROFILE_COLUMNS,
            )
            executor.run_all()
        ready = runtime.get_request(
            backend=backend,
            request_id=queued.request_id,
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
        )
        assert ready is not None
        assert ready.status.value == "ready", ready.diagnostic
        return json.loads(runtime.read_manifest_bytes(ready))
    finally:
        runtime.close()


def _persist_valid_or_short_lineage(
    backend: SQLModelBackend,
    *,
    amount: Decimal = Decimal("8"),
    calculation_id: str = "calculation-1",
) -> None:
    from core.engine.allocation_lineage import build_allocation_lineage_capture
    from core.storage.interface import AllocationLineageRunCapture

    origin = _aggregate()
    row = ChargebackRow(
        ecosystem="confluent_cloud",
        tenant_id="tenant-1",
        timestamp=origin.timestamp,
        resource_id=origin.resource_id,
        product_category=origin.product_category,
        product_type=origin.product_type,
        identity_id="sa-1",
        cost_type=CostType.USAGE,
        amount=amount,
        allocation_method="direct",
        allocation_detail="direct",
        metadata={"env_id": "env-1"},
    )
    capture = build_allocation_lineage_capture(origin=origin, rows=(row,))
    run = AllocationLineageRunCapture(
        ecosystem="confluent_cloud",
        tenant_id="tenant-1",
        tracking_date=origin.timestamp.date(),
        calculation_id=calculation_id,
        captures=(capture,),
    )
    with backend.create_unit_of_work() as uow:
        uow.chargebacks.replace_calculation_lineage(  # type: ignore[attr-defined]
            run,
            calculation_completed_at=datetime(2026, 7, 3, 2, tzinfo=UTC),
        )
        uow.commit()


def _persist_invalid_lineage(backend: SQLModelBackend) -> None:
    from core.storage.interface import (
        AllocationLineageCapture,
        AllocationLineageRunCapture,
        LineageCaptureReason,
        LineageCaptureStatus,
    )

    origin = _aggregate()
    capture = AllocationLineageCapture(
        origin_timestamp=origin.timestamp,
        origin_env_id=origin.env_id,
        origin_resource_id=origin.resource_id,
        origin_product_type=origin.product_type,
        origin_product_category=origin.product_category,
        status=LineageCaptureStatus.INVALID,
        reason=LineageCaptureReason.INVALID_METADATA,
        facts=(),
    )
    run = AllocationLineageRunCapture(
        ecosystem="confluent_cloud",
        tenant_id="tenant-1",
        tracking_date=origin.timestamp.date(),
        calculation_id="calculation-1",
        captures=(capture,),
    )
    with backend.create_unit_of_work() as uow:
        uow.chargebacks.replace_calculation_lineage(  # type: ignore[attr-defined]
            run,
            calculation_completed_at=datetime(2026, 7, 3, 2, tzinfo=UTC),
        )
        uow.commit()


def test_legacy_source_without_billing_association_fails_coverage_before_missing_lineage(tmp_path: Path) -> None:
    backend = _backend(tmp_path)
    _seed(backend, aggregate=_aggregate())
    _persist_legacy_source(backend)

    failed = _run_failure(tmp_path, backend)

    assert failed.diagnostic.code == "preview_source_coverage_incomplete"
    assert (
        failed.diagnostic.message == "Persisted source evidence does not completely cover the calculated Preview scope."
    )
    assert failed.diagnostic.retryable is False
    backend.dispose()


def test_complete_association_without_lineage_fails_exact_nonretryable_diagnostic(tmp_path: Path) -> None:
    backend = _backend(tmp_path)
    _seed(backend, source=_associated_source(), aggregate=_aggregate())

    failed = _run_failure(tmp_path, backend)

    assert failed.diagnostic.code == "preview_allocation_lineage_incomplete"
    assert failed.diagnostic.message == "Persisted allocation lineage is incomplete for one or more billing origins."
    assert failed.diagnostic.retryable is False
    assert len(failed.diagnostic.source_correlation_ids) == 1
    backend.dispose()


def test_invalid_run_uses_closed_lineage_diagnostic_without_exposing_safe_capture_reason(tmp_path: Path) -> None:
    backend = _backend(tmp_path)
    _seed(backend, source=_associated_source(), aggregate=_aggregate())
    _persist_invalid_lineage(backend)

    failed = _run_failure(tmp_path, backend)

    assert failed.diagnostic.code == "preview_allocation_lineage_incomplete"
    assert failed.diagnostic.message == "Persisted allocation lineage is incomplete for one or more billing origins."
    assert "invalid_metadata" not in failed.diagnostic.message
    assert failed.diagnostic.retryable is False
    backend.dispose()


def test_lineage_calculation_identity_mismatch_fails_closed(tmp_path: Path) -> None:
    backend = _backend(tmp_path)
    _seed(backend, source=_associated_source(), aggregate=_aggregate())
    _persist_valid_or_short_lineage(backend, calculation_id="different-calculation")

    failed = _run_failure(tmp_path, backend)

    assert failed.diagnostic.code == "preview_allocation_lineage_incomplete"
    assert failed.diagnostic.retryable is False
    backend.dispose()


def test_multiple_native_tier_sources_for_one_billing_origin_still_fail_scope_before_lineage(tmp_path: Path) -> None:
    backend = _backend(tmp_path)
    first = _associated_source(source_record_id="provider:tier-1", provider_cost_id="tier-1")
    second = _associated_source(
        source_record_id="provider:tier-2",
        provider_cost_id="tier-2",
        raw_payload={"id": "tier-2"},
    )
    _seed(backend, aggregate=_aggregate())
    with backend.create_unit_of_work() as uow:
        uow.billing.replace_source_window(
            "confluent_cloud",
            "tenant-1",
            datetime(2026, 6, 30, tzinfo=UTC),
            datetime(2026, 7, 3, tzinfo=UTC),
            [first, second],
        )
        uow.commit()

    failed = _run_failure(tmp_path, backend)

    assert failed.diagnostic.code == "preview_mapping_scope_unsupported"
    assert failed.diagnostic.message == "The complete source set exceeds the current Daily Full mapping scope."
    assert failed.diagnostic.retryable is False
    assert len(failed.diagnostic.source_correlation_ids) == 2
    backend.dispose()


def test_tableflow_provider_context_precedes_coverage_and_lineage(tmp_path: Path) -> None:
    backend = _backend(tmp_path)
    source = _associated_source(
        product="TABLEFLOW",
        line_type="TABLEFLOW_DATA_PROCESSED",
        description="Tableflow data processed",
        resource_id="tableflow-topic-1",
        billing_resource_id="tableflow-topic-1",
        billing_product_type="TABLEFLOW_DATA_PROCESSED",
        billing_product_category="TABLEFLOW",
    )
    _seed(backend, source=source)

    failed = _run_failure(tmp_path, backend)

    assert failed.diagnostic.code == "preview_provider_context_incomplete"
    assert failed.diagnostic.message == (
        "Authoritative provider resource context is unavailable for one or more source records."
    )
    backend.dispose()


def test_aggregate_currency_precedes_missing_lineage(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from plugins.confluent_cloud.storage.repositories import CCloudChargebackRepository

    backend = _backend(tmp_path)
    _seed(
        backend,
        source=_associated_source(),
        aggregate=_aggregate(currency="EUR"),
    )

    def forbidden(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("lineage must not be read before aggregate currency validation")

    monkeypatch.setattr(CCloudChargebackRepository, "iter_preview_allocations", forbidden)
    failed = _run_failure(tmp_path, backend)

    assert failed.diagnostic.code == "preview_billing_currency_unsupported"
    assert failed.diagnostic.message == "FOCUS Mapping Preview currently supports only USD billing currency."
    assert failed.diagnostic.retryable is False
    backend.dispose()


def test_exact_source_aggregate_equality_precedes_missing_lineage(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from plugins.confluent_cloud.storage.repositories import CCloudChargebackRepository

    backend = _backend(tmp_path)
    _seed(
        backend,
        source=_associated_source(),
        aggregate=_aggregate(total_cost=Decimal("7")),
    )

    def forbidden(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("lineage must not be read before exact aggregate reconciliation")

    monkeypatch.setattr(CCloudChargebackRepository, "iter_preview_allocations", forbidden)
    failed = _run_failure(tmp_path, backend)

    assert failed.diagnostic.code == "preview_source_reconciliation_failed"
    assert failed.diagnostic.message == "Persisted source, aggregate, or allocation evidence does not reconcile."
    assert failed.diagnostic.retryable is False
    backend.dispose()


@pytest.mark.parametrize("amount", [Decimal("7"), Decimal("9")], ids=("shortfall", "overage"))
def test_actual_portion_cost_or_quantity_mismatch_fails_exact_reconciliation(
    tmp_path: Path,
    amount: Decimal,
) -> None:
    backend = _backend(tmp_path)
    _seed(backend, source=_associated_source(), aggregate=_aggregate())
    _persist_valid_or_short_lineage(backend, amount=amount)

    failed = _run_failure(tmp_path, backend)

    assert failed.diagnostic.code == "preview_source_reconciliation_failed"
    assert failed.diagnostic.message == "Persisted source, aggregate, or allocation evidence does not reconcile."
    assert failed.diagnostic.retryable is False
    backend.dispose()


@pytest.mark.parametrize(
    ("column", "value"),
    [
        ("target_kind", "bogus"),
        ("method_id", ""),
        ("method_version", "v2"),
        ("allocation_ratio", "-1"),
        ("method_details_json", "not-json"),
    ],
)
def test_corrupt_persisted_portion_fails_lineage_before_reconciliation(
    tmp_path: Path,
    column: str,
    value: str,
) -> None:
    backend = _backend(tmp_path)
    _seed(backend, source=_associated_source(), aggregate=_aggregate())
    _persist_valid_or_short_lineage(backend)
    engine = get_or_create_engine(backend._connection_string)
    with engine.begin() as connection:
        connection.execute(
            text(f"UPDATE ccloud_allocation_lineage_portions SET {column} = :value"),  # noqa: S608
            {"value": value},
        )

    failed = _run_failure(tmp_path, backend)

    assert failed.diagnostic.code == "preview_allocation_lineage_incomplete"
    assert failed.diagnostic.message == "Persisted allocation lineage is incomplete for one or more billing origins."
    backend.dispose()


@pytest.mark.parametrize(
    ("assignments", "values"),
    [
        ("allocated_cost = :cost, allocation_ratio = :ratio", {"cost": "7", "ratio": "0.875"}),
        ("allocated_quantity = :quantity", {"quantity": "4"}),
    ],
    ids=("cost-only", "quantity-only"),
)
def test_isolated_portion_reconciliation_mismatch_has_exact_diagnostic(
    tmp_path: Path,
    assignments: str,
    values: dict[str, str],
) -> None:
    backend = _backend(tmp_path)
    _seed(backend, source=_associated_source(), aggregate=_aggregate())
    _persist_valid_or_short_lineage(backend)
    engine = get_or_create_engine(backend._connection_string)
    with engine.begin() as connection:
        connection.execute(
            text(f"UPDATE ccloud_allocation_lineage_portions SET {assignments}"),  # noqa: S608
            values,
        )

    failed = _run_failure(tmp_path, backend)

    assert failed.diagnostic.code == "preview_source_reconciliation_failed"
    assert failed.diagnostic.message == "Persisted source, aggregate, or allocation evidence does not reconcile."
    assert failed.diagnostic.retryable is False
    backend.dispose()


@pytest.mark.parametrize(
    ("target_kind", "target_id"),
    [("unallocated", "UNALLOCATED"), ("identity", None)],
    ids=("unallocated-with-id", "allocated-without-id"),
)
def test_invalid_unallocated_encoding_fails_closed_lineage_diagnostic(
    tmp_path: Path,
    target_kind: str,
    target_id: str | None,
) -> None:
    backend = _backend(tmp_path)
    _seed(backend, source=_associated_source(), aggregate=_aggregate())
    _persist_valid_or_short_lineage(backend)
    engine = get_or_create_engine(backend._connection_string)
    with engine.begin() as connection:
        connection.execute(
            text("UPDATE ccloud_allocation_lineage_portions SET target_kind = :target_kind, target_id = :target_id"),
            {"target_kind": target_kind, "target_id": target_id},
        )

    failed = _run_failure(tmp_path, backend)

    assert failed.diagnostic.code == "preview_allocation_lineage_incomplete"
    assert failed.diagnostic.message == "Persisted allocation lineage is incomplete for one or more billing origins."
    assert failed.diagnostic.retryable is False
    backend.dispose()


@pytest.mark.parametrize("reverse_sources", [False, True])
@pytest.mark.parametrize("reverse_aggregates", [False, True])
def test_source_issue_precedence_is_independent_of_complete_stream_iterator_order(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    reverse_sources: bool,
    reverse_aggregates: bool,
) -> None:
    from plugins.confluent_cloud.storage.repositories import CCloudBillingRepository

    backend = _backend(tmp_path, f"order-{reverse_sources}-{reverse_aggregates}.db")
    _seed(backend, aggregate=_aggregate())
    sources = [
        _associated_source(
            source_record_id="provider:classification",
            provider_cost_id="classification",
            line_type="FUTURE_LINE",
            billing_product_type="FUTURE_LINE",
            raw_payload={"id": "classification"},
        ),
        _associated_source(
            source_record_id="provider:structural",
            provider_cost_id="structural",
            malformed=True,
            raw_payload={"id": "structural"},
        ),
    ]
    with backend.create_unit_of_work() as uow:
        uow.billing.replace_source_window(
            "confluent_cloud",
            "tenant-1",
            datetime(2026, 6, 30, tzinfo=UTC),
            datetime(2026, 7, 3, tzinfo=UTC),
            sources,
        )
        uow.commit()
    original_sources = CCloudBillingRepository.iter_preview_sources
    original_aggregates = CCloudBillingRepository.iter_preview_aggregates

    def ordered_sources(self: Any, scope: Any) -> Any:
        rows = list(original_sources(self, scope))
        return iter(reversed(rows) if reverse_sources else rows)

    def ordered_aggregates(self: Any, scope: Any) -> Any:
        rows = list(original_aggregates(self, scope))
        return iter(reversed(rows) if reverse_aggregates else rows)

    monkeypatch.setattr(CCloudBillingRepository, "iter_preview_sources", ordered_sources)
    monkeypatch.setattr(CCloudBillingRepository, "iter_preview_aggregates", ordered_aggregates)

    failed = _run_failure(tmp_path, backend)

    assert failed.diagnostic.code == "preview_source_record_malformed"
    assert failed.diagnostic.retryable is False
    assert len(failed.diagnostic.source_correlation_ids) == 1
    backend.dispose()


@pytest.mark.parametrize(
    ("currency", "expected_code", "expected_message"),
    [
        (
            "",
            "preview_billing_currency_unknown",
            "Persisted billing currency evidence is unknown for one or more source records.",
        ),
        (
            "EUR",
            "preview_billing_currency_unsupported",
            "FOCUS Mapping Preview currently supports only USD billing currency.",
        ),
    ],
    ids=("unknown", "unsupported"),
)
def test_global_currency_stage_precedes_later_origin_equality_mismatch_for_all_stream_orders(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    currency: str,
    expected_code: str,
    expected_message: str,
) -> None:
    contracts: list[tuple[str, str, bool, tuple[str, ...]]] = []
    for index, permutation in enumerate(_STREAM_PERMUTATIONS):
        backend = _backend(tmp_path, f"currency-{currency or 'unknown'}-{index}.db")
        try:
            _seed_two_day_origins(
                backend,
                first_currency=currency,
                second_source_cost=Decimal("4"),
                second_aggregate_cost=Decimal("3"),
            )
            failed = _run_permuted_failure(tmp_path, monkeypatch, backend, permutation)
            contracts.append(
                (
                    failed.diagnostic.code,
                    failed.diagnostic.message,
                    failed.diagnostic.retryable,
                    tuple(failed.diagnostic.source_correlation_ids),
                )
            )
        finally:
            backend.dispose()

    assert len(set(contracts)) == 1
    code, message, retryable, correlations = contracts[0]
    assert code == expected_code
    assert message == expected_message
    assert retryable is False
    assert len(correlations) == 1


def test_global_lineage_structure_stage_precedes_earlier_origin_totals_mismatch_for_all_stream_orders(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    contracts: list[tuple[str, str, bool, tuple[str, ...]]] = []
    for index, permutation in enumerate(_STREAM_PERMUTATIONS):
        backend = _backend(tmp_path, f"lineage-{index}.db")
        try:
            _seed_two_day_origins(
                backend,
                first_allocated_cost=Decimal("7"),
                second_method_version="v2",
            )
            failed = _run_permuted_failure(tmp_path, monkeypatch, backend, permutation)
            contracts.append(
                (
                    failed.diagnostic.code,
                    failed.diagnostic.message,
                    failed.diagnostic.retryable,
                    tuple(failed.diagnostic.source_correlation_ids),
                )
            )
        finally:
            backend.dispose()

    assert len(set(contracts)) == 1
    code, message, retryable, correlations = contracts[0]
    assert code == "preview_allocation_lineage_incomplete"
    assert message == "Persisted allocation lineage is incomplete for one or more billing origins."
    assert retryable is False
    assert len(correlations) == 2


@pytest.mark.parametrize(
    ("precision", "rounding"),
    [(6, ROUND_DOWN), (9, ROUND_UP)],
    ids=("low-precision-down", "changed-precision-up"),
)
def test_package_reconciliation_totals_ignore_ambient_decimal_context_and_stream_order(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    precision: int,
    rounding: str,
) -> None:
    expected = "999999.000001"
    observed: list[dict[str, object]] = []
    for index, permutation in enumerate(_STREAM_PERMUTATIONS):
        backend = _backend(tmp_path, f"decimal-context-{precision}-{index}.db")
        try:
            _seed_two_day_origins(
                backend,
                first_source_cost=Decimal("999999"),
                first_aggregate_cost=Decimal("999999"),
                first_allocated_cost=Decimal("999999"),
                second_source_cost=Decimal("0.000001"),
                second_aggregate_cost=Decimal("0.000001"),
            )
            with localcontext() as ambient:
                ambient.prec = precision
                ambient.rounding = rounding
                manifest = _run_permuted_success(tmp_path, monkeypatch, backend, permutation)
            reconciliation = manifest["reconciliation"]
            assert isinstance(reconciliation, dict)
            observed.append(reconciliation)
        finally:
            backend.dispose()

    assert observed == [
        {
            "source_cost": expected,
            "allocated_cost": expected,
            "difference": "0",
            "source_quantity": "2",
            "allocated_quantity": "2",
            "quantity_difference": "0",
        }
    ] * len(_STREAM_PERMUTATIONS)


def test_recalculation_alone_cannot_recover_legacy_association_but_regather_then_calculation_can(
    tmp_path: Path,
) -> None:
    backend = _backend(tmp_path, "legacy-recovery.db")
    _seed(backend, aggregate=_aggregate())
    _persist_legacy_source(backend)
    plugin = ConfluentCloudPlugin()
    plugin.initialize({"ccloud_api": {"key": "key", "secret": "secret"}})  # pragma: allowlist secret
    calculation_ids = iter(("recalculation-only", "regather-calculation"))
    phase = CalculatePhase(
        ecosystem="confluent_cloud",
        tenant_id="tenant-1",
        bundle=EcosystemBundle.build(plugin),
        retry_checker=MagicMock(),
        metrics_source=None,
        allocator_registry=AllocatorRegistry(),
        identity_overrides={},
        allocator_params={},
        metrics_step=timedelta(hours=1),
        calculation_id_factory=lambda: next(calculation_ids),
        calculation_clock=lambda: datetime(2026, 7, 3, 2, tzinfo=UTC),
    )
    with backend.create_unit_of_work() as uow:
        assert phase.run(uow, datetime(2026, 7, 1, tzinfo=UTC).date()) == 1
        uow.commit()

    failed = _run_failure(tmp_path, backend)
    assert failed.diagnostic.code == "preview_source_coverage_incomplete"

    with backend.create_unit_of_work() as uow:
        uow.billing.replace_source_window(
            "confluent_cloud",
            "tenant-1",
            datetime(2026, 6, 30, tzinfo=UTC),
            datetime(2026, 7, 3, tzinfo=UTC),
            [_associated_source()],
        )
        uow.pipeline_state.mark_needs_recalculation(
            "confluent_cloud", "tenant-1", datetime(2026, 7, 1, tzinfo=UTC).date()
        )
        uow.commit()
    with backend.create_unit_of_work() as uow:
        uow.chargebacks.delete_by_date(
            "confluent_cloud",
            "tenant-1",
            datetime(2026, 7, 1, tzinfo=UTC).date(),
        )
        assert phase.run(uow, datetime(2026, 7, 1, tzinfo=UTC).date()) == 1
        uow.commit()

    artifacts = preview_module("artifacts")
    service = preview_module("service")
    executor = ControlledExecutor()
    runtime = service.PreviewRuntime(
        artifact_store=artifacts.LocalPreviewArtifactStore(tmp_path / "recovered-artifacts"),
        max_workers=1,
        clock=lambda: datetime(2026, 7, 4, tzinfo=UTC),
        request_id_factory=lambda: "request-recovered",
        executor=executor,
    )
    try:
        queued = _submit(runtime, backend)
        executor.run_all()
        ready = runtime.get_request(
            backend=backend,
            request_id=queued.request_id,
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
        )
        assert ready.status.value == "ready"
        assert len(runtime.read_file_bytes(ready, "cost-and-usage.csv")) > 0
    finally:
        runtime.close()
        backend.dispose()
        plugin.close()
