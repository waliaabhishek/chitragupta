"""Tests for GAP-116: Distinguish metrics prefetch failure from empty data.

Verification cases from design doc:
1. _prefetch_metrics returns (prefetched, failed_keys) tuple — three outcomes
2. AllocationContext.metrics_fetch_failed defaults to False
3. AllocationDetail.METRICS_FETCH_FAILED exists in enum
4. UsageRatioModel with metrics_fetch_failed=True → METRICS_FETCH_FAILED detail
5. UsageRatioModel with metrics_fetch_failed=False and empty usage → NO_USAGE_FOR_ACTIVE_IDENTITIES (unchanged)
6. allocate_by_usage_ratio defense-in-depth guard → METRICS_FETCH_FAILED detail
7. End-to-end: failed prefetch key produces METRICS_FETCH_FAILED allocation_detail
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from datetime import date as date_type
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock

import pytest  # noqa: TC002

from core.engine.allocation import AllocationContext, AllocatorRegistry
from core.engine.allocation_models import UsageRatioModel
from core.engine.helpers import allocate_by_usage_ratio
from core.engine.orchestrator import CalculatePhase
from core.models import IdentityResolution, IdentitySet
from core.models.billing import BillingLineItem, CoreBillingLineItem
from core.models.chargeback import AllocationDetail, ChargebackRow
from core.models.metrics import MetricQuery

from .conftest import make_billing_line, make_identity_resolution

# ---------- Constants ----------

NOW = datetime(2026, 2, 22, 12, 0, 0, tzinfo=UTC)
TODAY = NOW.date()
ECOSYSTEM = "test-eco"
TENANT_ID = "tenant-1"


# ---------- Factories ----------


def _make_billing_line(
    product_type: str = "KAFKA_CKU",
    resource_id: str = "cluster-1",
    total_cost: Decimal = Decimal("100.00"),
    timestamp: datetime | None = None,
) -> BillingLineItem:
    return CoreBillingLineItem(
        ecosystem=ECOSYSTEM,
        tenant_id=TENANT_ID,
        timestamp=timestamp or NOW,
        resource_id=resource_id,
        product_category="kafka",
        product_type=product_type,
        quantity=Decimal(1),
        unit_price=total_cost,
        total_cost=total_cost,
        granularity="daily",
    )


def _make_metric_query(key: str = "bytes_in") -> MetricQuery:
    return MetricQuery(
        key=key,
        query_expression=f"sum(metric_{key}{{}})",
        label_keys=("cluster_id",),
        resource_label="cluster_id",
    )


def _make_calculate_phase(
    bundle: Any | None = None,
    metrics_source: Any | None = None,
    allocator_registry: AllocatorRegistry | None = None,
    identity_overrides: dict | None = None,
    allocator_params: dict | None = None,
    metrics_prefetch_workers: int = 4,
) -> CalculatePhase:
    if bundle is None:
        bundle = MagicMock()
        bundle.product_type_to_handler = {}
    retry_checker = MagicMock()
    retry_checker.increment_and_check.return_value = (1, False)
    return CalculatePhase(
        ecosystem=ECOSYSTEM,
        tenant_id=TENANT_ID,
        bundle=bundle,
        retry_checker=retry_checker,
        metrics_source=metrics_source,
        allocator_registry=allocator_registry or AllocatorRegistry(),
        identity_overrides=identity_overrides or {},
        allocator_params=allocator_params or {},
        metrics_step=timedelta(hours=1),
        metrics_prefetch_workers=metrics_prefetch_workers,
    )


def _make_ctx(
    split_amount: Decimal = Decimal("10.00"),
    metrics_data: dict | None = None,
    metrics_fetch_failed: bool = False,
) -> AllocationContext:
    return AllocationContext(
        timeslice=NOW,
        billing_line=make_billing_line(),
        identities=make_identity_resolution(),
        split_amount=split_amount,
        metrics_data=metrics_data,
        metrics_fetch_failed=metrics_fetch_failed,
    )


# =============================================================================
# Test group 1: AllocationDetail.METRICS_FETCH_FAILED exists in enum
# =============================================================================


class TestAllocationDetailEnum:
    def test_metrics_fetch_failed_value_exists(self) -> None:
        """METRICS_FETCH_FAILED enum member must be present."""
        assert hasattr(AllocationDetail, "METRICS_FETCH_FAILED")

    def test_metrics_fetch_failed_string_value(self) -> None:
        """METRICS_FETCH_FAILED must serialize to 'metrics_fetch_failed'."""
        assert AllocationDetail.METRICS_FETCH_FAILED == "metrics_fetch_failed"


# =============================================================================
# Test group 2: AllocationContext.metrics_fetch_failed field
# =============================================================================


class TestAllocationContextMetricsFetchFailed:
    def test_metrics_fetch_failed_default_is_false(self) -> None:
        """AllocationContext.metrics_fetch_failed must default to False."""
        ctx = AllocationContext(
            timeslice=NOW,
            billing_line=make_billing_line(),
            identities=make_identity_resolution(),
        )
        assert ctx.metrics_fetch_failed is False

    def test_metrics_fetch_failed_can_be_set_true(self) -> None:
        """AllocationContext.metrics_fetch_failed can be explicitly set True."""
        ctx = AllocationContext(
            timeslice=NOW,
            billing_line=make_billing_line(),
            identities=make_identity_resolution(),
            metrics_fetch_failed=True,
        )
        assert ctx.metrics_fetch_failed is True

    def test_existing_fields_unaffected_by_new_field(self) -> None:
        """Adding metrics_fetch_failed must not break existing field access."""
        ctx = AllocationContext(
            timeslice=NOW,
            billing_line=make_billing_line(),
            identities=make_identity_resolution(),
            split_amount=Decimal("5.00"),
            metrics_data={"bytes_in": []},
            params={"k": "v"},
        )
        assert ctx.split_amount == Decimal("5.00")
        assert ctx.metrics_data == {"bytes_in": []}
        assert ctx.params == {"k": "v"}


# =============================================================================
# Test group 3: _prefetch_metrics returns tuple (prefetched, failed_keys)
# =============================================================================


class TestPrefetchMetricsReturnsTuple:
    def test_success_path_returns_tuple_key_not_in_failed_keys(self) -> None:
        """Successful query: prefetched has data, key not in failed_keys."""
        query = _make_metric_query()
        mock_handler = MagicMock()
        mock_handler.get_metrics_for_product_type.return_value = [query]
        mock_bundle = MagicMock()
        mock_bundle.product_type_to_handler.get.return_value = mock_handler

        expected_data = {"bytes_in": [{"value": 10.0, "timestamp": NOW}]}
        mock_metrics = MagicMock()
        mock_metrics.query.return_value = expected_data

        phase = _make_calculate_phase(bundle=mock_bundle, metrics_source=mock_metrics)
        lines = [_make_billing_line(resource_id="cluster-ok")]
        cache = phase._compute_line_window_cache(lines)

        result = phase._prefetch_metrics(lines, cache)

        # After fix: returns (prefetched_dict, frozenset) — must be a 2-tuple
        assert isinstance(result, tuple), f"Expected tuple, got {type(result)}"
        prefetched, failed_keys = result
        assert len(prefetched) == 1
        group_key = list(prefetched.keys())[0]
        assert group_key[0] == "cluster-ok"
        assert prefetched[group_key] == expected_data
        assert group_key not in failed_keys

    def test_empty_success_key_not_in_failed_keys(self) -> None:
        """Query returns {}: prefetched[key]=={}, key not in failed_keys."""
        query = _make_metric_query()
        mock_handler = MagicMock()
        mock_handler.get_metrics_for_product_type.return_value = [query]
        mock_bundle = MagicMock()
        mock_bundle.product_type_to_handler.get.return_value = mock_handler

        mock_metrics = MagicMock()
        mock_metrics.query.return_value = {}  # empty but successful

        phase = _make_calculate_phase(bundle=mock_bundle, metrics_source=mock_metrics)
        lines = [_make_billing_line(resource_id="cluster-idle")]
        cache = phase._compute_line_window_cache(lines)

        result = phase._prefetch_metrics(lines, cache)

        assert isinstance(result, tuple), f"Expected tuple, got {type(result)}"
        prefetched, failed_keys = result
        group_key = list(prefetched.keys())[0]
        assert prefetched[group_key] == {}
        assert group_key not in failed_keys

    def test_exception_key_in_failed_keys_prefetched_is_empty_dict(self, caplog: pytest.LogCaptureFixture) -> None:
        """Query raises exception: prefetched[key]=={}, key in failed_keys."""
        query = _make_metric_query()
        mock_handler = MagicMock()
        mock_handler.get_metrics_for_product_type.return_value = [query]
        mock_bundle = MagicMock()
        mock_bundle.product_type_to_handler.get.return_value = mock_handler

        mock_metrics = MagicMock()
        mock_metrics.query.side_effect = RuntimeError("Prometheus unreachable")

        phase = _make_calculate_phase(bundle=mock_bundle, metrics_source=mock_metrics)
        lines = [_make_billing_line(resource_id="cluster-fail")]
        cache = phase._compute_line_window_cache(lines)

        with caplog.at_level(logging.WARNING):
            result = phase._prefetch_metrics(lines, cache)

        assert isinstance(result, tuple), f"Expected tuple, got {type(result)}"
        prefetched, failed_keys = result
        group_key = list(prefetched.keys())[0]
        # prefetched stores {} for pass-through to identity handlers
        assert prefetched[group_key] == {}
        # key must be in failed_keys to distinguish from empty success
        assert group_key in failed_keys

    def test_partial_failure_only_failed_key_in_failed_keys(self) -> None:
        """Partial failure: only the failing resource key is in failed_keys."""
        query = _make_metric_query()
        mock_handler = MagicMock()
        mock_handler.get_metrics_for_product_type.return_value = [query]
        mock_bundle = MagicMock()
        mock_bundle.product_type_to_handler.get.return_value = mock_handler

        failing_resource = "cluster-fail"

        def selective_query(*args: Any, **kwargs: Any) -> dict:
            if kwargs.get("resource_id_filter") == failing_resource:
                raise RuntimeError("timeout")
            return {"bytes_in": [{"value": 5.0}]}

        mock_metrics = MagicMock()
        mock_metrics.query.side_effect = selective_query

        phase = _make_calculate_phase(bundle=mock_bundle, metrics_source=mock_metrics)
        lines = [
            _make_billing_line(resource_id="cluster-ok"),
            _make_billing_line(resource_id=failing_resource),
        ]
        cache = phase._compute_line_window_cache(lines)
        result = phase._prefetch_metrics(lines, cache)

        assert isinstance(result, tuple)
        prefetched, failed_keys = result
        failing_keys = [k for k in prefetched if k[0] == failing_resource]
        ok_keys = [k for k in prefetched if k[0] != failing_resource]
        assert len(failing_keys) == 1
        assert failing_keys[0] in failed_keys
        for ok_key in ok_keys:
            assert ok_key not in failed_keys

    def test_no_metrics_source_returns_tuple_with_empty_frozenset(self) -> None:
        """No metrics_source: returns ({}, frozenset()) — both empty."""
        query = _make_metric_query()
        mock_handler = MagicMock()
        mock_handler.get_metrics_for_product_type.return_value = [query]
        mock_bundle = MagicMock()
        mock_bundle.product_type_to_handler.get.return_value = mock_handler

        phase = _make_calculate_phase(bundle=mock_bundle, metrics_source=None)
        lines = [_make_billing_line(resource_id="cluster-1")]
        cache = phase._compute_line_window_cache(lines)

        result = phase._prefetch_metrics(lines, cache)

        assert isinstance(result, tuple)
        prefetched, failed_keys = result
        assert prefetched == {}
        assert failed_keys == frozenset()


# =============================================================================
# Test group 4: UsageRatioModel with metrics_fetch_failed
# =============================================================================


class TestUsageRatioModelMetricsFetchFailed:
    def test_metrics_fetch_failed_true_produces_metrics_fetch_failed_detail(self) -> None:
        """ctx.metrics_fetch_failed=True → METRICS_FETCH_FAILED, not NO_USAGE_FOR_ACTIVE_IDENTITIES."""

        def usage_source(ctx: AllocationContext) -> dict[str, float]:
            return {}  # would normally produce NO_USAGE_FOR_ACTIVE_IDENTITIES

        model = UsageRatioModel(usage_source=usage_source)
        ctx = _make_ctx(metrics_fetch_failed=True)
        result = model(ctx)

        assert len(result.rows) == 1
        row = result.rows[0]
        assert row.allocation_detail == AllocationDetail.METRICS_FETCH_FAILED
        assert row.identity_id == "UNALLOCATED"

    def test_metrics_fetch_failed_true_does_not_call_usage_source(self) -> None:
        """When metrics_fetch_failed=True, usage_source must not be called."""
        call_count = 0

        def usage_source(ctx: AllocationContext) -> dict[str, float]:
            nonlocal call_count
            call_count += 1
            return {}

        model = UsageRatioModel(usage_source=usage_source)
        ctx = _make_ctx(metrics_fetch_failed=True)
        model(ctx)

        assert call_count == 0, "usage_source must not be called when metrics_fetch_failed=True"

    def test_metrics_fetch_failed_false_empty_usage_produces_no_usage_detail(self) -> None:
        """Existing behavior: metrics_fetch_failed=False + empty usage → NO_USAGE_FOR_ACTIVE_IDENTITIES."""

        def usage_source(ctx: AllocationContext) -> dict[str, float]:
            return {}

        model = UsageRatioModel(usage_source=usage_source)
        ctx = _make_ctx(metrics_fetch_failed=False, metrics_data={})
        result = model(ctx)

        assert len(result.rows) == 1
        row = result.rows[0]
        assert row.allocation_detail == AllocationDetail.NO_USAGE_FOR_ACTIVE_IDENTITIES

    def test_metrics_fetch_failed_false_with_usage_allocates_normally(self) -> None:
        """Existing behavior: metrics_fetch_failed=False + usage data → normal allocation."""

        def usage_source(ctx: AllocationContext) -> dict[str, float]:
            return {"user-1": 100.0}

        model = UsageRatioModel(usage_source=usage_source)
        ctx = _make_ctx(metrics_fetch_failed=False, split_amount=Decimal("50.00"))
        result = model(ctx)

        assert len(result.rows) == 1
        assert result.rows[0].identity_id == "user-1"
        assert result.rows[0].allocation_detail != AllocationDetail.METRICS_FETCH_FAILED

    def test_usage_ratio_model_allocate_returns_none_on_metrics_fetch_failed(self) -> None:
        """UsageRatioModel.allocate() returns None when metrics_fetch_failed=True (propagates to __call__)."""

        def usage_source(ctx: AllocationContext) -> dict[str, float]:
            return {"user-1": 50.0}  # would succeed if not for fetch failure

        model = UsageRatioModel(usage_source=usage_source)
        ctx = _make_ctx(metrics_fetch_failed=True)
        result = model.allocate(ctx)

        assert result is None


# =============================================================================
# Test group 5: allocate_by_usage_ratio defense-in-depth guard
# =============================================================================


class TestAllocateByUsageRatioMetricsFetchFailed:
    def test_metrics_fetch_failed_true_produces_metrics_fetch_failed_detail(self) -> None:
        """Defense-in-depth: allocate_by_usage_ratio with metrics_fetch_failed=True → METRICS_FETCH_FAILED."""
        ctx = _make_ctx(metrics_fetch_failed=True)
        result = allocate_by_usage_ratio(ctx, {"user-1": 100.0})

        assert len(result.rows) == 1
        row = result.rows[0]
        assert row.allocation_detail == AllocationDetail.METRICS_FETCH_FAILED
        assert row.identity_id == "UNALLOCATED"

    def test_metrics_fetch_failed_false_allocates_by_ratio(self) -> None:
        """Existing behavior: metrics_fetch_failed=False → normal ratio allocation."""
        ctx = _make_ctx(metrics_fetch_failed=False, split_amount=Decimal("100.00"))
        result = allocate_by_usage_ratio(ctx, {"user-1": 75.0, "user-2": 25.0})

        identities = {row.identity_id for row in result.rows}
        assert "user-1" in identities
        assert "user-2" in identities
        assert "UNALLOCATED" not in identities


# =============================================================================
# Test group 6: End-to-end orchestrator integration
# =============================================================================

# Minimal mock infrastructure for e2e test


class _MockResourceRepo:
    def find_by_period(self, *args: Any, **kwargs: Any) -> tuple[list, int]:
        return [], 0


class _MockIdentityRepo:
    def find_by_period(self, *args: Any, **kwargs: Any) -> tuple[list, int]:
        return [], 0


class _MockBillingRepo:
    def __init__(self, lines: list[BillingLineItem]) -> None:
        self._lines = lines

    def find_by_date(self, ecosystem: str, tenant_id: str, target_date: date_type) -> list[BillingLineItem]:
        return [bl for bl in self._lines if bl.timestamp.date() == target_date]


class _MockChargebackRepo:
    def __init__(self) -> None:
        self.rows: list[ChargebackRow] = []

    def upsert_batch(self, rows: list[ChargebackRow]) -> int:
        self.rows.extend(rows)
        return len(rows)


class _MockPipelineStateRepo:
    def mark_chargeback_calculated(self, *args: Any) -> None:
        pass


class _MockUoW:
    def __init__(self, lines: list[BillingLineItem]) -> None:
        self.resources = _MockResourceRepo()
        self.identities = _MockIdentityRepo()
        self.billing = _MockBillingRepo(lines)
        self.chargebacks = _MockChargebackRepo()
        self.pipeline_state = _MockPipelineStateRepo()


class TestOrchestratorEndToEnd:
    def test_failed_prefetch_produces_metrics_fetch_failed_allocation_detail(self) -> None:
        """E2E: MetricsSource raises → ChargebackRow.allocation_detail == METRICS_FETCH_FAILED."""
        from core.engine.allocation_models import UsageRatioModel

        query = _make_metric_query()
        mock_handler = MagicMock()
        mock_handler.get_metrics_for_product_type.return_value = [query]
        mock_handler.service_type = "kafka"
        mock_handler.resolve_identities.return_value = IdentityResolution(
            resource_active=IdentitySet(),
            metrics_derived=IdentitySet(),
            tenant_period=IdentitySet(),
        )

        def failing_usage_source(ctx: AllocationContext) -> dict[str, float]:
            return {}

        usage_model = UsageRatioModel(usage_source=failing_usage_source)

        registry = AllocatorRegistry()
        registry.register("KAFKA_CKU", usage_model)

        mock_bundle = MagicMock()
        mock_bundle.product_type_to_handler.get.return_value = mock_handler
        mock_bundle.fallback_allocator = None

        mock_metrics = MagicMock()
        mock_metrics.query.side_effect = ConnectionError("Prometheus down")

        phase = _make_calculate_phase(
            bundle=mock_bundle,
            metrics_source=mock_metrics,
            allocator_registry=registry,
        )

        failing_line = _make_billing_line(resource_id="cluster-down", product_type="KAFKA_CKU")
        uow = _MockUoW([failing_line])

        phase.run(uow, TODAY)

        assert len(uow.chargebacks.rows) == 1
        row = uow.chargebacks.rows[0]
        assert row.allocation_detail == AllocationDetail.METRICS_FETCH_FAILED, (
            f"Expected METRICS_FETCH_FAILED, got {row.allocation_detail!r}"
        )

    def test_successful_prefetch_does_not_produce_metrics_fetch_failed(self) -> None:
        """E2E: Successful MetricsSource → allocation_detail is NOT METRICS_FETCH_FAILED."""
        query = _make_metric_query()
        mock_handler = MagicMock()
        mock_handler.get_metrics_for_product_type.return_value = [query]
        mock_handler.service_type = "kafka"
        mock_handler.resolve_identities.return_value = IdentityResolution(
            resource_active=IdentitySet(),
            metrics_derived=IdentitySet(),
            tenant_period=IdentitySet(),
        )

        def empty_usage_source(ctx: AllocationContext) -> dict[str, float]:
            return {}

        usage_model = UsageRatioModel(usage_source=empty_usage_source)

        registry = AllocatorRegistry()
        registry.register("KAFKA_CKU", usage_model)

        mock_bundle = MagicMock()
        mock_bundle.product_type_to_handler.get.return_value = mock_handler
        mock_bundle.fallback_allocator = None

        mock_metrics = MagicMock()
        mock_metrics.query.return_value = {}  # empty but successful

        phase = _make_calculate_phase(
            bundle=mock_bundle,
            metrics_source=mock_metrics,
            allocator_registry=registry,
        )

        line = _make_billing_line(resource_id="cluster-idle", product_type="KAFKA_CKU")
        uow = _MockUoW([line])

        phase.run(uow, TODAY)

        assert len(uow.chargebacks.rows) == 1
        row = uow.chargebacks.rows[0]
        assert row.allocation_detail != AllocationDetail.METRICS_FETCH_FAILED, (
            f"Expected non-METRICS_FETCH_FAILED for successful empty query, got {row.allocation_detail!r}"
        )
        assert row.allocation_detail == AllocationDetail.NO_USAGE_FOR_ACTIVE_IDENTITIES
