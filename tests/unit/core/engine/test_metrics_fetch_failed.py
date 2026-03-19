"""Tests for GAP-116: Distinguish metrics prefetch failure from empty data.

Verification cases from design doc (doc-116):
1. _prefetch_metrics three outcomes (success, empty success, exception)
2. AllocationContext.metrics_fetch_failed defaults to False
3. UsageRatioModel with metrics_fetch_failed=True → METRICS_FETCH_FAILED detail
4. UsageRatioModel with metrics_fetch_failed=False + empty usage → NO_USAGE_FOR_ACTIVE_IDENTITIES
5. _kafka_usage_allocation with metrics_fetch_failed=True → METRICS_FETCH_FAILED
6. _kafka_usage_allocation with metrics_fetch_failed=False + metrics_data={} → NO_METRICS_LOCATED
7. allocate_by_usage_ratio defense-in-depth guard → METRICS_FETCH_FAILED
8. End-to-end orchestrator integration
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
from core.models import CoreIdentity, IdentityResolution, IdentitySet
from core.models.billing import BillingLineItem, CoreBillingLineItem
from core.models.chargeback import AllocationDetail, ChargebackRow
from core.models.metrics import MetricQuery
from plugins.confluent_cloud.allocators.kafka_allocators import _kafka_usage_allocation

from .conftest import make_billing_line, make_identity_resolution

# ---------- Constants ----------

NOW = datetime(2026, 2, 22, 12, 0, 0, tzinfo=UTC)
TODAY = NOW.date()
ECOSYSTEM = "test-eco"
TENANT_ID = "tenant-1"


# ---------- Shared factories ----------


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


def _make_kafka_ctx(
    metrics_data: dict | None = None,
    metrics_fetch_failed: bool = False,
) -> AllocationContext:
    billing_line = CoreBillingLineItem(
        ecosystem="confluent_cloud",
        tenant_id="org-123",
        timestamp=NOW,
        resource_id="lkc-abc",
        product_category="KAFKA",
        product_type="KAFKA_NETWORK_READ",
        quantity=Decimal("1"),
        unit_price=Decimal("100"),
        total_cost=Decimal("100"),
    )
    sa_set = IdentitySet()
    sa_set.add(
        CoreIdentity(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            identity_id="sa-1",
            identity_type="service_account",
        )
    )
    identities = IdentityResolution(
        resource_active=sa_set,
        metrics_derived=IdentitySet(),
        tenant_period=IdentitySet(),
        context={"api_key_to_owner": {}},
    )
    return AllocationContext(
        timeslice=NOW,
        billing_line=billing_line,
        identities=identities,
        split_amount=Decimal("100.00"),
        metrics_data=metrics_data,
        metrics_fetch_failed=metrics_fetch_failed,
    )


# ---------- Minimal UoW stubs for e2e test ----------


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


# =============================================================================
# Verification 1: Three _prefetch_metrics outcomes
# =============================================================================


class TestPrefetchMetricsThreeOutcomes:
    def test_success_path_data_present_key_not_in_failed_keys(self) -> None:
        """Success path: prefetched[key] has data, key not in failed_keys."""
        query = _make_metric_query()
        mock_handler = MagicMock()
        mock_handler.get_metrics_for_product_type.return_value = [query]
        mock_bundle = MagicMock()
        mock_bundle.product_type_to_handler.get.return_value = mock_handler

        expected = {"bytes_in": [{"value": 10.0}]}
        mock_metrics = MagicMock()
        mock_metrics.query.return_value = expected

        phase = _make_calculate_phase(bundle=mock_bundle, metrics_source=mock_metrics)
        lines = [_make_billing_line(resource_id="cluster-ok")]
        cache = phase._compute_line_window_cache(lines)

        result = phase._prefetch_metrics(lines, cache)

        # Must return 2-tuple after fix
        assert isinstance(result, tuple), f"Expected tuple, got {type(result)}"
        prefetched, failed_keys = result
        group_key = list(prefetched.keys())[0]
        assert prefetched[group_key] == expected
        assert group_key not in failed_keys

    def test_empty_success_key_not_in_failed_keys(self) -> None:
        """Empty success: prefetched[key]=={}, key not in failed_keys."""
        query = _make_metric_query()
        mock_handler = MagicMock()
        mock_handler.get_metrics_for_product_type.return_value = [query]
        mock_bundle = MagicMock()
        mock_bundle.product_type_to_handler.get.return_value = mock_handler

        mock_metrics = MagicMock()
        mock_metrics.query.return_value = {}  # successful empty response

        phase = _make_calculate_phase(bundle=mock_bundle, metrics_source=mock_metrics)
        lines = [_make_billing_line(resource_id="cluster-idle")]
        cache = phase._compute_line_window_cache(lines)

        result = phase._prefetch_metrics(lines, cache)

        assert isinstance(result, tuple), f"Expected tuple, got {type(result)}"
        prefetched, failed_keys = result
        group_key = list(prefetched.keys())[0]
        assert prefetched[group_key] == {}
        assert group_key not in failed_keys  # empty ≠ failure

    def test_exception_key_in_failed_keys_prefetched_is_empty_dict(self, caplog: pytest.LogCaptureFixture) -> None:
        """Exception raised: prefetched[key]=={}, key in failed_keys, warning logged."""
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
        assert prefetched[group_key] == {}  # still {} for identity handler pass-through
        assert group_key in failed_keys  # distinguishes failure from empty success
        warnings = [r.message for r in caplog.records if r.levelno == logging.WARNING]
        assert any("cluster-fail" in m for m in warnings)


# =============================================================================
# Verification 2: AllocationContext.metrics_fetch_failed default
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


# =============================================================================
# Verification 3: UsageRatioModel with metrics_fetch_failed=True
# =============================================================================


class TestUsageRatioModelFetchFailed:
    def test_metrics_fetch_failed_true_produces_metrics_fetch_failed_detail(self) -> None:
        """ctx.metrics_fetch_failed=True → METRICS_FETCH_FAILED, not NO_USAGE_FOR_ACTIVE_IDENTITIES."""
        model = UsageRatioModel(usage_source=lambda ctx: {})
        ctx = _make_ctx(metrics_fetch_failed=True)
        result = model(ctx)

        assert len(result.rows) == 1
        assert result.rows[0].allocation_detail == AllocationDetail.METRICS_FETCH_FAILED
        assert result.rows[0].identity_id == "UNALLOCATED"

    def test_metrics_fetch_failed_true_does_not_call_usage_source(self) -> None:
        """usage_source must NOT be called when metrics_fetch_failed=True."""
        called = []

        def tracking_source(ctx: AllocationContext) -> dict[str, float]:
            called.append(True)
            return {}

        model = UsageRatioModel(usage_source=tracking_source)
        model(_make_ctx(metrics_fetch_failed=True))

        assert not called, "usage_source must not be called when metrics_fetch_failed=True"


# =============================================================================
# Verification 4: UsageRatioModel with metrics_fetch_failed=False + empty usage
# =============================================================================


class TestUsageRatioModelEmptyUsageUnchanged:
    def test_metrics_fetch_failed_false_empty_usage_produces_no_usage_detail(self) -> None:
        """Existing behavior: metrics_fetch_failed=False + empty usage → NO_USAGE_FOR_ACTIVE_IDENTITIES."""
        model = UsageRatioModel(usage_source=lambda ctx: {})
        ctx = _make_ctx(metrics_fetch_failed=False, metrics_data={})
        result = model(ctx)

        assert len(result.rows) == 1
        assert result.rows[0].allocation_detail == AllocationDetail.NO_USAGE_FOR_ACTIVE_IDENTITIES


# =============================================================================
# Verification 5: _kafka_usage_allocation with metrics_fetch_failed=True
# =============================================================================


class TestKafkaUsageAllocationFetchFailed:
    def test_metrics_fetch_failed_true_produces_metrics_fetch_failed_detail(self) -> None:
        """metrics_fetch_failed=True → METRICS_FETCH_FAILED (not NO_METRICS_LOCATED)."""
        ctx = _make_kafka_ctx(metrics_data={}, metrics_fetch_failed=True)
        result = _kafka_usage_allocation(ctx)

        assert len(result.rows) == 1
        assert result.rows[0].allocation_detail == AllocationDetail.METRICS_FETCH_FAILED
        assert result.rows[0].identity_id == "UNALLOCATED"

    def test_metrics_fetch_failed_true_not_no_metrics_located(self) -> None:
        """metrics_fetch_failed=True must NOT produce NO_METRICS_LOCATED."""
        ctx = _make_kafka_ctx(metrics_data={}, metrics_fetch_failed=True)
        result = _kafka_usage_allocation(ctx)

        details = {row.allocation_detail for row in result.rows}
        assert AllocationDetail.NO_METRICS_LOCATED not in details


# =============================================================================
# Verification 6: _kafka_usage_allocation with metrics_fetch_failed=False + empty metrics
# =============================================================================


class TestKafkaUsageAllocationEmptyMetricsUnchanged:
    def test_metrics_fetch_failed_false_empty_metrics_produces_no_metrics_located(self) -> None:
        """Existing behavior: metrics_fetch_failed=False + metrics_data={} → no METRICS_FETCH_FAILED."""
        ctx = _make_kafka_ctx(metrics_data={}, metrics_fetch_failed=False)
        result = _kafka_usage_allocation(ctx)

        details = {row.allocation_detail for row in result.rows}
        assert AllocationDetail.METRICS_FETCH_FAILED not in details
        # Must be a legitimate no-metrics code
        no_metrics_codes = {
            AllocationDetail.NO_METRICS_LOCATED,
            AllocationDetail.NO_METRICS_NO_ACTIVE_IDENTITIES_LOCATED,
            AllocationDetail.NO_IDENTITIES_LOCATED,
        }
        assert details & no_metrics_codes, f"Expected a no-metrics detail code, got {details}"


# =============================================================================
# Verification 7: allocate_by_usage_ratio defense-in-depth guard
# =============================================================================


class TestAllocateByUsageRatioFetchFailed:
    def test_metrics_fetch_failed_true_produces_metrics_fetch_failed_detail(self) -> None:
        """Defense-in-depth: allocate_by_usage_ratio with metrics_fetch_failed=True → METRICS_FETCH_FAILED."""
        ctx = _make_ctx(metrics_fetch_failed=True)
        result = allocate_by_usage_ratio(ctx, {"user-1": 100.0})

        assert len(result.rows) == 1
        assert result.rows[0].allocation_detail == AllocationDetail.METRICS_FETCH_FAILED
        assert result.rows[0].identity_id == "UNALLOCATED"

    def test_metrics_fetch_failed_false_allocates_by_ratio(self) -> None:
        """Existing behavior: metrics_fetch_failed=False → normal ratio allocation."""
        ctx = _make_ctx(metrics_fetch_failed=False, split_amount=Decimal("100.00"))
        result = allocate_by_usage_ratio(ctx, {"user-1": 75.0, "user-2": 25.0})

        identities = {row.identity_id for row in result.rows}
        assert "user-1" in identities
        assert "user-2" in identities
        assert "UNALLOCATED" not in identities


# =============================================================================
# Verification 8: End-to-end orchestrator integration
# =============================================================================


class TestOrchestratorEndToEnd:
    def test_failed_prefetch_produces_metrics_fetch_failed_allocation_detail(self) -> None:
        """E2E: MetricsSource.query raises → ChargebackRow.allocation_detail == METRICS_FETCH_FAILED."""
        query = _make_metric_query()
        mock_handler = MagicMock()
        mock_handler.get_metrics_for_product_type.return_value = [query]
        mock_handler.service_type = "kafka"
        mock_handler.resolve_identities.return_value = IdentityResolution(
            resource_active=IdentitySet(),
            metrics_derived=IdentitySet(),
            tenant_period=IdentitySet(),
        )

        usage_model = UsageRatioModel(usage_source=lambda ctx: {})
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

        line = _make_billing_line(resource_id="cluster-down", product_type="KAFKA_CKU")
        uow = _MockUoW([line])
        phase.run(uow, TODAY)

        assert len(uow.chargebacks.rows) == 1
        assert uow.chargebacks.rows[0].allocation_detail == AllocationDetail.METRICS_FETCH_FAILED, (
            f"Expected METRICS_FETCH_FAILED, got {uow.chargebacks.rows[0].allocation_detail!r}"
        )

    def test_successful_empty_prefetch_produces_no_usage_detail(self) -> None:
        """E2E: Successful empty MetricsSource → NO_USAGE_FOR_ACTIVE_IDENTITIES (not METRICS_FETCH_FAILED)."""
        query = _make_metric_query()
        mock_handler = MagicMock()
        mock_handler.get_metrics_for_product_type.return_value = [query]
        mock_handler.service_type = "kafka"
        mock_handler.resolve_identities.return_value = IdentityResolution(
            resource_active=IdentitySet(),
            metrics_derived=IdentitySet(),
            tenant_period=IdentitySet(),
        )

        usage_model = UsageRatioModel(usage_source=lambda ctx: {})
        registry = AllocatorRegistry()
        registry.register("KAFKA_CKU", usage_model)

        mock_bundle = MagicMock()
        mock_bundle.product_type_to_handler.get.return_value = mock_handler
        mock_bundle.fallback_allocator = None

        mock_metrics = MagicMock()
        mock_metrics.query.return_value = {}  # successful but empty

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
        assert row.allocation_detail != AllocationDetail.METRICS_FETCH_FAILED
        assert row.allocation_detail == AllocationDetail.NO_USAGE_FOR_ACTIVE_IDENTITIES
