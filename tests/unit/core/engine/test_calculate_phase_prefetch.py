from __future__ import annotations

import logging
import time
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from core.engine.allocation import AllocatorRegistry
from core.engine.orchestrator import CalculatePhase
from core.models.billing import BillingLineItem, CoreBillingLineItem
from core.models.metrics import MetricQuery

# ---------- Constants ----------

NOW = datetime(2026, 2, 22, 12, 0, 0, tzinfo=UTC)
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
    retry_checker: Any | None = None,
    metrics_source: Any | None = None,
    allocator_registry: AllocatorRegistry | None = None,
    identity_overrides: dict | None = None,
    allocator_params: dict | None = None,
    metrics_step: timedelta = timedelta(hours=1),
    metrics_prefetch_workers: int = 4,
) -> CalculatePhase:
    """Factory for CalculatePhase with metrics_prefetch_workers support."""
    if bundle is None:
        bundle = MagicMock()
        bundle.product_type_to_handler = {}

    if retry_checker is None:
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
        metrics_step=metrics_step,
        metrics_prefetch_workers=metrics_prefetch_workers,
    )


# =============================================================================
# Test 1: Parallelism — 5 groups × 50ms each finishes in < 150ms
# =============================================================================


class TestPrefetchParallelism:
    def test_five_groups_run_concurrently_not_serially(self) -> None:
        """5 groups × 50ms each → total < 150ms (proves concurrency, not serial 250ms)."""
        query = _make_metric_query(key="bytes_in")

        mock_handler = MagicMock()
        mock_handler.get_metrics_for_product_type.return_value = [query]

        mock_bundle = MagicMock()
        mock_bundle.product_type_to_handler.get.return_value = mock_handler

        call_count = 0

        def slow_query(*args: Any, **kwargs: Any) -> dict[str, list]:
            nonlocal call_count
            call_count += 1
            time.sleep(0.05)
            return {}

        mock_metrics = MagicMock()
        mock_metrics.query.side_effect = slow_query

        phase = _make_calculate_phase(
            bundle=mock_bundle,
            metrics_source=mock_metrics,
            metrics_prefetch_workers=5,
        )

        # 5 billing lines with distinct resource_ids → 5 separate prefetch groups
        lines = [_make_billing_line(resource_id=f"cluster-{i}", timestamp=NOW) for i in range(1, 6)]

        start = time.monotonic()
        result = phase._prefetch_metrics(lines)
        elapsed = time.monotonic() - start

        # All 5 groups must be in result
        assert len(result) == 5
        # Must be faster than serial (5 × 0.05 = 0.25s); allow 0.15s for thread overhead
        assert elapsed < 0.15, f"Expected < 0.15s (parallel), got {elapsed:.3f}s (serial?)"
        assert call_count == 5


# =============================================================================
# Test 2: Partial failure — one failing group → warning logged, others proceed
# =============================================================================


class TestPrefetchPartialFailure:
    def test_failing_group_maps_to_empty_dict_others_succeed(self, caplog: pytest.LogCaptureFixture) -> None:
        """RuntimeError for one group key → that key maps to {}, others keep results."""
        query = _make_metric_query(key="bytes_in")

        mock_handler = MagicMock()
        mock_handler.get_metrics_for_product_type.return_value = [query]

        mock_bundle = MagicMock()
        mock_bundle.product_type_to_handler.get.return_value = mock_handler

        FAILING_RESOURCE = "cluster-fail"

        def selective_query(*args: Any, **kwargs: Any) -> dict[str, list]:
            resource_id = kwargs.get("resource_id_filter", "")
            if resource_id == FAILING_RESOURCE:
                raise RuntimeError(f"Prometheus unreachable for {resource_id}")
            return {"bytes_in": [{"value": 42.0, "timestamp": NOW}]}

        mock_metrics = MagicMock()
        mock_metrics.query.side_effect = selective_query

        phase = _make_calculate_phase(
            bundle=mock_bundle,
            metrics_source=mock_metrics,
            metrics_prefetch_workers=4,
        )

        lines = [
            _make_billing_line(resource_id="cluster-ok-1", timestamp=NOW),
            _make_billing_line(resource_id="cluster-ok-2", timestamp=NOW),
            _make_billing_line(resource_id=FAILING_RESOURCE, timestamp=NOW),
        ]

        with caplog.at_level(logging.WARNING):
            result = phase._prefetch_metrics(lines)

        # All 3 groups must be in result
        assert len(result) == 3

        # Find the key for the failing resource
        failing_keys = [k for k in result if k[0] == FAILING_RESOURCE]
        assert len(failing_keys) == 1
        # Failing group maps to empty dict
        assert result[failing_keys[0]] == {}

        # Successful groups have data
        ok_keys = [k for k in result if k[0] != FAILING_RESOURCE]
        for ok_key in ok_keys:
            assert result[ok_key] != {}, f"Expected non-empty result for {ok_key[0]}"

        # Warning must be logged
        warning_messages = [r.message for r in caplog.records if r.levelno == logging.WARNING]
        assert any(FAILING_RESOURCE in msg for msg in warning_messages), (
            f"Expected warning mentioning '{FAILING_RESOURCE}', got: {warning_messages}"
        )


# =============================================================================
# Test 3: TenantConfig field validation
# =============================================================================


class TestTenantConfigMetricsPrefetchWorkers:
    def test_metrics_prefetch_workers_zero_raises_validation_error(self) -> None:
        """metrics_prefetch_workers=0 is below minimum (ge=1) → ValidationError."""
        from pydantic import ValidationError

        from core.config.models import TenantConfig

        with pytest.raises(ValidationError):
            TenantConfig(
                ecosystem=ECOSYSTEM,
                tenant_id=TENANT_ID,
                lookback_days=30,
                cutoff_days=5,
                metrics_prefetch_workers=0,
            )

    def test_metrics_prefetch_workers_21_raises_validation_error(self) -> None:
        """metrics_prefetch_workers=21 exceeds maximum (le=20) → ValidationError."""
        from pydantic import ValidationError

        from core.config.models import TenantConfig

        with pytest.raises(ValidationError):
            TenantConfig(
                ecosystem=ECOSYSTEM,
                tenant_id=TENANT_ID,
                lookback_days=30,
                cutoff_days=5,
                metrics_prefetch_workers=21,
            )

    def test_metrics_prefetch_workers_4_is_accepted(self) -> None:
        """metrics_prefetch_workers=4 is within [1, 20] → no error."""
        from core.config.models import TenantConfig

        config = TenantConfig(
            ecosystem=ECOSYSTEM,
            tenant_id=TENANT_ID,
            lookback_days=30,
            cutoff_days=5,
            metrics_prefetch_workers=4,
        )
        assert config.metrics_prefetch_workers == 4

    def test_metrics_prefetch_workers_default_is_4(self) -> None:
        """Default value is 4 when not specified."""
        from core.config.models import TenantConfig

        config = TenantConfig(
            ecosystem=ECOSYSTEM,
            tenant_id=TENANT_ID,
            lookback_days=30,
            cutoff_days=5,
        )
        assert config.metrics_prefetch_workers == 4


# =============================================================================
# Test 4: None metrics_source → returns {} immediately without spawning threads
# =============================================================================


class TestPrefetchNoMetricsSource:
    def test_none_metrics_source_returns_empty_dict(self) -> None:
        """When metrics_source=None, _prefetch_metrics returns {} immediately."""
        query = _make_metric_query(key="bytes_in")

        mock_handler = MagicMock()
        mock_handler.get_metrics_for_product_type.return_value = [query]

        mock_bundle = MagicMock()
        mock_bundle.product_type_to_handler.get.return_value = mock_handler

        phase = _make_calculate_phase(
            bundle=mock_bundle,
            metrics_source=None,
            metrics_prefetch_workers=4,
        )

        lines = [_make_billing_line(resource_id=f"cluster-{i}") for i in range(3)]

        with patch("concurrent.futures.ThreadPoolExecutor") as mock_executor:
            result = phase._prefetch_metrics(lines)

        assert result == {}
        # ThreadPoolExecutor must NOT be instantiated when metrics_source is None
        mock_executor.assert_not_called()


# =============================================================================
# Test 5: Single-group degenerate case — no crash, correct result
# =============================================================================


class TestPrefetchSingleGroup:
    def test_single_group_returns_correct_result(self) -> None:
        """Single billing line group completes correctly (degenerate input regression)."""
        query = _make_metric_query(key="bytes_in")
        expected_data = {"bytes_in": [{"value": 10.0, "timestamp": NOW}]}

        mock_handler = MagicMock()
        mock_handler.get_metrics_for_product_type.return_value = [query]

        mock_bundle = MagicMock()
        mock_bundle.product_type_to_handler.get.return_value = mock_handler

        mock_metrics = MagicMock()
        mock_metrics.query.return_value = expected_data

        # Use workers=4 but only 1 group → min(4, 1) = 1 worker used
        phase = _make_calculate_phase(
            bundle=mock_bundle,
            metrics_source=mock_metrics,
            metrics_prefetch_workers=4,
        )

        lines = [_make_billing_line(resource_id="cluster-solo", timestamp=NOW)]

        result = phase._prefetch_metrics(lines)

        assert len(result) == 1
        group_key = list(result.keys())[0]
        assert group_key[0] == "cluster-solo"
        assert result[group_key] == expected_data
        # query called exactly once
        mock_metrics.query.assert_called_once()
