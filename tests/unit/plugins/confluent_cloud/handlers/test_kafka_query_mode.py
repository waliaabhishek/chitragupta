"""Tests for task-051: CCloud Kafka instant→range query mode fix.

Tests 1 & 2 fail until _KAFKA_READ_METRICS and _KAFKA_WRITE_METRICS switch to query_mode='range'.
Tests 3-5 validate allocator and parse behavior with range (multi-row) data.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import MagicMock

import httpx
import pytest

from core.engine.allocation import AllocationContext, AllocationResult
from core.metrics.prometheus import PrometheusConfig, PrometheusMetricsSource
from core.models import (
    CoreIdentity,
    IdentityResolution,
    IdentitySet,
    MetricRow,
)
from core.models.billing import CoreBillingLineItem
from core.models.metrics import MetricQuery


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_identity_set(*identity_ids: str) -> IdentitySet:
    iset = IdentitySet()
    for iid in identity_ids:
        iset.add(
            CoreIdentity(
                ecosystem="confluent_cloud",
                tenant_id="org-test",
                identity_id=iid,
                identity_type="service_account",
            )
        )
    return iset


def _make_billing_line(product_type: str = "KAFKA_NETWORK_READ", total_cost: Decimal = Decimal("100")) -> CoreBillingLineItem:
    return CoreBillingLineItem(
        ecosystem="confluent_cloud",
        tenant_id="org-test",
        timestamp=datetime(2026, 3, 1, tzinfo=UTC),
        resource_id="lkc-abc",
        product_category="KAFKA",
        product_type=product_type,
        quantity=Decimal("1"),
        unit_price=total_cost,
        total_cost=total_cost,
    )


def _make_prometheus_source(response_text: str) -> PrometheusMetricsSource:
    mock_post = MagicMock(return_value=MagicMock(text=response_text, status_code=200))
    client = MagicMock(spec=httpx.Client)
    client.post = mock_post
    config = PrometheusConfig(url="http://prom:9090/", max_retries=1, base_delay=0.0)
    return PrometheusMetricsSource(config, client=client)


# ---------------------------------------------------------------------------
# Test 1: _KAFKA_READ_METRICS uses range mode
# ---------------------------------------------------------------------------


class TestKafkaReadMetricsQueryMode:
    def test_kafka_read_metrics_query_mode_is_range(self) -> None:
        """_KAFKA_READ_METRICS[0].query_mode must be 'range', not 'instant'."""
        from plugins.confluent_cloud.handlers.kafka import _KAFKA_READ_METRICS

        assert _KAFKA_READ_METRICS[0].query_mode == "range", (
            f"Expected query_mode='range', got '{_KAFKA_READ_METRICS[0].query_mode}'. "
            "Instant mode misses principals that stopped producing before billing end."
        )


# ---------------------------------------------------------------------------
# Test 2: _KAFKA_WRITE_METRICS uses range mode
# ---------------------------------------------------------------------------


class TestKafkaWriteMetricsQueryMode:
    def test_kafka_write_metrics_query_mode_is_range(self) -> None:
        """_KAFKA_WRITE_METRICS[0].query_mode must be 'range', not 'instant'."""
        from plugins.confluent_cloud.handlers.kafka import _KAFKA_WRITE_METRICS

        assert _KAFKA_WRITE_METRICS[0].query_mode == "range", (
            f"Expected query_mode='range', got '{_KAFKA_WRITE_METRICS[0].query_mode}'. "
            "Instant mode misses principals that stopped consuming before billing end."
        )


# ---------------------------------------------------------------------------
# Test 3: range query returns multiple rows, allocator sums correctly
# ---------------------------------------------------------------------------


class TestKafkaNetworkAllocatorRangeRows:
    def test_allocator_sums_multiple_rows_per_principal(self) -> None:
        """kafka_network_allocator sums all MetricRows per principal across range steps."""
        from plugins.confluent_cloud.allocators.kafka_allocators import kafka_network_allocator

        ts1 = datetime(2026, 3, 1, 0, tzinfo=UTC)
        ts2 = datetime(2026, 3, 1, 1, tzinfo=UTC)
        ts3 = datetime(2026, 3, 1, 2, tzinfo=UTC)

        # sa-1 has 3 steps: 100 + 200 + 300 = 600 bytes
        # sa-2 has 2 steps: 50 + 50 = 100 bytes
        # Total: 700 bytes. sa-1 ratio: 600/700 ≈ 0.857, sa-2 ratio: 100/700 ≈ 0.143
        metrics_data: dict[str, list[MetricRow]] = {
            "bytes_out": [
                MetricRow(ts1, "bytes_out", 100.0, {"principal_id": "sa-1"}),
                MetricRow(ts2, "bytes_out", 200.0, {"principal_id": "sa-1"}),
                MetricRow(ts3, "bytes_out", 300.0, {"principal_id": "sa-1"}),
                MetricRow(ts1, "bytes_out", 50.0, {"principal_id": "sa-2"}),
                MetricRow(ts2, "bytes_out", 50.0, {"principal_id": "sa-2"}),
            ],
            "bytes_in": [],
        }
        billing_line = _make_billing_line("KAFKA_NETWORK_READ", Decimal("700"))
        identity_set = _make_identity_set("sa-1", "sa-2")
        resolution = IdentityResolution(
            resource_active=identity_set,
            metrics_derived=IdentitySet(),
            tenant_period=IdentitySet(),
        )
        ctx = AllocationContext(
            timeslice=billing_line.timestamp,
            billing_line=billing_line,
            identities=resolution,
            split_amount=Decimal("700"),
            metrics_data=metrics_data,
            params={},
        )

        result = kafka_network_allocator(ctx)

        assert isinstance(result, AllocationResult)
        amounts_by_id = {row.identity_id: row.amount for row in result.rows}
        total = sum(amounts_by_id.values())
        assert total == Decimal("700")

        # sa-1 should get 600/700 of the total
        expected_sa1 = Decimal("600") / Decimal("700") * Decimal("700")
        assert abs(float(amounts_by_id["sa-1"]) - float(expected_sa1)) < 0.01, (
            f"sa-1 expected ~{float(expected_sa1):.2f}, got {float(amounts_by_id['sa-1']):.2f}"
        )

        # sa-2 should get 100/700 of the total
        expected_sa2 = Decimal("100") / Decimal("700") * Decimal("700")
        assert abs(float(amounts_by_id["sa-2"]) - float(expected_sa2)) < 0.01, (
            f"sa-2 expected ~{float(expected_sa2):.2f}, got {float(amounts_by_id['sa-2']):.2f}"
        )


# ---------------------------------------------------------------------------
# Test 4: bursty principal allocation — 1 min vs 59 min
# ---------------------------------------------------------------------------


class TestKafkaNetworkAllocatorBurstyPrincipal:
    def test_bursty_principal_allocation_ratios(self) -> None:
        """Principal active for 1 step vs 59 steps — ratio ≈ 1.67% vs 98.33%.

        Principal A: 100 bytes × 1 step  → 100 total
        Principal B: 100 bytes × 59 steps → 5900 total
        Grand total: 6000. A ≈ 1.667%, B ≈ 98.333%.
        """
        from plugins.confluent_cloud.allocators.kafka_allocators import kafka_network_allocator

        rows: list[MetricRow] = []
        # principal A: 1 step
        rows.append(MetricRow(datetime(2026, 3, 1, 0, tzinfo=UTC), "bytes_out", 100.0, {"principal_id": "sa-A"}))
        # principal B: 59 steps
        base = datetime(2026, 3, 1, 0, tzinfo=UTC)
        for step in range(1, 60):
            rows.append(
                MetricRow(
                    base + timedelta(minutes=step),
                    "bytes_out",
                    100.0,
                    {"principal_id": "sa-B"},
                )
            )

        metrics_data: dict[str, list[MetricRow]] = {"bytes_out": rows, "bytes_in": []}
        billing_line = _make_billing_line("KAFKA_NETWORK_READ", Decimal("6000"))
        identity_set = _make_identity_set("sa-A", "sa-B")
        resolution = IdentityResolution(
            resource_active=identity_set,
            metrics_derived=IdentitySet(),
            tenant_period=IdentitySet(),
        )
        ctx = AllocationContext(
            timeslice=billing_line.timestamp,
            billing_line=billing_line,
            identities=resolution,
            split_amount=Decimal("6000"),
            metrics_data=metrics_data,
            params={},
        )

        result = kafka_network_allocator(ctx)

        amounts_by_id = {row.identity_id: row.amount for row in result.rows}
        total = sum(amounts_by_id.values())
        assert total == Decimal("6000")

        ratio_a = float(amounts_by_id["sa-A"]) / 6000.0
        ratio_b = float(amounts_by_id["sa-B"]) / 6000.0

        # sa-A: 100/6000 ≈ 1.667%
        assert abs(ratio_a - (100.0 / 6000.0)) < 1e-4, f"sa-A ratio expected ~1.667%, got {ratio_a * 100:.4f}%"
        # sa-B: 5900/6000 ≈ 98.333%
        assert abs(ratio_b - (5900.0 / 6000.0)) < 1e-4, f"sa-B ratio expected ~98.333%, got {ratio_b * 100:.4f}%"


# ---------------------------------------------------------------------------
# Test 5: _parse_response produces one MetricRow per step per series
# ---------------------------------------------------------------------------


class TestParseResponseRangeSteps:
    def test_range_response_24_steps_produces_24_rows_per_series(self) -> None:
        """A range response with 24 steps for 1 series → 24 MetricRow objects."""
        # Build a 24-step range response for 2 series
        base_ts = 1740787200.0  # 2026-03-01T00:00:00Z
        step_seconds = 3600.0

        series_a_values = [[base_ts + i * step_seconds, str(float(i * 100))] for i in range(24)]
        series_b_values = [[base_ts + i * step_seconds, str(float(i * 50))] for i in range(24)]

        range_response = json.dumps(
            {
                "status": "success",
                "data": {
                    "resultType": "matrix",
                    "result": [
                        {
                            "metric": {"kafka_id": "lkc-test", "principal_id": "sa-aaa"},
                            "values": series_a_values,
                        },
                        {
                            "metric": {"kafka_id": "lkc-test", "principal_id": "sa-bbb"},
                            "values": series_b_values,
                        },
                    ],
                },
            }
        )

        src = _make_prometheus_source(range_response)

        mq = MetricQuery(
            key="bytes_out",
            query_expression="sum by (kafka_id, principal_id) (confluent_kafka_server_response_bytes{})",
            label_keys=("kafka_id", "principal_id"),
            resource_label="kafka_id",
            query_mode="range",
        )

        rows = src._parse_response(range_response, mq)

        # 2 series × 24 steps = 48 rows total
        assert len(rows) == 48, f"Expected 48 MetricRows (2 series × 24 steps), got {len(rows)}"

        # Each series contributes exactly 24 rows
        rows_aaa = [r for r in rows if r.labels.get("principal_id") == "sa-aaa"]
        rows_bbb = [r for r in rows if r.labels.get("principal_id") == "sa-bbb"]
        assert len(rows_aaa) == 24, f"Expected 24 rows for sa-aaa, got {len(rows_aaa)}"
        assert len(rows_bbb) == 24, f"Expected 24 rows for sa-bbb, got {len(rows_bbb)}"


# ---------------------------------------------------------------------------
# Test 6: uniform usage — instant (1 row) and range (N rows) produce same ratio
# ---------------------------------------------------------------------------


class TestUniformUsageEquivalentRatios:
    def test_uniform_usage_instant_and_range_same_ratio(self) -> None:
        """Uniform bytes per step: 1 row (instant) vs N rows (range) → identical allocation ratio.

        Both modes should produce the same ratio when each step has equal bytes.
        Instant: sa-aaa=100, sa-bbb=200 → sa-aaa 33.3%, sa-bbb 66.7%
        Range (3 steps): sa-aaa=100×3=300, sa-bbb=200×3=600 → sa-aaa 33.3%, sa-bbb 66.7%
        """
        from plugins.confluent_cloud.allocators.kafka_allocators import kafka_network_allocator

        billing_line = _make_billing_line("KAFKA_NETWORK_READ", Decimal("100"))
        identity_set = _make_identity_set("sa-aaa", "sa-bbb")
        resolution = IdentityResolution(
            resource_active=identity_set,
            metrics_derived=IdentitySet(),
            tenant_period=IdentitySet(),
        )

        ts = datetime(2026, 3, 1, 0, tzinfo=UTC)

        # Instant-like: 1 row per principal
        instant_metrics: dict[str, list[MetricRow]] = {
            "bytes_out": [
                MetricRow(ts, "bytes_out", 100.0, {"principal_id": "sa-aaa"}),
                MetricRow(ts, "bytes_out", 200.0, {"principal_id": "sa-bbb"}),
            ],
            "bytes_in": [],
        }

        # Range-like: 3 rows per principal, same bytes per step
        range_metrics: dict[str, list[MetricRow]] = {
            "bytes_out": [
                MetricRow(ts + timedelta(hours=h), "bytes_out", 100.0, {"principal_id": "sa-aaa"})
                for h in range(3)
            ] + [
                MetricRow(ts + timedelta(hours=h), "bytes_out", 200.0, {"principal_id": "sa-bbb"})
                for h in range(3)
            ],
            "bytes_in": [],
        }

        ctx_instant = AllocationContext(
            timeslice=billing_line.timestamp,
            billing_line=billing_line,
            identities=resolution,
            split_amount=Decimal("100"),
            metrics_data=instant_metrics,
            params={},
        )
        ctx_range = AllocationContext(
            timeslice=billing_line.timestamp,
            billing_line=billing_line,
            identities=resolution,
            split_amount=Decimal("100"),
            metrics_data=range_metrics,
            params={},
        )

        result_instant = kafka_network_allocator(ctx_instant)
        result_range = kafka_network_allocator(ctx_range)

        def ratio(result: AllocationResult, identity_id: str) -> float:
            total = sum(float(r.amount) for r in result.rows)
            matched = sum(float(r.amount) for r in result.rows if r.identity_id == identity_id)
            return matched / total if total > 0 else 0.0

        ratio_aaa_instant = ratio(result_instant, "sa-aaa")
        ratio_aaa_range = ratio(result_range, "sa-aaa")
        ratio_bbb_instant = ratio(result_instant, "sa-bbb")
        ratio_bbb_range = ratio(result_range, "sa-bbb")

        assert abs(ratio_aaa_instant - ratio_aaa_range) < 1e-6, (
            f"sa-aaa ratio differs: instant={ratio_aaa_instant:.6f} range={ratio_aaa_range:.6f}"
        )
        assert abs(ratio_bbb_instant - ratio_bbb_range) < 1e-6, (
            f"sa-bbb ratio differs: instant={ratio_bbb_instant:.6f} range={ratio_bbb_range:.6f}"
        )
