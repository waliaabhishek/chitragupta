"""Allocation behavior for self-managed Kafka cost rows."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from core.engine.allocation import AllocationContext
from core.models import CoreBillingLineItem, CoreIdentity, IdentityResolution, IdentitySet, MetricRow
from core.models.chargeback import AllocationDetail, CostType


def _billing_line(product_type: str) -> CoreBillingLineItem:
    return CoreBillingLineItem(
        ecosystem="self_managed_kafka",
        tenant_id="tenant-1",
        timestamp=datetime(2026, 8, 1, tzinfo=UTC),
        resource_id="billing-cluster-a",
        product_category="kafka",
        product_type=product_type,
        quantity=Decimal("1"),
        unit_price=Decimal("100"),
        total_cost=Decimal("100"),
    )


def _resolution(
    *,
    status: str,
    detail: str,
    static_ids: tuple[str, ...] = (),
    metrics_scope_status: str | None = None,
    metrics_scope_detail: str | None = None,
) -> IdentityResolution:
    resource_active = IdentitySet()
    for identity_id in static_ids:
        resource_active.add(CoreIdentity("self_managed_kafka", "tenant-1", identity_id, "team"))
    context: dict[str, object] = {
        "principal_attribution_status": status,
        "principal_attribution_detail": detail,
        "measured_usage": False,
    }
    if metrics_scope_status is not None:
        context["metrics_scope_status"] = metrics_scope_status
    if metrics_scope_detail is not None:
        context["metrics_scope_detail"] = metrics_scope_detail
    return IdentityResolution(
        resource_active=resource_active,
        metrics_derived=IdentitySet(),
        tenant_period=IdentitySet(),
        context=context,
    )


def _allocate(
    product_type: str,
    resolution: IdentityResolution,
    metrics_data: dict[str, list[MetricRow]] | None = None,
):
    from plugins.self_managed_kafka.config import SelfManagedKafkaConfig
    from plugins.self_managed_kafka.handlers.kafka import SelfManagedKafkaHandler

    config = SelfManagedKafkaConfig.from_plugin_settings(
        {
            "cluster_id": "billing-cluster-a",
            "metrics_identifier": "kraft-a-001",
            "broker_count": 3,
            "cost_model": {
                "compute_hourly_rate": "0.10",
                "storage_per_gib_hourly": "0.0001",
                "network_ingress_per_gib": "0.01",
                "network_egress_per_gib": "0.02",
            },
            "identity_source": {"source": "static"},
            "metrics": {"url": "http://prometheus:9090"},
        }
    )
    allocator = SelfManagedKafkaHandler(config, object()).get_allocator(product_type)
    return allocator(
        AllocationContext(
            timeslice=datetime(2026, 8, 1, tzinfo=UTC),
            billing_line=_billing_line(product_type),
            identities=resolution,
            split_amount=Decimal("100"),
            metrics_data=metrics_data,
        )
    )


@pytest.mark.parametrize(
    "product_type",
    [
        "SELF_KAFKA_COMPUTE",
        "SELF_KAFKA_STORAGE",
        "SELF_KAFKA_NETWORK_INGRESS",
        "SELF_KAFKA_NETWORK_EGRESS",
    ],
)
def test_static_policy_allocation_is_shared_success_not_measured_usage(product_type: str) -> None:
    result = _allocate(
        product_type,
        _resolution(
            status="policy_only_configured",
            detail="policy_only_configured",
            static_ids=("team-data", "team-platform"),
        ),
    )

    assert {row.identity_id for row in result.rows} == {"team-data", "team-platform"}
    assert {row.amount for row in result.rows} == {Decimal("50.00")}
    assert {row.cost_type for row in result.rows} == {CostType.SHARED}
    assert {row.allocation_detail for row in result.rows} == {AllocationDetail.EVEN_SPLIT_ALLOCATION}
    assert {row.metadata["allocation_basis"] for row in result.rows} == {"static_policy"}
    assert {row.metadata["measured_usage"] for row in result.rows} == {False}


@pytest.mark.parametrize(
    ("status", "detail", "expected_detail"),
    [
        ("not_observed", "principal_telemetry_not_observed", "principal_telemetry_not_observed"),
        ("invalid", "principal_telemetry_invalid", "principal_telemetry_invalid"),
        ("transient_failure", "metrics_fetch_failed", AllocationDetail.METRICS_FETCH_FAILED),
    ],
)
def test_unavailable_principal_telemetry_uses_the_distinct_plugin_or_core_detail(
    status: str, detail: str, expected_detail: str
) -> None:
    result = _allocate("SELF_KAFKA_NETWORK_INGRESS", _resolution(status=status, detail=detail))

    assert len(result.rows) == 1
    assert result.rows[0].identity_id == "UNALLOCATED"
    assert result.rows[0].cost_type == CostType.SHARED
    assert result.rows[0].allocation_detail == expected_detail
    assert result.rows[0].metadata["principal_attribution_status"] == status
    assert result.rows[0].metadata["measured_usage"] is False


def test_topic_counter_data_cannot_switch_static_policy_to_usage_ratio() -> None:
    topic_counter_data = {
        "topic_bytes": [
            MetricRow(
                timestamp=datetime(2026, 8, 1, tzinfo=UTC),
                metric_key="topic_bytes",
                value=4096.0,
                labels={"broker": "1", "topic": "orders", "kafka_cluster_id": "kraft-a-001"},
            )
        ]
    }

    result = _allocate(
        "SELF_KAFKA_NETWORK_EGRESS",
        _resolution(
            status="not_observed",
            detail="principal_telemetry_not_observed",
            static_ids=("team-data", "team-platform"),
        ),
        topic_counter_data,
    )

    assert {row.identity_id for row in result.rows} == {"team-data", "team-platform"}
    assert {row.allocation_detail for row in result.rows} == {AllocationDetail.EVEN_SPLIT_ALLOCATION}
    assert {row.metadata["allocation_basis"] for row in result.rows} == {"static_policy"}


def test_validated_scope_status_and_detail_are_retained_on_allocation_rows() -> None:
    result = _allocate(
        "SELF_KAFKA_COMPUTE",
        _resolution(
            status="policy_only_configured",
            detail="policy_only_configured",
            static_ids=("team-data",),
            metrics_scope_status="valid",
            metrics_scope_detail="target healthy",
        ),
    )

    assert result.rows[0].metadata["metrics_scope_status"] == "valid"
    assert result.rows[0].metadata["metrics_scope_detail"] == "target healthy"
