"""Allocation behavior for self-managed Kafka cost rows."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from core.engine.allocation import AllocationContext
from core.metrics.protocol import MetricsSource
from core.models import CoreBillingLineItem, CoreIdentity, IdentityResolution, IdentitySet, MetricRow
from core.models.chargeback import AllocationDetail, CostType
from plugins.self_managed_kafka.principal_attribution import (
    PrincipalAttributionState,
    PrincipalDirectionEvaluation,
    PrincipalWeight,
)
from plugins.self_managed_kafka.telemetry_contract import PrincipalTelemetryEvidence, PrincipalTelemetryStatus


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
    allocator = SelfManagedKafkaHandler(config, MagicMock(spec=MetricsSource)).get_allocator(product_type)
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
    assert {row.principal_team for row in result.rows} == {None}
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


def _allocation_context(
    product_type: str,
    resolution: IdentityResolution,
    amount: Decimal,
) -> AllocationContext:
    return AllocationContext(
        timeslice=datetime(2026, 8, 1, tzinfo=UTC),
        billing_line=_billing_line(product_type),
        identities=resolution,
        split_amount=amount,
        metrics_data=None,
    )


def _degraded_evaluation(*, client_only_weight: Decimal) -> PrincipalDirectionEvaluation:
    return PrincipalDirectionEvaluation(
        direction="ingress",
        quota_type="Produce",
        state=PrincipalAttributionState.DEGRADED,
        detail="degraded",
        user_weights=(PrincipalWeight("User:alice", "UNASSIGNED", Decimal("1")),),
        client_only_weight=client_only_weight,
        total_weight=Decimal("1") + client_only_weight,
        coverage_complete=True,
        declared_scrape_interval=timedelta(seconds=5),
        observed_deltas=(Decimal("5"),),
    )


def test_quota_allocator_emits_zero_client_only_residual_and_only_user_rows_receive_a_team() -> None:
    from plugins.self_managed_kafka.allocation_models import QuotaPrincipalAllocationModel

    evaluation = _degraded_evaluation(client_only_weight=Decimal("1"))
    evidence = PrincipalTelemetryEvidence(
        window_start=datetime(2026, 8, 1, tzinfo=UTC),
        window_end=datetime(2026, 8, 2, tzinfo=UTC),
        status=PrincipalTelemetryStatus.OBSERVED,
        detail="quota_identity_observed",
        ingress=evaluation,
    )
    resolution = _resolution(status="observed", detail="quota_identity_observed")
    resolution.context["principal_telemetry_evidence"] = evidence

    result = QuotaPrincipalAllocationModel("ingress")(
        _allocation_context("SELF_KAFKA_NETWORK_INGRESS", resolution, Decimal("0.0000"))
    )

    assert [(row.identity_id, row.amount, row.allocation_detail, row.principal_team) for row in result.rows] == [
        ("User:alice", Decimal("0.0000"), AllocationDetail.USAGE_RATIO_ALLOCATION, "UNASSIGNED"),
        ("UNALLOCATED", Decimal("0.0000"), "principal_client_only_residual", None),
    ]


def test_enabled_fixed_policy_uses_sorted_configured_identities_even_when_discovery_is_prometheus() -> None:
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
            "identity_source": {
                "source": "prometheus",
                "static_identities": [
                    {"identity_id": "Team:zeta", "identity_type": "team"},
                    {"identity_id": "Team:alpha", "identity_type": "team"},
                    {"identity_id": "Team:beta", "identity_type": "team"},
                ],
            },
            "principal_attribution": {
                "enabled": True,
                "scrape_interval_seconds": 5,
                "max_gap_seconds": 10,
                "compute_policy": "static_even_v1",
            },
            "metrics": {"url": "http://prometheus:9090"},
        }
    )
    allocator = SelfManagedKafkaHandler(config, MagicMock(spec=MetricsSource)).get_allocator("SELF_KAFKA_COMPUTE")

    result = allocator(
        _allocation_context(
            "SELF_KAFKA_COMPUTE",
            _resolution(status="observed", detail="quota_identity_observed"),
            Decimal("1.0000"),
        )
    )

    assert [(row.identity_id, row.amount, row.allocation_method, row.principal_team) for row in result.rows] == [
        ("Team:alpha", Decimal("0.3333"), "static_even_v1", None),
        ("Team:beta", Decimal("0.3333"), "static_even_v1", None),
        ("Team:zeta", Decimal("0.3333"), "static_even_v1", None),
        ("UNALLOCATED", Decimal("0.0001"), "static_even_v1", None),
    ]
    assert result.rows[-1].allocation_detail == "principal_rounding_residual"


def test_enabled_fixed_policy_with_no_configured_recipients_is_explicitly_unattributed() -> None:
    from plugins.self_managed_kafka.allocation_models import FixedPrincipalPolicyAllocationModel

    result = FixedPrincipalPolicyAllocationModel("static_even_v1")(
        _allocation_context(
            "SELF_KAFKA_STORAGE",
            _resolution(status="observed", detail="quota_identity_observed"),
            Decimal("1.0000"),
        )
    )

    assert [
        (row.identity_id, row.amount, row.allocation_method, row.allocation_detail, row.principal_team)
        for row in result.rows
    ] == [("UNALLOCATED", Decimal("1.0000"), "principal_unattributed_v1", "principal_policy_unattributed", None)]


def test_fixed_policy_deduplicates_identities_and_persists_an_exact_pool(tmp_path: Path) -> None:
    from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend
    from plugins.self_managed_kafka.allocation_models import FixedPrincipalPolicyAllocationModel
    from plugins.self_managed_kafka.storage.module import SelfManagedKafkaStorageModule

    result = FixedPrincipalPolicyAllocationModel(
        "static_even_v1",
        identities=("Team:zeta", "Team:alpha", "Team:zeta"),
    )(
        _allocation_context(
            "SELF_KAFKA_COMPUTE",
            _resolution(status="observed", detail="quota_identity_observed"),
            Decimal("1.0000"),
        )
    )
    backend = SQLModelBackend(
        f"sqlite:///{tmp_path / 'fixed-policy.db'}",
        SelfManagedKafkaStorageModule(),
        use_migrations=False,
    )
    backend.create_tables()
    try:
        with backend.create_unit_of_work() as uow:
            assert uow.chargebacks.upsert_batch(result.rows) == 2
            uow.commit()
        with backend.create_read_only_unit_of_work() as uow:
            stored = uow.chargebacks.find_by_date("self_managed_kafka", "tenant-1", datetime(2026, 8, 1).date())
    finally:
        backend.dispose()

    assert [(row.identity_id, row.amount) for row in result.rows] == [
        ("Team:alpha", Decimal("0.5000")),
        ("Team:zeta", Decimal("0.5000")),
    ]
    assert [(row.identity_id, row.amount) for row in stored] == [
        ("Team:alpha", Decimal("0.5000")),
        ("Team:zeta", Decimal("0.5000")),
    ]
    assert sum((row.amount for row in stored), Decimal("0")) == Decimal("1.0000")


@pytest.mark.parametrize(
    ("state", "detail", "expected_method", "expected_detail"),
    [
        (
            PrincipalAttributionState.UNAVAILABLE,
            "principal_telemetry_not_observed",
            "principal_quota_unavailable_v1",
            "principal_telemetry_not_observed",
        ),
        (
            PrincipalAttributionState.ZERO_USAGE,
            "zero_usage",
            "principal_quota_zero_usage_v1",
            "principal_zero_usage",
        ),
    ],
)
def test_quota_allocator_preserves_unavailable_and_zero_usage_as_unassigned_rows(
    state: PrincipalAttributionState,
    detail: str,
    expected_method: str,
    expected_detail: str,
) -> None:
    from plugins.self_managed_kafka.allocation_models import QuotaPrincipalAllocationModel

    evaluation = PrincipalDirectionEvaluation(
        direction="ingress",
        quota_type="Produce",
        state=state,
        detail=detail,
        user_weights=(),
        client_only_weight=Decimal("0"),
        total_weight=Decimal("0"),
        coverage_complete=state is not PrincipalAttributionState.UNAVAILABLE,
        declared_scrape_interval=timedelta(seconds=5),
        observed_deltas=(),
    )
    evidence = PrincipalTelemetryEvidence(
        window_start=datetime(2026, 8, 1, tzinfo=UTC),
        window_end=datetime(2026, 8, 2, tzinfo=UTC),
        status=PrincipalTelemetryStatus.UNAVAILABLE,
        detail=detail,
        ingress=evaluation,
    )
    resolution = _resolution(status="unavailable", detail=detail)
    resolution.context["principal_telemetry_evidence"] = evidence

    result = QuotaPrincipalAllocationModel("ingress")(
        _allocation_context("SELF_KAFKA_NETWORK_INGRESS", resolution, Decimal("1.0000"))
    )

    assert [
        (row.identity_id, row.amount, row.allocation_method, row.allocation_detail, row.principal_team)
        for row in result.rows
    ] == [("UNALLOCATED", Decimal("1.0000"), expected_method, expected_detail, None)]


def test_quota_allocator_preserves_positive_pool_rounding_residual_without_a_team() -> None:
    from plugins.self_managed_kafka.allocation_models import QuotaPrincipalAllocationModel

    evaluation = PrincipalDirectionEvaluation(
        direction="ingress",
        quota_type="Produce",
        state=PrincipalAttributionState.READY,
        detail="ready",
        user_weights=(
            PrincipalWeight("User:alice", "team-data", Decimal("1")),
            PrincipalWeight("User:bob", "team-platform", Decimal("2")),
        ),
        client_only_weight=Decimal("0"),
        total_weight=Decimal("3"),
        coverage_complete=True,
        declared_scrape_interval=timedelta(seconds=5),
        observed_deltas=(Decimal("5"),),
    )
    evidence = PrincipalTelemetryEvidence(
        window_start=datetime(2026, 8, 1, tzinfo=UTC),
        window_end=datetime(2026, 8, 2, tzinfo=UTC),
        status=PrincipalTelemetryStatus.OBSERVED,
        detail="quota_identity_observed",
        ingress=evaluation,
    )
    resolution = _resolution(status="observed", detail="quota_identity_observed")
    resolution.context["principal_telemetry_evidence"] = evidence

    result = QuotaPrincipalAllocationModel("ingress")(
        _allocation_context("SELF_KAFKA_NETWORK_INGRESS", resolution, Decimal("1.0000"))
    )

    assert [(row.identity_id, row.amount, row.allocation_detail, row.principal_team) for row in result.rows] == [
        ("User:alice", Decimal("0.3333"), AllocationDetail.USAGE_RATIO_ALLOCATION, "team-data"),
        ("User:bob", Decimal("0.6666"), AllocationDetail.USAGE_RATIO_ALLOCATION, "team-platform"),
        ("UNALLOCATED", Decimal("0.0001"), "principal_rounding_residual", None),
    ]
    assert sum((row.amount for row in result.rows), Decimal("0")) == Decimal("1.0000")
