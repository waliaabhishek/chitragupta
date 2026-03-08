"""Tests for task-037: ResolveContext passed to handlers (TDD RED phase).

All tests fail until implementation adds context param to resolve_identities.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

from core.models import CoreIdentity, CoreResource, IdentitySet, MetricRow, ResourceStatus

# ---------------------------------------------------------------------------
# Test 1: Kafka uses cached_identities — no DB call
# ---------------------------------------------------------------------------


def test_kafka_handler_uses_cached_identities(mock_uow: MagicMock) -> None:
    """KafkaHandler uses cached_identities from context; find_by_period NOT called."""
    from plugins.confluent_cloud.handlers.kafka import KafkaHandler

    api_key = CoreIdentity(
        ecosystem="confluent_cloud",
        tenant_id="org-123",
        identity_id="api-key-1",
        identity_type="api_key",
        metadata={"resource_id": "lkc-abc", "owner_id": "sa-owner"},
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
    )
    sa_owner = CoreIdentity(
        ecosystem="confluent_cloud",
        tenant_id="org-123",
        identity_id="sa-owner",
        identity_type="service_account",
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
    )
    cached = IdentitySet()
    cached.add(api_key)
    cached.add(sa_owner)

    handler = KafkaHandler(connection=None, config=None, ecosystem="confluent_cloud")
    result = handler.resolve_identities(
        tenant_id="org-123",
        resource_id="lkc-abc",
        billing_timestamp=datetime(2026, 2, 1, tzinfo=UTC),
        billing_duration=timedelta(hours=24),
        metrics_data=None,
        uow=mock_uow,
        context={"cached_identities": cached, "cached_resources": {}},
    )

    mock_uow.identities.find_by_period.assert_not_called()
    assert "sa-owner" in result.resource_active.ids()


# ---------------------------------------------------------------------------
# Test 2: Kafka fallback — context=None triggers DB call
# ---------------------------------------------------------------------------


def test_kafka_handler_fallback_without_context(mock_uow: MagicMock) -> None:
    """KafkaHandler calls find_by_period when context=None (unchanged behavior)."""
    from plugins.confluent_cloud.handlers.kafka import KafkaHandler

    handler = KafkaHandler(connection=None, config=None, ecosystem="confluent_cloud")
    handler.resolve_identities(
        tenant_id="org-123",
        resource_id="lkc-abc",
        billing_timestamp=datetime(2026, 2, 1, tzinfo=UTC),
        billing_duration=timedelta(hours=24),
        metrics_data=None,
        uow=mock_uow,
        context=None,
    )

    mock_uow.identities.find_by_period.assert_called_once()


# ---------------------------------------------------------------------------
# Test 3: Flink primary path uses cached_resources — no DB call
# ---------------------------------------------------------------------------


def test_flink_primary_path_uses_cached_resources(mock_uow: MagicMock) -> None:
    """FlinkHandler primary metrics path uses cached_resources; find_by_period NOT called."""
    from plugins.confluent_cloud.handlers.flink import FlinkHandler

    stmt = CoreResource(
        ecosystem="confluent_cloud",
        tenant_id="org-123",
        resource_id="stmt-aaa",
        resource_type="flink_statement",
        display_name="my-statement",
        owner_id="sa-owner",
        status=ResourceStatus.ACTIVE,
        metadata={"compute_pool_id": "lfcp-pool-1"},
    )
    cached_resources = {stmt.resource_id: stmt}

    metrics_data = {
        "flink_cfu_primary": [
            MetricRow(
                timestamp=datetime(2026, 2, 1, tzinfo=UTC),
                metric_key="flink_cfu_primary",
                value=10.0,
                labels={"compute_pool_id": "lfcp-pool-1", "flink_statement_name": "my-statement"},
            )
        ]
    }

    handler = FlinkHandler(connection=None, config=None, ecosystem="confluent_cloud")
    result = handler.resolve_identities(
        tenant_id="org-123",
        resource_id="lfcp-pool-1",
        billing_timestamp=datetime(2026, 2, 1, tzinfo=UTC),
        billing_duration=timedelta(hours=24),
        metrics_data=metrics_data,
        uow=mock_uow,
        context={"cached_identities": None, "cached_resources": cached_resources},
    )

    mock_uow.resources.find_by_period.assert_not_called()
    assert "sa-owner" in result.context.get("stmt_owner_cfu", {})


# ---------------------------------------------------------------------------
# Test 4: Flink fallback path uses cached_resources — no DB call
# ---------------------------------------------------------------------------


def test_flink_fallback_uses_cached_resources(mock_uow: MagicMock) -> None:
    """FlinkHandler fallback (metrics_data=None) uses cached_resources; find_by_period NOT called."""
    from plugins.confluent_cloud.handlers.flink import FlinkHandler

    stmt = CoreResource(
        ecosystem="confluent_cloud",
        tenant_id="org-123",
        resource_id="stmt-bbb",
        resource_type="flink_statement",
        display_name="running-stmt",
        owner_id="sa-owner",
        status=ResourceStatus.ACTIVE,
        metadata={"compute_pool_id": "lfcp-pool-1", "is_stopped": False},
    )
    cached_resources = {stmt.resource_id: stmt}

    handler = FlinkHandler(connection=None, config=None, ecosystem="confluent_cloud")
    result = handler.resolve_identities(
        tenant_id="org-123",
        resource_id="lfcp-pool-1",
        billing_timestamp=datetime(2026, 2, 1, tzinfo=UTC),
        billing_duration=timedelta(hours=24),
        metrics_data=None,
        uow=mock_uow,
        context={"cached_identities": None, "cached_resources": cached_resources},
    )

    mock_uow.resources.find_by_period.assert_not_called()
    assert "sa-owner" in result.resource_active.ids()


# ---------------------------------------------------------------------------
# Test 5: Flink resource_type filtering — only flink_statement used
# ---------------------------------------------------------------------------


def test_flink_resource_type_filtering(mock_uow: MagicMock) -> None:
    """cached_resources with mixed types: only flink_statement entries are used by Flink resolution."""
    from plugins.confluent_cloud.handlers.flink import FlinkHandler

    kafka_cluster = CoreResource(
        ecosystem="confluent_cloud",
        tenant_id="org-123",
        resource_id="lkc-xyz",
        resource_type="kafka_cluster",
        status=ResourceStatus.ACTIVE,
        metadata={},
    )
    flink_stmt = CoreResource(
        ecosystem="confluent_cloud",
        tenant_id="org-123",
        resource_id="stmt-ccc",
        resource_type="flink_statement",
        display_name="target-statement",
        owner_id="sa-correct-owner",
        status=ResourceStatus.ACTIVE,
        metadata={"compute_pool_id": "lfcp-pool-1"},
    )
    cached_resources = {kafka_cluster.resource_id: kafka_cluster, flink_stmt.resource_id: flink_stmt}

    metrics_data = {
        "flink_cfu_primary": [
            MetricRow(
                timestamp=datetime(2026, 2, 1, tzinfo=UTC),
                metric_key="flink_cfu_primary",
                value=5.0,
                labels={"compute_pool_id": "lfcp-pool-1", "flink_statement_name": "target-statement"},
            )
        ]
    }

    handler = FlinkHandler(connection=None, config=None, ecosystem="confluent_cloud")
    result = handler.resolve_identities(
        tenant_id="org-123",
        resource_id="lfcp-pool-1",
        billing_timestamp=datetime(2026, 2, 1, tzinfo=UTC),
        billing_duration=timedelta(hours=24),
        metrics_data=metrics_data,
        uow=mock_uow,
        context={"cached_identities": None, "cached_resources": cached_resources},
    )

    stmt_owner_cfu = result.context.get("stmt_owner_cfu", {})
    assert "sa-correct-owner" in stmt_owner_cfu
    assert "lkc-xyz" not in stmt_owner_cfu


# ---------------------------------------------------------------------------
# Test 6: Other handlers accept context without error
# ---------------------------------------------------------------------------


def test_other_handlers_accept_context(mock_uow: MagicMock) -> None:
    """ConnectorHandler and KsqldbHandler accept context kwarg without raising TypeError."""
    from plugins.confluent_cloud.handlers.connectors import ConnectorHandler
    from plugins.confluent_cloud.handlers.ksqldb import KsqldbHandler

    context = {"cached_identities": IdentitySet(), "cached_resources": {}}

    connector = ConnectorHandler(connection=None, config=None, ecosystem="confluent_cloud")
    result_c = connector.resolve_identities(
        tenant_id="org-123",
        resource_id="lcc-connector-1",
        billing_timestamp=datetime(2026, 2, 1, tzinfo=UTC),
        billing_duration=timedelta(hours=24),
        metrics_data=None,
        uow=mock_uow,
        context=context,
    )
    assert result_c is not None

    ksqldb = KsqldbHandler(connection=None, config=None, ecosystem="confluent_cloud")
    result_k = ksqldb.resolve_identities(
        tenant_id="org-123",
        resource_id="lksqlc-cluster-1",
        billing_timestamp=datetime(2026, 2, 1, tzinfo=UTC),
        billing_duration=timedelta(hours=24),
        metrics_data=None,
        uow=mock_uow,
        context=context,
    )
    assert result_k is not None


# ---------------------------------------------------------------------------
# Test 7: Orchestrator passes context to handler.resolve_identities
# ---------------------------------------------------------------------------


def test_orchestrator_passes_context_to_handler() -> None:
    """_process_billing_line passes context with cached_identities and cached_resources."""
    from decimal import Decimal

    from core.engine.allocation import AllocatorRegistry
    from core.engine.orchestrator import CalculatePhase
    from core.models.billing import CoreBillingLineItem
    from core.models.identity import IdentityResolution, IdentitySet
    from core.models.resource import CoreResource

    ecosystem = "test-eco"
    tenant_id = "tenant-1"
    now = datetime(2026, 2, 22, 12, 0, 0, tzinfo=UTC)

    mock_handler = MagicMock()
    mock_handler.service_type = "kafka"
    mock_handler.get_metrics_for_product_type.return_value = []
    mock_handler.resolve_identities.return_value = IdentityResolution(
        resource_active=IdentitySet(),
        metrics_derived=IdentitySet(),
        tenant_period=IdentitySet(),
    )
    mock_handler.get_allocator.return_value = MagicMock(return_value=MagicMock(rows=[]))

    mock_bundle = MagicMock()
    mock_bundle.product_type_to_handler.get.return_value = mock_handler

    mock_retry = MagicMock()
    mock_retry.increment_and_check.return_value = (1, False)

    phase = CalculatePhase(
        ecosystem=ecosystem,
        tenant_id=tenant_id,
        bundle=mock_bundle,
        retry_checker=mock_retry,
        metrics_source=None,
        allocator_registry=AllocatorRegistry(),
        identity_overrides={},
        allocator_params={},
        metrics_step=timedelta(hours=1),
        metrics_prefetch_workers=4,
    )

    line = CoreBillingLineItem(
        ecosystem=ecosystem,
        tenant_id=tenant_id,
        timestamp=now,
        resource_id="cluster-1",
        product_category="kafka",
        product_type="KAFKA_CKU",
        quantity=Decimal(1),
        unit_price=Decimal("100.00"),
        total_cost=Decimal("100.00"),
        granularity="daily",
    )

    resource = CoreResource(
        ecosystem=ecosystem,
        tenant_id=tenant_id,
        resource_id="cluster-1",
        resource_type="kafka_cluster",
    )
    resource_cache = {"cluster-1": resource}

    b_start = now
    b_end = now + timedelta(hours=24)
    tenant_period_cache = {(b_start, b_end): IdentitySet()}

    mock_uow = MagicMock()
    mock_uow.chargebacks = MagicMock()

    phase._process_billing_line(line, mock_uow, {}, tenant_period_cache, resource_cache)

    mock_handler.resolve_identities.assert_called_once()
    call = mock_handler.resolve_identities.call_args
    context = call.kwargs.get("context")
    assert context is not None, "resolve_identities must receive context kwarg"
    assert "cached_identities" in context
    assert "cached_resources" in context
    assert context["cached_resources"] is resource_cache


# ---------------------------------------------------------------------------
# Test 8: Kafka handler with metrics_data exercises principal extraction path
# ---------------------------------------------------------------------------


def test_kafka_handler_with_metrics_data(mock_uow: MagicMock) -> None:
    """KafkaHandler with cached_identities and metrics_data resolves principals from metrics."""
    from plugins.confluent_cloud.handlers.kafka import KafkaHandler

    sa_principal = CoreIdentity(
        ecosystem="confluent_cloud",
        tenant_id="org-123",
        identity_id="User:sa-123",
        identity_type="service_account",
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
    )
    cached_identities = IdentitySet()
    cached_identities.add(sa_principal)

    metrics_data = {
        "bytes_in": [
            MetricRow(
                timestamp=datetime(2026, 2, 1, tzinfo=UTC),
                metric_key="bytes_in",
                value=1024.0,
                labels={"kafka_id": "lkc-abc", "principal_id": "User:sa-123"},
            )
        ]
    }

    handler = KafkaHandler(connection=None, config=None, ecosystem="confluent_cloud")
    result = handler.resolve_identities(
        tenant_id="org-123",
        resource_id="lkc-abc",
        billing_timestamp=datetime(2026, 2, 1, tzinfo=UTC),
        billing_duration=timedelta(hours=24),
        metrics_data=metrics_data,
        uow=mock_uow,
        context={"cached_identities": cached_identities, "cached_resources": {}},
    )

    mock_uow.identities.find_by_period.assert_not_called()
    assert "User:sa-123" in result.metrics_derived.ids()
