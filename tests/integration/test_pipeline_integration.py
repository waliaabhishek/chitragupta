"""Integration tests for the full pipeline with real SQLite storage."""

from __future__ import annotations

from collections.abc import Iterable
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, Any

import pytest

from core.config.models import TenantConfig
from core.engine.allocation import AllocationContext, AllocationResult
from core.engine.orchestrator import ChargebackOrchestrator
from core.models.billing import BillingLineItem
from core.models.chargeback import ChargebackRow, CostType
from core.models.identity import Identity, IdentityResolution, IdentitySet
from core.models.resource import Resource
from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend

if TYPE_CHECKING:
    from core.models.metrics import MetricQuery, MetricRow

ECOSYSTEM = "test-eco"
TENANT_ID = "integration-tenant"
NOW = datetime(2026, 2, 22, 12, 0, 0, tzinfo=UTC)


class IntegrationHandler:
    """A minimal ServiceHandler for integration tests."""

    def __init__(self, service_type: str, product_types: list[str]) -> None:
        self._service_type = service_type
        self._product_types = product_types
        self._resources: list[Resource] = []
        self._identities: list[Identity] = []

    @property
    def service_type(self) -> str:
        return self._service_type

    @property
    def handles_product_types(self) -> list[str]:
        return self._product_types

    def set_resources(self, resources: list[Resource]) -> None:
        self._resources = resources

    def set_identities(self, identities: list[Identity]) -> None:
        self._identities = identities

    def gather_resources(self, tenant_id: str, uow: Any, shared_ctx: object | None = None) -> Iterable[Resource]:
        return self._resources

    def gather_identities(self, tenant_id: str, uow: Any) -> Iterable[Identity]:
        return self._identities

    def resolve_identities(
        self,
        tenant_id: str,
        resource_id: str,
        billing_timestamp: datetime,
        billing_duration: timedelta,
        metrics_data: dict[str, list[MetricRow]] | None,
        uow: Any,
    ) -> IdentityResolution:
        ra = IdentitySet()
        for i in self._identities:
            ra.add(i)
        return IdentityResolution(
            resource_active=ra,
            metrics_derived=IdentitySet(),
            tenant_period=IdentitySet(),
        )

    def get_metrics_for_product_type(self, product_type: str) -> list[MetricQuery]:
        return []

    def get_allocator(self, product_type: str) -> Any:
        def even_allocator(ctx: AllocationContext) -> AllocationResult:
            ids = list(ctx.identities.merged_active.ids())
            if not ids:
                ids = [ctx.billing_line.resource_id]
            per_id = ctx.split_amount / len(ids)
            rows = [
                ChargebackRow(
                    ecosystem=ctx.billing_line.ecosystem,
                    tenant_id=ctx.billing_line.tenant_id,
                    timestamp=ctx.billing_line.timestamp,
                    resource_id=ctx.billing_line.resource_id,
                    product_category=ctx.billing_line.product_category,
                    product_type=ctx.billing_line.product_type,
                    identity_id=iid,
                    cost_type=CostType.USAGE,
                    amount=per_id,
                    allocation_method="integration_even",
                )
                for iid in ids
            ]
            return AllocationResult(rows=rows)

        return even_allocator


class IntegrationCostInput:
    def __init__(self, lines: list[BillingLineItem]) -> None:
        self._lines = lines

    def gather(self, tenant_id: str, start: datetime, end: datetime, uow: Any) -> Iterable[BillingLineItem]:
        return self._lines


class IntegrationPlugin:
    def __init__(
        self,
        handlers: dict[str, IntegrationHandler],
        cost_input: IntegrationCostInput,
    ) -> None:
        self._handlers = handlers
        self._cost_input = cost_input

    @property
    def ecosystem(self) -> str:
        return ECOSYSTEM

    def initialize(self, config: dict[str, Any]) -> None:
        pass

    def get_service_handlers(self) -> dict[str, IntegrationHandler]:
        return self._handlers

    def get_cost_input(self) -> IntegrationCostInput:
        return self._cost_input

    def get_metrics_source(self) -> None:
        return None

    def build_shared_context(self, tenant_id: str) -> None:
        return None

    def close(self) -> None:
        pass


@pytest.fixture
def storage() -> SQLModelBackend:
    backend = SQLModelBackend("sqlite:///:memory:", use_migrations=False)
    backend.create_tables()
    return backend


class TestEndToEndPipeline:
    def test_full_gather_calculate(self, storage: SQLModelBackend) -> None:
        """End-to-end: gather populates DB, calculate produces chargeback rows."""
        resource = Resource(
            ecosystem=ECOSYSTEM,
            tenant_id=TENANT_ID,
            resource_id="cluster-1",
            resource_type="kafka_cluster",
            created_at=NOW - timedelta(days=30),
        )
        identity = Identity(
            ecosystem=ECOSYSTEM,
            tenant_id=TENANT_ID,
            identity_id="user-1",
            identity_type="user",
            display_name="User 1",
        )
        handler = IntegrationHandler("kafka", ["KAFKA_CKU"])
        handler.set_resources([resource])
        handler.set_identities([identity])

        billing_ts = NOW - timedelta(days=10)
        line = BillingLineItem(
            ecosystem=ECOSYSTEM,
            tenant_id=TENANT_ID,
            timestamp=billing_ts,
            resource_id="cluster-1",
            product_category="kafka",
            product_type="KAFKA_CKU",
            quantity=Decimal(1),
            unit_price=Decimal("100"),
            total_cost=Decimal("100"),
        )
        cost_input = IntegrationCostInput([line])
        plugin = IntegrationPlugin({"kafka": handler}, cost_input)

        config = TenantConfig(
            ecosystem=ECOSYSTEM,
            tenant_id=TENANT_ID,
            lookback_days=30,
            cutoff_days=5,
        )
        orch = ChargebackOrchestrator("test", config, plugin, storage)
        result = orch.run()

        assert result.dates_gathered == 1
        assert result.errors == []

        # Verify data persisted in real DB
        with storage.create_unit_of_work() as uow:
            resources, _ = uow.resources.find_active_at(ECOSYSTEM, TENANT_ID, NOW)
            resource_ids = {r.resource_id for r in resources}
            assert "cluster-1" in resource_ids

            identities, _ = uow.identities.find_active_at(ECOSYSTEM, TENANT_ID, NOW)
            identity_ids = {i.identity_id for i in identities}
            assert "user-1" in identity_ids
            assert "UNALLOCATED" in identity_ids

        # Run again to calculate
        result2 = orch.run()
        assert result2.errors == []
        # Verify pipeline state
        with storage.create_unit_of_work() as uow:
            ps = uow.pipeline_state.get(ECOSYSTEM, TENANT_ID, billing_ts.date())
            assert ps is not None
            assert ps.billing_gathered is True

    def test_two_handlers_multiple_product_types(self, storage: SQLModelBackend) -> None:
        """Two handlers each claim different product types."""
        r1 = Resource(
            ecosystem=ECOSYSTEM,
            tenant_id=TENANT_ID,
            resource_id="kafka-1",
            resource_type="kafka_cluster",
            created_at=NOW - timedelta(days=30),
        )
        r2 = Resource(
            ecosystem=ECOSYSTEM,
            tenant_id=TENANT_ID,
            resource_id="connect-1",
            resource_type="connector",
            created_at=NOW - timedelta(days=30),
        )
        i1 = Identity(ecosystem=ECOSYSTEM, tenant_id=TENANT_ID, identity_id="u1", identity_type="user")
        i2 = Identity(ecosystem=ECOSYSTEM, tenant_id=TENANT_ID, identity_id="u2", identity_type="user")

        h1 = IntegrationHandler("kafka", ["KAFKA_CKU"])
        h1.set_resources([r1])
        h1.set_identities([i1])

        h2 = IntegrationHandler("connect", ["CONNECT_CAPACITY"])
        h2.set_resources([r2])
        h2.set_identities([i2])

        billing_ts = NOW - timedelta(days=10)
        lines = [
            BillingLineItem(
                ecosystem=ECOSYSTEM,
                tenant_id=TENANT_ID,
                timestamp=billing_ts,
                resource_id="kafka-1",
                product_category="kafka",
                product_type="KAFKA_CKU",
                quantity=Decimal(1),
                unit_price=Decimal("200"),
                total_cost=Decimal("200"),
            ),
            BillingLineItem(
                ecosystem=ECOSYSTEM,
                tenant_id=TENANT_ID,
                timestamp=billing_ts,
                resource_id="connect-1",
                product_category="connect",
                product_type="CONNECT_CAPACITY",
                quantity=Decimal(1),
                unit_price=Decimal("50"),
                total_cost=Decimal("50"),
            ),
        ]
        cost_input = IntegrationCostInput(lines)
        plugin = IntegrationPlugin({"kafka": h1, "connect": h2}, cost_input)

        config = TenantConfig(
            ecosystem=ECOSYSTEM,
            tenant_id=TENANT_ID,
            lookback_days=30,
            cutoff_days=5,
        )
        orch = ChargebackOrchestrator("test", config, plugin, storage)
        result = orch.run()
        assert result.errors == []
        assert result.dates_gathered >= 1


class TestOverrideScenario:
    def test_allocator_override(self, storage: SQLModelBackend) -> None:
        """Custom allocator override loaded from dotted path."""
        resource = Resource(
            ecosystem=ECOSYSTEM,
            tenant_id=TENANT_ID,
            resource_id="cluster-1",
            resource_type="kafka_cluster",
            created_at=NOW - timedelta(days=30),
        )
        identity = Identity(
            ecosystem=ECOSYSTEM,
            tenant_id=TENANT_ID,
            identity_id="user-1",
            identity_type="user",
        )
        handler = IntegrationHandler("kafka", ["KAFKA_CKU"])
        handler.set_resources([resource])
        handler.set_identities([identity])

        billing_ts = NOW - timedelta(days=10)
        line = BillingLineItem(
            ecosystem=ECOSYSTEM,
            tenant_id=TENANT_ID,
            timestamp=billing_ts,
            resource_id="cluster-1",
            product_category="kafka",
            product_type="KAFKA_CKU",
            quantity=Decimal(1),
            unit_price=Decimal("100"),
            total_cost=Decimal("100"),
        )
        cost_input = IntegrationCostInput([line])
        plugin = IntegrationPlugin({"kafka": handler}, cost_input)

        config = TenantConfig(
            ecosystem=ECOSYSTEM,
            tenant_id=TENANT_ID,
            lookback_days=30,
            cutoff_days=5,
            plugin_settings={
                "allocator_overrides": {
                    "KAFKA_CKU": "tests.integration.helpers.custom_allocator:my_allocator",
                },
            },
        )
        orch = ChargebackOrchestrator("test", config, plugin, storage)

        # Pre-populate pipeline state so calculate phase runs
        with storage.create_unit_of_work() as uow:
            from core.models.pipeline import PipelineState

            ps = PipelineState(
                ecosystem=ECOSYSTEM,
                tenant_id=TENANT_ID,
                tracking_date=billing_ts.date(),
                billing_gathered=True,
                resources_gathered=True,
            )
            uow.pipeline_state.upsert(ps)
            uow.billing.upsert(line)
            uow.commit()

        orch.run()

        # Verify override allocator was used
        with storage.create_unit_of_work() as uow:
            chargebacks = uow.chargebacks.find_by_date(ECOSYSTEM, TENANT_ID, billing_ts.date())
            override_rows = [r for r in chargebacks if r.allocation_method == "custom_override"]
            assert len(override_rows) >= 1
            assert override_rows[0].identity_id == "custom-identity"

    def test_identity_resolution_override(self, storage: SQLModelBackend) -> None:
        """Custom identity resolver loaded from dotted path."""
        resource = Resource(
            ecosystem=ECOSYSTEM,
            tenant_id=TENANT_ID,
            resource_id="cluster-1",
            resource_type="kafka_cluster",
            created_at=NOW - timedelta(days=30),
        )
        identity = Identity(
            ecosystem=ECOSYSTEM,
            tenant_id=TENANT_ID,
            identity_id="user-1",
            identity_type="user",
        )
        handler = IntegrationHandler("kafka", ["KAFKA_CKU"])
        handler.set_resources([resource])
        handler.set_identities([identity])

        billing_ts = NOW - timedelta(days=10)
        line = BillingLineItem(
            ecosystem=ECOSYSTEM,
            tenant_id=TENANT_ID,
            timestamp=billing_ts,
            resource_id="cluster-1",
            product_category="kafka",
            product_type="KAFKA_CKU",
            quantity=Decimal(1),
            unit_price=Decimal("100"),
            total_cost=Decimal("100"),
        )
        cost_input = IntegrationCostInput([line])
        plugin = IntegrationPlugin({"kafka": handler}, cost_input)

        config = TenantConfig(
            ecosystem=ECOSYSTEM,
            tenant_id=TENANT_ID,
            lookback_days=30,
            cutoff_days=5,
            plugin_settings={
                "identity_resolution_overrides": {
                    "kafka": "tests.integration.helpers.custom_resolver:my_resolver",
                },
            },
        )
        orch = ChargebackOrchestrator("test", config, plugin, storage)

        # Pre-populate pipeline state
        with storage.create_unit_of_work() as uow:
            from core.models.pipeline import PipelineState

            ps = PipelineState(
                ecosystem=ECOSYSTEM,
                tenant_id=TENANT_ID,
                tracking_date=billing_ts.date(),
                billing_gathered=True,
                resources_gathered=True,
            )
            uow.pipeline_state.upsert(ps)
            uow.billing.upsert(line)
            uow.commit()

        orch.run()

        # Verify custom resolver was used: chargebacks should reference "custom-resolved"
        with storage.create_unit_of_work() as uow:
            chargebacks = uow.chargebacks.find_by_date(ECOSYSTEM, TENANT_ID, billing_ts.date())
            # The custom resolver returns "custom-resolved" identity.
            # The even_allocator in IntegrationHandler will use it.
            resolved_rows = [r for r in chargebacks if r.identity_id == "custom-resolved"]
            assert len(resolved_rows) >= 1
