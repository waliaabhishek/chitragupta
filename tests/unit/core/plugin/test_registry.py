from __future__ import annotations

from collections.abc import Iterable, Sequence  # noqa: TC003
from datetime import datetime, timedelta  # noqa: TC003
from typing import Any  # noqa: TC003

import pytest  # noqa: TC002

from core.models import (  # noqa: TC001
    BillingLineItem,
    Identity,
    IdentityResolution,
    IdentitySet,
    MetricQuery,
    MetricRow,
    Resource,
)
from core.plugin.protocols import CostAllocator, CostInput, ServiceHandler  # noqa: TC001
from core.plugin.registry import EcosystemBundle, PluginRegistry  # noqa: TC001

# --- Fixtures / helpers ---


class StubAllocator:
    def __call__(self, ctx: Any) -> Any:
        return None


class StubCostInput:
    def gather(
        self,
        tenant_id: str,
        start: datetime,
        end: datetime,
        uow: Any,
    ) -> Iterable[BillingLineItem]:
        return []


class StubHandler:
    """Configurable stub handler."""

    def __init__(self, svc_type: str, product_types: Sequence[str]) -> None:
        self._service_type = svc_type
        self._product_types = product_types

    @property
    def service_type(self) -> str:
        return self._service_type

    @property
    def handles_product_types(self) -> Sequence[str]:
        return self._product_types

    def gather_resources(self, tenant_id: str, uow: Any) -> Iterable[Resource]:
        return []

    def gather_identities(self, tenant_id: str, uow: Any) -> Iterable[Identity]:
        return []

    def resolve_identities(
        self,
        tenant_id: str,
        resource_id: str,
        billing_timestamp: datetime,
        billing_duration: timedelta,
        metrics_data: dict[str, list[MetricRow]] | None,
        uow: Any,
    ) -> IdentityResolution:
        return IdentityResolution(
            resource_active=IdentitySet(),
            metrics_derived=IdentitySet(),
            tenant_period=IdentitySet(),
        )

    def get_metrics_for_product_type(self, product_type: str) -> list[MetricQuery]:
        return []

    def get_allocator(self, product_type: str) -> CostAllocator:
        return StubAllocator()


class StubPlugin:
    """Configurable stub plugin."""

    def __init__(
        self,
        eco: str = "test_eco",
        handlers: dict[str, ServiceHandler] | None = None,
    ) -> None:
        self._eco = eco
        self._handlers = handlers or {}

    @property
    def ecosystem(self) -> str:
        return self._eco

    def initialize(self, config: dict[str, Any]) -> None:
        pass

    def get_service_handlers(self) -> dict[str, ServiceHandler]:
        return self._handlers

    def get_cost_input(self) -> CostInput:
        return StubCostInput()


# --- PluginRegistry tests ---


class TestPluginRegistry:
    def test_register_and_create(self) -> None:
        registry = PluginRegistry()
        registry.register("eco1", lambda: StubPlugin("eco1"))
        plugin = registry.create("eco1")
        assert plugin.ecosystem == "eco1"

    def test_create_returns_new_instance_each_call(self) -> None:
        registry = PluginRegistry()
        registry.register("eco1", lambda: StubPlugin("eco1"))
        p1 = registry.create("eco1")
        p2 = registry.create("eco1")
        assert p1 is not p2

    def test_duplicate_ecosystem_raises(self) -> None:
        registry = PluginRegistry()
        registry.register("eco1", lambda: StubPlugin("eco1"))
        with pytest.raises(ValueError, match="already registered"):
            registry.register("eco1", lambda: StubPlugin("eco1"))

    def test_list_ecosystems(self) -> None:
        registry = PluginRegistry()
        registry.register("alpha", lambda: StubPlugin("alpha"))
        registry.register("beta", lambda: StubPlugin("beta"))
        ecosystems = registry.list_ecosystems()
        assert set(ecosystems) == {"alpha", "beta"}

    def test_create_unknown_ecosystem_raises(self) -> None:
        registry = PluginRegistry()
        with pytest.raises(KeyError, match="Unknown ecosystem"):
            registry.create("nonexistent")


# --- EcosystemBundle tests ---


class TestEcosystemBundle:
    def test_build_indexes_product_types(self) -> None:
        kafka = StubHandler("kafka", ["KAFKA_CKUS", "KAFKA_STORAGE", "KAFKA_CONNECT"])
        flink = StubHandler("flink", ["FLINK_CSU", "FLINK_STORAGE"])
        plugin = StubPlugin("ccloud", {"kafka": kafka, "flink": flink})

        bundle = EcosystemBundle.build(plugin)

        assert bundle.plugin is plugin
        assert bundle.handlers == {"kafka": kafka, "flink": flink}
        assert bundle.product_type_to_handler["KAFKA_CKUS"] is kafka
        assert bundle.product_type_to_handler["KAFKA_STORAGE"] is kafka
        assert bundle.product_type_to_handler["KAFKA_CONNECT"] is kafka
        assert bundle.product_type_to_handler["FLINK_CSU"] is flink
        assert bundle.product_type_to_handler["FLINK_STORAGE"] is flink
        assert len(bundle.product_type_to_handler) == 5

    def test_duplicate_product_type_raises(self) -> None:
        h1 = StubHandler("kafka", ["SHARED_TYPE"])
        h2 = StubHandler("flink", ["SHARED_TYPE"])
        plugin = StubPlugin("ccloud", {"kafka": h1, "flink": h2})

        with pytest.raises(ValueError, match="Duplicate product_type 'SHARED_TYPE'"):
            EcosystemBundle.build(plugin)

    def test_empty_handles_product_types(self) -> None:
        identity_only = StubHandler("identity_svc", [])
        kafka = StubHandler("kafka", ["KAFKA_CKUS"])
        plugin = StubPlugin("ccloud", {"identity_svc": identity_only, "kafka": kafka})

        bundle = EcosystemBundle.build(plugin)

        assert "identity_svc" in bundle.handlers
        assert "kafka" in bundle.handlers
        # identity_only handler has no product_type entries
        assert len(bundle.product_type_to_handler) == 1
        assert bundle.product_type_to_handler["KAFKA_CKUS"] is kafka

    def test_handler_lookup_correct_handler(self) -> None:
        kafka = StubHandler("kafka", ["KAFKA_CKUS"])
        flink = StubHandler("flink", ["FLINK_CSU"])
        plugin = StubPlugin("ccloud", {"kafka": kafka, "flink": flink})

        bundle = EcosystemBundle.build(plugin)

        assert bundle.product_type_to_handler["KAFKA_CKUS"] is kafka
        assert bundle.product_type_to_handler["FLINK_CSU"] is flink

    def test_missing_product_type_raises_key_error(self) -> None:
        plugin = StubPlugin("ccloud", {"kafka": StubHandler("kafka", ["KAFKA_CKUS"])})
        bundle = EcosystemBundle.build(plugin)

        with pytest.raises(KeyError):
            bundle.product_type_to_handler["NONEXISTENT"]
