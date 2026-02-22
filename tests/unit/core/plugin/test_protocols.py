from __future__ import annotations

from collections.abc import Iterable, Sequence  # noqa: TC003
from datetime import datetime, timedelta  # noqa: TC003
from typing import Any  # noqa: TC003

from core.models import (  # noqa: TC001
    BillingLineItem,
    Identity,
    IdentityResolution,
    IdentitySet,
    MetricQuery,
    MetricRow,
    Resource,
)
from core.plugin.protocols import (  # noqa: TC001
    CostAllocator,
    CostInput,
    EcosystemPlugin,
    ServiceHandler,
)

# --- Conforming implementations ---


class FakeCostAllocator:
    def __call__(self, ctx: Any) -> Any:
        return None


class FakeCostInput:
    def gather(
        self,
        tenant_id: str,
        start: datetime,
        end: datetime,
        uow: Any,
    ) -> Iterable[BillingLineItem]:
        return []


class FakeServiceHandler:
    @property
    def service_type(self) -> str:
        return "kafka"

    @property
    def handles_product_types(self) -> Sequence[str]:
        return ["KAFKA_NUM_CKUS", "KAFKA_STORAGE"]

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
        return FakeCostAllocator()


class FakeEcosystemPlugin:
    @property
    def ecosystem(self) -> str:
        return "confluent_cloud"

    def initialize(self, config: dict[str, Any]) -> None:
        pass

    def get_service_handlers(self) -> dict[str, ServiceHandler]:
        return {"kafka": FakeServiceHandler()}

    def get_cost_input(self) -> CostInput:
        return FakeCostInput()


# --- Non-conforming implementations ---


class NotACostAllocator:
    """Missing __call__."""

    def compute(self) -> None:
        pass


class NotACostInput:
    """Missing gather() entirely."""

    def collect(self) -> list[Any]:
        return []


class NotAServiceHandler:
    """Missing most required methods/properties."""

    def service_type(self) -> str:  # not a property
        return "kafka"


class NotAnEcosystemPlugin:
    """Missing required methods."""

    def ecosystem(self) -> str:  # not a property
        return "test"


# --- Tests ---


class TestCostAllocatorProtocol:
    def test_conforming_instance(self) -> None:
        assert isinstance(FakeCostAllocator(), CostAllocator)

    def test_non_conforming_instance(self) -> None:
        assert not isinstance(NotACostAllocator(), CostAllocator)


class TestCostInputProtocol:
    def test_conforming_instance(self) -> None:
        assert isinstance(FakeCostInput(), CostInput)

    def test_non_conforming_instance(self) -> None:
        assert not isinstance(NotACostInput(), CostInput)


class TestServiceHandlerProtocol:
    def test_conforming_instance(self) -> None:
        assert isinstance(FakeServiceHandler(), ServiceHandler)

    def test_non_conforming_instance(self) -> None:
        assert not isinstance(NotAServiceHandler(), ServiceHandler)


class TestEcosystemPluginProtocol:
    def test_conforming_instance(self) -> None:
        assert isinstance(FakeEcosystemPlugin(), EcosystemPlugin)

    def test_non_conforming_instance(self) -> None:
        assert not isinstance(NotAnEcosystemPlugin(), EcosystemPlugin)
