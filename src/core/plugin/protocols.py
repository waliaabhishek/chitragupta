from __future__ import annotations

from collections.abc import Iterable, Sequence
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
    # Runtime import not needed — protocols use string annotations via
    # __future__.annotations.
    from core.engine.allocation import AllocationContext, AllocationResult
    from core.metrics.protocol import MetricsSource
    from core.models import (
        BillingLineItem,
        Identity,
        IdentityResolution,
        MetricQuery,
        MetricRow,
        Resource,
    )
    from core.storage.interface import UnitOfWork


@runtime_checkable
class CostAllocator(Protocol):
    def __call__(self, ctx: AllocationContext) -> AllocationResult: ...


@runtime_checkable
class IdentityResolver(Protocol):
    """Protocol for standalone identity resolution override callables.

    Matches the parameter signature of ``ServiceHandler.resolve_identities``
    but without ``self`` — the loaded object must be a plain function or
    callable instance, not an uninstantiated class.
    """

    def __call__(
        self,
        tenant_id: str,
        resource_id: str,
        billing_timestamp: datetime,
        billing_duration: timedelta,
        metrics_data: dict[str, list[MetricRow]] | None,
        uow: UnitOfWork,
    ) -> IdentityResolution: ...


@runtime_checkable
class CostInput(Protocol):
    def gather(
        self,
        tenant_id: str,
        start: datetime,
        end: datetime,
        uow: UnitOfWork,
    ) -> Iterable[BillingLineItem]: ...


@runtime_checkable
class ServiceHandler(Protocol):
    @property
    def service_type(self) -> str: ...

    @property
    def handles_product_types(self) -> Sequence[str]: ...

    def gather_resources(
        self, tenant_id: str, uow: UnitOfWork, shared_ctx: object | None = None
    ) -> Iterable[Resource]: ...

    def gather_identities(self, tenant_id: str, uow: UnitOfWork) -> Iterable[Identity]: ...

    def resolve_identities(
        self,
        tenant_id: str,
        resource_id: str,
        billing_timestamp: datetime,
        billing_duration: timedelta,
        metrics_data: dict[str, list[MetricRow]] | None,
        uow: UnitOfWork,
    ) -> IdentityResolution: ...

    def get_metrics_for_product_type(self, product_type: str) -> list[MetricQuery]: ...

    def get_allocator(self, product_type: str) -> CostAllocator: ...


@runtime_checkable
class EcosystemPlugin(Protocol):
    @property
    def ecosystem(self) -> str: ...

    def initialize(self, config: dict[str, Any]) -> None: ...

    def get_service_handlers(self) -> dict[str, ServiceHandler]: ...

    def get_cost_input(self) -> CostInput: ...

    def get_metrics_source(self) -> MetricsSource | None: ...

    def build_shared_context(self, tenant_id: str) -> object | None: ...

    def close(self) -> None: ...
