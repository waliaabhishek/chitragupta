from __future__ import annotations

import logging
from collections.abc import Iterable, Sequence
from datetime import date as date_type
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
    from core.models.chargeback import ChargebackRow
    from core.storage.interface import UnitOfWork
logger = logging.getLogger(__name__)


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


@runtime_checkable
class Emitter(Protocol):
    """Protocol for output sinks — called after chargeback calculation is committed.

    An emitter is a callable that receives a batch of chargeback rows for one
    tenant/date and writes them to an external sink (CSV, webhook, etc.).

    Emitters MUST be idempotent — they may be called again if the pipeline re-runs
    for the same date (recalculation window). Implementations should overwrite/upsert.

    Failures in emit do NOT roll back calculated chargebacks. Emit is best-effort.
    """

    def __call__(
        self,
        tenant_id: str,
        date: date_type,
        rows: Sequence[ChargebackRow],
    ) -> None: ...
