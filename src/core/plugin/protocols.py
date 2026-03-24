from __future__ import annotations

import logging
from collections.abc import Iterable, Sequence
from datetime import date as date_type
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, Protocol, TypedDict, runtime_checkable

if TYPE_CHECKING:
    # Runtime import not needed — protocols use string annotations via
    # __future__.annotations.
    from sqlalchemy import Engine
    from sqlmodel import Session

    from core.engine.allocation import AllocationContext, AllocationResult
    from core.metrics.protocol import MetricsSource
    from core.models import (
        BillingLineItem,
        Identity,
        IdentityResolution,
        IdentitySet,
        MetricQuery,
        MetricRow,
        Resource,
    )
    from core.models.chargeback import ChargebackRow
    from core.storage.interface import (
        BillingRepository,
        ChargebackRepository,
        IdentityRepository,
        ResourceRepository,
        UnitOfWork,
    )


class ResolveContext(TypedDict, total=False):
    cached_identities: IdentitySet
    cached_resources: dict[str, Resource]


logger = logging.getLogger(__name__)


@runtime_checkable
class StorageModule(Protocol):
    """Plugin-owned factory for billing, resource, identity, and chargeback repositories."""

    def create_billing_repository(self, session: Session) -> BillingRepository: ...

    def create_resource_repository(self, session: Session) -> ResourceRepository: ...

    def create_identity_repository(self, session: Session) -> IdentityRepository: ...

    def create_chargeback_repository(self, session: Session) -> ChargebackRepository: ...

    def register_tables(self, engine: Engine) -> None: ...


@runtime_checkable
class CostAllocator(Protocol):
    def __call__(self, ctx: AllocationContext) -> AllocationResult: ...


@runtime_checkable
class IdentityResolver(Protocol):
    """Protocol for standalone identity resolution override callables.

    Matches the parameter signature of ``ServiceHandler.resolve_identities``
    but without ``self`` — the loaded object must be a plain function or
    callable instance, not an uninstantiated class.

    The ``context`` parameter is optional and carries pre-fetched identity/resource
    caches. Include it (defaulting to ``None``) to match the full protocol signature.
    """

    def __call__(
        self,
        tenant_id: str,
        resource_id: str,
        billing_timestamp: datetime,
        billing_duration: timedelta,
        metrics_data: dict[str, list[MetricRow]] | None,
        uow: UnitOfWork,
        context: ResolveContext | None = None,
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
        context: ResolveContext | None = None,
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

    def get_fallback_allocator(self) -> CostAllocator | None: ...

    def build_shared_context(self, tenant_id: str) -> object | None: ...

    def get_storage_module(self) -> StorageModule: ...

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
