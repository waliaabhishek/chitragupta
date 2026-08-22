from __future__ import annotations

import logging
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import date as date_type
from datetime import datetime, timedelta
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Protocol, TypedDict, runtime_checkable

if TYPE_CHECKING:
    # Runtime import not needed — protocols use string annotations via
    # __future__.annotations.
    from sqlalchemy import Connection, Engine
    from sqlmodel import Session

    from core.engine.allocation import AllocationContext, AllocationResult
    from core.engine.topic_attribution_provider import TopicAttributionProvider
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
    from core.preview.persistence import (
        PreviewEvidenceBootstrap,
        PreviewEvidenceStorageBackend,
        PreviewEvidenceWriteUnitOfWork,
        PreviewGenerationReadUnitOfWork,
        PreviewSourceAttemptFallbackWriter,
    )
    from core.preview.storage_availability import PreviewEvidenceAvailability
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
class UnitOfWorkRepositoryAttachment(Protocol):
    """Optional plugin hook for repositories that are not core-owned."""

    def attach_unit_of_work_repositories(self, uow: UnitOfWork, session: Session) -> None: ...


@runtime_checkable
class PreviewEvidenceStorageModule(Protocol):
    def prepare_preview_evidence_migration(
        self,
        connection: Connection,
        *,
        target_revision: str,
    ) -> None: ...

    def downgrade_preview_evidence_migration(
        self,
        connection: Connection,
        *,
        target_revision: str,
    ) -> None: ...

    def register_preview_evidence_tables(self, engine: Engine) -> None: ...

    def create_preview_evidence_unit_of_work(
        self,
        connection_string: str,
        availability: PreviewEvidenceAvailability,
    ) -> PreviewEvidenceWriteUnitOfWork: ...

    def create_preview_generation_read_unit_of_work(
        self,
        connection_string: str,
        availability: PreviewEvidenceAvailability,
    ) -> PreviewGenerationReadUnitOfWork: ...

    def create_preview_evidence_bootstrap(
        self,
        backend: PreviewEvidenceStorageBackend,
    ) -> PreviewEvidenceBootstrap: ...


@runtime_checkable
class PluginStorageMigrationModule(Protocol):
    """Optional owner of plugin-specific DDL inside the core migration chain."""

    def prepare_plugin_storage_migration(
        self,
        connection: Connection,
        *,
        target_revision: str,
    ) -> None: ...

    def downgrade_plugin_storage_migration(
        self,
        connection: Connection,
        *,
        target_revision: str,
    ) -> None: ...


@runtime_checkable
class PreviewSourceAttemptFallbackStorageModule(Protocol):
    def create_preview_source_attempt_fallback_repository(
        self, session: Session
    ) -> PreviewSourceAttemptFallbackWriter: ...


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

    @property
    def gathered_resource_types(self) -> Sequence[str]:
        """Resource types this handler produces via gather_resources().

        Used by deletion detection and cache loading to scope queries to
        billing-relevant types only. Handlers that gather no resources return [].
        """
        ...

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


class ScopeGateDecision(StrEnum):
    ALLOW = "allow"
    BLOCKED = "blocked"
    RECOVERY_READY = "recovery_ready"


@dataclass(frozen=True)
class ScopeGateResult:
    decision: ScopeGateDecision
    scope_id: str
    detail: str
    blocked_window_start: datetime | None = None
    blocked_window_end: datetime | None = None
    recovery_start: datetime | None = None
    recovery_end: datetime | None = None
    retention_gap_start: datetime | None = None
    retention_gap_end: datetime | None = None
    status: str | None = None
    reason: str | None = None
    evidence: object | None = None
    probe_only: bool = False


class ScopeBlockedError(RuntimeError):
    """A plugin could not establish the scope required for pipeline work."""

    def __init__(self, result: ScopeGateResult) -> None:
        super().__init__(result.detail)
        self.result = result


@runtime_checkable
class ScopeGatePlugin(Protocol):
    """Optional core-owned gate for provider-specific telemetry scope checks."""

    def prepare_gather_scope(
        self,
        tenant_id: str,
        start: datetime,
        end: datetime,
        uow: UnitOfWork,
    ) -> ScopeGateResult: ...

    def prepare_calculation_scope(
        self,
        tenant_id: str,
        windows: Sequence[tuple[datetime, datetime]],
        uow: UnitOfWork,
    ) -> ScopeGateResult: ...

    def persist_scope_blocked(self, tenant_id: str, result: ScopeGateResult, uow: UnitOfWork) -> None: ...

    def persist_scope_probe(self, tenant_id: str, result: ScopeGateResult, uow: UnitOfWork) -> None: ...

    def persist_scope_recovery(self, tenant_id: str, result: ScopeGateResult, uow: UnitOfWork) -> None: ...

    def persist_scope_closed(self, tenant_id: str, result: ScopeGateResult, uow: UnitOfWork) -> None: ...


@runtime_checkable
class SupplementalResourceGatherer(Protocol):
    """Optional plugin capability for isolated, non-billing resource inventory."""

    @property
    def supplemental_resource_types(self) -> tuple[str, ...]: ...

    def gather_supplemental_resources(
        self,
        tenant_id: str,
        resource_type: str,
        uow: UnitOfWork,
    ) -> Iterable[Resource]: ...


@runtime_checkable
class PreviewOrganizationGatherer(Protocol):
    """Optional provider capability for Preview organization authority."""

    def gather_preview_organizations(self, tenant_id: str) -> tuple[Resource, ...]: ...


@runtime_checkable
class TopicDiscoveryPlugin(Protocol):
    """Plugin capability for topic resource discovery via metrics.

    Implement alongside EcosystemPlugin to enable topic attribution gather.
    """

    def gather_topic_resources(
        self,
        tenant_id: str,
        cluster_ids: list[str],
    ) -> Iterable[Resource]: ...


@runtime_checkable
class TopicAttributionProviderPlugin(Protocol):
    """Optional plugin capability for an ecosystem-owned attribution strategy."""

    def get_topic_attribution_provider(self) -> TopicAttributionProvider | None: ...

    def reset_topic_attribution_inventory_proof(self) -> None: ...

    def topic_attribution_inventory_ready(self, shared_context: object | None) -> bool: ...


@runtime_checkable
class OverlayConfig(Protocol):
    """Base protocol for overlay configuration."""

    enabled: bool


@runtime_checkable
class OverlayPlugin(Protocol):
    """Plugin capability for providing overlay configurations.

    Implement alongside EcosystemPlugin to provide overlay-specific config
    to core code without getattr probing.
    """

    def get_overlay_config(self, name: str) -> OverlayConfig | None: ...


@runtime_checkable
class Emitter(Protocol):
    """Protocol for output sinks — called after pipeline calculation is committed.

    Emitters MUST be idempotent. Failures do NOT roll back calculated rows.
    Uses Sequence[Any] at the protocol level — runtime_checkable protocols cannot
    check generic parameters. Concrete implementations carry their own row-specific types.
    """

    def __call__(
        self,
        tenant_id: str,
        date: date_type,
        rows: Sequence[Any],
    ) -> None: ...
