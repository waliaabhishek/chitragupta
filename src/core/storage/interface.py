from __future__ import annotations

from datetime import date, datetime
from typing import TYPE_CHECKING, Protocol, Self, runtime_checkable

if TYPE_CHECKING:
    from core.models.billing import BillingLineItem
    from core.models.chargeback import ChargebackRow, CustomTag
    from core.models.identity import Identity
    from core.models.pipeline import PipelineState
    from core.models.resource import Resource


@runtime_checkable
class ResourceRepository(Protocol):
    """Repository for resource persistence with temporal query support."""

    def upsert(self, resource: Resource) -> Resource: ...

    def get(self, ecosystem: str, tenant_id: str, resource_id: str) -> Resource | None: ...

    def find_active_at(self, ecosystem: str, tenant_id: str, timestamp: datetime) -> list[Resource]:
        """Point-in-time query: resources active at the given timestamp.

        Active means: (created_at IS NULL OR created_at <= timestamp)
                  AND (deleted_at IS NULL OR deleted_at > timestamp)
        """
        ...

    def find_by_period(self, ecosystem: str, tenant_id: str, start: datetime, end: datetime) -> list[Resource]:
        """Half-open interval [start, end): resources that overlapped this period.

        Overlapped means: (created_at IS NULL OR created_at < end)
                      AND (deleted_at IS NULL OR deleted_at >= start)
        """
        ...

    def find_by_type(self, ecosystem: str, tenant_id: str, resource_type: str) -> list[Resource]: ...

    def mark_deleted(self, ecosystem: str, tenant_id: str, resource_id: str, deleted_at: datetime) -> None: ...

    def delete_before(self, ecosystem: str, tenant_id: str, before: datetime) -> int: ...


@runtime_checkable
class IdentityRepository(Protocol):
    """Repository for identity persistence with temporal query support."""

    def upsert(self, identity: Identity) -> Identity: ...

    def get(self, ecosystem: str, tenant_id: str, identity_id: str) -> Identity | None: ...

    def find_active_at(self, ecosystem: str, tenant_id: str, timestamp: datetime) -> list[Identity]:
        """Point-in-time query. Same semantics as ResourceRepository.find_active_at."""
        ...

    def find_by_period(self, ecosystem: str, tenant_id: str, start: datetime, end: datetime) -> list[Identity]:
        """Half-open interval [start, end). Same semantics as ResourceRepository.find_by_period."""
        ...

    def find_by_type(self, ecosystem: str, tenant_id: str, identity_type: str) -> list[Identity]: ...

    def mark_deleted(self, ecosystem: str, tenant_id: str, identity_id: str, deleted_at: datetime) -> None: ...

    def delete_before(self, ecosystem: str, tenant_id: str, before: datetime) -> int: ...


@runtime_checkable
class BillingRepository(Protocol):
    """Repository for billing line items."""

    def upsert(self, line: BillingLineItem) -> BillingLineItem: ...

    def find_by_date(self, ecosystem: str, tenant_id: str, date: date) -> list[BillingLineItem]: ...

    def find_by_range(
        self, ecosystem: str, tenant_id: str, start: datetime, end: datetime
    ) -> list[BillingLineItem]: ...

    def delete_before(self, ecosystem: str, tenant_id: str, before: datetime) -> int: ...


@runtime_checkable
class ChargebackRepository(Protocol):
    """Repository for chargeback rows (star schema: dimension + fact)."""

    def upsert(self, row: ChargebackRow) -> ChargebackRow: ...

    def find_by_date(self, ecosystem: str, tenant_id: str, date: date) -> list[ChargebackRow]: ...

    def find_by_range(self, ecosystem: str, tenant_id: str, start: datetime, end: datetime) -> list[ChargebackRow]: ...

    def find_by_identity(self, ecosystem: str, tenant_id: str, identity_id: str) -> list[ChargebackRow]: ...

    def delete_by_date(self, ecosystem: str, tenant_id: str, date: date) -> int: ...

    def delete_before(self, ecosystem: str, tenant_id: str, before: datetime) -> int: ...


@runtime_checkable
class PipelineStateRepository(Protocol):
    """Repository for pipeline execution state tracking."""

    def upsert(self, state: PipelineState) -> PipelineState: ...

    def get(self, ecosystem: str, tenant_id: str, tracking_date: date) -> PipelineState | None: ...

    def find_needing_calculation(self, ecosystem: str, tenant_id: str) -> list[PipelineState]:
        """Returns states where billing_gathered=True AND chargeback_calculated=False."""
        ...

    def find_by_range(self, ecosystem: str, tenant_id: str, start: date, end: date) -> list[PipelineState]: ...

    def mark_billing_gathered(self, ecosystem: str, tenant_id: str, tracking_date: date) -> None: ...

    def mark_chargeback_calculated(self, ecosystem: str, tenant_id: str, tracking_date: date) -> None: ...


@runtime_checkable
class TagRepository(Protocol):
    """Repository for custom tags on chargeback dimensions."""

    def add_tag(self, dimension_id: int, tag_key: str, tag_value: str, created_by: str) -> CustomTag: ...

    def get_tags(self, dimension_id: int) -> list[CustomTag]: ...

    def delete_tag(self, tag_id: int) -> None: ...


@runtime_checkable
class UnitOfWork(Protocol):
    """Transaction coordinator. Provides repository access and commit/rollback."""

    resources: ResourceRepository
    identities: IdentityRepository
    billing: BillingRepository
    chargebacks: ChargebackRepository
    pipeline_state: PipelineStateRepository
    tags: TagRepository

    def __enter__(self) -> Self: ...
    def __exit__(self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: object) -> None: ...
    def commit(self) -> None: ...
    def rollback(self) -> None: ...


@runtime_checkable
class StorageBackend(Protocol):
    """Factory for UnitOfWork instances. Owns engine lifecycle."""

    def create_unit_of_work(self) -> UnitOfWork: ...
    def create_tables(self) -> None: ...
    def dispose(self) -> None: ...
