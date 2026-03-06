from __future__ import annotations

import logging
from collections.abc import Iterator
from datetime import date, datetime
from typing import TYPE_CHECKING, Protocol, Self, runtime_checkable

if TYPE_CHECKING:
    from core.models.billing import BillingLineItem
    from core.models.chargeback import AggregationRow, ChargebackDimensionInfo, ChargebackRow, CustomTag
    from core.models.identity import Identity
    from core.models.pipeline import PipelineRun, PipelineState
    from core.models.resource import Resource
logger = logging.getLogger(__name__)


@runtime_checkable
class ResourceRepository(Protocol):
    """Repository for resource persistence with temporal query support."""

    def upsert(self, resource: Resource) -> Resource: ...

    def get(self, ecosystem: str, tenant_id: str, resource_id: str) -> Resource | None: ...

    def find_active_at(
        self,
        ecosystem: str,
        tenant_id: str,
        timestamp: datetime,
        *,
        resource_type: str | None = None,
        status: str | None = None,
        limit: int | None = None,
        offset: int = 0,
    ) -> tuple[list[Resource], int]:
        """Point-in-time query: resources active at the given timestamp.

        Active means: (created_at IS NULL OR created_at <= timestamp)
                  AND (deleted_at IS NULL OR deleted_at > timestamp)

        Returns (page_of_resources, total_count). Filters and pagination applied at SQL level.
        """
        ...

    def find_by_period(
        self,
        ecosystem: str,
        tenant_id: str,
        start: datetime,
        end: datetime,
        *,
        resource_type: str | None = None,
        status: str | None = None,
        limit: int | None = None,
        offset: int = 0,
    ) -> tuple[list[Resource], int]:
        """Half-open interval [start, end): resources that overlapped this period.

        Overlapped means: (created_at IS NULL OR created_at < end)
                      AND (deleted_at IS NULL OR deleted_at >= start)

        Returns (page_of_resources, total_count). Filters and pagination applied at SQL level.
        """
        ...

    def find_by_type(self, ecosystem: str, tenant_id: str, resource_type: str) -> list[Resource]: ...

    def find_paginated(
        self,
        ecosystem: str,
        tenant_id: str,
        limit: int,
        offset: int,
        resource_type: str | None = None,
        status: str | None = None,
    ) -> tuple[list[Resource], int]:
        """Returns (items, total_count) for pagination. Database-level LIMIT/OFFSET."""
        ...

    def mark_deleted(self, ecosystem: str, tenant_id: str, resource_id: str, deleted_at: datetime) -> None: ...

    def delete_before(self, ecosystem: str, tenant_id: str, before: datetime) -> int: ...


@runtime_checkable
class IdentityRepository(Protocol):
    """Repository for identity persistence with temporal query support."""

    def upsert(self, identity: Identity) -> Identity: ...

    def get(self, ecosystem: str, tenant_id: str, identity_id: str) -> Identity | None: ...

    def find_active_at(
        self,
        ecosystem: str,
        tenant_id: str,
        timestamp: datetime,
        *,
        identity_type: str | None = None,
        limit: int | None = None,
        offset: int = 0,
    ) -> tuple[list[Identity], int]:
        """Point-in-time query. Same semantics as ResourceRepository.find_active_at.

        Returns (page_of_identities, total_count). Filters and pagination applied at SQL level.
        """
        ...

    def find_by_period(
        self,
        ecosystem: str,
        tenant_id: str,
        start: datetime,
        end: datetime,
        *,
        identity_type: str | None = None,
        limit: int | None = None,
        offset: int = 0,
    ) -> tuple[list[Identity], int]:
        """Half-open interval [start, end). Same semantics as ResourceRepository.find_by_period.

        Returns (page_of_identities, total_count). Filters and pagination applied at SQL level.
        """
        ...

    def find_by_type(self, ecosystem: str, tenant_id: str, identity_type: str) -> list[Identity]: ...

    def find_paginated(
        self,
        ecosystem: str,
        tenant_id: str,
        limit: int,
        offset: int,
        identity_type: str | None = None,
    ) -> tuple[list[Identity], int]:
        """Returns (items, total_count) for pagination. Database-level LIMIT/OFFSET."""
        ...

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

    def increment_allocation_attempts(
        self,
        ecosystem: str,
        tenant_id: str,
        timestamp: datetime,
        resource_id: str,
        product_type: str,
    ) -> int:
        """Increments allocation_attempts in DB and returns the new value.

        Identifies the billing line by its composite key. The domain model
        (BillingLineItem) is not modified — it remains frozen.
        """
        ...

    def delete_before(self, ecosystem: str, tenant_id: str, before: datetime) -> int: ...

    def find_by_filters(
        self,
        ecosystem: str,
        tenant_id: str,
        start: datetime | None = None,
        end: datetime | None = None,
        product_type: str | None = None,
        resource_id: str | None = None,
        limit: int = 1000,
        offset: int = 0,
    ) -> tuple[list[BillingLineItem], int]:
        """Returns (items, total_count). Filters applied at SQL level."""
        ...


@runtime_checkable
class ChargebackRepository(Protocol):
    """Repository for chargeback rows (star schema: dimension + fact)."""

    def upsert(self, row: ChargebackRow) -> ChargebackRow: ...

    def find_by_date(self, ecosystem: str, tenant_id: str, date: date) -> list[ChargebackRow]: ...

    def find_by_range(self, ecosystem: str, tenant_id: str, start: datetime, end: datetime) -> list[ChargebackRow]: ...

    def find_by_identity(self, ecosystem: str, tenant_id: str, identity_id: str) -> list[ChargebackRow]: ...

    def delete_by_date(self, ecosystem: str, tenant_id: str, date: date) -> int: ...

    def delete_before(self, ecosystem: str, tenant_id: str, before: datetime) -> int: ...

    def find_by_filters(
        self,
        ecosystem: str,
        tenant_id: str,
        start: datetime | None = None,
        end: datetime | None = None,
        identity_id: str | None = None,
        product_type: str | None = None,
        resource_id: str | None = None,
        cost_type: str | None = None,
        limit: int = 1000,
        offset: int = 0,
    ) -> tuple[list[ChargebackRow], int]:
        """Returns (items, total_count). Filters and pagination at SQL level."""
        ...

    def iter_by_filters(
        self,
        ecosystem: str,
        tenant_id: str,
        start: datetime | None = None,
        end: datetime | None = None,
        identity_id: str | None = None,
        product_type: str | None = None,
        resource_id: str | None = None,
        cost_type: str | None = None,
        batch_size: int = 5000,
    ) -> Iterator[ChargebackRow]:
        """Yield rows matching filters in batches. No limit cap; bounded memory."""
        ...

    def get_dimension(self, dimension_id: int) -> ChargebackDimensionInfo | None:
        """Get a single dimension by ID for tenant isolation checks."""
        ...

    def get_dimensions_batch(self, dimension_ids: list[int]) -> dict[int, ChargebackDimensionInfo]:
        """Batch fetch dimensions by IDs. Returns dict keyed by dimension_id."""
        ...

    def find_dimension_ids_by_filters(
        self,
        ecosystem: str,
        tenant_id: str,
        start: datetime,
        end: datetime,
        identity_id: str | None = None,
        product_type: str | None = None,
        resource_id: str | None = None,
        cost_type: str | None = None,
    ) -> list[int]:
        """Return distinct dimension_ids matching filters. No pagination."""
        ...

    def aggregate(
        self,
        ecosystem: str,
        tenant_id: str,
        group_by: list[str],
        time_bucket: str,
        start: datetime | None = None,
        end: datetime | None = None,
        identity_id: str | None = None,
        product_type: str | None = None,
        resource_id: str | None = None,
        cost_type: str | None = None,
        limit: int = 10000,
    ) -> list[AggregationRow]:
        """Server-side aggregation with GROUP BY. Returns pre-aggregated buckets."""
        ...


@runtime_checkable
class PipelineStateRepository(Protocol):
    """Repository for pipeline execution state tracking."""

    def upsert(self, state: PipelineState) -> PipelineState: ...

    def get(self, ecosystem: str, tenant_id: str, tracking_date: date) -> PipelineState | None: ...

    def find_needing_calculation(self, ecosystem: str, tenant_id: str) -> list[PipelineState]:
        """Returns states where billing_gathered=True AND resources_gathered=True AND chargeback_calculated=False.

        Results are ordered by tracking_date ascending (oldest first).
        """
        ...

    def find_by_range(self, ecosystem: str, tenant_id: str, start: date, end: date) -> list[PipelineState]: ...

    def mark_billing_gathered(self, ecosystem: str, tenant_id: str, tracking_date: date) -> None: ...

    def mark_resources_gathered(self, ecosystem: str, tenant_id: str, tracking_date: date) -> None:
        """Sets resources_gathered=True for the given date."""
        ...

    def mark_needs_recalculation(self, ecosystem: str, tenant_id: str, tracking_date: date) -> None:
        """Resets chargeback_calculated=False for the given date (for recalculation window)."""
        ...

    def mark_chargeback_calculated(self, ecosystem: str, tenant_id: str, tracking_date: date) -> None: ...

    def count_pending(self, ecosystem: str, tenant_id: str) -> int:
        """Count dates where billing+resources gathered but chargeback not calculated."""
        ...

    def count_calculated(self, ecosystem: str, tenant_id: str) -> int:
        """Count dates where chargeback has been calculated."""
        ...

    def get_last_calculated_date(self, ecosystem: str, tenant_id: str) -> date | None:
        """Return the most recent tracking_date where chargeback_calculated=True, or None."""
        ...


@runtime_checkable
class TagRepository(Protocol):
    """Repository for custom tags on chargeback dimensions."""

    def add_tag(self, dimension_id: int, tag_key: str, display_name: str, created_by: str) -> CustomTag:
        """Create tag. Backend auto-generates tag_value = uuid4()."""
        ...

    def get_tag(self, tag_id: int) -> CustomTag | None: ...

    def get_tags(self, dimension_id: int) -> list[CustomTag]: ...

    def find_tags_for_tenant(
        self,
        ecosystem: str,
        tenant_id: str,
        limit: int = 100,
        offset: int = 0,
        search: str | None = None,
    ) -> tuple[list[CustomTag], int]:
        """Find all tags for dimensions belonging to a tenant. Returns (items, total).

        search: case-insensitive LIKE on tag_key, tag_value, or display_name.
        """
        ...

    def update_display_name(self, tag_id: int, display_name: str) -> CustomTag:
        """Update display_name only. tag_value remains immutable."""
        ...

    def find_by_dimension_and_key(self, dimension_id: int, tag_key: str) -> CustomTag | None:
        """Find existing tag by dimension and key. Used for upsert/override logic."""
        ...

    def delete_tag(self, tag_id: int) -> None: ...


@runtime_checkable
class PipelineRunRepository(Protocol):
    """Repository for persisted pipeline run history."""

    def create_run(self, tenant_name: str, started_at: datetime) -> PipelineRun:
        """Insert a new run record with status='running'. Returns the persisted run with id set."""
        ...

    def update_run(self, run: PipelineRun) -> PipelineRun:
        """Persist updated run state (status, ended_at, counters, error_message)."""
        ...

    def get_run(self, run_id: int) -> PipelineRun | None: ...

    def list_runs_for_tenant(self, tenant_name: str, limit: int = 100) -> list[PipelineRun]:
        """List runs for a tenant ordered by started_at descending."""
        ...

    def get_latest_run(self, tenant_name: str) -> PipelineRun | None:
        """Return the most recently started run for this tenant, or None."""
        ...


@runtime_checkable
class UnitOfWork(Protocol):
    """Transaction coordinator. Provides repository access and commit/rollback."""

    resources: ResourceRepository
    identities: IdentityRepository
    billing: BillingRepository
    chargebacks: ChargebackRepository
    pipeline_state: PipelineStateRepository
    pipeline_runs: PipelineRunRepository
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
