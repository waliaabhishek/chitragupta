from __future__ import annotations

import logging
from collections.abc import Iterator, Sequence
from datetime import date, datetime
from typing import TYPE_CHECKING, Any, Protocol, Self, runtime_checkable

if TYPE_CHECKING:
    from typing import Literal

    from core.emitters.repository import EmissionRepository
    from core.models.billing import BillingLineItem
    from core.models.chargeback import (
        AggregationRow,
        AllocationIssueRow,
        ChargebackDimensionInfo,
        ChargebackRow,
    )
    from core.models.counts import TypeStatusCounts
    from core.models.entity_tag import EntityTag
    from core.models.identity import Identity
    from core.models.pipeline import PipelineRun, PipelineState
    from core.models.resource import Resource
    from core.models.topic_attribution import TopicAttributionAggregationResult, TopicAttributionRow
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
        resource_type: str | Sequence[str],
        status: str | None = None,
        limit: int | None = None,
        offset: int = 0,
        count: bool = True,
    ) -> tuple[list[Resource], int]:
        """Point-in-time query: resources active at the given timestamp.

        Active means: (created_at IS NULL OR created_at <= timestamp)
                  AND (deleted_at IS NULL OR deleted_at > timestamp)

        Returns (page_of_resources, total_count). Filters and pagination applied at SQL level.
        When count=False, skips the COUNT query and returns 0 for total_count.
        """
        ...

    def find_by_period(
        self,
        ecosystem: str,
        tenant_id: str,
        start: datetime,
        end: datetime,
        *,
        parent_id: str | None = None,
        resource_type: str | Sequence[str],
        status: str | None = None,
        metadata_filter: dict[str, str | int | float | bool | None] | None = None,
        limit: int | None = None,
        offset: int = 0,
        count: bool = True,
    ) -> tuple[list[Resource], int]:
        """Half-open interval [start, end): resources that overlapped this period.

        Overlapped means: (created_at IS NULL OR created_at < end)
                      AND (deleted_at IS NULL OR deleted_at >= start)

        If parent_id is provided, only resources with that parent_id are returned.

        metadata_filter: dict of {key: scalar_value} matched via json_extract on metadata_json.
        All entries are ANDed. Values must be scalars (str/int/float/bool/None) — nested
        dicts or lists would silently return zero rows.

        Returns (page_of_resources, total_count). Filters and pagination applied at SQL level.
        When count=False, skips the COUNT query and returns 0 for total_count.
        """
        ...

    def find_by_type(self, ecosystem: str, tenant_id: str, resource_type: str) -> list[Resource]: ...

    def find_by_parent(
        self,
        ecosystem: str,
        tenant_id: str,
        parent_id: str,
        *,
        resource_type: str | Sequence[str],
    ) -> list[Resource]:
        """Return resources with the given parent_id, optionally filtered by resource_type.

        Only returns non-deleted resources (deleted_at IS NULL).
        """
        ...

    def find_paginated(
        self,
        ecosystem: str,
        tenant_id: str,
        limit: int,
        offset: int,
        *,
        resource_type: str | Sequence[str],
        status: str | None = None,
        search: str | None = None,
        sort_by: str | None = None,
        sort_order: str = "asc",
        tag_key: str | None = None,
        tag_value: str | None = None,
        tags_repo: EntityTagRepository | None = None,
    ) -> tuple[list[Resource], int]:
        """Returns (items, total_count) for pagination. Database-level LIMIT/OFFSET."""
        ...

    def mark_deleted(self, ecosystem: str, tenant_id: str, resource_id: str, deleted_at: datetime) -> None: ...

    def delete_before(self, ecosystem: str, tenant_id: str, before: datetime) -> int: ...

    def count_by_type(self, ecosystem: str, tenant_id: str) -> dict[str, TypeStatusCounts]:
        """Return counts GROUP BY (resource_type, status) for the given tenant.

        Returns a dict mapping resource_type string to TypeStatusCounts with
        total, active, and deleted fields. Returns empty dict when no resources
        exist for this tenant.
        """
        ...


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
        count: bool = True,
    ) -> tuple[list[Identity], int]:
        """Point-in-time query. Same semantics as ResourceRepository.find_active_at.

        Returns (page_of_identities, total_count). Filters and pagination applied at SQL level.
        When count=False, skips the COUNT query and returns 0 for total_count.
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
        count: bool = True,
    ) -> tuple[list[Identity], int]:
        """Half-open interval [start, end). Same semantics as ResourceRepository.find_by_period.

        Returns (page_of_identities, total_count). Filters and pagination applied at SQL level.
        When count=False, skips the COUNT query and returns 0 for total_count.
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
        search: str | None = None,
        sort_by: str | None = None,
        sort_order: str = "asc",
        tag_key: str | None = None,
        tag_value: str | None = None,
        tags_repo: EntityTagRepository | None = None,
    ) -> tuple[list[Identity], int]:
        """Returns (items, total_count) for pagination. Database-level LIMIT/OFFSET."""
        ...

    def mark_deleted(self, ecosystem: str, tenant_id: str, identity_id: str, deleted_at: datetime) -> None: ...

    def delete_before(self, ecosystem: str, tenant_id: str, before: datetime) -> int: ...

    def count_by_type(self, ecosystem: str, tenant_id: str) -> dict[str, TypeStatusCounts]:
        """Return counts GROUP BY (identity_type, derived_status) for the given tenant.

        Status is derived from deleted_at: NULL=active, non-NULL=deleted.
        Returns a dict mapping identity_type string to TypeStatusCounts with
        total, active, and deleted fields. Returns empty dict when no identities
        exist for this tenant.
        """
        ...


@runtime_checkable
class BillingRepository(Protocol):
    """Repository for billing line items."""

    def upsert(self, line: BillingLineItem) -> BillingLineItem: ...

    def find_by_date(self, ecosystem: str, tenant_id: str, date: date) -> list[BillingLineItem]: ...

    def find_by_range(
        self, ecosystem: str, tenant_id: str, start: datetime, end: datetime
    ) -> list[BillingLineItem]: ...

    def increment_allocation_attempts(self, line: BillingLineItem) -> int:
        """Increments allocation_attempts in DB and returns the new value.

        Identifies the billing line via the domain object's composite key.
        The domain model (BillingLineItem) is not modified — it remains frozen.
        """
        ...

    def increment_topic_attribution_attempts(self, line: BillingLineItem) -> int:
        """Increments topic_attribution_attempts in DB and returns the new value.

        Identifies the billing line via the domain object's composite key.
        The domain model (BillingLineItem) is not modified — it remains frozen.
        """
        ...

    def reset_allocation_attempts_by_date(self, ecosystem: str, tenant_id: str, tracking_date: date) -> int:
        """Reset allocation_attempts to 0 for all billing rows on tracking_date.

        Returns the number of rows updated.
        """
        ...

    def reset_topic_attribution_attempts_by_date(self, ecosystem: str, tenant_id: str, tracking_date: date) -> int:
        """Reset topic_attribution_attempts to 0 for all billing rows on tracking_date.

        Returns the number of rows updated.
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

    def upsert_batch(self, rows: list[ChargebackRow]) -> int:
        """Insert all rows in a single batch. Returns count of rows written."""
        ...

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
        tag_key: str | None = None,
        tag_value: str | None = None,
        tags_repo: EntityTagRepository | None = None,
    ) -> tuple[list[ChargebackRow], int]:
        """Returns (items, total_count). Filters and pagination at SQL level.
        If tags_repo is provided, row.tags is populated from entity tags (2 batch queries).
        tag_key/tag_value filter rows to those whose resource or identity has the matching tag.
        """
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
        tag_key: str | None = None,
        tag_value: str | None = None,
        tags_repo: EntityTagRepository | None = None,
    ) -> Iterator[ChargebackRow]:
        """Yield rows matching filters in batches. No limit cap; bounded memory.
        If tags_repo is provided, row.tags is populated per batch (2 queries per batch).
        """
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
        limit: int | None = None,
        tag_group_by: list[str] | None = None,  # tag keys to group by
        tag_filters: dict[str, list[str]] | None = None,  # {tag_key: [values]} ANDed
    ) -> list[AggregationRow]:
        """Server-side aggregation with GROUP BY. Returns pre-aggregated buckets."""
        ...

    def get_distinct_dates(self, ecosystem: str, tenant_id: str) -> list[date]:
        """Return sorted list of distinct dates that have chargeback facts for the tenant."""
        ...

    def find_aggregated_for_emit(
        self,
        ecosystem: str,
        tenant_id: str,
        start: date,
        end: date,
        granularity: Literal["daily", "monthly"],
    ) -> list[ChargebackRow]:
        """SQL GROUP BY aggregation for emit. Returns ChargebackRow with floored timestamp, dimension_id=None."""
        ...

    def find_allocation_issues(
        self,
        ecosystem: str,
        tenant_id: str,
        start: datetime | None = None,
        end: datetime | None = None,
        identity_id: str | None = None,
        product_type: str | None = None,
        resource_id: str | None = None,
        limit: int = 1000,
        offset: int = 0,
    ) -> tuple[list[AllocationIssueRow], int]:
        """Returns (items, total_count) of failed-allocation groups, ordered by total_cost DESC."""
        ...


@runtime_checkable
class TopicAttributionRepository(Protocol):
    """Repository for topic attribution star schema."""

    def upsert_batch(self, rows: list[TopicAttributionRow]) -> int:
        """Insert all rows. Get-or-create dimensions, then add facts. Returns count written."""
        ...

    def find_by_date(
        self,
        ecosystem: str,
        tenant_id: str,
        target_date: date,
    ) -> list[TopicAttributionRow]: ...

    def find_by_cluster(
        self,
        ecosystem: str,
        tenant_id: str,
        cluster_resource_id: str,
        start: datetime,
        end: datetime,
    ) -> list[TopicAttributionRow]: ...

    def find_by_filters(
        self,
        ecosystem: str,
        tenant_id: str,
        start: datetime | None = None,
        end: datetime | None = None,
        cluster_resource_id: str | None = None,
        topic_name: str | None = None,
        product_type: str | None = None,
        attribution_method: str | None = None,
        limit: int = 1000,
        offset: int = 0,
    ) -> tuple[list[TopicAttributionRow], int]:
        """Returns (items, total_count). All filters applied at SQL level."""
        ...

    def iter_by_filters(
        self,
        ecosystem: str,
        tenant_id: str,
        start: datetime | None = None,
        end: datetime | None = None,
        cluster_resource_id: str | None = None,
        topic_name: str | None = None,
        product_type: str | None = None,
        attribution_method: str | None = None,
        batch_size: int = 5000,
    ) -> Iterator[TopicAttributionRow]:
        """Yield rows matching filters in batches. No limit cap; bounded memory."""
        ...

    def aggregate(
        self,
        ecosystem: str,
        tenant_id: str,
        group_by: list[str],
        time_bucket: str,
        start: datetime | None = None,
        end: datetime | None = None,
        cluster_resource_id: str | None = None,
        topic_name: str | None = None,
        product_type: str | None = None,
        tag_group_by: list[str] | None = None,
        tag_filters: dict[str, list[str]] | None = None,
    ) -> TopicAttributionAggregationResult:
        """SQL GROUP BY aggregation. Returns domain type."""
        ...

    def get_distinct_dates(self, ecosystem: str, tenant_id: str) -> list[date]:
        """Return sorted list of distinct dates that have topic attribution facts."""
        ...

    def delete_by_date(self, ecosystem: str, tenant_id: str, target_date: date) -> int:
        """Delete all facts for a specific date. Returns count deleted."""
        ...

    def delete_before(self, ecosystem: str, tenant_id: str, before: datetime) -> int:
        """Delete facts older than cutoff, prune orphaned dimensions. Returns deleted fact count."""
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

    def mark_topic_overlay_gathered(self, ecosystem: str, tenant_id: str, tracking_date: date) -> None:
        """Sets topic_overlay_gathered=True for the given date."""
        ...

    def mark_topic_attribution_calculated(self, ecosystem: str, tenant_id: str, tracking_date: date) -> None:
        """Sets topic_attribution_calculated=True for the given date."""
        ...

    def find_needing_topic_attribution(self, ecosystem: str, tenant_id: str) -> list[PipelineState]:
        """Returns states where topic_overlay_gathered=True AND topic_attribution_calculated=False.

        Results ordered by tracking_date ascending.
        """
        ...

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
class EntityTagRepository(Protocol):
    """Repository for entity-level tags (resources and identities)."""

    def add_tag(
        self,
        tenant_id: str,
        entity_type: str,
        entity_id: str,
        tag_key: str,
        tag_value: str,
        created_by: str,
    ) -> EntityTag:
        """Create a tag. Raises IntegrityError on duplicate (tenant_id, entity_type, entity_id, tag_key)."""
        ...

    def get_tags(self, tenant_id: str, entity_type: str, entity_id: str) -> list[EntityTag]: ...

    def update_tag(self, tag_id: int, tag_value: str) -> EntityTag:
        """Update tag_value for an existing tag."""
        ...

    def delete_tag(self, tag_id: int) -> None: ...

    def find_tags_for_tenant(
        self,
        tenant_id: str,
        limit: int = 100,
        offset: int = 0,
        entity_type: str | None = None,
        tag_key: str | None = None,
    ) -> tuple[list[EntityTag], int]:
        """Paginated listing. Optional filters: entity_type, tag_key (case-insensitive LIKE)."""
        ...

    def find_tags_for_entities(
        self,
        tenant_id: str,
        entity_type: str,
        entity_ids: list[str],
    ) -> dict[str, list[EntityTag]]:
        """Batch-fetch tags for multiple entity_ids. Returns dict keyed by entity_id.
        entity_ids absent from the result had no tags. Chunks to avoid SQLite param limits."""
        ...

    def bulk_add_tags(
        self,
        tenant_id: str,
        items: list[dict[str, Any]],
        override_existing: bool,
        created_by: str,
    ) -> tuple[int, int, int]:
        """Create/update tags in bulk. Returns (created_count, updated_count, skipped_count)."""
        ...

    def get_distinct_keys(
        self,
        tenant_id: str,
        entity_type: str | None = None,
    ) -> list[str]:
        """Return alphabetically sorted distinct tag_key values for the tenant.
        Optionally filtered by entity_type."""
        ...

    def get_distinct_values(
        self,
        tenant_id: str,
        tag_key: str,
        entity_type: str | None = None,
        q: str | None = None,
    ) -> list[str]:
        """Return alphabetically sorted distinct tag_value values for the given key.
        Optional entity_type filter. Optional case-insensitive prefix filter via q."""
        ...


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
class ReadOnlyUnitOfWork(Protocol):
    """Read-only transaction coordinator. No commit/rollback."""

    resources: ResourceRepository
    identities: IdentityRepository
    billing: BillingRepository
    chargebacks: ChargebackRepository
    pipeline_state: PipelineStateRepository
    pipeline_runs: PipelineRunRepository
    tags: EntityTagRepository
    emissions: EmissionRepository  # NEW
    topic_attributions: TopicAttributionRepository  # lazy; only active when TA enabled

    def __enter__(self) -> Self: ...
    def __exit__(self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: object) -> None: ...


@runtime_checkable
class UnitOfWork(ReadOnlyUnitOfWork, Protocol):
    """Transaction coordinator with commit/rollback."""

    def commit(self) -> None: ...
    def rollback(self) -> None: ...


@runtime_checkable
class StorageBackend(Protocol):
    """Factory for UnitOfWork instances. Owns engine lifecycle."""

    def create_unit_of_work(self) -> UnitOfWork: ...
    def create_read_only_unit_of_work(self) -> ReadOnlyUnitOfWork: ...
    def create_tables(self) -> None: ...
    def dispose(self) -> None: ...
