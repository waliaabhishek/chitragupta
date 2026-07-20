from __future__ import annotations

import calendar
import logging
import time
import uuid
from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from datetime import date as date_type
from decimal import Decimal
from inspect import getattr_static
from typing import TYPE_CHECKING, Any, Protocol, cast

from core.engine.allocation import AllocationContext, AllocatorRegistry
from core.engine.allocation_lineage import build_allocation_lineage_capture
from core.engine.helpers import compute_active_fraction
from core.engine.loading import load_protocol_callable
from core.models.chargeback import ChargebackRow, CostType
from core.models.identity import SENTINEL_IDENTITY_TYPES, CoreIdentity, IdentityResolution, IdentitySet
from core.models.pipeline import PipelineState
from core.plugin.protocols import OverlayPlugin, SupplementalResourceGatherer, TopicDiscoveryPlugin
from core.plugin.registry import EcosystemBundle
from core.storage.interface import (
    AllocationLineageRepository,
    AllocationLineageRunCapture,
)

if TYPE_CHECKING:
    from core.config.models import PluginSettingsBase, TenantConfig
    from core.engine.topic_attribution import TopicAttributionPhase
    from core.metrics.protocol import MetricsSource
    from core.models.billing import BillingLineItem
    from core.models.metrics import MetricQuery, MetricRow
    from core.models.resource import Resource
    from core.plugin.protocols import CostAllocator, EcosystemPlugin, OverlayConfig, ResolveContext, ServiceHandler
    from core.storage.interface import ResourceRepository, StorageBackend, UnitOfWork

    class _EntityRepo(Protocol):
        """Structural minimum for deletion detection — covers ResourceRepository and IdentityRepository."""

        def find_active_at(
            self, ecosystem: str, tenant_id: str, timestamp: datetime, *, count: bool = True
        ) -> tuple[Sequence[Any], int]: ...

        def mark_deleted(self, ecosystem: str, tenant_id: str, entity_id: str, deleted_at: datetime) -> None: ...


def _get_ta_config(plugin: EcosystemPlugin) -> OverlayConfig | None:
    """Return topic attribution overlay config from the plugin."""
    if isinstance(plugin, OverlayPlugin):
        return plugin.get_overlay_config("topic_attribution")
    return None


logger = logging.getLogger(__name__)


def _new_calculation_id() -> str:
    return str(uuid.uuid4())


def _calculation_utc_now() -> datetime:
    return datetime.now(UTC)


def _allocation_lineage_repository(value: object) -> AllocationLineageRepository | None:
    """Resolve the optional writer without triggering dynamic mock attributes."""
    try:
        getattr_static(value, "replace_calculation_lineage")
    except AttributeError:
        return None
    writer = getattr(value, "replace_calculation_lineage", None)
    return cast("AllocationLineageRepository", value) if callable(writer) else None


class GatherFailureThresholdError(Exception):
    """Raised when consecutive gather failures exceed threshold."""


_DEFAULT_GRANULARITY_DURATION: dict[str, timedelta] = {
    "hourly": timedelta(hours=1),
    "daily": timedelta(hours=24),
}


def billing_window(
    line: BillingLineItem,
    durations: dict[str, timedelta] | None = None,
) -> tuple[datetime, datetime, timedelta]:
    """Derive (start, end, duration) from billing line's timestamp + granularity.

    Args:
        line: The billing line item.
        durations: Complete granularity→timedelta mapping to use. Callers are
            responsible for merging built-in defaults with any plugin-supplied
            entries before passing. If None or empty, falls back to
            ``_DEFAULT_GRANULARITY_DURATION``.
    """
    durations = durations if durations else _DEFAULT_GRANULARITY_DURATION

    if line.granularity == "monthly":
        year, month = line.timestamp.year, line.timestamp.month
        _, days_in_month = calendar.monthrange(year, month)
        duration = timedelta(days=days_in_month)
    elif line.granularity in durations:
        duration = durations[line.granularity]
    else:
        raise ValueError(f"Unknown billing granularity: {line.granularity!r}")
    return line.timestamp, line.timestamp + duration, duration


def _ensure_utc(dt: datetime) -> datetime:
    """Validate datetime is UTC-aware. Convert if timezone-aware but not UTC."""
    if dt.tzinfo is None:
        raise ValueError(f"Naive datetime not allowed — must be UTC-aware: {dt}")
    return dt.astimezone(UTC)


@dataclass
class PipelineRunResult:
    tenant_name: str
    tenant_id: str
    dates_gathered: int
    dates_calculated: int
    chargeback_rows_written: int
    dates_pending_calculation: int = 0
    errors: list[str] = field(default_factory=list)
    already_running: bool = False
    fatal: bool = False  # True when tenant is permanently failed


@dataclass
class GatherResult:
    """Result from a single GatherPhase.run() call."""

    dates_gathered: int
    errors: list[str]
    skipped: bool = False  # True when throttled — no gather performed


class RetryChecker(Protocol):
    """DIP boundary — CalculatePhase depends on this, not on RetryManager directly."""

    def increment_and_check(self, line: BillingLineItem) -> tuple[int, bool]: ...


class RetryManager:
    """Persists per-line retry counters and determines fallback behavior.

    Opens a separate UoW (committed immediately) so the counter survives
    the caller's UoW rollback on allocation failure.

    increment_fn: called inside the UoW to increment and return the new counter.
    Defaults to allocation_attempts for backward compatibility.
    """

    def __init__(
        self,
        storage_backend: StorageBackend,
        limit: int,
        increment_fn: Callable[[UnitOfWork, BillingLineItem], int] | None = None,
    ) -> None:
        self._storage_backend = storage_backend
        self._limit = limit
        self._increment_fn: Callable[[UnitOfWork, BillingLineItem], int] = (
            increment_fn
            if increment_fn is not None
            else lambda uow, line: uow.billing.increment_allocation_attempts(line)
        )

    def increment_and_check(self, line: BillingLineItem) -> tuple[int, bool]:
        """Increment attempt counter. Returns (new_attempts, should_fallback)."""
        with self._storage_backend.create_unit_of_work() as uow:
            new_attempts = self._increment_fn(uow, line)
            uow.commit()
        return new_attempts, new_attempts >= self._limit


class GatherPhase:
    """Handles resource/identity/billing gather and deletion detection for one tenant."""

    def __init__(
        self,
        ecosystem: str,
        tenant_id: str,
        tenant_config: TenantConfig,
        bundle: EcosystemBundle,
        min_refresh_gap: timedelta = timedelta(seconds=0),
        uow_storage: Any = None,
        gather_failure_threshold: int = 3,
    ) -> None:
        self._ecosystem = ecosystem
        self._tenant_id = tenant_id
        self._tenant_config = tenant_config
        self._bundle = bundle
        self._min_refresh_gap = min_refresh_gap
        self._uow_storage = uow_storage
        self._gather_failure_threshold = gather_failure_threshold
        # In-memory state — must survive across run() calls on same instance
        self._last_resource_gather_at: datetime | None = None
        self._zero_gather_counters: dict[str, int] = {"resources": 0, "identities": 0}
        ta_config = _get_ta_config(bundle.plugin)
        self._topic_attribution_enabled: bool = bool(ta_config and ta_config.enabled)

    def run(self, uow: UnitOfWork | None = None) -> GatherResult:
        """Execute full gather cycle.

        When called without uow (new path): runs Phase 1 (build_shared_context) +
        Phase 2 (handler loop with shared_ctx). Phase 1 failure is fatal.
        When called with uow (existing path): full gather cycle with deletion
        detection and billing. Caller owns UoW lifecycle (open + commit).
        """
        if uow is None:
            return self._run_gather_only()
        return self._run_full(uow)

    def _run_gather_only(self) -> GatherResult:
        """Phase 1 + Phase 2 gather only. Used when run() called without uow.

        Phase 1 failure (build_shared_context raising) is fatal to the entire
        gather cycle — if environments/clusters cannot be fetched, all downstream
        handlers produce empty results anyway. The exception propagates to caller.
        """
        gather_errors: list[str] = []

        # Phase 1: Build shared gather context (plugin-level, once per cycle).
        # Fatal if raises — propagates to caller.
        shared_ctx = self._bundle.plugin.build_shared_context(self._tenant_id)

        # Phase 2: Gather resources and identities from each handler.
        if self._uow_storage is not None:
            with self._uow_storage as uow:
                for handler in self._bundle.handlers.values():
                    try:
                        self._gather_resources_and_identities(handler, uow, shared_ctx)
                    except Exception as exc:
                        logger.exception(
                            "Handler %s gather failed — skipping deletion detection: %s",
                            handler.service_type,
                            exc,
                        )
                        gather_errors.append(f"Handler {handler.service_type} gather failed: {exc}")
                self._run_supplemental_gather(uow, datetime.now(UTC), gather_errors)

        return GatherResult(dates_gathered=0, errors=gather_errors)

    def _run_full(self, uow: UnitOfWork) -> GatherResult:
        """Full gather cycle with deletion detection and billing. Caller owns UoW."""
        now = datetime.now(UTC)

        if not self._should_refresh(now):
            assert self._last_resource_gather_at is not None  # guaranteed by _should_refresh logic
            logger.debug(
                "Skipping resource/billing refresh — last gather was %s ago",
                now - self._last_resource_gather_at,
            )
            return GatherResult(dates_gathered=0, errors=[], skipped=True)

        handlers = tuple(self._bundle.handlers.items())
        declared_handlers_by_type: dict[str, set[str]] = {}
        successful_handlers_by_type: dict[str, set[str]] = {}
        observed_declared_resource_ids_by_type: dict[str, set[str]] = {}
        for handler_name, handler in handlers:
            for resource_type in dict.fromkeys(handler.gathered_resource_types):
                declaring_handlers = declared_handlers_by_type.get(resource_type)
                if declaring_handlers is None:
                    declaring_handlers = set()
                    declared_handlers_by_type[resource_type] = declaring_handlers
                    successful_handlers_by_type[resource_type] = set()
                    observed_declared_resource_ids_by_type[resource_type] = set()
                declaring_handlers.add(handler_name)
        all_gathered_identity_ids: set[str] = set()
        gather_complete = True
        gather_errors: list[str] = []

        # Phase 1: Build shared context once for all handlers.
        shared_ctx = self._bundle.plugin.build_shared_context(self._tenant_id)

        for handler_name, handler in handlers:
            try:
                handler_ids_by_type, i_ids = self._gather_resources_and_identities(handler, uow, shared_ctx)
                all_gathered_identity_ids.update(i_ids)
                for resource_type in dict.fromkeys(handler.gathered_resource_types):
                    successful_handlers_by_type[resource_type].add(handler_name)
                    resource_ids = handler_ids_by_type.get(resource_type)
                    if resource_ids is not None:
                        observed_declared_resource_ids_by_type[resource_type].update(resource_ids)
            except Exception as exc:
                logger.exception("Handler %s gather failed: %s", handler.service_type, exc)
                gather_complete = False
                gather_errors.append(f"Handler {handler.service_type} gather failed: {exc}")

        resource_ids_by_type = {
            resource_type: observed_declared_resource_ids_by_type[resource_type]
            for resource_type, declaring_handlers in declared_handlers_by_type.items()
            if successful_handlers_by_type[resource_type] == declaring_handlers
        }

        # Supplemental inventory is isolated from ordinary handler completion,
        # billing readiness, and billing-resource deletion detection.
        self._run_supplemental_gather(uow, now, gather_errors)

        excluded_resource_types = (
            self._bundle.plugin.supplemental_resource_types
            if isinstance(self._bundle.plugin, SupplementalResourceGatherer)
            else ()
        )
        if resource_ids_by_type:
            for resource_type in sorted(resource_ids_by_type):
                self._detect_resource_deletions(
                    uow.resources,
                    resource_ids_by_type[resource_type],
                    now,
                    (resource_type,),
                    excluded_resource_types,
                    counter_name=f"resources:{resource_type}",
                )
            self._zero_gather_counters["resources"] = max(
                (count for name, count in self._zero_gather_counters.items() if name.startswith("resources:")),
                default=0,
            )
        if gather_complete:
            self._detect_deletions(
                uow,
                now,
                set(),
                all_gathered_identity_ids,
                (),
            )
        else:
            logger.warning("Skipping identity deletion detection — incomplete gather for %s", self._tenant_id)

        gathered_billing_dates = self._gather_billing(uow, now)

        for billing_date in gathered_billing_dates:
            _ensure_pipeline_state(uow, self._ecosystem, self._tenant_id, billing_date)
            uow.pipeline_state.mark_billing_gathered(self._ecosystem, self._tenant_id, billing_date)
            if gather_complete:
                uow.pipeline_state.mark_resources_gathered(self._ecosystem, self._tenant_id, billing_date)

        if (
            self._topic_attribution_enabled
            and gathered_billing_dates
            and isinstance(self._bundle.plugin, TopicDiscoveryPlugin)
        ):
            cluster_ids = [r.resource_id for r in (getattr(shared_ctx, "kafka_cluster_resources", None) or [])]
            try:
                topic_resources = list(self._bundle.plugin.gather_topic_resources(self._tenant_id, cluster_ids))
                for resource in topic_resources:
                    uow.resources.upsert(resource)
                for billing_date in gathered_billing_dates:
                    uow.pipeline_state.mark_topic_overlay_gathered(
                        self._ecosystem,
                        self._tenant_id,
                        billing_date,
                    )
            except Exception:
                logger.warning(
                    "Topic discovery failed for tenant=%s — topic_overlay_gathered stays False",
                    self._tenant_id,
                    exc_info=True,
                )

        self._apply_recalculation_window(uow, gathered_billing_dates, now)
        self._last_resource_gather_at = now

        return GatherResult(dates_gathered=len(gathered_billing_dates), errors=gather_errors)

    def _should_refresh(self, now: datetime) -> bool:
        return self._last_resource_gather_at is None or (now - self._last_resource_gather_at) >= self._min_refresh_gap

    def _gather_resources_and_identities(
        self, handler: ServiceHandler, uow: UnitOfWork, shared_ctx: object | None = None
    ) -> tuple[dict[str, set[str]], set[str]]:
        gathered_resource_ids_by_type: dict[str, set[str]] = {}
        gathered_identity_ids: set[str] = set()
        for resource in handler.gather_resources(self._tenant_id, uow, shared_ctx):
            if resource.created_at is not None:
                resource = replace(resource, created_at=_ensure_utc(resource.created_at))  # type: ignore[type-var]  # runtime objects are dataclasses behind Resource Protocol
            uow.resources.upsert(resource)
            resource_ids = gathered_resource_ids_by_type.get(resource.resource_type)
            if resource_ids is None:
                resource_ids = set()
                gathered_resource_ids_by_type[resource.resource_type] = resource_ids
            resource_ids.add(resource.resource_id)
        for identity in handler.gather_identities(self._tenant_id, uow):
            if identity.created_at is not None:
                identity = replace(identity, created_at=_ensure_utc(identity.created_at))  # type: ignore[type-var]  # runtime objects are dataclasses behind Identity Protocol
            uow.identities.upsert(identity)
            gathered_identity_ids.add(identity.identity_id)
        return gathered_resource_ids_by_type, gathered_identity_ids

    def _run_supplemental_gather(
        self,
        uow: UnitOfWork,
        now: datetime,
        gather_errors: list[str],
    ) -> None:
        plugin = self._bundle.plugin
        if not isinstance(plugin, SupplementalResourceGatherer):
            return
        for resource_type in plugin.supplemental_resource_types:
            try:
                resources = list(plugin.gather_supplemental_resources(self._tenant_id, resource_type, uow))
                if any(resource.resource_type != resource_type for resource in resources):
                    raise ValueError(f"supplemental {resource_type} gather returned another resource type")
                if resource_type == "organization":
                    self._reconcile_organization_resources(uow, resources, now)
                else:
                    for resource in resources:
                        uow.resources.upsert(resource)
                    observed_ids = {resource.resource_id for resource in resources}
                    active, _ = uow.resources.find_active_at(
                        self._ecosystem,
                        self._tenant_id,
                        now,
                        resource_type=resource_type,
                        count=False,
                    )
                    for existing in active:
                        if existing.resource_type == resource_type and existing.resource_id not in observed_ids:
                            uow.resources.mark_deleted(
                                self._ecosystem,
                                self._tenant_id,
                                existing.resource_id,
                                now,
                            )
            except Exception as exc:
                logger.exception("Supplemental %s gather failed: %s", resource_type, exc)
                gather_errors.append(f"Supplemental {resource_type} gather failed: {exc}")

    def _reconcile_organization_resources(
        self,
        uow: UnitOfWork,
        observed: Sequence[Resource],
        now: datetime,
    ) -> None:
        """Persist one immutable provider organization binding per tenant partition."""
        active, _ = uow.resources.find_active_at(
            self._ecosystem,
            self._tenant_id,
            now,
            resource_type="organization",
            count=False,
        )
        active_organizations = [resource for resource in active if resource.resource_type == "organization"]
        bound = [
            resource
            for resource in active_organizations
            if resource.metadata.get("organization_binding_state") == "bound"
        ]
        if len(bound) > 1:
            raise ValueError("multiple provider organization bindings are active")
        observed_by_id = {resource.resource_id: resource for resource in observed if resource.resource_id.strip()}
        bound_id = bound[0].resource_id if bound else None
        if len(observed) != 1 or len(observed_by_id) != 1:
            for resource in observed_by_id.values():
                state = "bound" if resource.resource_id == bound_id else "conflicting_observation"
                uow.resources.upsert(
                    replace(  # type: ignore[type-var]  # runtime Resource implementations are dataclasses
                        resource,
                        metadata={**resource.metadata, "organization_binding_state": state},
                    )
                )
            raise ValueError("provider organization acquisition must return exactly one nonblank ID")
        observed_id, resource = next(iter(observed_by_id.items()))
        if bound_id is not None and observed_id != bound_id:
            uow.resources.upsert(
                replace(  # type: ignore[type-var]  # runtime Resource implementations are dataclasses
                    resource, metadata={**resource.metadata, "organization_binding_state": "conflicting_observation"}
                )
            )
            raise ValueError("provider organization observation conflicts with the immutable binding")
        uow.resources.upsert(
            replace(  # type: ignore[type-var]  # runtime Resource implementations are dataclasses
                resource,
                metadata={**resource.metadata, "organization_binding_state": "bound"},
            )
        )
        for existing in active_organizations:
            if existing.resource_id != observed_id:
                uow.resources.mark_deleted(self._ecosystem, self._tenant_id, existing.resource_id, now)

    def _run_deletion_scan(
        self,
        active_entities: Sequence[Any],
        gathered_ids: set[str],
        entity_name: str,
        now: datetime,
        id_getter: Callable[[Any], str],
        mark_fn: Callable[[str, datetime], None],
    ) -> None:
        """Shared deletion logic: zero-gather counter, threshold checks, mark-deleted loop.

        Callers pre-fetch active_entities using their own typed find_active_at call,
        then pass the results here along with id_getter and mark_fn closures.
        """
        threshold = self._tenant_config.zero_gather_deletion_threshold
        self._zero_gather_counters.setdefault(entity_name, 0)
        if not gathered_ids and active_entities:
            self._zero_gather_counters[entity_name] += 1
            consecutive = self._zero_gather_counters[entity_name]
            if threshold == -1 or consecutive < threshold:
                logger.warning(
                    "Zero %s gathered but %d active — skipping %s deletion (consecutive: %d)",
                    entity_name,
                    len(active_entities),
                    entity_name,
                    consecutive,
                )
            else:
                logger.warning(
                    "Zero %s gathered for %d consecutive runs — proceeding with deletion",
                    entity_name,
                    consecutive,
                )
                for entity in active_entities:
                    entity_id = id_getter(entity)
                    if entity_id not in gathered_ids:
                        mark_fn(entity_id, now)
                self._zero_gather_counters[entity_name] = 0
        else:
            self._zero_gather_counters[entity_name] = 0
            for entity in active_entities:
                entity_id = id_getter(entity)
                if entity_id not in gathered_ids:
                    mark_fn(entity_id, now)

    def _detect_resource_deletions(
        self,
        repo: ResourceRepository,
        gathered_ids: set[str],
        now: datetime,
        resource_types: Sequence[str],
        excluded_resource_types: Sequence[str] = (),
        counter_name: str = "resources",
    ) -> None:
        """Deletion detection scoped to billing-relevant resource types.

        Uses ResourceRepository directly (not _EntityRepo) to pass the mandatory
        resource_type parameter type-safely. Delegates shared logic to _run_deletion_scan.
        """
        active_resources, _ = repo.find_active_at(
            self._ecosystem,
            self._tenant_id,
            now,
            resource_type=resource_types,
            count=False,
        )
        if excluded_resource_types:
            active_resources = [
                resource
                for resource in active_resources
                if resource.resource_type not in excluded_resource_types
                and (not resource_types or resource.resource_type in resource_types)
            ]
        self._run_deletion_scan(
            active_resources,
            gathered_ids,
            counter_name,
            now,
            id_getter=lambda r: r.resource_id,
            mark_fn=lambda rid, ts: repo.mark_deleted(self._ecosystem, self._tenant_id, rid, ts),
        )

    def _detect_entity_deletions(
        self,
        repo: _EntityRepo,
        gathered_ids: set[str],
        entity_name: str,
        id_getter: Callable[[Any], str],
        now: datetime,
    ) -> None:
        active_entities, _ = repo.find_active_at(self._ecosystem, self._tenant_id, now, count=False)
        self._run_deletion_scan(
            active_entities,
            gathered_ids,
            entity_name,
            now,
            id_getter=id_getter,
            mark_fn=lambda eid, ts: repo.mark_deleted(self._ecosystem, self._tenant_id, eid, ts),
        )

    def _detect_deletions(
        self,
        uow: UnitOfWork,
        now: datetime,
        gathered_resource_ids: set[str],
        gathered_identity_ids: set[str],
        gathered_resource_types: Sequence[str],
        excluded_resource_types: Sequence[str] = (),
    ) -> None:
        if gathered_resource_types:
            self._detect_resource_deletions(
                uow.resources,
                gathered_resource_ids,
                now,
                gathered_resource_types,
                excluded_resource_types,
            )
        self._detect_entity_deletions(uow.identities, gathered_identity_ids, "identities", lambda i: i.identity_id, now)

    def _gather_billing(self, uow: UnitOfWork, now: datetime) -> set[date_type]:
        start = now - timedelta(days=self._tenant_config.lookback_days)
        end = now - timedelta(days=self._tenant_config.cutoff_days)
        cost_input = self._bundle.plugin.get_cost_input()
        gathered: set[date_type] = set()
        for line in cost_input.gather(self._tenant_id, start, end, uow):
            line = replace(line, timestamp=_ensure_utc(line.timestamp))  # type: ignore[type-var]  # runtime objects are dataclasses behind BillingLineItem Protocol
            uow.billing.upsert(line)
            gathered.add(line.timestamp.date())
        return gathered

    def _apply_recalculation_window(
        self, uow: UnitOfWork, gathered_billing_dates: set[date_type], now: datetime
    ) -> None:
        recalc_cutoff = (now - timedelta(days=self._tenant_config.cutoff_days)).date()
        for billing_date in gathered_billing_dates:
            if billing_date >= recalc_cutoff:
                existing_state = uow.pipeline_state.get(self._ecosystem, self._tenant_id, billing_date)
                if existing_state and existing_state.chargeback_calculated:
                    uow.chargebacks.delete_by_date(self._ecosystem, self._tenant_id, billing_date)
                    uow.topic_attributions.delete_by_date(self._ecosystem, self._tenant_id, billing_date)
                    uow.pipeline_state.mark_needs_recalculation(self._ecosystem, self._tenant_id, billing_date)
                    uow.billing.reset_allocation_attempts_by_date(self._ecosystem, self._tenant_id, billing_date)
                    uow.billing.reset_topic_attribution_attempts_by_date(self._ecosystem, self._tenant_id, billing_date)
                    logger.info("Date %s within recalculation window — will recompute", billing_date)


class CalculatePhase:
    """Handles metrics prefetch, identity resolution, and per-line allocation for one tenant."""

    def __init__(
        self,
        ecosystem: str,
        tenant_id: str,
        bundle: EcosystemBundle,
        retry_checker: RetryChecker,
        metrics_source: MetricsSource | None,
        allocator_registry: AllocatorRegistry,
        identity_overrides: dict[str, Callable[..., IdentityResolution]],
        allocator_params: dict[str, float | int | str | bool],
        metrics_step: timedelta,
        extra_granularity_durations: dict[str, timedelta] | None = None,
        metrics_prefetch_workers: int = 4,
        *,
        calculation_id_factory: Callable[[], str] = _new_calculation_id,
        calculation_clock: Callable[[], datetime] = _calculation_utc_now,
    ) -> None:
        self._ecosystem = ecosystem
        self._tenant_id = tenant_id
        self._bundle = bundle
        self._retry_checker = retry_checker
        self._metrics_source = metrics_source
        self._allocator_registry = allocator_registry
        self._identity_overrides = identity_overrides
        self._allocator_params = allocator_params
        self._metrics_step = metrics_step
        self._metrics_prefetch_workers = metrics_prefetch_workers
        self._calculation_id_factory = calculation_id_factory
        self._calculation_clock = calculation_clock
        self._merged_granularity_durations: dict[str, timedelta] = {
            **_DEFAULT_GRANULARITY_DURATION,
            **(extra_granularity_durations or {}),
        }

    def run(
        self,
        uow: UnitOfWork,
        tracking_date: date_type,
        *,
        calculation_run_id: int | None = None,
    ) -> int:
        """Calculate chargebacks for a single date. Returns rows written."""
        calculation_id = self._calculation_id_factory()
        if not calculation_id:
            raise ValueError("calculation_id must not be empty")
        lineage_repo = _allocation_lineage_repository(getattr(uow, "chargebacks", None))
        billing_lines = uow.billing.find_by_date(self._ecosystem, self._tenant_id, tracking_date)

        if not billing_lines:
            completed_at = self._completion_time()
            if lineage_repo is not None:
                lineage_repo.replace_calculation_lineage(
                    AllocationLineageRunCapture(
                        ecosystem=self._ecosystem,
                        tenant_id=self._tenant_id,
                        tracking_date=tracking_date,
                        calculation_id=calculation_id,
                        captures=(),
                    ),
                    calculation_completed_at=completed_at,
                )
            self._mark_success(
                uow,
                tracking_date,
                calculation_run_id,
                calculation_id=calculation_id,
                completed_at=completed_at,
            )
            return 0

        line_window_cache = self._compute_line_window_cache(billing_lines)
        billing_windows = self._compute_billing_windows(billing_lines, line_window_cache)
        prefetched_metrics, failed_metric_keys = self._prefetch_metrics(billing_lines, line_window_cache)
        tenant_period_cache = self._build_tenant_period_cache(uow, billing_windows)
        resource_cache = self._build_resource_cache(uow, billing_windows)

        all_rows: list[ChargebackRow] = []
        lineage_captures = []
        for line in billing_lines:
            rows = self._collect_billing_line_rows(
                line,
                uow,
                prefetched_metrics,
                failed_metric_keys,
                tenant_period_cache,
                resource_cache,
                line_window_cache,
            )
            all_rows.extend(rows)
            if lineage_repo is not None:
                lineage_captures.append(build_allocation_lineage_capture(origin=line, rows=tuple(rows)))

        total_rows = uow.chargebacks.upsert_batch(all_rows)
        completed_at = self._completion_time()
        if lineage_repo is not None:
            lineage_repo.replace_calculation_lineage(
                AllocationLineageRunCapture(
                    ecosystem=self._ecosystem,
                    tenant_id=self._tenant_id,
                    tracking_date=tracking_date,
                    calculation_id=calculation_id,
                    captures=tuple(lineage_captures),
                ),
                calculation_completed_at=completed_at,
            )
        self._mark_success(
            uow,
            tracking_date,
            calculation_run_id,
            calculation_id=calculation_id,
            completed_at=completed_at,
        )
        return total_rows

    def _completion_time(self) -> datetime:
        completed_at = self._calculation_clock()
        if completed_at.tzinfo is None or completed_at.utcoffset() is None:
            raise ValueError("calculation completion time must be timezone-aware")
        return completed_at.astimezone(UTC)

    def _mark_success(
        self,
        uow: UnitOfWork,
        tracking_date: date_type,
        calculation_run_id: int | None,
        *,
        calculation_id: str,
        completed_at: datetime,
    ) -> None:
        uow.pipeline_state.mark_chargeback_calculated(
            self._ecosystem,
            self._tenant_id,
            tracking_date,
            calculation_id=calculation_id,
            calculation_completed_at=completed_at,
            calculation_run_id=calculation_run_id,
        )

    def _compute_line_window_cache(
        self, billing_lines: list[BillingLineItem]
    ) -> dict[int, tuple[datetime, datetime, timedelta]]:
        """Compute billing_window() once per line. Keyed by id(line)."""
        return {id(line): billing_window(line, self._merged_granularity_durations) for line in billing_lines}

    def _prefetch_metrics(
        self,
        billing_lines: list[BillingLineItem],
        line_window_cache: dict[int, tuple[datetime, datetime, timedelta]],
    ) -> tuple[
        dict[tuple[str, datetime, datetime], dict[str, list[MetricRow]]],
        frozenset[tuple[str, datetime, datetime]],
    ]:
        metrics_groups: dict[tuple[str, datetime, datetime], list[MetricQuery]] = {}
        for line in billing_lines:
            b_start, b_end, _ = line_window_cache[id(line)]
            handler = self._bundle.product_type_to_handler.get(line.product_type)
            if handler:
                metrics_needed = handler.get_metrics_for_product_type(line.product_type)
                if metrics_needed:
                    group_key = (line.resource_id, b_start, b_end)
                    existing = metrics_groups.get(group_key, [])
                    seen_keys = {m.key for m in existing}
                    for query in metrics_needed:
                        if query.key not in seen_keys:
                            existing.append(query)
                            seen_keys.add(query.key)
                    metrics_groups[group_key] = existing

        prefetched: dict[tuple[str, datetime, datetime], dict[str, list[MetricRow]]] = {}
        failed_keys: set[tuple[str, datetime, datetime]] = set()
        if not self._metrics_source:
            return prefetched, frozenset()
        if not metrics_groups:
            return prefetched, frozenset()

        def _fetch_group(
            key: tuple[str, datetime, datetime],
            queries: list[MetricQuery],
        ) -> tuple[tuple[str, datetime, datetime], dict[str, list[MetricRow]]]:
            resource_id, m_start, m_end = key
            result = self._metrics_source.query(  # type: ignore[union-attr]  # non-None: guarded by early-return above
                queries,
                start=m_start,
                end=m_end,
                step=self._metrics_step,
                resource_id_filter=resource_id,
            )
            return key, result

        n_workers = min(self._metrics_prefetch_workers, len(metrics_groups))
        with ThreadPoolExecutor(max_workers=n_workers) as executor:
            future_map = {executor.submit(_fetch_group, key, queries): key for key, queries in metrics_groups.items()}
            for future in as_completed(future_map):
                key = future_map[future]
                try:
                    _, result = future.result()
                    prefetched[key] = result
                except Exception:
                    resource_id, m_start, m_end = key
                    logger.warning(
                        "Metrics prefetch failed for resource=%s window=[%s, %s] — skipping",
                        resource_id,
                        m_start,
                        m_end,
                        exc_info=True,
                    )
                    prefetched[key] = {}
                    failed_keys.add(key)

        return prefetched, frozenset(failed_keys)

    def _compute_billing_windows(
        self,
        billing_lines: list[BillingLineItem],
        line_window_cache: dict[int, tuple[datetime, datetime, timedelta]],
    ) -> set[tuple[datetime, datetime]]:
        windows: set[tuple[datetime, datetime]] = set()
        for line in billing_lines:
            b_start, b_end, _ = line_window_cache[id(line)]
            windows.add((b_start, b_end))
        return windows

    def _build_tenant_period_cache(
        self, uow: UnitOfWork, billing_windows: set[tuple[datetime, datetime]]
    ) -> dict[tuple[datetime, datetime], IdentitySet]:
        cache: dict[tuple[datetime, datetime], IdentitySet] = {}
        for b_start, b_end in billing_windows:
            identities, _ = uow.identities.find_by_period(self._ecosystem, self._tenant_id, b_start, b_end, count=False)
            tp = IdentitySet()
            for identity in identities:
                if identity.identity_type not in SENTINEL_IDENTITY_TYPES:
                    tp.add(identity)
            cache[(b_start, b_end)] = tp
        return cache

    def _build_resource_cache(
        self, uow: UnitOfWork, billing_windows: set[tuple[datetime, datetime]]
    ) -> dict[tuple[datetime, datetime], dict[str, Resource]]:
        cache: dict[tuple[datetime, datetime], dict[str, Resource]] = {}
        billing_types = self._bundle.billing_resource_types
        for b_start, b_end in billing_windows:
            resources, _ = uow.resources.find_by_period(
                self._ecosystem,
                self._tenant_id,
                b_start,
                b_end,
                resource_type=billing_types,
                count=False,
            )
            cache[(b_start, b_end)] = {r.resource_id: r for r in resources}
        return cache

    def _collect_billing_line_rows(
        self,
        line: BillingLineItem,
        uow: UnitOfWork,
        prefetched_metrics: dict[tuple[str, datetime, datetime], dict[str, list[MetricRow]]],
        failed_metric_keys: frozenset[tuple[str, datetime, datetime]],
        tenant_period_cache: dict[tuple[datetime, datetime], IdentitySet],
        resource_cache: dict[tuple[datetime, datetime], dict[str, Resource]],
        line_window_cache: dict[int, tuple[datetime, datetime, timedelta]],
    ) -> list[ChargebackRow]:
        # Extract plugin-specific dimension metadata from the billing line.
        # env_id is present on CCloudBillingLineItem; absent on core BillingLineItem.
        # Must be before the try block so the except handler can reference it safely.
        dimension_metadata: dict[str, Any] = {}
        env_id = getattr(line, "env_id", None)
        if env_id is not None:
            dimension_metadata["env_id"] = env_id

        try:
            b_start, b_end, b_duration = line_window_cache[id(line)]

            handler = self._bundle.product_type_to_handler.get(line.product_type)
            if handler is None:
                if self._bundle.fallback_allocator is None:
                    logger.warning(
                        "No handler and no fallback_allocator for product_type %s — skipping",
                        line.product_type,
                    )
                    return []
                ctx = AllocationContext(
                    timeslice=b_start,
                    billing_line=line,
                    identities=IdentityResolution(
                        resource_active=IdentitySet(),
                        metrics_derived=IdentitySet(),
                        tenant_period=IdentitySet(),
                    ),
                    split_amount=line.total_cost,
                    metrics_data=None,
                    params=self._allocator_params,
                    dimension_metadata=dimension_metadata,
                )
                result = self._bundle.fallback_allocator(ctx)
                return list(result.rows)

            metrics_data = prefetched_metrics.get((line.resource_id, b_start, b_end))
            metrics_fetch_failed = (line.resource_id, b_start, b_end) in failed_metric_keys
            window_resources = resource_cache.get((b_start, b_end), {})
            resource = window_resources.get(line.resource_id)
            active_fraction = Decimal(1) if resource is None else compute_active_fraction(resource, b_start, b_end)
            split_amount = line.total_cost * active_fraction

            if handler.service_type in self._identity_overrides:
                identity_resolution = self._identity_overrides[handler.service_type](
                    self._tenant_id, line.resource_id, b_start, b_duration, metrics_data, uow
                )
            else:
                resolve_context: ResolveContext = {
                    "cached_identities": tenant_period_cache.get((b_start, b_end), IdentitySet()),
                    "cached_resources": window_resources,
                }
                identity_resolution = handler.resolve_identities(
                    self._tenant_id,
                    line.resource_id,
                    b_start,
                    b_duration,
                    metrics_data,
                    uow,
                    context=resolve_context,
                )

            if identity_resolution.tenant_period and len(identity_resolution.tenant_period) > 0:
                logger.warning(
                    "Handler %s returned non-empty tenant_period (%d identities) — "
                    "orchestrator will replace it with temporally-filtered set",
                    handler.service_type,
                    len(identity_resolution.tenant_period),
                )

            identity_resolution = IdentityResolution(
                resource_active=identity_resolution.resource_active,
                metrics_derived=identity_resolution.metrics_derived,
                tenant_period=tenant_period_cache[(b_start, b_end)],
                context=identity_resolution.context,
            )

            allocator = self._resolve_allocator(line.product_type, handler)
            ctx = AllocationContext(
                timeslice=b_start,
                billing_line=line,
                identities=identity_resolution,
                split_amount=split_amount,
                metrics_data=metrics_data,
                metrics_fetch_failed=metrics_fetch_failed,
                params=self._allocator_params,
                dimension_metadata=dimension_metadata,
            )
            result = allocator(ctx)

            return list(result.rows)

        except Exception as exc:
            try:
                new_attempts, should_fallback = self._retry_checker.increment_and_check(line)
            except Exception as retry_exc:
                logger.warning("Failed to persist retry counter: %s", retry_exc)
                raise exc from None

            if not should_fallback:
                logger.exception(
                    "Billing line %s/%s failed (attempt %d): %s — failing date",
                    line.resource_id,
                    line.product_type,
                    new_attempts,
                    exc,
                )
                raise

            logger.exception(
                "Billing line %s/%s failed after %d attempts: %s — allocating to UNALLOCATED",
                line.resource_id,
                line.product_type,
                new_attempts,
                exc,
            )
            row = self._allocate_to_unallocated(
                line, "ALLOCATION_FAILED", f"Failed after {new_attempts} attempts: {exc}", metadata=dimension_metadata
            )
            return [row]

    def _resolve_allocator(self, product_type: str, handler: ServiceHandler) -> CostAllocator:
        try:
            return self._allocator_registry.get(product_type)
        except KeyError:
            return handler.get_allocator(product_type)

    def _allocate_to_unallocated(
        self, line: BillingLineItem, reason: str, detail: str | None = None, metadata: dict[str, Any] | None = None
    ) -> ChargebackRow:
        return ChargebackRow(
            ecosystem=line.ecosystem,
            tenant_id=line.tenant_id,
            timestamp=line.timestamp,
            resource_id=line.resource_id,
            product_category=line.product_category,
            product_type=line.product_type,
            identity_id="UNALLOCATED",
            cost_type=CostType.SHARED,
            amount=line.total_cost,
            allocation_method=reason,
            allocation_detail=detail,
            metadata=metadata or {},
        )


class ChargebackOrchestrator:
    """Thin coordinator: runs gather -> calculate pipeline for one tenant."""

    def __init__(
        self,
        tenant_name: str,
        tenant_config: TenantConfig,
        plugin: EcosystemPlugin | None = None,
        storage_backend: StorageBackend | None = None,
        metrics_source: MetricsSource | None = None,
        shutdown_check: Callable[[], bool] | None = None,
        progress_callback: Callable[[str | None, date_type | None], None] | None = None,
        *,
        plugin_bundle: Any = None,
        ecosystem: str | None = None,  # ignored — derived from tenant_config
        tenant_id: str | None = None,  # ignored — derived from tenant_config
        metrics_step: timedelta | None = None,  # ignored — derived from _load_overrides
    ) -> None:
        self._tenant_name = tenant_name
        self._tenant_id = tenant_config.tenant_id
        self._ecosystem = tenant_config.ecosystem
        assert storage_backend is not None, "storage_backend is required"
        self._storage_backend = storage_backend
        self._tenant_config = tenant_config  # kept for backward compatibility
        self._shutdown_check = shutdown_check
        self._progress_callback = progress_callback

        if plugin_bundle is not None:
            bundle = plugin_bundle
        else:
            assert plugin is not None, "Either plugin or plugin_bundle must be provided"
            bundle = EcosystemBundle.build(plugin)
        (
            allocator_registry,
            identity_overrides,
            allocator_params,
            min_refresh_gap,
            metrics_step,
            extra_granularity_durations,
        ) = _load_overrides(tenant_config.plugin_settings)
        self._gather_phase = GatherPhase(
            ecosystem=self._ecosystem,
            tenant_id=self._tenant_id,
            tenant_config=tenant_config,
            bundle=bundle,
            min_refresh_gap=min_refresh_gap,
        )
        retry_checker = RetryManager(
            storage_backend=storage_backend,
            limit=tenant_config.allocation_retry_limit,
        )
        self._calculate_phase = CalculatePhase(
            ecosystem=self._ecosystem,
            tenant_id=self._tenant_id,
            bundle=bundle,
            retry_checker=retry_checker,
            metrics_source=metrics_source,
            allocator_registry=allocator_registry,
            identity_overrides=identity_overrides,
            allocator_params=allocator_params,
            metrics_step=metrics_step,
            extra_granularity_durations=extra_granularity_durations,
            metrics_prefetch_workers=tenant_config.metrics_prefetch_workers,
        )
        self._consecutive_gather_failures = 0
        self._gather_failure_threshold = tenant_config.gather_failure_threshold

        self._topic_overlay_phase: TopicAttributionPhase | None = None
        ta_config = _get_ta_config(bundle.plugin)
        if ta_config and ta_config.enabled:
            from core.engine.topic_attribution_models import TopicAttributionConfigProtocol

            if isinstance(ta_config, TopicAttributionConfigProtocol):
                from core.engine.topic_attribution import TopicAttributionPhase

                topic_retry_manager = RetryManager(
                    storage_backend=storage_backend,
                    limit=tenant_config.topic_attribution_retry_limit,
                    increment_fn=lambda uow, line: uow.billing.increment_topic_attribution_attempts(line),
                )
                self._topic_overlay_phase = TopicAttributionPhase(
                    ecosystem=self._ecosystem,
                    tenant_id=self._tenant_id,
                    metrics_source=metrics_source,
                    config=ta_config,
                    metrics_step=metrics_step,
                    retry_checker=topic_retry_manager,
                )

        with storage_backend.create_unit_of_work() as uow:
            _ensure_unallocated_identity(uow, self._ecosystem, self._tenant_id)
            uow.commit()

    # ------------------------------------------------------------------
    # Backward-compatibility delegation — pre-existing tests access these
    # on ChargebackOrchestrator directly; they now live in the phase objects.
    # ------------------------------------------------------------------

    @property
    def _bundle(self) -> EcosystemBundle:
        return self._gather_phase._bundle

    @property
    def _zero_gather_counters(self) -> dict[str, int]:
        return self._gather_phase._zero_gather_counters

    @property
    def _min_refresh_gap(self) -> timedelta:
        return self._gather_phase._min_refresh_gap

    @property
    def _metrics_step(self) -> timedelta:
        return self._calculate_phase._metrics_step

    def _detect_entity_deletions(self, *args: Any, **kwargs: Any) -> None:
        return self._gather_phase._detect_entity_deletions(*args, **kwargs)

    def _process_billing_line(
        self,
        line: BillingLineItem,
        uow: UnitOfWork,
        prefetched_metrics: dict[tuple[str, datetime, datetime], dict[str, list[MetricRow]]],
        tenant_period_cache: dict[tuple[datetime, datetime], IdentitySet],
        allocation_retry_limit: int,
        resource_cache: dict[tuple[datetime, datetime], dict[str, Resource]],
    ) -> int:
        """Backward-compatible wrapper — allocation_retry_limit is ignored (RetryManager owns it)."""
        line_window_cache = self._calculate_phase._compute_line_window_cache([line])
        rows = self._calculate_phase._collect_billing_line_rows(
            line, uow, prefetched_metrics, frozenset(), tenant_period_cache, resource_cache, line_window_cache
        )
        return uow.chargebacks.upsert_batch(rows)

    def _calculate_date(
        self,
        uow: UnitOfWork,
        tracking_date: date_type,
        *,
        calculation_run_id: int | None = None,
    ) -> int:
        """Backward-compatible wrapper — delegates to CalculatePhase.run()."""
        if calculation_run_id is None:
            return self._calculate_phase.run(uow, tracking_date)
        return self._calculate_phase.run(uow, tracking_date, calculation_run_id=calculation_run_id)

    def _report_progress(self, stage: str | None, current_date: date_type | None = None) -> None:
        if self._progress_callback is not None:
            self._progress_callback(stage, current_date)

    def run(self, *, calculation_run_id: int | None = None) -> PipelineRunResult:
        errors: list[str] = []
        dates_gathered = 0
        dates_calculated = 0
        chargeback_rows_written = 0

        self._report_progress("gathering")
        try:
            with self._storage_backend.create_unit_of_work() as uow:
                gather_result = self._gather_phase.run(uow)
                dates_gathered = gather_result.dates_gathered
                errors.extend(gather_result.errors)
                uow.commit()
            self._consecutive_gather_failures = 0
        except Exception as exc:
            logger.exception("Gather phase failed for %s: %s", self._tenant_name, exc)
            errors.append(f"Gather phase failed: {exc}")
            self._consecutive_gather_failures += 1
            if self._consecutive_gather_failures >= self._gather_failure_threshold:
                raise GatherFailureThresholdError(
                    f"Tenant {self._tenant_name} gather failed {self._consecutive_gather_failures} "
                    f"consecutive times (threshold: {self._gather_failure_threshold})."
                ) from exc
            return PipelineRunResult(
                tenant_name=self._tenant_name,
                tenant_id=self._tenant_id,
                dates_gathered=0,
                dates_calculated=0,
                chargeback_rows_written=0,
                dates_pending_calculation=0,
                errors=errors,
            )

        with self._storage_backend.create_unit_of_work() as uow:
            pending_states = uow.pipeline_state.find_needing_calculation(self._ecosystem, self._tenant_id)

        for pipeline_state in pending_states:
            if self._shutdown_check is not None and self._shutdown_check():
                logger.info(
                    "Shutdown requested — stopping after %d dates processed for %s",
                    dates_calculated,
                    self._tenant_name,
                )
                break

            tracking_date = pipeline_state.tracking_date
            self._report_progress("calculating", tracking_date)
            logger.info("Processing billing date: %s", tracking_date)
            start_time = time.time()
            try:
                with self._storage_backend.create_unit_of_work() as uow:
                    if calculation_run_id is None:
                        rows = self._calculate_phase.run(uow, tracking_date)
                    else:
                        rows = self._calculate_phase.run(
                            uow,
                            tracking_date,
                            calculation_run_id=calculation_run_id,
                        )
                    chargeback_rows_written += rows
                    dates_calculated += 1
                    uow.commit()
                elapsed = int(time.time() - start_time)
                logger.info(
                    "Processed %d chargeback rows for billing date: %s in %d seconds",
                    rows,
                    tracking_date,
                    elapsed,
                )
            except Exception as exc:
                logger.exception(
                    "Calculate failed for %s date %s: %s",
                    self._tenant_name,
                    tracking_date,
                    exc,
                )
                errors.append(f"Calculate failed for date {tracking_date}: {exc}")

        if self._topic_overlay_phase is not None:
            with self._storage_backend.create_unit_of_work() as uow:
                overlay_pending = uow.pipeline_state.find_needing_topic_attribution(
                    self._ecosystem,
                    self._tenant_id,
                )

            for pipeline_state in overlay_pending:
                if self._shutdown_check is not None and self._shutdown_check():
                    break
                tracking_date = pipeline_state.tracking_date
                self._report_progress("topic_overlay", tracking_date)
                logger.info("Running topic attribution for date: %s", tracking_date)
                try:
                    with self._storage_backend.create_unit_of_work() as uow:
                        rows = self._topic_overlay_phase.run(uow, tracking_date)
                        uow.commit()
                    logger.info("Topic attribution: %d rows for date %s", rows, tracking_date)
                except Exception as exc:
                    logger.exception(
                        "Topic overlay failed for %s date %s: %s",
                        self._tenant_name,
                        tracking_date,
                        exc,
                    )
                    errors.append(f"Topic overlay failed for date {tracking_date}: {exc}")

        self._report_progress(None, None)
        return PipelineRunResult(
            tenant_name=self._tenant_name,
            tenant_id=self._tenant_id,
            dates_gathered=dates_gathered,
            dates_calculated=dates_calculated,
            chargeback_rows_written=chargeback_rows_written,
            dates_pending_calculation=len(pending_states),
            errors=errors,
        )


def _load_overrides(
    plugin_settings: PluginSettingsBase,
) -> tuple[
    AllocatorRegistry,
    dict[str, Callable[..., IdentityResolution]],
    dict[str, float | int | str | bool],
    timedelta,
    timedelta,
    dict[str, timedelta],
]:
    """Pure function — extracts and validates overrides from plugin_settings.

    Returns (registry, identity_overrides, allocator_params, min_refresh_gap, metrics_step,
    extra_granularity_durations).
    """
    from core.plugin.protocols import CostAllocator as CostAllocatorProtocol

    registry = AllocatorRegistry()
    for product_type, dotted_path in plugin_settings.allocator_overrides.items():
        fn = load_protocol_callable(dotted_path, CostAllocatorProtocol)
        registry.register_override(product_type, fn)

    identity_overrides: dict[str, Callable[..., IdentityResolution]] = {}
    for service_type, dotted_path in plugin_settings.identity_resolution_overrides.items():
        identity_overrides[service_type] = _load_identity_resolver(dotted_path)

    min_refresh_gap = timedelta(seconds=plugin_settings.min_refresh_gap_seconds)
    metrics_step = timedelta(seconds=plugin_settings.metrics_step_seconds)
    extra_granularity_durations: dict[str, timedelta] = {
        name: timedelta(hours=hours) for name, hours in plugin_settings.granularity_durations.items()
    }
    return (
        registry,
        identity_overrides,
        plugin_settings.allocator_params,
        min_refresh_gap,
        metrics_step,
        extra_granularity_durations,
    )


def _ensure_unallocated_identity(uow: UnitOfWork, ecosystem: str, tenant_id: str) -> None:
    """Upsert the UNALLOCATED system identity (idempotent)."""
    unallocated = CoreIdentity(
        ecosystem=ecosystem,
        tenant_id=tenant_id,
        identity_id="UNALLOCATED",
        identity_type="system",
        display_name="Unallocated Costs",
    )
    uow.identities.upsert(unallocated)


def _ensure_pipeline_state(uow: UnitOfWork, ecosystem: str, tenant_id: str, tracking_date: date_type) -> None:
    """Ensure a PipelineState row exists for the given date (no-op if present)."""
    existing = uow.pipeline_state.get(ecosystem, tenant_id, tracking_date)
    if existing is None:
        uow.pipeline_state.upsert(PipelineState(ecosystem=ecosystem, tenant_id=tenant_id, tracking_date=tracking_date))


def _load_identity_resolver(dotted_path: str) -> Callable[..., IdentityResolution]:
    """Load an identity resolution override callable and validate its signature."""
    from core.plugin.protocols import IdentityResolver

    return cast(
        "Callable[..., IdentityResolution]",
        load_protocol_callable(dotted_path, IdentityResolver),
    )
