from __future__ import annotations

import calendar
import logging
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from datetime import date as date_type
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Protocol, cast

from core.engine.allocation import AllocationContext, AllocatorRegistry
from core.engine.helpers import compute_active_fraction
from core.engine.loading import load_protocol_callable
from core.models.chargeback import AllocationDetail, ChargebackRow, CostType
from core.models.identity import Identity, IdentityResolution, IdentitySet
from core.models.pipeline import PipelineState
from core.plugin.registry import EcosystemBundle

if TYPE_CHECKING:
    from core.config.models import PluginSettingsBase, TenantConfig
    from core.metrics.protocol import MetricsSource
    from core.models.billing import BillingLineItem
    from core.models.metrics import MetricQuery, MetricRow
    from core.models.resource import Resource
    from core.plugin.protocols import CostAllocator, EcosystemPlugin, ServiceHandler
    from core.storage.interface import StorageBackend, UnitOfWork

    class _EntityRepo(Protocol):
        """Structural minimum for deletion detection — covers ResourceRepository and IdentityRepository."""

        def find_active_at(self, ecosystem: str, tenant_id: str, timestamp: datetime) -> tuple[Sequence[Any], int]: ...

        def mark_deleted(self, ecosystem: str, tenant_id: str, entity_id: str, deleted_at: datetime) -> None: ...


logger = logging.getLogger(__name__)


class GatherFailureThresholdError(Exception):
    """Raised when consecutive gather failures exceed threshold."""


GRANULARITY_DURATION: dict[str, timedelta] = {
    "hourly": timedelta(hours=1),
    "daily": timedelta(hours=24),
}


def billing_window(line: BillingLineItem) -> tuple[datetime, datetime, timedelta]:
    """Derive (start, end, duration) from billing line's timestamp + granularity."""
    if line.granularity == "monthly":
        year, month = line.timestamp.year, line.timestamp.month
        _, days_in_month = calendar.monthrange(year, month)
        duration = timedelta(days=days_in_month)
    elif line.granularity in GRANULARITY_DURATION:
        duration = GRANULARITY_DURATION[line.granularity]
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

    def increment_and_check(self, ecosystem: str, tenant_id: str, line: BillingLineItem) -> tuple[int, bool]: ...


class RetryManager:
    """Persists per-line retry counters and determines fallback behavior.

    Opens a separate UoW (committed immediately) so the counter survives
    the caller's UoW rollback on allocation failure.
    """

    def __init__(self, storage_backend: StorageBackend, limit: int) -> None:
        self._storage_backend = storage_backend
        self._limit = limit

    def increment_and_check(self, ecosystem: str, tenant_id: str, line: BillingLineItem) -> tuple[int, bool]:
        """Increment attempt counter. Returns (new_attempts, should_fallback_to_unallocated)."""
        with self._storage_backend.create_unit_of_work() as uow:
            new_attempts = uow.billing.increment_allocation_attempts(
                ecosystem,
                tenant_id,
                line.timestamp,
                line.resource_id,
                line.product_type,
            )
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
                        logger.error(
                            "Handler %s gather failed — skipping deletion detection: %s",
                            handler.service_type,
                            exc,
                        )
                        gather_errors.append(f"Handler {handler.service_type} gather failed: {exc}")

        return GatherResult(dates_gathered=0, errors=gather_errors)

    def _run_full(self, uow: UnitOfWork) -> GatherResult:
        """Full gather cycle with deletion detection and billing. Caller owns UoW."""
        now = datetime.now(UTC)

        if not self._should_refresh(now):
            logger.debug(
                "Skipping resource/billing refresh — last gather was %s ago",
                now - self._last_resource_gather_at,
            )
            return GatherResult(dates_gathered=0, errors=[], skipped=True)

        all_gathered_resource_ids: set[str] = set()
        all_gathered_identity_ids: set[str] = set()
        gather_complete = True
        gather_errors: list[str] = []

        # Phase 1: Build shared context once for all handlers.
        shared_ctx = self._bundle.plugin.build_shared_context(self._tenant_id)

        for handler in self._bundle.handlers.values():
            try:
                r_ids, i_ids = self._gather_resources_and_identities(handler, uow, shared_ctx)
                all_gathered_resource_ids.update(r_ids)
                all_gathered_identity_ids.update(i_ids)
            except Exception as exc:
                logger.error("Handler %s gather failed: %s", handler.service_type, exc)
                gather_complete = False
                gather_errors.append(f"Handler {handler.service_type} gather failed: {exc}")

        if gather_complete:
            self._detect_deletions(uow, now, all_gathered_resource_ids, all_gathered_identity_ids)
        else:
            logger.warning("Skipping deletion detection — incomplete gather for %s", self._tenant_id)

        gathered_billing_dates = self._gather_billing(uow, now)

        for billing_date in gathered_billing_dates:
            _ensure_pipeline_state(uow, self._ecosystem, self._tenant_id, billing_date)
            uow.pipeline_state.mark_billing_gathered(self._ecosystem, self._tenant_id, billing_date)
            if gather_complete:
                uow.pipeline_state.mark_resources_gathered(self._ecosystem, self._tenant_id, billing_date)

        self._apply_recalculation_window(uow, gathered_billing_dates, now)
        self._last_resource_gather_at = now

        return GatherResult(dates_gathered=len(gathered_billing_dates), errors=gather_errors)

    def _should_refresh(self, now: datetime) -> bool:
        return self._last_resource_gather_at is None or (now - self._last_resource_gather_at) >= self._min_refresh_gap

    def _gather_resources_and_identities(
        self, handler: ServiceHandler, uow: UnitOfWork, shared_ctx: object | None = None
    ) -> tuple[set[str], set[str]]:
        gathered_resource_ids: set[str] = set()
        gathered_identity_ids: set[str] = set()
        for resource in handler.gather_resources(self._tenant_id, uow, shared_ctx):
            if resource.created_at is not None:
                resource = replace(resource, created_at=_ensure_utc(resource.created_at))
            uow.resources.upsert(resource)
            gathered_resource_ids.add(resource.resource_id)
        for identity in handler.gather_identities(self._tenant_id, uow):
            if identity.created_at is not None:
                identity = replace(identity, created_at=_ensure_utc(identity.created_at))
            uow.identities.upsert(identity)
            gathered_identity_ids.add(identity.identity_id)
        return gathered_resource_ids, gathered_identity_ids

    def _detect_entity_deletions(
        self,
        repo: _EntityRepo,
        gathered_ids: set[str],
        entity_name: str,
        id_getter: Callable[[Any], str],
        now: datetime,
    ) -> None:
        threshold = self._tenant_config.zero_gather_deletion_threshold
        active_entities, _ = repo.find_active_at(self._ecosystem, self._tenant_id, now)
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
                        repo.mark_deleted(self._ecosystem, self._tenant_id, entity_id, now)
                self._zero_gather_counters[entity_name] = 0
        else:
            self._zero_gather_counters[entity_name] = 0
            for entity in active_entities:
                entity_id = id_getter(entity)
                if entity_id not in gathered_ids:
                    repo.mark_deleted(self._ecosystem, self._tenant_id, entity_id, now)

    def _detect_deletions(
        self,
        uow: UnitOfWork,
        now: datetime,
        gathered_resource_ids: set[str],
        gathered_identity_ids: set[str],
    ) -> None:
        self._detect_entity_deletions(uow.resources, gathered_resource_ids, "resources", lambda r: r.resource_id, now)
        self._detect_entity_deletions(uow.identities, gathered_identity_ids, "identities", lambda i: i.identity_id, now)

    def _gather_billing(self, uow: UnitOfWork, now: datetime) -> set[date_type]:
        start = now - timedelta(days=self._tenant_config.lookback_days)
        end = now - timedelta(days=self._tenant_config.cutoff_days)
        cost_input = self._bundle.plugin.get_cost_input()
        gathered: set[date_type] = set()
        for line in cost_input.gather(self._tenant_id, start, end, uow):
            line = replace(line, timestamp=_ensure_utc(line.timestamp))
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
                    uow.pipeline_state.mark_needs_recalculation(self._ecosystem, self._tenant_id, billing_date)
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

    def run(self, uow: UnitOfWork, tracking_date: date_type) -> int:
        """Calculate chargebacks for a single date. Returns rows written."""
        billing_lines = uow.billing.find_by_date(self._ecosystem, self._tenant_id, tracking_date)

        if not billing_lines:
            uow.pipeline_state.mark_chargeback_calculated(self._ecosystem, self._tenant_id, tracking_date)
            return 0

        prefetched_metrics = self._prefetch_metrics(billing_lines)
        tenant_period_cache = self._build_tenant_period_cache(uow, billing_lines)
        resource_cache = self._build_resource_cache(uow, billing_lines)

        total_rows = 0
        for line in billing_lines:
            rows = self._process_billing_line(line, uow, prefetched_metrics, tenant_period_cache, resource_cache)
            total_rows += rows

        uow.pipeline_state.mark_chargeback_calculated(self._ecosystem, self._tenant_id, tracking_date)
        return total_rows

    def _prefetch_metrics(
        self, billing_lines: list[BillingLineItem]
    ) -> dict[tuple[str, datetime, datetime], dict[str, list[MetricRow]]]:
        metrics_groups: dict[tuple[str, datetime, datetime], list[MetricQuery]] = {}
        for line in billing_lines:
            b_start, b_end, _ = billing_window(line)
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
        if self._metrics_source:
            for (resource_id, m_start, m_end), queries in metrics_groups.items():
                prefetched[(resource_id, m_start, m_end)] = self._metrics_source.query(
                    queries,
                    start=m_start,
                    end=m_end,
                    step=self._metrics_step,
                    resource_id_filter=resource_id,
                )
        return prefetched

    @staticmethod
    def _compute_billing_windows(billing_lines: list[BillingLineItem]) -> set[tuple[datetime, datetime]]:
        windows: set[tuple[datetime, datetime]] = set()
        for line in billing_lines:
            b_start, b_end, _ = billing_window(line)
            windows.add((b_start, b_end))
        return windows

    def _build_tenant_period_cache(
        self, uow: UnitOfWork, billing_lines: list[BillingLineItem]
    ) -> dict[tuple[datetime, datetime], IdentitySet]:
        cache: dict[tuple[datetime, datetime], IdentitySet] = {}
        for b_start, b_end in self._compute_billing_windows(billing_lines):
            identities, _ = uow.identities.find_by_period(self._ecosystem, self._tenant_id, b_start, b_end)
            tp = IdentitySet()
            for identity in identities:
                if identity.identity_type != "system":
                    tp.add(identity)
            cache[(b_start, b_end)] = tp
        return cache

    def _build_resource_cache(self, uow: UnitOfWork, billing_lines: list[BillingLineItem]) -> dict[str, Resource]:
        cache: dict[str, Resource] = {}
        for b_start, b_end in self._compute_billing_windows(billing_lines):
            resources, _ = uow.resources.find_by_period(self._ecosystem, self._tenant_id, b_start, b_end)
            for r in resources:
                cache.setdefault(r.resource_id, r)
        return cache

    def _process_billing_line(
        self,
        line: BillingLineItem,
        uow: UnitOfWork,
        prefetched_metrics: dict[tuple[str, datetime, datetime], dict[str, list[MetricRow]]],
        tenant_period_cache: dict[tuple[datetime, datetime], IdentitySet],
        resource_cache: dict[str, Resource],
    ) -> int:
        try:
            b_start, b_end, b_duration = billing_window(line)
            handler = self._bundle.product_type_to_handler.get(line.product_type)
            if handler is None:
                logger.warning(
                    "No handler for product_type %s — allocating to UNALLOCATED",
                    line.product_type,
                )
                row = self._allocate_to_unallocated(
                    line, "UNKNOWN_PRODUCT_TYPE", AllocationDetail.USING_UNKNOWN_ALLOCATOR
                )
                uow.chargebacks.upsert(row)
                return 1

            metrics_data = prefetched_metrics.get((line.resource_id, b_start, b_end))
            resource = resource_cache.get(line.resource_id)
            active_fraction = Decimal(1) if resource is None else compute_active_fraction(resource, b_start, b_end)
            split_amount = line.total_cost * active_fraction

            if handler.service_type in self._identity_overrides:
                identity_resolution = self._identity_overrides[handler.service_type](
                    self._tenant_id, line.resource_id, b_start, b_duration, metrics_data, uow
                )
            else:
                identity_resolution = handler.resolve_identities(
                    self._tenant_id, line.resource_id, b_start, b_duration, metrics_data, uow
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
                params=self._allocator_params,
            )
            result = allocator(ctx)

            rows_written = 0
            for row in result.rows:
                uow.chargebacks.upsert(row)
                rows_written += 1
            return rows_written

        except Exception as exc:
            try:
                new_attempts, should_fallback = self._retry_checker.increment_and_check(
                    self._ecosystem, self._tenant_id, line
                )
            except Exception as retry_exc:
                logger.warning("Failed to persist retry counter: %s", retry_exc)
                raise exc from None

            if not should_fallback:
                logger.error(
                    "Billing line %s/%s failed (attempt %d): %s — failing date",
                    line.resource_id,
                    line.product_type,
                    new_attempts,
                    exc,
                )
                raise

            logger.error(
                "Billing line %s/%s failed after %d attempts: %s — allocating to UNALLOCATED",
                line.resource_id,
                line.product_type,
                new_attempts,
                exc,
            )
            row = self._allocate_to_unallocated(
                line, "ALLOCATION_FAILED", f"Failed after {new_attempts} attempts: {exc}"
            )
            uow.chargebacks.upsert(row)
            return 1

    def _resolve_allocator(self, product_type: str, handler: ServiceHandler) -> CostAllocator:
        try:
            return self._allocator_registry.get(product_type)
        except KeyError:
            return handler.get_allocator(product_type)

    def _allocate_to_unallocated(self, line: BillingLineItem, reason: str, detail: str | None = None) -> ChargebackRow:
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
        )


class ChargebackOrchestrator:
    """Thin coordinator: runs gather -> calculate pipeline for one tenant."""

    def __init__(
        self,
        tenant_name: str,
        tenant_config: TenantConfig,
        plugin: EcosystemPlugin,
        storage_backend: StorageBackend,
        metrics_source: MetricsSource | None = None,
    ) -> None:
        self._tenant_name = tenant_name
        self._tenant_id = tenant_config.tenant_id
        self._ecosystem = tenant_config.ecosystem
        self._storage_backend = storage_backend
        self._tenant_config = tenant_config  # kept for backward compatibility

        bundle = EcosystemBundle.build(plugin)
        allocator_registry, identity_overrides, allocator_params, min_refresh_gap, metrics_step = _load_overrides(
            tenant_config.plugin_settings
        )

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
        )
        self._consecutive_gather_failures = 0
        self._gather_failure_threshold = tenant_config.gather_failure_threshold
        self._max_dates_per_run = tenant_config.max_dates_per_run

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
        resource_cache: dict[str, Resource],
    ) -> int:
        """Backward-compatible wrapper — allocation_retry_limit is ignored (RetryManager owns it)."""
        return self._calculate_phase._process_billing_line(
            line, uow, prefetched_metrics, tenant_period_cache, resource_cache
        )

    def _calculate_date(self, uow: UnitOfWork, tracking_date: date_type) -> int:
        """Backward-compatible wrapper — delegates to CalculatePhase.run()."""
        return self._calculate_phase.run(uow, tracking_date)

    def run(self) -> PipelineRunResult:
        errors: list[str] = []
        dates_gathered = 0
        dates_calculated = 0
        chargeback_rows_written = 0

        try:
            with self._storage_backend.create_unit_of_work() as uow:
                gather_result = self._gather_phase.run(uow)
                dates_gathered = gather_result.dates_gathered
                errors.extend(gather_result.errors)
                uow.commit()
            self._consecutive_gather_failures = 0
        except Exception as exc:
            logger.error("Gather phase failed for %s: %s", self._tenant_name, exc)
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
                errors=errors,
            )

        with self._storage_backend.create_unit_of_work() as uow:
            all_pending = uow.pipeline_state.find_needing_calculation(self._ecosystem, self._tenant_id)
            pending_states = all_pending[: self._max_dates_per_run]

        for pipeline_state in pending_states:
            try:
                with self._storage_backend.create_unit_of_work() as uow:
                    rows = self._calculate_phase.run(uow, pipeline_state.tracking_date)
                    chargeback_rows_written += rows
                    dates_calculated += 1
                    uow.commit()
            except Exception as exc:
                logger.error(
                    "Calculate failed for %s date %s: %s",
                    self._tenant_name,
                    pipeline_state.tracking_date,
                    exc,
                )
                errors.append(f"Calculate failed for date {pipeline_state.tracking_date}: {exc}")

        return PipelineRunResult(
            tenant_name=self._tenant_name,
            tenant_id=self._tenant_id,
            dates_gathered=dates_gathered,
            dates_calculated=dates_calculated,
            chargeback_rows_written=chargeback_rows_written,
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
]:
    """Pure function — extracts and validates overrides from plugin_settings.

    Returns (registry, identity_overrides, allocator_params, min_refresh_gap, metrics_step).
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
    return registry, identity_overrides, plugin_settings.allocator_params, min_refresh_gap, metrics_step


def _ensure_unallocated_identity(uow: UnitOfWork, ecosystem: str, tenant_id: str) -> None:
    """Upsert the UNALLOCATED system identity (idempotent)."""
    unallocated = Identity(
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
