from __future__ import annotations

import calendar
import inspect
import logging
from collections.abc import Callable
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from datetime import date as date_type
from decimal import Decimal
from typing import TYPE_CHECKING, Any, cast

from core.engine.allocation import AllocationContext, AllocatorRegistry
from core.engine.helpers import compute_active_fraction
from core.engine.loading import load_protocol_callable
from core.models.chargeback import AllocationDetail, ChargebackRow, CostType
from core.models.identity import Identity, IdentityResolution, IdentitySet
from core.models.pipeline import PipelineState
from core.plugin.registry import EcosystemBundle

if TYPE_CHECKING:
    from core.config.models import TenantConfig
    from core.metrics.protocol import MetricsSource
    from core.models.billing import BillingLineItem
    from core.models.metrics import MetricQuery, MetricRow
    from core.models.resource import Resource
    from core.plugin.protocols import CostAllocator, EcosystemPlugin, ServiceHandler
    from core.storage.interface import StorageBackend, UnitOfWork

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


class ChargebackOrchestrator:
    """Runs the gather→calculate pipeline for one tenant."""

    def __init__(
        self,
        tenant_name: str,
        tenant_config: TenantConfig,
        plugin: EcosystemPlugin,
        storage_backend: StorageBackend,
        metrics_source: MetricsSource | None = None,
    ) -> None:
        self._tenant_name = tenant_name
        self._tenant_config = tenant_config
        self._storage_backend = storage_backend
        self._metrics_source = metrics_source
        self._ecosystem = tenant_config.ecosystem
        self._tenant_id = tenant_config.tenant_id

        # Plugin should already be initialized by caller (workflow_runner)
        self._bundle = EcosystemBundle.build(plugin)

        # Allocator overrides
        self._allocator_registry = AllocatorRegistry()
        self._identity_overrides: dict[str, Callable[..., IdentityResolution]] = {}
        self._allocator_params: dict[str, Any] = {}
        self._load_overrides(tenant_config.plugin_settings)

        # GAP-04: API object refresh throttle
        self._last_resource_gather_at: datetime | None = None

        # Consecutive gather failure tracking
        self._consecutive_gather_failures: int = 0

        # Zero-gather counters (in-memory, reset on restart)
        self._consecutive_zero_resource_gathers = 0
        self._consecutive_zero_identity_gathers = 0

        # Ensure UNALLOCATED identity exists
        with self._storage_backend.create_unit_of_work() as uow:
            self._ensure_unallocated_identity(uow)
            uow.commit()

    def _load_overrides(self, plugin_settings: dict[str, Any]) -> None:
        """Load allocator and identity resolution overrides from plugin_settings."""
        from core.plugin.protocols import CostAllocator as CostAllocatorProtocol

        self._allocator_params = plugin_settings.get("allocator_params", {})
        allocator_overrides = plugin_settings.get("allocator_overrides", {})
        for product_type, dotted_path in allocator_overrides.items():
            fn = load_protocol_callable(dotted_path, CostAllocatorProtocol)
            self._allocator_registry.register_override(product_type, fn)

        identity_overrides = plugin_settings.get("identity_resolution_overrides", {})
        for service_type, dotted_path in identity_overrides.items():
            fn = _load_identity_resolver(dotted_path)
            self._identity_overrides[service_type] = fn

        # GAP-04: configurable refresh throttle (default 30 min)
        self._min_refresh_gap = timedelta(seconds=plugin_settings.get("min_refresh_gap_seconds", 1800))

    def _ensure_unallocated_identity(self, uow: UnitOfWork) -> None:
        """Upsert the UNALLOCATED identity for this tenant (idempotent)."""
        unallocated = Identity(
            ecosystem=self._ecosystem,
            tenant_id=self._tenant_id,
            identity_id="UNALLOCATED",
            identity_type="system",
            display_name="Unallocated Costs",
        )
        uow.identities.upsert(unallocated)

    def run(self) -> PipelineRunResult:
        errors: list[str] = []
        dates_gathered = 0
        dates_calculated = 0
        chargeback_rows_written = 0

        # 1. Gather phase
        gather_failed = False
        try:
            with self._storage_backend.create_unit_of_work() as uow:
                dates_gathered, gather_errors = self._gather(uow)
                errors.extend(gather_errors)
                uow.commit()
            # Reset on success
            self._consecutive_gather_failures = 0
        except Exception as exc:
            logger.error("Gather phase failed for %s: %s", self._tenant_name, exc)
            errors.append(f"Gather phase failed: {exc}")
            gather_failed = True
            # Increment and check threshold
            self._consecutive_gather_failures += 1
            threshold = self._tenant_config.gather_failure_threshold
            if self._consecutive_gather_failures >= threshold:
                raise GatherFailureThresholdError(
                    f"Tenant {self._tenant_name} gather failed {self._consecutive_gather_failures} "
                    f"consecutive times (threshold: {threshold}). Exiting for operator attention."
                ) from exc

        if gather_failed:
            return PipelineRunResult(
                tenant_name=self._tenant_name,
                tenant_id=self._tenant_id,
                dates_gathered=dates_gathered,
                dates_calculated=dates_calculated,
                chargeback_rows_written=chargeback_rows_written,
                errors=errors,
            )

        # 2. Find dates needing calculation
        max_dates = self._tenant_config.max_dates_per_run
        with self._storage_backend.create_unit_of_work() as uow:
            all_pending = uow.pipeline_state.find_needing_calculation(self._ecosystem, self._tenant_id)
            pending_states = all_pending[:max_dates]

        # 3. Calculate phase — one UoW per date
        for pipeline_state in pending_states:
            try:
                with self._storage_backend.create_unit_of_work() as uow:
                    rows = self._calculate_date(uow, pipeline_state.tracking_date)
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

    def _gather(self, uow: UnitOfWork) -> tuple[int, list[str]]:
        """Run gather phase. Returns (billing_dates_count, errors)."""
        now = datetime.now(UTC)

        # GAP-04: throttle resource/billing refresh
        should_refresh_resources = (
            self._last_resource_gather_at is None or (now - self._last_resource_gather_at) >= self._min_refresh_gap
        )

        if not should_refresh_resources:
            logger.debug(
                "Skipping resource/billing refresh — last gather was %s ago",
                now - self._last_resource_gather_at,
            )
            return 0, []

        all_gathered_resource_ids: set[str] = set()
        all_gathered_identity_ids: set[str] = set()
        gather_complete = True
        gather_errors: list[str] = []

        # Gather resources and identities from each handler
        for handler in self._bundle.handlers.values():
            try:
                r_ids, i_ids = self._gather_resources_and_identities(handler, uow)
                all_gathered_resource_ids.update(r_ids)
                all_gathered_identity_ids.update(i_ids)
            except Exception as exc:
                logger.error(
                    "Handler %s gather failed — skipping deletion detection: %s",
                    handler.service_type,
                    exc,
                )
                gather_complete = False
                gather_errors.append(f"Handler {handler.service_type} gather failed: {exc}")

        # Deletion detection
        if not gather_complete:
            logger.warning("Skipping deletion detection — incomplete gather for %s", self._tenant_id)
        else:
            self._detect_deletions(uow, now, all_gathered_resource_ids, all_gathered_identity_ids)

        # Billing gather
        start = now - timedelta(days=self._tenant_config.lookback_days)
        end = now - timedelta(days=self._tenant_config.cutoff_days)
        cost_input = self._bundle.plugin.get_cost_input()
        gathered_billing_dates: set[date_type] = set()

        for line in cost_input.gather(self._tenant_id, start, end, uow):
            line = replace(line, timestamp=_ensure_utc(line.timestamp))
            uow.billing.upsert(line)
            gathered_billing_dates.add(line.timestamp.date())

        # GAP-001: mark pipeline state per billing date (not just today)
        for billing_date in gathered_billing_dates:
            _ensure_pipeline_state(uow, self._ecosystem, self._tenant_id, billing_date)
            uow.pipeline_state.mark_billing_gathered(self._ecosystem, self._tenant_id, billing_date)
            if gather_complete:
                uow.pipeline_state.mark_resources_gathered(self._ecosystem, self._tenant_id, billing_date)

        # Recalculation window
        self._apply_recalculation_window(uow, gathered_billing_dates, now)

        # GAP-04: update last gather timestamp on success
        self._last_resource_gather_at = now

        return len(gathered_billing_dates), gather_errors

    def _gather_resources_and_identities(
        self,
        handler: ServiceHandler,
        uow: UnitOfWork,
    ) -> tuple[set[str], set[str]]:
        """Gather resources and identities from a handler. Returns (resource_ids, identity_ids)."""
        gathered_resource_ids: set[str] = set()
        gathered_identity_ids: set[str] = set()

        for resource in handler.gather_resources(self._tenant_id, uow):
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

    def _detect_deletions(
        self,
        uow: UnitOfWork,
        now: datetime,
        gathered_resource_ids: set[str],
        gathered_identity_ids: set[str],
    ) -> None:
        """Detect resource and identity deletions with zero-gather protection."""
        threshold = self._tenant_config.zero_gather_deletion_threshold

        # Resource deletions
        active_resources, _ = uow.resources.find_active_at(self._ecosystem, self._tenant_id, now)
        if len(gathered_resource_ids) == 0 and len(active_resources) > 0:
            self._consecutive_zero_resource_gathers += 1
            if threshold == -1 or self._consecutive_zero_resource_gathers < threshold:
                logger.warning(
                    "Zero resources gathered but %d active — skipping resource deletion (consecutive: %d)",
                    len(active_resources),
                    self._consecutive_zero_resource_gathers,
                )
            else:
                logger.warning(
                    "Zero resources gathered for %d consecutive runs — proceeding with deletion",
                    self._consecutive_zero_resource_gathers,
                )
                for r in active_resources:
                    if r.resource_id not in gathered_resource_ids:
                        uow.resources.mark_deleted(self._ecosystem, self._tenant_id, r.resource_id, now)
                self._consecutive_zero_resource_gathers = 0
        else:
            self._consecutive_zero_resource_gathers = 0
            for r in active_resources:
                if r.resource_id not in gathered_resource_ids:
                    uow.resources.mark_deleted(self._ecosystem, self._tenant_id, r.resource_id, now)

        # Identity deletions
        active_identities, _ = uow.identities.find_active_at(self._ecosystem, self._tenant_id, now)
        if len(gathered_identity_ids) == 0 and len(active_identities) > 0:
            self._consecutive_zero_identity_gathers += 1
            if threshold == -1 or self._consecutive_zero_identity_gathers < threshold:
                logger.warning(
                    "Zero identities gathered but %d active — skipping identity deletion (consecutive: %d)",
                    len(active_identities),
                    self._consecutive_zero_identity_gathers,
                )
            else:
                logger.warning(
                    "Zero identities gathered for %d consecutive runs — proceeding with deletion",
                    self._consecutive_zero_identity_gathers,
                )
                for i in active_identities:
                    if i.identity_id not in gathered_identity_ids:
                        uow.identities.mark_deleted(self._ecosystem, self._tenant_id, i.identity_id, now)
                self._consecutive_zero_identity_gathers = 0
        else:
            self._consecutive_zero_identity_gathers = 0
            for i in active_identities:
                if i.identity_id not in gathered_identity_ids:
                    uow.identities.mark_deleted(self._ecosystem, self._tenant_id, i.identity_id, now)

    def _apply_recalculation_window(
        self,
        uow: UnitOfWork,
        gathered_billing_dates: set[date_type],
        now: datetime,
    ) -> None:
        """Reset chargeback_calculated for dates within the recalculation window."""
        recalc_cutoff = (now - timedelta(days=self._tenant_config.cutoff_days)).date()
        for billing_date in gathered_billing_dates:
            if billing_date >= recalc_cutoff:
                existing_state = uow.pipeline_state.get(self._ecosystem, self._tenant_id, billing_date)
                if existing_state and existing_state.chargeback_calculated:
                    uow.chargebacks.delete_by_date(self._ecosystem, self._tenant_id, billing_date)
                    uow.pipeline_state.mark_needs_recalculation(self._ecosystem, self._tenant_id, billing_date)
                    logger.info("Date %s within recalculation window — will recompute", billing_date)

    def _calculate_date(self, uow: UnitOfWork, tracking_date: date_type) -> int:
        """Calculate chargebacks for a single date. Returns rows written."""
        billing_lines = uow.billing.find_by_date(self._ecosystem, self._tenant_id, tracking_date)

        if not billing_lines:
            uow.pipeline_state.mark_chargeback_calculated(self._ecosystem, self._tenant_id, tracking_date)
            return 0

        # Pre-computation: billing windows + metrics groups
        metrics_groups: dict[tuple[str, datetime, datetime], list[MetricQuery]] = {}
        billing_windows: set[tuple[datetime, datetime]] = set()

        for line in billing_lines:
            b_start, b_end, _ = billing_window(line)
            billing_windows.add((b_start, b_end))

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

        # Pre-fetch metrics
        prefetched_metrics: dict[tuple[str, datetime, datetime], dict[str, list[MetricRow]]] = {}
        if self._metrics_source:
            for (resource_id, m_start, m_end), queries in metrics_groups.items():
                prefetched_metrics[(resource_id, m_start, m_end)] = self._metrics_source.query(
                    queries,
                    start=m_start,
                    end=m_end,
                    step=timedelta(hours=1),
                    resource_id_filter=resource_id,
                )

        # Pre-compute tenant_period cache
        tenant_period_cache: dict[tuple[datetime, datetime], IdentitySet] = {}
        for b_start, b_end in billing_windows:
            identities, _ = uow.identities.find_by_period(self._ecosystem, self._tenant_id, b_start, b_end)
            tp = IdentitySet()
            for identity in identities:
                if identity.identity_type != "system":
                    tp.add(identity)
            tenant_period_cache[(b_start, b_end)] = tp

        # Pre-fetch all resources for this date's billing windows into a flat lookup
        resource_cache: dict[str, Resource] = {}
        for b_start, b_end in billing_windows:
            resources, _ = uow.resources.find_by_period(self._ecosystem, self._tenant_id, b_start, b_end)
            for r in resources:
                resource_cache.setdefault(r.resource_id, r)

        # Per-line processing
        allocation_retry_limit = self._tenant_config.allocation_retry_limit
        total_rows = 0

        for line in billing_lines:
            rows = self._process_billing_line(
                line,
                uow,
                prefetched_metrics,
                tenant_period_cache,
                allocation_retry_limit,
                resource_cache=resource_cache,
            )
            total_rows += rows

        uow.pipeline_state.mark_chargeback_calculated(self._ecosystem, self._tenant_id, tracking_date)
        return total_rows

    def _process_billing_line(
        self,
        line: BillingLineItem,
        uow: UnitOfWork,
        prefetched_metrics: dict[tuple[str, datetime, datetime], dict[str, list[MetricRow]]],
        tenant_period_cache: dict[tuple[datetime, datetime], IdentitySet],
        allocation_retry_limit: int,
        resource_cache: dict[str, Resource],
    ) -> int:
        """Process a single billing line. Returns number of chargeback rows written."""
        try:
            b_start, b_end, b_duration = billing_window(line)

            handler = self._bundle.product_type_to_handler.get(line.product_type)
            if handler is None:
                logger.warning(
                    "No handler for product_type %s — allocating to UNALLOCATED",
                    line.product_type,
                )
                row = self._allocate_to_unallocated(
                    line,
                    reason="UNKNOWN_PRODUCT_TYPE",
                    detail=AllocationDetail.USING_UNKNOWN_ALLOCATOR,
                )
                uow.chargebacks.upsert(row)
                return 1

            # Metrics lookup
            metrics_data = prefetched_metrics.get((line.resource_id, b_start, b_end))

            # Active fraction
            resource = resource_cache.get(line.resource_id)
            active_fraction = Decimal(1) if resource is None else compute_active_fraction(resource, b_start, b_end)
            split_amount = line.total_cost * active_fraction

            # Identity resolution
            if handler.service_type in self._identity_overrides:
                identity_resolution = self._identity_overrides[handler.service_type](
                    self._tenant_id,
                    line.resource_id,
                    b_start,
                    b_duration,
                    metrics_data,
                    uow,
                )
            else:
                identity_resolution = handler.resolve_identities(
                    self._tenant_id,
                    line.resource_id,
                    b_start,
                    b_duration,
                    metrics_data,
                    uow,
                )

            # Warn if handler populated tenant_period
            if identity_resolution.tenant_period and len(identity_resolution.tenant_period) > 0:
                logger.warning(
                    "Handler %s returned non-empty tenant_period (%d identities) — "
                    "orchestrator will replace it with temporally-filtered set",
                    handler.service_type,
                    len(identity_resolution.tenant_period),
                )

            # Inject orchestrator-managed tenant_period
            identity_resolution = IdentityResolution(
                resource_active=identity_resolution.resource_active,
                metrics_derived=identity_resolution.metrics_derived,
                tenant_period=tenant_period_cache[(b_start, b_end)],
                context=identity_resolution.context,
            )

            # Allocator dispatch
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
            # Persist attempt increment in a separate transaction so it survives rollback
            try:
                with self._storage_backend.create_unit_of_work() as retry_uow:
                    new_attempts = retry_uow.billing.increment_allocation_attempts(
                        self._ecosystem,
                        self._tenant_id,
                        line.timestamp,
                        line.resource_id,
                        line.product_type,
                    )
                    retry_uow.commit()
            except Exception as retry_exc:
                logger.warning("Failed to persist retry counter: %s", retry_exc)
                raise exc from None  # re-raise original allocator exception; counter persistence is best-effort

            if new_attempts < allocation_retry_limit:
                logger.error(
                    "Billing line %s/%s failed (attempt %d/%d): %s — failing date",
                    line.resource_id,
                    line.product_type,
                    new_attempts,
                    allocation_retry_limit,
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
                line,
                reason="ALLOCATION_FAILED",
                detail=f"Failed after {new_attempts} attempts: {exc}",
            )
            uow.chargebacks.upsert(row)
            return 1

    def _resolve_allocator(
        self,
        product_type: str,
        handler: ServiceHandler,
    ) -> CostAllocator:
        """Dispatch: override registry first, then handler."""
        try:
            return self._allocator_registry.get(product_type)
        except KeyError:
            return handler.get_allocator(product_type)

    def _allocate_to_unallocated(
        self,
        line: BillingLineItem,
        reason: str,
        detail: str | None = None,
    ) -> ChargebackRow:
        """Create a ChargebackRow allocating full cost to UNALLOCATED identity."""
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


def _ensure_pipeline_state(uow: UnitOfWork, ecosystem: str, tenant_id: str, tracking_date: date_type) -> None:
    """Ensure a PipelineState row exists for the given date (no-op if present)."""
    existing = uow.pipeline_state.get(ecosystem, tenant_id, tracking_date)
    if existing is None:
        uow.pipeline_state.upsert(PipelineState(ecosystem=ecosystem, tenant_id=tenant_id, tracking_date=tracking_date))


def _load_identity_resolver(dotted_path: str) -> Callable[..., IdentityResolution]:
    """Load an identity resolution override callable and validate its signature."""
    if not dotted_path or ":" not in dotted_path:
        raise ValueError(f"Expected 'module:attribute' format, got {dotted_path!r}")

    import importlib

    module_path, attr_name = dotted_path.rsplit(":", 1)
    module = importlib.import_module(module_path)
    obj = getattr(module, attr_name)

    if not callable(obj):
        raise TypeError(f"Loaded object {obj!r} is not callable")

    # Validate parameter count matches resolve_identities signature (6 params)
    sig = inspect.signature(obj)
    params = [
        p
        for name, p in sig.parameters.items()
        if name != "self" and p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)
    ]
    if len(params) != 6:
        raise TypeError(
            f"Identity resolver {obj!r} must accept 6 positional parameters "
            f"(tenant_id, resource_id, billing_timestamp, billing_duration, metrics_data, uow), "
            f"got {len(params)}"
        )

    return cast("Callable[..., IdentityResolution]", obj)
