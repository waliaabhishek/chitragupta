from __future__ import annotations

import calendar
import logging
import time
import uuid
from collections.abc import Callable, Iterable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from datetime import date as date_type
from decimal import Decimal
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Protocol, cast

from core.engine.allocation import AllocationContext, AllocatorRegistry
from core.engine.allocation_lineage import build_allocation_lineage_capture
from core.engine.helpers import compute_active_fraction
from core.engine.loading import load_protocol_callable
from core.engine.topic_attribution_provider import ChunkedTopicEvidenceProvider
from core.logging_context import safe_exception_context, safe_log_context
from core.models.chargeback import ChargebackRow, CostType
from core.models.identity import SENTINEL_IDENTITY_TYPES, CoreIdentity, IdentityResolution, IdentitySet
from core.models.pipeline import PipelineState
from core.plugin.protocols import (
    OverlayPlugin,
    PostRecoveryGatherScopeValidator,
    PreviewOrganizationGatherer,
    ScopeBlockedError,
    ScopeGateDecision,
    ScopeGatePlugin,
    ScopeGateResult,
    ScopeGateRunLifecycle,
    SupplementalResourceGatherer,
    TopicAttributionProviderPlugin,
    TopicDiscoveryPlugin,
)
from core.plugin.registry import EcosystemBundle
from core.preview.evidence import (
    AllocationLineageRunStatus,
    AllocationLineageUnavailableReason,
    AllocationLineageUnavailableRun,
    PreviewSourceAttempt,
    SourceAttemptFailureReason,
    SourceAttemptFinalStatus,
    SourceAttemptStatus,
)
from core.preview.evidence_capture import (
    NativeSourceEvidenceCapture,
    NativeSourceEvidenceCostInput,
    SourceAttemptBeginFailure,
    SourceCaptureFailure,
    SourceEvidenceStorageUnavailable,
)
from core.preview.organization_authority import (
    OrganizationAuthorityFailureReason,
    OrganizationAuthorityFinalStatus,
)
from core.storage.interface import AllocationLineageRunCapture

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


def _recovery_target_dates(
    pending_states: Sequence[PipelineState],
    result: ScopeGateResult,
) -> set[date_type]:
    """Return pending dates whose daily interval intersects the recovered range."""
    if result.recovery_start is None or result.recovery_end is None:
        return set()
    dates: set[date_type] = set()
    for pipeline_state in pending_states:
        day_start = result.recovery_start.replace(
            year=pipeline_state.tracking_date.year,
            month=pipeline_state.tracking_date.month,
            day=pipeline_state.tracking_date.day,
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
        )
        day_end = day_start + timedelta(days=1)
        if day_start < result.recovery_end and day_end > result.recovery_start:
            dates.add(pipeline_state.tracking_date)
    return dates


def _safe_database_root_cause(exc: BaseException) -> str:
    """Classify known database causes without rendering statements or parameters."""
    origin = getattr(exc, "orig", None)
    if origin is not None:
        try:
            if "UNIQUE constraint failed" in str(origin):
                return "UNIQUE constraint failed"
        except BaseException:
            pass
        return type(origin).__name__
    return type(exc).__name__


def _new_calculation_id() -> str:
    return str(uuid.uuid4())


def _calculation_utc_now() -> datetime:
    return datetime.now(UTC)


class GatherFailureThresholdError(Exception):
    """Raised when consecutive gather failures exceed threshold."""


class LineageCaptureFailureReason(StrEnum):
    CONSTRUCTION_FAILED = "construction_failed"


@dataclass(frozen=True)
class CalculationPhaseResult:
    ecosystem: str
    tenant_id: str
    tracking_date: date_type
    rows_written: int
    calculation_id: str
    calculation_completed_at: datetime
    lineage_capture: AllocationLineageRunCapture | None
    lineage_failure: LineageCaptureFailureReason | None

    def __post_init__(self) -> None:
        if not self.ecosystem.strip() or not self.tenant_id.strip() or not self.calculation_id.strip():
            raise ValueError("calculation identity must not be blank")
        if (
            self.rows_written < 0
            or self.calculation_completed_at.tzinfo is None
            or self.calculation_completed_at.utcoffset() is None
        ):
            raise ValueError("invalid calculation result")
        if (self.lineage_capture is None) == (self.lineage_failure is None):
            raise ValueError("calculation result requires exactly one lineage outcome")
        if self.lineage_failure is not None and not isinstance(self.lineage_failure, LineageCaptureFailureReason):
            raise ValueError("calculation result has an invalid lineage failure")
        capture = self.lineage_capture
        if capture is not None and (
            capture.ecosystem != self.ecosystem
            or capture.tenant_id != self.tenant_id
            or capture.tracking_date != self.tracking_date
            or capture.calculation_id != self.calculation_id
        ):
            raise ValueError("lineage capture identity does not match calculation")


@dataclass(frozen=True)
class HistoricalRepairDateResult:
    source_capture: NativeSourceEvidenceCapture
    calculation: CalculationPhaseResult
    billing_rows_written: int


class HistoricalRepairProviderSourceError(RuntimeError):
    """Historical provider data or its native capture was unavailable."""


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


class SourceGatherDisposition(StrEnum):
    NOT_ATTEMPTED = "not_attempted"
    ATTEMPTED = "attempted"
    BEGIN_FAILED = "begin_failed"
    STORAGE_UNAVAILABLE = "storage_unavailable"


@dataclass
class GatherResult:
    """Result from a single GatherPhase.run() call."""

    dates_gathered: int
    errors: list[str]
    skipped: bool = False  # True when throttled — no gather performed
    topic_attribution_inventory_ready: bool = False
    source_disposition: SourceGatherDisposition = SourceGatherDisposition.NOT_ATTEMPTED
    source_refresh_token: str | None = None
    source_attempt_sequence: int | None = None
    source_capture: NativeSourceEvidenceCapture | None = None
    source_failure: SourceCaptureFailure | None = None

    def __post_init__(self) -> None:
        if self.skipped and (
            self.dates_gathered != 0
            or self.errors
            or self.topic_attribution_inventory_ready
            or self.source_disposition is not SourceGatherDisposition.NOT_ATTEMPTED
            or self.source_refresh_token is not None
            or self.source_attempt_sequence is not None
            or self.source_capture is not None
            or self.source_failure is not None
        ):
            raise ValueError("skipped gather result must have an empty not-attempted outcome")
        BillingGatherResult(
            dates=frozenset(),
            source_disposition=self.source_disposition,
            source_refresh_token=self.source_refresh_token,
            source_attempt_sequence=self.source_attempt_sequence,
            source_capture=self.source_capture,
            source_failure=self.source_failure,
        )


@dataclass(frozen=True)
class GatherPlan:
    now: datetime
    refresh_start: datetime
    refresh_end: datetime
    should_refresh: bool

    def __post_init__(self) -> None:
        if not all(
            value.tzinfo is not None and value.utcoffset() is not None
            for value in (
                self.now,
                self.refresh_start,
                self.refresh_end,
            )
        ):
            raise ValueError("gather plan timestamps must be timezone-aware")
        if self.refresh_start >= self.refresh_end:
            raise ValueError("gather plan bounds must be ordered")


class PreviewOrganizationBindingConflictError(RuntimeError):
    """A provider organization observation conflicts with the immutable binding."""


@dataclass(frozen=True)
class BillingGatherResult:
    dates: frozenset[date_type]
    source_disposition: SourceGatherDisposition = SourceGatherDisposition.NOT_ATTEMPTED
    source_refresh_token: str | None = None
    source_attempt_sequence: int | None = None
    source_capture: NativeSourceEvidenceCapture | None = None
    source_failure: SourceCaptureFailure | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.source_disposition, SourceGatherDisposition):
            raise ValueError("invalid source gather disposition")
        if self.source_failure is not None and not isinstance(self.source_failure, SourceCaptureFailure):
            raise ValueError("invalid source gather failure")
        payload_empty = (
            self.source_refresh_token is None
            and self.source_attempt_sequence is None
            and self.source_capture is None
            and self.source_failure is None
        )
        if self.source_disposition in {
            SourceGatherDisposition.NOT_ATTEMPTED,
            SourceGatherDisposition.STORAGE_UNAVAILABLE,
        }:
            if not payload_empty:
                raise ValueError("source gather disposition cannot carry source payload")
            return
        if not self.source_refresh_token or not self.source_attempt_sequence or self.source_attempt_sequence <= 0:
            raise ValueError("attempted source gather requires durable attempt identity")
        if self.source_disposition is SourceGatherDisposition.BEGIN_FAILED:
            if self.source_capture is not None or self.source_failure is not SourceCaptureFailure.ATTEMPT_BEGIN_FAILED:
                raise ValueError("begin-failed source gather requires its closed failure")
            return
        if (self.source_capture is None) == (self.source_failure is None):
            raise ValueError("attempted source gather requires exactly one capture outcome")
        if self.source_failure is SourceCaptureFailure.ATTEMPT_BEGIN_FAILED:
            raise ValueError("attempted source gather cannot carry begin failure")


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

    def run(
        self,
        uow: UnitOfWork | None = None,
        *,
        plan: GatherPlan | None = None,
        source_attempt: PreviewSourceAttempt
        | SourceAttemptBeginFailure
        | SourceEvidenceStorageUnavailable
        | None = None,
    ) -> GatherResult:
        """Execute full gather cycle.

        When called without uow (new path): runs Phase 1 (build_shared_context) +
        Phase 2 (handler loop with shared_ctx). Phase 1 failure is fatal.
        When called with uow (existing path): full gather cycle with deletion
        detection and billing. Caller owns UoW lifecycle (open + commit).
        """
        if uow is None:
            if plan is not None or source_attempt is not None:
                raise ValueError("resource-only gather does not accept a plan or source state")
            return self._run_gather_only()
        if plan is None:
            if source_attempt is not None:
                raise ValueError("source state requires an explicit gather plan")
            effective_plan = self.plan_refresh(datetime.now(UTC))
            accept_direct_refresh = True
        else:
            effective_plan = plan
            accept_direct_refresh = False
        if not effective_plan.should_refresh:
            return GatherResult(dates_gathered=0, errors=[], skipped=True)
        result = self._run_full(uow, plan=effective_plan, source_attempt=source_attempt)
        if accept_direct_refresh:
            self.accept_refresh(effective_plan)
        return result

    def plan_refresh(self, now: datetime) -> GatherPlan:
        normalized = _ensure_utc(now)
        raw_start = normalized - timedelta(days=self._tenant_config.lookback_days)
        raw_end = normalized - timedelta(days=self._tenant_config.cutoff_days)
        if self._tenant_config.focus_preview_enabled:
            start = datetime(raw_start.year, raw_start.month, raw_start.day, tzinfo=UTC)
            end = datetime(raw_end.year, raw_end.month, raw_end.day, tzinfo=UTC)
        else:
            start = raw_start
            end = raw_end
        return GatherPlan(
            now=normalized,
            refresh_start=start,
            refresh_end=end,
            should_refresh=self._should_refresh(normalized),
        )

    def _daily_replacement_date_window(self, plan: GatherPlan) -> tuple[date_type, date_type]:
        refresh_start = _ensure_utc(plan.refresh_start)
        refresh_end = _ensure_utc(plan.refresh_end)
        return refresh_start.date(), refresh_end.date()

    def accept_refresh(self, plan: GatherPlan) -> None:
        if not plan.should_refresh:
            raise ValueError("cannot accept a throttled gather plan")
        self._last_resource_gather_at = plan.now

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
                        logger.warning(
                            "handler_gather_failed%s",
                            safe_log_context(
                                tenant_id=self._tenant_id,
                                stage="gather",
                                outcome="deletion_detection_skipped",
                                retryable=True,
                                service_type=handler.service_type,
                                **safe_exception_context(exc),
                            ),
                        )
                        gather_errors.append(f"Handler {handler.service_type} gather failed: {exc}")
                self._run_supplemental_gather(uow, datetime.now(UTC), gather_errors)

        return GatherResult(dates_gathered=0, errors=gather_errors)

    def _run_full(
        self,
        uow: UnitOfWork,
        *,
        plan: GatherPlan,
        source_attempt: PreviewSourceAttempt | SourceAttemptBeginFailure | SourceEvidenceStorageUnavailable | None,
    ) -> GatherResult:
        """Full gather cycle with deletion detection and billing. Caller owns UoW."""
        if not plan.should_refresh:
            raise ValueError("throttled gather plan cannot execute")
        now = plan.now

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
                logger.warning(
                    "handler_gather_failed%s",
                    safe_log_context(
                        tenant_id=self._tenant_id,
                        stage="gather",
                        outcome="partial",
                        retryable=True,
                        service_type=handler.service_type,
                        **safe_exception_context(exc),
                    ),
                )
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

        plugin = self._bundle.plugin
        if isinstance(plugin, ScopeGatePlugin):
            scope_result = plugin.prepare_gather_scope(
                self._tenant_id,
                plan.refresh_start,
                plan.refresh_end,
                uow,
            )
            if scope_result.decision is not ScopeGateDecision.ALLOW:
                raise ScopeBlockedError(scope_result)

        billing_result = self._gather_billing(uow, plan, source_attempt=source_attempt)
        gathered_billing_dates = set(billing_result.dates)

        for billing_date in gathered_billing_dates:
            _ensure_pipeline_state(uow, self._ecosystem, self._tenant_id, billing_date)
            uow.pipeline_state.mark_billing_gathered(self._ecosystem, self._tenant_id, billing_date)
            if gather_complete:
                uow.pipeline_state.mark_resources_gathered(self._ecosystem, self._tenant_id, billing_date)

        topic_attribution_inventory_ready = False
        if self._topic_attribution_enabled and isinstance(self._bundle.plugin, TopicAttributionProviderPlugin):
            topic_attribution_inventory_ready = self._bundle.plugin.topic_attribution_inventory_ready(shared_ctx)
            if topic_attribution_inventory_ready:
                for billing_date in gathered_billing_dates:
                    uow.pipeline_state.mark_topic_overlay_gathered(
                        self._ecosystem,
                        self._tenant_id,
                        billing_date,
                    )
        elif self._topic_attribution_enabled and gathered_billing_dates:
            if isinstance(self._bundle.plugin, TopicDiscoveryPlugin):
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
                except Exception as exc:
                    logger.warning(
                        "topic_discovery_failed%s",
                        safe_log_context(
                            tenant_id=self._tenant_id,
                            stage="topic_discovery",
                            outcome="overlay_pending",
                            retryable=True,
                            **safe_exception_context(exc),
                        ),
                    )

        self._apply_recalculation_window(uow, gathered_billing_dates, plan)
        return GatherResult(
            dates_gathered=len(gathered_billing_dates),
            errors=gather_errors,
            topic_attribution_inventory_ready=topic_attribution_inventory_ready,
            source_disposition=billing_result.source_disposition,
            source_refresh_token=billing_result.source_refresh_token,
            source_attempt_sequence=billing_result.source_attempt_sequence,
            source_capture=billing_result.source_capture,
            source_failure=billing_result.source_failure,
        )

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
            if resource_type == "organization":
                continue
            try:
                resources = list(plugin.gather_supplemental_resources(self._tenant_id, resource_type, uow))
                if any(resource.resource_type != resource_type for resource in resources):
                    raise ValueError(f"supplemental {resource_type} gather returned another resource type")
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
                logger.warning(
                    "supplemental_gather_failed resource_type=%s%s",
                    resource_type,
                    safe_log_context(
                        tenant_id=self._tenant_id,
                        stage="supplemental_gather",
                        outcome="partial",
                        retryable=True,
                        **safe_exception_context(exc),
                    ),
                )
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
        legacy_unclassified = [
            resource for resource in active_organizations if "organization_binding_state" not in resource.metadata
        ]
        if len(bound) > 1:
            raise PreviewOrganizationBindingConflictError("multiple provider organization bindings are active")
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
            raise PreviewOrganizationBindingConflictError(
                "provider organization observation conflicts with the immutable binding"
            )
        if (
            not bound
            and legacy_unclassified
            and (len(active_organizations) != 1 or observed_id != legacy_unclassified[0].resource_id)
        ):
            if observed_id not in {item.resource_id for item in legacy_unclassified}:
                uow.resources.upsert(
                    replace(  # type: ignore[type-var]  # runtime Resource implementations are dataclasses
                        resource,
                        metadata={**resource.metadata, "organization_binding_state": "conflicting_observation"},
                    )
                )
            raise PreviewOrganizationBindingConflictError(
                "provider organization observation conflicts with legacy organization state"
            )
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

    def _gather_billing(
        self,
        uow: UnitOfWork,
        plan: GatherPlan,
        *,
        source_attempt: PreviewSourceAttempt | SourceAttemptBeginFailure | SourceEvidenceStorageUnavailable | None,
    ) -> BillingGatherResult:
        start = plan.refresh_start
        end = plan.refresh_end
        cost_input = self._bundle.plugin.get_cost_input()
        gathered: set[date_type] = set()
        source_capture = None
        source_failure = None
        lines: Iterable[BillingLineItem]
        if isinstance(source_attempt, PreviewSourceAttempt) and isinstance(cost_input, NativeSourceEvidenceCostInput):
            native_result = cost_input.gather_with_native_source_evidence(self._tenant_id, start, end)
            lines = native_result.billing_lines
            source_capture = native_result.capture
            source_failure = native_result.capture_failure
        else:
            lines = cost_input.gather(self._tenant_id, start, end, uow)
            if isinstance(source_attempt, PreviewSourceAttempt):
                source_failure = SourceCaptureFailure.CAPABILITY_UNAVAILABLE
        for line in lines:
            line = replace(line, timestamp=_ensure_utc(line.timestamp))  # type: ignore[type-var]  # runtime objects are dataclasses behind BillingLineItem Protocol
            uow.billing.upsert(line)
            gathered.add(line.timestamp.date())
        if source_attempt is None:
            return BillingGatherResult(dates=frozenset(gathered))
        if isinstance(source_attempt, SourceEvidenceStorageUnavailable):
            return BillingGatherResult(
                dates=frozenset(gathered),
                source_disposition=SourceGatherDisposition.STORAGE_UNAVAILABLE,
            )
        if isinstance(source_attempt, SourceAttemptBeginFailure):
            from core.preview.persistence import PreviewSourceAttemptFallbackUnitOfWork

            if not isinstance(uow, PreviewSourceAttemptFallbackUnitOfWork):
                raise RuntimeError("source attempt fallback storage is unavailable")
            fallback = uow.source_attempt_fallback.ensure_begin_failed(
                source_attempt,
                completed_at=datetime.now(UTC),
            )
            return BillingGatherResult(
                dates=frozenset(gathered),
                source_disposition=SourceGatherDisposition.BEGIN_FAILED,
                source_refresh_token=source_attempt.refresh_token,
                source_attempt_sequence=fallback.attempt_sequence,
                source_capture=None,
                source_failure=SourceCaptureFailure.ATTEMPT_BEGIN_FAILED,
            )
        return BillingGatherResult(
            dates=frozenset(gathered),
            source_disposition=SourceGatherDisposition.ATTEMPTED,
            source_refresh_token=source_attempt.refresh_token,
            source_attempt_sequence=source_attempt.attempt_sequence,
            source_capture=source_capture,
            source_failure=source_failure,
        )

    def _apply_recalculation_window(
        self, uow: UnitOfWork, gathered_billing_dates: set[date_type], plan: GatherPlan
    ) -> None:
        replacement_start, replacement_end = self._daily_replacement_date_window(plan)
        for billing_date in gathered_billing_dates:
            if replacement_start <= billing_date < replacement_end:
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
        result = self._calculate(
            uow,
            tracking_date,
            calculation_run_id=calculation_run_id,
            capture_lineage=False,
        )
        assert isinstance(result, int)
        return result

    def run_with_lineage_capture(
        self,
        uow: UnitOfWork,
        tracking_date: date_type,
        *,
        calculation_run_id: int | None = None,
    ) -> CalculationPhaseResult:
        """Calculate normally and return Preview lineage without persisting it."""
        result = self._calculate(
            uow,
            tracking_date,
            calculation_run_id=calculation_run_id,
            capture_lineage=True,
        )
        assert isinstance(result, CalculationPhaseResult)
        return result

    def _calculate(
        self,
        uow: UnitOfWork,
        tracking_date: date_type,
        *,
        calculation_run_id: int | None,
        capture_lineage: bool,
    ) -> int | CalculationPhaseResult:
        calculation_id = self._calculation_id_factory()
        if not calculation_id:
            raise ValueError("calculation_id must not be empty")
        billing_lines = uow.billing.find_by_date(self._ecosystem, self._tenant_id, tracking_date)
        lineage_captures = []
        lineage_failure = None
        if not billing_lines:
            total_rows = 0
        else:
            line_window_cache = self._compute_line_window_cache(billing_lines)
            billing_windows = self._compute_billing_windows(billing_lines, line_window_cache)
            plugin = self._bundle.plugin
            if isinstance(plugin, ScopeGatePlugin):
                scope_result = plugin.prepare_calculation_scope(
                    self._tenant_id,
                    sorted(billing_windows),
                    uow,
                )
                if scope_result.decision is not ScopeGateDecision.ALLOW:
                    raise ScopeBlockedError(scope_result)
            prefetched_metrics, failed_metric_keys = self._prefetch_metrics(billing_lines, line_window_cache)
            tenant_period_cache = self._build_tenant_period_cache(uow, billing_windows)
            resource_cache = self._build_resource_cache(uow, billing_windows)
            all_rows: list[ChargebackRow] = []
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
                if capture_lineage:
                    try:
                        lineage_captures.append(build_allocation_lineage_capture(origin=line, rows=tuple(rows)))
                    except (TypeError, ValueError) as exc:
                        lineage_failure = LineageCaptureFailureReason.CONSTRUCTION_FAILED
                        logger.warning(
                            "lineage_capture_failed%s",
                            safe_log_context(
                                tenant_id=self._tenant_id,
                                pipeline_run_id=calculation_run_id,
                                calculation_id=calculation_id,
                                tracking_date=tracking_date,
                                stage="lineage_capture",
                                outcome="capture_failed",
                                retryable=False,
                                **safe_exception_context(exc),
                            ),
                        )
            total_rows = uow.chargebacks.upsert_batch(all_rows)
        completed_at = self._completion_time()
        self._mark_success(
            uow,
            tracking_date,
            calculation_run_id,
            calculation_id=calculation_id,
            completed_at=completed_at,
        )
        if not capture_lineage:
            return total_rows
        capture = (
            None
            if lineage_failure is not None
            else AllocationLineageRunCapture(
                ecosystem=self._ecosystem,
                tenant_id=self._tenant_id,
                tracking_date=tracking_date,
                calculation_id=calculation_id,
                captures=tuple(lineage_captures),
            )
        )
        return CalculationPhaseResult(
            ecosystem=self._ecosystem,
            tenant_id=self._tenant_id,
            tracking_date=tracking_date,
            rows_written=total_rows,
            calculation_id=calculation_id,
            calculation_completed_at=completed_at,
            lineage_capture=capture,
            lineage_failure=lineage_failure,
        )

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
        self, billing_lines: Iterable[BillingLineItem]
    ) -> dict[int, tuple[datetime, datetime, timedelta]]:
        """Compute billing_window() once per line. Keyed by id(line)."""
        return {id(line): billing_window(line, self._merged_granularity_durations) for line in billing_lines}

    def plan_billing_windows(
        self,
        billing_lines: Sequence[BillingLineItem],
    ) -> tuple[tuple[datetime, datetime], ...]:
        """Plan distinct billing windows without deriving provider policy."""
        line_window_cache = self._compute_line_window_cache(billing_lines)
        return self._billing_windows_from_cache(billing_lines, line_window_cache)

    def _prefetch_metrics(
        self,
        billing_lines: Iterable[BillingLineItem],
        line_window_cache: Mapping[int, tuple[datetime, datetime, timedelta]],
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
                except Exception as exc:
                    resource_id, m_start, m_end = key
                    logger.warning(
                        "metrics_prefetch_failed window_start=%s window_end=%s%s",
                        m_start,
                        m_end,
                        safe_log_context(
                            tenant_id=self._tenant_id,
                            stage="metrics_prefetch",
                            outcome="skipped",
                            retryable=True,
                            resource_id=resource_id,
                            **safe_exception_context(exc),
                        ),
                    )
                    prefetched[key] = {}
                    failed_keys.add(key)

        return prefetched, frozenset(failed_keys)

    def _billing_windows_from_cache(
        self,
        billing_lines: Iterable[BillingLineItem],
        line_window_cache: Mapping[int, tuple[datetime, datetime, timedelta]],
    ) -> tuple[tuple[datetime, datetime], ...]:
        windows: set[tuple[datetime, datetime]] = set()
        for line in billing_lines:
            b_start, b_end, _ = line_window_cache[id(line)]
            windows.add((b_start, b_end))
        return tuple(sorted(windows))

    def _compute_billing_windows(
        self,
        billing_lines: Iterable[BillingLineItem],
        line_window_cache: Mapping[int, tuple[datetime, datetime, timedelta]],
    ) -> set[tuple[datetime, datetime]]:
        """Backward-compatible set-returning wrapper for legacy callers."""
        return set(self._billing_windows_from_cache(billing_lines, line_window_cache))

    def _build_tenant_period_cache(
        self, uow: UnitOfWork, billing_windows: Iterable[tuple[datetime, datetime]]
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
        self, uow: UnitOfWork, billing_windows: Iterable[tuple[datetime, datetime]]
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
        line_window_cache: Mapping[int, tuple[datetime, datetime, timedelta]],
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
                logger.warning(
                    "allocation_retry_counter_failed%s",
                    safe_log_context(
                        tenant_id=self._tenant_id,
                        stage="allocation_retry_counter",
                        outcome="failed",
                        retryable=True,
                        resource_id=line.resource_id,
                        product_type=line.product_type,
                        **safe_exception_context(retry_exc),
                    ),
                )
                raise exc from None

            if not should_fallback:
                logger.error(
                    "allocation_failed%s",
                    safe_log_context(
                        tenant_id=self._tenant_id,
                        stage="allocation",
                        outcome="date_failed",
                        retryable=True,
                        attempt_number=new_attempts,
                        resource_id=line.resource_id,
                        product_type=line.product_type,
                        **safe_exception_context(exc),
                    ),
                )
                raise

            logger.warning(
                "allocation_fallback_selected%s",
                safe_log_context(
                    tenant_id=self._tenant_id,
                    stage="allocation",
                    outcome="unallocated",
                    retryable=False,
                    attempt_number=new_attempts,
                    resource_id=line.resource_id,
                    product_type=line.product_type,
                    **safe_exception_context(exc),
                ),
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
        self._topic_overlay_provider_enabled = False
        ta_config = _get_ta_config(bundle.plugin)
        if ta_config and ta_config.enabled:
            from core.engine.topic_attribution import TopicAttributionPhase
            from core.engine.topic_attribution_models import TopicAttributionConfigProtocol

            topic_retry_manager = RetryManager(
                storage_backend=storage_backend,
                limit=tenant_config.topic_attribution_retry_limit,
                increment_fn=lambda uow, line: uow.billing.increment_topic_attribution_attempts(line),
            )
            provider = (
                bundle.plugin.get_topic_attribution_provider()
                if isinstance(bundle.plugin, TopicAttributionProviderPlugin)
                else None
            )
            if provider is not None:
                self._topic_overlay_provider_enabled = True
                self._topic_overlay_phase = TopicAttributionPhase(
                    ecosystem=self._ecosystem,
                    tenant_id=self._tenant_id,
                    metrics_source=metrics_source,
                    config=provider.config,
                    metrics_step=metrics_step,
                    retry_checker=topic_retry_manager,
                    provider=provider,
                )
            elif isinstance(ta_config, TopicAttributionConfigProtocol):
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

    def _prepare_preview_source_state(
        self,
        plan: GatherPlan,
    ) -> PreviewSourceAttempt | SourceAttemptBeginFailure | SourceEvidenceStorageUnavailable | None:
        if not self._tenant_config.focus_preview_enabled:
            return None
        from core.preview.persistence import PreviewEvidenceStorageBackend
        from core.preview.storage_availability import PreviewEvidenceAvailabilityState

        if (
            not isinstance(self._storage_backend, PreviewEvidenceStorageBackend)
            or self._storage_backend.preview_evidence_availability.state is not PreviewEvidenceAvailabilityState.READY
        ):
            return SourceEvidenceStorageUnavailable(
                ecosystem=self._ecosystem,
                tenant_id=self._tenant_id,
                refresh_start=plan.refresh_start,
                refresh_end=plan.refresh_end,
            )
        token = str(uuid.uuid4())
        try:
            with self._storage_backend.create_preview_evidence_unit_of_work() as evidence_uow:
                attempt = evidence_uow.source_readiness.begin_attempt(
                    self._ecosystem,
                    self._tenant_id,
                    token,
                    plan.refresh_start,
                    plan.refresh_end,
                    plan.now,
                )
                evidence_uow.commit()
            return attempt
        except Exception as exc:
            logger.warning(
                "Preview source attempt begin failed tenant=%s error_type=%s",
                self._tenant_id,
                type(exc).__name__,
            )
            return SourceAttemptBeginFailure(
                refresh_token=token,
                ecosystem=self._ecosystem,
                tenant_id=self._tenant_id,
                refresh_start=plan.refresh_start,
                refresh_end=plan.refresh_end,
                started_at=plan.now,
            )

    def _finalize_preview_source_failure(
        self,
        attempt_sequence: int,
        *,
        status: SourceAttemptFinalStatus,
        reason: SourceAttemptFailureReason,
    ) -> None:
        from core.preview.persistence import PreviewEvidenceStorageBackend

        if not isinstance(self._storage_backend, PreviewEvidenceStorageBackend):
            return
        try:
            with self._storage_backend.create_preview_evidence_unit_of_work() as evidence_uow:
                evidence_uow.source_readiness.finalize_attempt(
                    attempt_sequence,
                    status,
                    completed_at=datetime.now(UTC),
                    reason=reason,
                )
                evidence_uow.commit()
        except Exception as exc:
            logger.warning(
                "Preview source attempt finalization failed tenant=%s error_type=%s",
                self._tenant_id,
                type(exc).__name__,
            )

    def _persist_preview_source_capture(self, result: GatherResult) -> None:
        if result.source_disposition is not SourceGatherDisposition.ATTEMPTED:
            return
        assert result.source_attempt_sequence is not None
        if result.source_capture is None:
            assert result.source_failure is not None
            reason = {
                SourceCaptureFailure.CONSTRUCTION_FAILED: SourceAttemptFailureReason.CONSTRUCTION_FAILED,
                SourceCaptureFailure.CAPABILITY_UNAVAILABLE: SourceAttemptFailureReason.CAPABILITY_UNAVAILABLE,
                SourceCaptureFailure.ATTEMPT_BEGIN_FAILED: SourceAttemptFailureReason.ATTEMPT_BEGIN_FAILED,
            }[result.source_failure]
            self._finalize_preview_source_failure(
                result.source_attempt_sequence,
                status=SourceAttemptFinalStatus.FAILED,
                reason=reason,
            )
            return
        from core.preview.persistence import PreviewEvidenceStorageBackend

        if not isinstance(self._storage_backend, PreviewEvidenceStorageBackend):
            return
        try:
            with self._storage_backend.create_preview_evidence_unit_of_work() as evidence_uow:
                result.source_capture.persist(
                    evidence_uow.source_windows,
                    evidence_uow.source_readiness,
                    attempt_sequence=result.source_attempt_sequence,
                    captured_at=datetime.now(UTC),
                )
                evidence_uow.commit()
        except Exception as exc:
            logger.warning(
                "Preview source capture persistence failed tenant=%s error_type=%s",
                self._tenant_id,
                type(exc).__name__,
            )
            self._finalize_preview_source_failure(
                result.source_attempt_sequence,
                status=SourceAttemptFinalStatus.FAILED,
                reason=SourceAttemptFailureReason.PERSISTENCE_FAILED,
            )

    def _abort_preview_source_state(
        self,
        source_state: PreviewSourceAttempt | SourceAttemptBeginFailure | SourceEvidenceStorageUnavailable | None,
        *,
        reason: SourceAttemptFailureReason,
    ) -> None:
        from core.preview.persistence import PreviewEvidenceStorageBackend

        if not isinstance(self._storage_backend, PreviewEvidenceStorageBackend):
            return
        try:
            with self._storage_backend.create_preview_evidence_unit_of_work() as evidence_uow:
                attempt: PreviewSourceAttempt | None
                if isinstance(source_state, PreviewSourceAttempt):
                    attempt = source_state
                elif isinstance(source_state, SourceAttemptBeginFailure):
                    attempt = evidence_uow.source_readiness.get_by_token(
                        source_state.ecosystem,
                        source_state.tenant_id,
                        source_state.refresh_token,
                    )
                    if attempt is None or attempt.status is not SourceAttemptStatus.PENDING:
                        return
                else:
                    return
                evidence_uow.source_readiness.finalize_attempt(
                    attempt.attempt_sequence,
                    SourceAttemptFinalStatus.ABORTED,
                    completed_at=datetime.now(UTC),
                    reason=reason,
                )
                evidence_uow.commit()
        except Exception as exc:
            logger.warning(
                "Preview source attempt abort failed tenant=%s error_type=%s",
                self._tenant_id,
                type(exc).__name__,
            )

    def _refresh_preview_organization_authority(self) -> None:
        if not self._tenant_config.focus_preview_enabled:
            return
        from core.preview.persistence import PreviewEvidenceStorageBackend

        if not isinstance(self._storage_backend, PreviewEvidenceStorageBackend):
            return
        evidence_backend = self._storage_backend
        started_at = datetime.now(UTC)
        try:
            with evidence_backend.create_preview_evidence_unit_of_work() as evidence_uow:
                attempt = evidence_uow.organization_authority.begin(self._ecosystem, self._tenant_id, started_at)
                evidence_uow.commit()
        except Exception as exc:
            logger.warning(
                "Preview organization authority begin failed tenant=%s error_type=%s",
                self._tenant_id,
                type(exc).__name__,
            )
            return

        status = OrganizationAuthorityFinalStatus.UNAVAILABLE
        reason: OrganizationAuthorityFailureReason | None = OrganizationAuthorityFailureReason.CAPABILITY_UNAVAILABLE
        organization_id = None
        plugin = self._bundle.plugin
        if isinstance(plugin, PreviewOrganizationGatherer):
            try:
                resources = plugin.gather_preview_organizations(self._tenant_id)
            except Exception as exc:
                reason = OrganizationAuthorityFailureReason.PROVIDER_ERROR
                logger.warning(
                    "Preview organization provider call failed tenant=%s error_type=%s",
                    self._tenant_id,
                    type(exc).__name__,
                )
            else:
                valid = tuple(resource for resource in resources if resource.resource_id.strip())
                if len(resources) > 1:
                    status = OrganizationAuthorityFinalStatus.CONFLICTING
                    reason = OrganizationAuthorityFailureReason.INVALID_CARDINALITY
                elif len(valid) != 1:
                    reason = OrganizationAuthorityFailureReason.INVALID_CARDINALITY
                else:
                    binding_conflict = False
                    try:
                        with self._storage_backend.create_unit_of_work() as resource_uow:
                            try:
                                self._gather_phase._reconcile_organization_resources(
                                    resource_uow, valid, datetime.now(UTC)
                                )
                            except PreviewOrganizationBindingConflictError:
                                binding_conflict = True
                            resource_uow.commit()
                    except Exception as exc:
                        reason = OrganizationAuthorityFailureReason.RESOURCE_PERSISTENCE_FAILED
                        logger.warning(
                            "Preview organization resource persistence failed tenant=%s error_type=%s",
                            self._tenant_id,
                            type(exc).__name__,
                        )
                    else:
                        if binding_conflict:
                            status = OrganizationAuthorityFinalStatus.CONFLICTING
                            reason = OrganizationAuthorityFailureReason.BINDING_CONFLICT
                        else:
                            status = OrganizationAuthorityFinalStatus.AVAILABLE
                            reason = None
                            organization_id = valid[0].resource_id
        completed_at = datetime.now(UTC)
        for finalize_attempt in range(2):
            try:
                with evidence_backend.create_preview_evidence_unit_of_work() as evidence_uow:
                    evidence_uow.organization_authority.finalize(
                        attempt.attempt_sequence,
                        status,
                        completed_at=completed_at,
                        organization_id=organization_id,
                        reason=reason,
                    )
                    evidence_uow.commit()
                break
            except Exception as exc:
                if finalize_attempt == 0:
                    continue
                logger.warning(
                    "Preview organization authority finalization failed tenant=%s error_type=%s",
                    self._tenant_id,
                    type(exc).__name__,
                )

    def _lineage_unavailable(
        self,
        result: CalculationPhaseResult,
        reason: AllocationLineageUnavailableReason,
    ) -> AllocationLineageUnavailableRun:
        return AllocationLineageUnavailableRun(
            ecosystem=result.ecosystem,
            tenant_id=result.tenant_id,
            tracking_date=result.tracking_date,
            calculation_id=result.calculation_id,
            calculation_completed_at=result.calculation_completed_at,
            status=AllocationLineageRunStatus.UNAVAILABLE,
            reason=reason,
        )

    def _persist_preview_lineage(
        self,
        result: CalculationPhaseResult,
        *,
        calculation_run_id: int | None = None,
    ) -> None:
        from core.preview.persistence import PreviewEvidenceStorageBackend

        if not isinstance(self._storage_backend, PreviewEvidenceStorageBackend):
            return
        evidence_backend = self._storage_backend
        try:
            with evidence_backend.create_preview_evidence_unit_of_work() as evidence_uow:
                if result.lineage_capture is not None:
                    evidence_uow.allocation_lineage.replace_calculation_lineage(
                        result.lineage_capture,
                        calculation_completed_at=result.calculation_completed_at,
                    )
                else:
                    evidence_uow.allocation_lineage.mark_calculation_lineage_unavailable(
                        self._lineage_unavailable(result, AllocationLineageUnavailableReason.CAPTURE_FAILED)
                    )
                evidence_uow.commit()
            return
        except Exception as exc:
            persistence_error = exc
        logger.warning(
            "lineage_persistence_failed db_root_cause=%s%s",
            _safe_database_root_cause(persistence_error),
            safe_log_context(
                tenant_name=self._tenant_name,
                tenant_id=result.tenant_id,
                pipeline_run_id=calculation_run_id,
                calculation_id=result.calculation_id,
                tracking_date=result.tracking_date,
                stage="lineage_persistence",
                outcome="mark_unavailable",
                retryable=False,
                **safe_exception_context(persistence_error),
            ),
        )
        try:
            with evidence_backend.create_preview_evidence_unit_of_work() as evidence_uow:
                evidence_uow.allocation_lineage.mark_calculation_lineage_unavailable(
                    self._lineage_unavailable(result, AllocationLineageUnavailableReason.PERSISTENCE_FAILED)
                )
                evidence_uow.commit()
            logger.warning(
                "lineage_fallback_persisted reason=%s%s",
                AllocationLineageUnavailableReason.PERSISTENCE_FAILED.value,
                safe_log_context(
                    tenant_name=self._tenant_name,
                    tenant_id=result.tenant_id,
                    pipeline_run_id=calculation_run_id,
                    calculation_id=result.calculation_id,
                    tracking_date=result.tracking_date,
                    stage="lineage_persistence",
                    outcome="lineage_unavailable",
                    retryable=False,
                ),
            )
        except Exception as exc:
            logger.error(
                "Preview allocation lineage fallback failed primary_error_type=%s%s",
                type(persistence_error).__name__,
                safe_log_context(
                    tenant_name=self._tenant_name,
                    tenant_id=result.tenant_id,
                    pipeline_run_id=calculation_run_id,
                    calculation_id=result.calculation_id,
                    tracking_date=result.tracking_date,
                    stage="lineage_persistence",
                    outcome="fallback_failed",
                    retryable=False,
                    **safe_exception_context(exc),
                ),
            )

    def repair_historical_date(self, tracking_date: date_type) -> HistoricalRepairDateResult:
        from core.storage.interface import HistoricalRepairBillingWriter

        start = datetime.combine(tracking_date, datetime.min.time(), tzinfo=UTC)
        end = start + timedelta(days=1)
        cost_input = self._bundle.plugin.get_cost_input()
        if not isinstance(cost_input, NativeSourceEvidenceCostInput):
            raise HistoricalRepairProviderSourceError("historical provider source capture is unavailable")
        try:
            native = cost_input.gather_with_native_source_evidence(self._tenant_id, start, end)
        except Exception as exc:
            raise HistoricalRepairProviderSourceError("historical provider source acquisition failed") from exc
        if native.capture is None:
            raise HistoricalRepairProviderSourceError("historical provider source capture is unavailable")
        with self._storage_backend.create_unit_of_work() as uow:
            billing = uow.billing
            if not isinstance(billing, HistoricalRepairBillingWriter):
                raise RuntimeError("historical repair billing replacement is unavailable")
            billing_rows_written = billing.replace_for_date(
                self._ecosystem,
                self._tenant_id,
                tracking_date,
                native.billing_lines,
            )
            uow.chargebacks.delete_by_date(self._ecosystem, self._tenant_id, tracking_date)
            uow.topic_attributions.delete_by_date(
                self._ecosystem,
                self._tenant_id,
                tracking_date,
            )
            uow.pipeline_state.mark_needs_recalculation(
                self._ecosystem,
                self._tenant_id,
                tracking_date,
            )
            billing.reset_allocation_attempts_by_date(
                self._ecosystem,
                self._tenant_id,
                tracking_date,
            )
            billing.reset_topic_attribution_attempts_by_date(
                self._ecosystem,
                self._tenant_id,
                tracking_date,
            )
            calculation = self._calculate_phase.run_with_lineage_capture(
                uow,
                tracking_date,
                calculation_run_id=None,
            )
            uow.commit()
        return HistoricalRepairDateResult(
            source_capture=native.capture,
            calculation=calculation,
            billing_rows_written=billing_rows_written,
        )

    def run(self, *, calculation_run_id: int | None = None) -> PipelineRunResult:
        errors: list[str] = []
        dates_gathered = 0
        dates_calculated = 0
        chargeback_rows_written = 0
        scope_blocked = False
        recovery_result: ScopeGateResult | None = None
        recovery_target_dates: set[date_type] = set()
        completed_pending_dates: set[date_type] = set()
        overlay_recovery_target_dates: set[date_type] = set()
        completed_overlay_recovery_dates: set[date_type] = set()
        recovery_targets_checked = False
        recovery_active = False
        recovery_interrupted = False
        topic_attribution_inventory_ready = False
        terminal_scope_blocked = False
        progress_started = False

        logger.info(
            "pipeline_orchestration_started%s",
            safe_log_context(
                tenant_name=self._tenant_name,
                tenant_id=self._tenant_id,
                pipeline_run_id=calculation_run_id,
                stage="gather",
                operation="pipeline_run",
                outcome="started",
            ),
        )

        plugin = self._bundle.plugin
        defer_progress_until_scope = isinstance(plugin, ScopeGatePlugin) and isinstance(
            plugin,
            ScopeGateRunLifecycle,
        )
        if isinstance(plugin, ScopeGateRunLifecycle):
            plugin.begin_scope_gate_run()

        if getattr(self, "_topic_overlay_provider_enabled", False) and isinstance(
            plugin,
            TopicAttributionProviderPlugin,
        ):
            reset_inventory_proof = getattr(plugin, "reset_topic_attribution_inventory_proof", None)
            if callable(reset_inventory_proof):
                reset_inventory_proof()

        if not defer_progress_until_scope:
            self._report_progress("gathering")
            progress_started = True
        plan = self._gather_phase.plan_refresh(datetime.now(UTC))
        source_state: PreviewSourceAttempt | SourceAttemptBeginFailure | SourceEvidenceStorageUnavailable | None = None
        gather_completed = False
        try:
            if plan.should_refresh:
                effective_plan = plan
                if isinstance(plugin, ScopeGatePlugin):
                    with self._storage_backend.create_unit_of_work() as scope_uow:
                        scope_result = plugin.prepare_gather_scope(
                            self._tenant_id,
                            plan.refresh_start,
                            plan.refresh_end,
                            scope_uow,
                        )
                    if scope_result.decision is ScopeGateDecision.BLOCKED:
                        with self._storage_backend.create_unit_of_work() as persistence_uow:
                            plugin.persist_scope_probe(self._tenant_id, scope_result, persistence_uow)
                            persistence_uow.commit()
                        errors.append(f"Gather scope blocked: {scope_result.detail}")
                        gather_result = GatherResult(dates_gathered=0, errors=[errors[-1]])
                        scope_blocked = True
                        if defer_progress_until_scope and scope_result.probe_only:
                            terminal_scope_blocked = True
                    elif scope_result.decision is ScopeGateDecision.RECOVERY_READY:
                        with self._storage_backend.create_unit_of_work() as persistence_uow:
                            plugin.persist_scope_recovery(self._tenant_id, scope_result, persistence_uow)
                            persistence_uow.commit()
                        recovery_result = scope_result
                        recovery_active = (
                            scope_result.recovery_start is not None and scope_result.recovery_end is not None
                        )
                        if scope_result.recovery_start is not None:
                            effective_plan = replace(plan, refresh_start=scope_result.recovery_start)
                        if isinstance(plugin, PostRecoveryGatherScopeValidator):
                            try:
                                with self._storage_backend.create_unit_of_work() as validation_uow:
                                    validation_result = plugin.prepare_post_recovery_gather_scope(
                                        self._tenant_id,
                                        effective_plan.refresh_start,
                                        effective_plan.refresh_end,
                                        validation_uow,
                                    )
                            except ScopeBlockedError as exc:
                                validation_result = exc.result
                            if validation_result.decision is not ScopeGateDecision.ALLOW:
                                with self._storage_backend.create_unit_of_work() as persistence_uow:
                                    plugin.persist_scope_blocked(self._tenant_id, validation_result, persistence_uow)
                                    persistence_uow.commit()
                                errors.append(f"Gather scope blocked: {validation_result.detail}")
                                terminal_scope_blocked = True
                                scope_blocked = True
                                gather_result = GatherResult(dates_gathered=0, errors=[errors[-1]])
                            else:
                                if defer_progress_until_scope:
                                    self._report_progress("gathering")
                                    progress_started = True
                    elif scope_result.recovery_start is not None and scope_result.recovery_end is not None:
                        recovery_result = scope_result
                        recovery_active = True
                        effective_plan = replace(plan, refresh_start=scope_result.recovery_start)
                if not scope_blocked:
                    if defer_progress_until_scope and not progress_started:
                        self._report_progress("gathering")
                        progress_started = True
                    source_state = self._prepare_preview_source_state(effective_plan)
                    with self._storage_backend.create_unit_of_work() as uow:
                        gather_result = self._gather_phase.run(
                            uow,
                            plan=effective_plan,
                            source_attempt=source_state,
                        )
                        dates_gathered = gather_result.dates_gathered
                        errors.extend(gather_result.errors)
                        topic_attribution_inventory_ready = gather_result.topic_attribution_inventory_ready
                        if recovery_active and gather_result.errors:
                            recovery_interrupted = True
                        gather_completed = True
                        uow.commit()
                    self._gather_phase.accept_refresh(effective_plan)
                    self._persist_preview_source_capture(gather_result)
            else:
                gather_result = GatherResult(dates_gathered=0, errors=[], skipped=True)
            self._consecutive_gather_failures = 0
            if not gather_result.skipped:
                self._refresh_preview_organization_authority()
            logger.info(
                "gather_completed dates_gathered=%d error_count=%d%s",
                dates_gathered,
                len(gather_result.errors),
                safe_log_context(
                    tenant_name=self._tenant_name,
                    tenant_id=self._tenant_id,
                    pipeline_run_id=calculation_run_id,
                    stage="gather",
                    operation="pipeline_run",
                    outcome="skipped" if gather_result.skipped else "completed",
                ),
            )
        except ScopeBlockedError as exc:
            plugin = self._bundle.plugin
            if isinstance(plugin, ScopeGatePlugin):
                with self._storage_backend.create_unit_of_work() as persistence_uow:
                    if exc.result.probe_only:
                        plugin.persist_scope_probe(self._tenant_id, exc.result, persistence_uow)
                    else:
                        plugin.persist_scope_blocked(self._tenant_id, exc.result, persistence_uow)
                    persistence_uow.commit()
            errors.append(f"Gather scope blocked: {exc.result.detail}")
            with self._storage_backend.create_unit_of_work() as pending_uow:
                pending_count = len(
                    pending_uow.pipeline_state.find_needing_calculation(self._ecosystem, self._tenant_id)
                )
            return PipelineRunResult(
                tenant_name=self._tenant_name,
                tenant_id=self._tenant_id,
                dates_gathered=0,
                dates_calculated=0,
                chargeback_rows_written=0,
                dates_pending_calculation=pending_count,
                errors=errors,
            )
        except Exception as exc:
            self._abort_preview_source_state(
                source_state,
                reason=(
                    SourceAttemptFailureReason.GENERIC_COMMIT_FAILED
                    if gather_completed
                    else SourceAttemptFailureReason.GENERIC_GATHER_FAILED
                ),
            )
            logger.error(
                "gather_failed%s",
                safe_log_context(
                    tenant_name=self._tenant_name,
                    tenant_id=self._tenant_id,
                    pipeline_run_id=calculation_run_id,
                    stage="gather",
                    operation="pipeline_run",
                    outcome="failed",
                    retryable=True,
                    **safe_exception_context(exc),
                ),
            )
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

        if terminal_scope_blocked:
            with self._storage_backend.create_unit_of_work() as pending_uow:
                pending_count = len(
                    pending_uow.pipeline_state.find_needing_calculation(self._ecosystem, self._tenant_id)
                )
            return PipelineRunResult(
                tenant_name=self._tenant_name,
                tenant_id=self._tenant_id,
                dates_gathered=0,
                dates_calculated=0,
                chargeback_rows_written=0,
                dates_pending_calculation=pending_count,
                errors=errors,
            )

        pending_states = []
        calculation_scope_blocked = scope_blocked
        plugin = self._bundle.plugin
        try:
            with self._storage_backend.create_unit_of_work() as uow:
                pending_states = uow.pipeline_state.find_needing_calculation(self._ecosystem, self._tenant_id)
                if recovery_active and recovery_result is not None:
                    recovery_target_dates.update(_recovery_target_dates(pending_states, recovery_result))
                    recovery_targets_checked = True
                if not calculation_scope_blocked and isinstance(plugin, ScopeGatePlugin):
                    for pipeline_state in pending_states:
                        billing_lines = uow.billing.find_by_date(
                            self._ecosystem,
                            self._tenant_id,
                            pipeline_state.tracking_date,
                        )
                        if not billing_lines:
                            continue
                        billing_windows = self._calculate_phase.plan_billing_windows(billing_lines)
                        scope_result = plugin.prepare_calculation_scope(
                            self._tenant_id,
                            sorted(billing_windows),
                            uow,
                        )
                        if scope_result.recovery_start is not None and scope_result.recovery_end is not None:
                            recovery_result = scope_result
                            recovery_active = True
                            recovery_target_dates.update(_recovery_target_dates([pipeline_state], scope_result))
                            recovery_targets_checked = True
                        if scope_result.decision is not ScopeGateDecision.ALLOW:
                            raise ScopeBlockedError(scope_result)
        except ScopeBlockedError as exc:
            if isinstance(plugin, ScopeGatePlugin):
                with self._storage_backend.create_unit_of_work() as persistence_uow:
                    if exc.result.decision is ScopeGateDecision.RECOVERY_READY:
                        plugin.persist_scope_recovery(self._tenant_id, exc.result, persistence_uow)
                    elif exc.result.probe_only:
                        plugin.persist_scope_probe(self._tenant_id, exc.result, persistence_uow)
                    else:
                        plugin.persist_scope_blocked(self._tenant_id, exc.result, persistence_uow)
                    persistence_uow.commit()
            if exc.result.decision is ScopeGateDecision.RECOVERY_READY:
                recovery_result = exc.result
                recovery_active = exc.result.recovery_start is not None and exc.result.recovery_end is not None
                with self._storage_backend.create_unit_of_work() as refreshed_uow:
                    pending_states = refreshed_uow.pipeline_state.find_needing_calculation(
                        self._ecosystem,
                        self._tenant_id,
                    )
                recovery_target_dates.update(_recovery_target_dates(pending_states, recovery_result))
                recovery_targets_checked = True
                if defer_progress_until_scope and isinstance(plugin, ScopeGatePlugin):
                    try:
                        with self._storage_backend.create_unit_of_work() as validation_uow:
                            for pipeline_state in pending_states:
                                billing_lines = validation_uow.billing.find_by_date(
                                    self._ecosystem,
                                    self._tenant_id,
                                    pipeline_state.tracking_date,
                                )
                                if not billing_lines:
                                    continue
                                validation = plugin.prepare_calculation_scope(
                                    self._tenant_id,
                                    self._calculate_phase.plan_billing_windows(billing_lines),
                                    validation_uow,
                                )
                                if validation.decision is not ScopeGateDecision.ALLOW:
                                    raise ScopeBlockedError(validation)
                    except ScopeBlockedError as validation_exc:
                        with self._storage_backend.create_unit_of_work() as persistence_uow:
                            plugin.persist_scope_blocked(
                                self._tenant_id,
                                validation_exc.result,
                                persistence_uow,
                            )
                            persistence_uow.commit()
                        errors.append(f"Calculate scope blocked: {validation_exc.result.detail}")
                        calculation_scope_blocked = True
                        terminal_scope_blocked = True
            else:
                recovery_interrupted = True
                errors.append(f"Calculate scope blocked: {exc.result.detail}")
                calculation_scope_blocked = True
                if defer_progress_until_scope and exc.result.probe_only:
                    terminal_scope_blocked = True

        calculated_pending_states = pending_states if not calculation_scope_blocked else []
        for pipeline_state in calculated_pending_states:
            if self._shutdown_check is not None and self._shutdown_check():
                recovery_interrupted = recovery_interrupted or recovery_active
                logger.info(
                    "Shutdown requested — stopping after %d dates processed for %s",
                    dates_calculated,
                    self._tenant_name,
                )
                break

            tracking_date = pipeline_state.tracking_date
            self._report_progress("calculating", tracking_date)
            progress_started = True
            logger.info(
                "calculation_started%s",
                safe_log_context(
                    tenant_name=self._tenant_name,
                    tenant_id=self._tenant_id,
                    pipeline_run_id=calculation_run_id,
                    tracking_date=tracking_date,
                    stage="calculate",
                    operation="calculation",
                    outcome="started",
                ),
            )
            start_time = time.time()
            try:
                calculation_result: CalculationPhaseResult | None = None
                with self._storage_backend.create_unit_of_work() as uow:
                    if self._tenant_config.focus_preview_enabled:
                        calculation_result = self._calculate_phase.run_with_lineage_capture(
                            uow,
                            tracking_date,
                            calculation_run_id=calculation_run_id,
                        )
                        rows = calculation_result.rows_written
                    elif calculation_run_id is None:
                        rows = self._calculate_phase.run(uow, tracking_date)
                    else:
                        rows = self._calculate_phase.run(
                            uow,
                            tracking_date,
                            calculation_run_id=calculation_run_id,
                        )
                    uow.commit()
                chargeback_rows_written += rows
                dates_calculated += 1
                completed_pending_dates.add(tracking_date)
                if calculation_result is not None:
                    self._persist_preview_lineage(
                        calculation_result,
                        calculation_run_id=calculation_run_id,
                    )
                elapsed = int(time.time() - start_time)
                logger.info(
                    "calculation_completed rows_written=%d elapsed_seconds=%d%s",
                    rows,
                    elapsed,
                    safe_log_context(
                        tenant_name=self._tenant_name,
                        tenant_id=self._tenant_id,
                        pipeline_run_id=calculation_run_id,
                        calculation_id=(calculation_result.calculation_id if calculation_result is not None else None),
                        tracking_date=tracking_date,
                        stage="calculate",
                        operation="calculation",
                        outcome="completed",
                    ),
                )
            except ScopeBlockedError as exc:
                plugin = self._bundle.plugin
                if isinstance(plugin, ScopeGatePlugin):
                    with self._storage_backend.create_unit_of_work() as persistence_uow:
                        if exc.result.decision is ScopeGateDecision.RECOVERY_READY:
                            plugin.persist_scope_recovery(self._tenant_id, exc.result, persistence_uow)
                            recovery_result = exc.result
                            recovery_active = (
                                exc.result.recovery_start is not None and exc.result.recovery_end is not None
                            )
                        elif exc.result.probe_only:
                            plugin.persist_scope_probe(self._tenant_id, exc.result, persistence_uow)
                        else:
                            plugin.persist_scope_blocked(self._tenant_id, exc.result, persistence_uow)
                        persistence_uow.commit()
                recovery_interrupted = True
                errors.append(f"Calculate scope blocked for date {tracking_date}: {exc.result.detail}")
                break
            except Exception as exc:
                logger.error(
                    "calculation_failed%s",
                    safe_log_context(
                        tenant_name=self._tenant_name,
                        tenant_id=self._tenant_id,
                        pipeline_run_id=calculation_run_id,
                        tracking_date=tracking_date,
                        stage="calculate",
                        operation="calculation",
                        outcome="failed",
                        retryable=True,
                        **safe_exception_context(exc),
                    ),
                )
                recovery_interrupted = True
                errors.append(f"Calculate failed for date {tracking_date}: {exc}")

        if (
            recovery_result is not None
            and recovery_active
            and recovery_targets_checked
            and recovery_target_dates
            and recovery_target_dates.issubset(completed_pending_dates)
            and not recovery_interrupted
            and not errors
            and isinstance(plugin, ScopeGatePlugin)
            and not getattr(self, "_topic_overlay_provider_enabled", False)
        ):
            with self._storage_backend.create_unit_of_work() as persistence_uow:
                plugin.persist_scope_closed(self._tenant_id, recovery_result, persistence_uow)
                persistence_uow.commit()

        if self._topic_overlay_phase is not None and not terminal_scope_blocked:
            with self._storage_backend.create_unit_of_work() as uow:
                overlay_pending = uow.pipeline_state.find_needing_topic_attribution(
                    self._ecosystem,
                    self._tenant_id,
                )
            overlay_inventory_unavailable = (
                getattr(self, "_topic_overlay_provider_enabled", False) and not topic_attribution_inventory_ready
            )
            if overlay_inventory_unavailable and defer_progress_until_scope:
                inventory_ready = getattr(plugin, "topic_attribution_inventory_ready", None)
                if callable(inventory_ready) and inventory_ready(None):
                    topic_attribution_inventory_ready = True
                    overlay_inventory_unavailable = False
            if overlay_inventory_unavailable and overlay_pending:
                logger.info(
                    "topic_overlay_waiting_for_current_inventory tenant_id=%s pending_dates=%d",
                    self._tenant_id,
                    len(overlay_pending),
                )

            chunk_provider: ChunkedTopicEvidenceProvider | None = None
            if isinstance(plugin, TopicAttributionProviderPlugin):
                candidate_provider = plugin.get_topic_attribution_provider()
                if isinstance(candidate_provider, ChunkedTopicEvidenceProvider):
                    chunk_provider = candidate_provider
            chunk_by_date: dict[date_type, tuple[tuple[datetime, datetime], ...]] = {}
            chunk_by_window: dict[tuple[datetime, datetime], tuple[tuple[datetime, datetime], ...]] = {}
            if chunk_provider is not None and overlay_pending:
                planned_windows: list[tuple[datetime, datetime]] = []
                with self._storage_backend.create_unit_of_work() as planning_uow:
                    for pending_state in overlay_pending:
                        lines = planning_uow.billing.find_by_date(
                            self._ecosystem,
                            self._tenant_id,
                            pending_state.tracking_date,
                        )
                        windows_for_date = self._calculate_phase.plan_billing_windows(lines)
                        if windows_for_date:
                            chunk_by_date[pending_state.tracking_date] = windows_for_date
                            planned_windows.extend(windows_for_date)
                unique_windows = tuple(dict.fromkeys(planned_windows))
                for evidence_chunk in chunk_provider.iter_evidence_chunks(unique_windows):
                    for window in evidence_chunk:
                        chunk_by_window[window] = evidence_chunk
            active_evidence_chunk: tuple[tuple[datetime, datetime], ...] | None = None
            overlay_scope_preflight_complete = False
            if (
                defer_progress_until_scope
                and chunk_provider is not None
                and overlay_pending
                and isinstance(plugin, ScopeGatePlugin)
            ):
                try:
                    with self._storage_backend.create_unit_of_work() as scope_uow:
                        scope_result = plugin.prepare_calculation_scope(
                            self._tenant_id,
                            tuple(dict.fromkeys(planned_windows)),
                            scope_uow,
                        )
                    if scope_result.decision is ScopeGateDecision.RECOVERY_READY:
                        with self._storage_backend.create_unit_of_work() as persistence_uow:
                            plugin.persist_scope_recovery(self._tenant_id, scope_result, persistence_uow)
                            persistence_uow.commit()
                        recovery_result = scope_result
                        recovery_active = (
                            scope_result.recovery_start is not None and scope_result.recovery_end is not None
                        )
                        try:
                            with self._storage_backend.create_unit_of_work() as validation_uow:
                                scope_result = plugin.prepare_calculation_scope(
                                    self._tenant_id,
                                    tuple(dict.fromkeys(planned_windows)),
                                    validation_uow,
                                )
                        except ScopeBlockedError as exc:
                            scope_result = exc.result
                    if scope_result.decision is ScopeGateDecision.ALLOW:
                        if scope_result.recovery_start is not None and scope_result.recovery_end is not None:
                            recovery_result = scope_result
                            recovery_active = True
                            overlay_recovery_target_dates.update(_recovery_target_dates(overlay_pending, scope_result))
                            recovery_targets_checked = True
                        overlay_scope_preflight_complete = True
                    else:
                        with self._storage_backend.create_unit_of_work() as persistence_uow:
                            if scope_result.probe_only:
                                plugin.persist_scope_probe(self._tenant_id, scope_result, persistence_uow)
                            else:
                                plugin.persist_scope_blocked(self._tenant_id, scope_result, persistence_uow)
                            persistence_uow.commit()
                        errors.append(f"Topic overlay scope blocked: {scope_result.detail}")
                        terminal_scope_blocked = terminal_scope_blocked or scope_result.probe_only
                        recovery_interrupted = True
                except ScopeBlockedError as exc:
                    with self._storage_backend.create_unit_of_work() as persistence_uow:
                        if exc.result.probe_only:
                            plugin.persist_scope_probe(self._tenant_id, exc.result, persistence_uow)
                        else:
                            plugin.persist_scope_blocked(self._tenant_id, exc.result, persistence_uow)
                        persistence_uow.commit()
                    errors.append(f"Topic overlay scope blocked: {exc.result.detail}")
                    terminal_scope_blocked = terminal_scope_blocked or exc.result.probe_only
                    recovery_interrupted = True
            if (
                getattr(self, "_topic_overlay_provider_enabled", False)
                and recovery_active
                and recovery_result is not None
            ):
                overlay_recovery_target_dates.update(_recovery_target_dates(overlay_pending, recovery_result))
                recovery_targets_checked = True

            try:
                for pipeline_state in overlay_pending:
                    if overlay_inventory_unavailable:
                        continue
                    if self._shutdown_check is not None and self._shutdown_check():
                        recovery_interrupted = recovery_interrupted or recovery_active
                        break
                    tracking_date = pipeline_state.tracking_date
                    if not defer_progress_until_scope:
                        self._report_progress("topic_overlay", tracking_date)
                        progress_started = True
                    logger.info("Running topic attribution for date: %s", tracking_date)
                    if (
                        getattr(self, "_topic_overlay_provider_enabled", False)
                        and isinstance(plugin, ScopeGatePlugin)
                        and not overlay_scope_preflight_complete
                    ):
                        try:
                            with self._storage_backend.create_unit_of_work() as scope_uow:
                                billing_lines = scope_uow.billing.find_by_date(
                                    self._ecosystem,
                                    self._tenant_id,
                                    tracking_date,
                                )
                                billing_windows = self._calculate_phase.plan_billing_windows(billing_lines)
                                scope_result = plugin.prepare_calculation_scope(
                                    self._tenant_id,
                                    sorted(billing_windows),
                                    scope_uow,
                                )
                        except ScopeBlockedError as exc:
                            scope_result = exc.result

                        if scope_result.decision is ScopeGateDecision.RECOVERY_READY:
                            with self._storage_backend.create_unit_of_work() as persistence_uow:
                                plugin.persist_scope_recovery(self._tenant_id, scope_result, persistence_uow)
                                persistence_uow.commit()
                            recovery_result = scope_result
                            recovery_active = (
                                scope_result.recovery_start is not None and scope_result.recovery_end is not None
                            )
                            if recovery_active:
                                overlay_recovery_target_dates.update(
                                    _recovery_target_dates(overlay_pending, scope_result)
                                )
                                recovery_targets_checked = True
                            if defer_progress_until_scope:
                                try:
                                    with self._storage_backend.create_unit_of_work() as validation_uow:
                                        validation = plugin.prepare_calculation_scope(
                                            self._tenant_id,
                                            self._calculate_phase.plan_billing_windows(
                                                validation_uow.billing.find_by_date(
                                                    self._ecosystem,
                                                    self._tenant_id,
                                                    tracking_date,
                                                )
                                            ),
                                            validation_uow,
                                        )
                                        if validation.decision is not ScopeGateDecision.ALLOW:
                                            raise ScopeBlockedError(validation)
                                except ScopeBlockedError as validation_exc:
                                    with self._storage_backend.create_unit_of_work() as persistence_uow:
                                        plugin.persist_scope_blocked(
                                            self._tenant_id,
                                            validation_exc.result,
                                            persistence_uow,
                                        )
                                        persistence_uow.commit()
                                    recovery_interrupted = True
                                    terminal_scope_blocked = True
                                    errors.append(
                                        f"Topic overlay scope blocked for date {tracking_date}: "
                                        f"{validation_exc.result.detail}"
                                    )
                                    break
                        elif scope_result.decision is not ScopeGateDecision.ALLOW:
                            with self._storage_backend.create_unit_of_work() as persistence_uow:
                                if scope_result.probe_only:
                                    plugin.persist_scope_probe(self._tenant_id, scope_result, persistence_uow)
                                else:
                                    plugin.persist_scope_blocked(self._tenant_id, scope_result, persistence_uow)
                                persistence_uow.commit()
                            recovery_interrupted = recovery_interrupted or recovery_active
                            errors.append(
                                f"Topic overlay scope blocked for date {tracking_date}: {scope_result.detail}"
                            )
                            break
                        elif scope_result.recovery_start is not None and scope_result.recovery_end is not None:
                            recovery_result = scope_result
                            recovery_active = True
                            overlay_recovery_target_dates.update(_recovery_target_dates(overlay_pending, scope_result))
                            recovery_targets_checked = True
                        if defer_progress_until_scope:
                            self._report_progress("topic_overlay", tracking_date)
                            progress_started = True
                    elif defer_progress_until_scope and overlay_scope_preflight_complete:
                        self._report_progress("topic_overlay", tracking_date)
                        progress_started = True
                    if chunk_provider is not None:
                        windows_for_date = chunk_by_date.get(tracking_date, ())
                        target_chunk = next(
                            (chunk_by_window[window] for window in windows_for_date if window in chunk_by_window),
                            (),
                        )
                        if target_chunk != active_evidence_chunk:
                            if active_evidence_chunk is not None:
                                chunk_provider.clear_evidence_chunk()
                                active_evidence_chunk = None
                            if target_chunk:
                                try:
                                    chunk_provider.prepare_evidence_chunk(target_chunk, self._metrics_step)
                                except Exception:
                                    chunk_provider.clear_evidence_chunk()
                                    raise
                                active_evidence_chunk = target_chunk
                    try:
                        with self._storage_backend.create_unit_of_work() as uow:
                            rows = self._topic_overlay_phase.run(uow, tracking_date)
                            uow.commit()
                        logger.info("Topic attribution: %d rows for date %s", rows, tracking_date)
                        if tracking_date in overlay_recovery_target_dates:
                            with self._storage_backend.create_unit_of_work() as state_uow:
                                state_after_overlay = state_uow.pipeline_state.get(
                                    self._ecosystem,
                                    self._tenant_id,
                                    tracking_date,
                                )
                            if state_after_overlay is not None and state_after_overlay.topic_attribution_calculated:
                                completed_overlay_recovery_dates.add(tracking_date)
                    except Exception as exc:
                        if chunk_provider is not None:
                            chunk_provider.clear_evidence_chunk()
                            active_evidence_chunk = None
                        recovery_interrupted = recovery_interrupted or recovery_active
                        logger.error(
                            "topic_overlay_failed%s",
                            safe_log_context(
                                tenant_name=self._tenant_name,
                                tenant_id=self._tenant_id,
                                pipeline_run_id=calculation_run_id,
                                tracking_date=tracking_date,
                                stage="topic_overlay",
                                outcome="failed",
                                retryable=True,
                                **safe_exception_context(exc),
                            ),
                        )
                        errors.append(f"Topic overlay failed for date {tracking_date}: {exc}")
            finally:
                if chunk_provider is not None:
                    chunk_provider.clear_evidence_chunk()
                    active_evidence_chunk = None

        if (
            getattr(self, "_topic_overlay_provider_enabled", False)
            and recovery_result is not None
            and recovery_active
            and recovery_targets_checked
            and (recovery_target_dates or overlay_recovery_target_dates)
            and (not recovery_target_dates or recovery_target_dates.issubset(completed_pending_dates))
            and (
                not overlay_recovery_target_dates
                or overlay_recovery_target_dates.issubset(completed_overlay_recovery_dates)
            )
            and not recovery_interrupted
            and not errors
            and isinstance(plugin, ScopeGatePlugin)
        ):
            with self._storage_backend.create_unit_of_work() as persistence_uow:
                plugin.persist_scope_closed(self._tenant_id, recovery_result, persistence_uow)
                persistence_uow.commit()

        if not defer_progress_until_scope or progress_started:
            self._report_progress(None, None)
        logger.info(
            "pipeline_orchestration_completed dates_gathered=%d dates_calculated=%d rows_written=%d error_count=%d%s",
            dates_gathered,
            dates_calculated,
            chargeback_rows_written,
            len(errors),
            safe_log_context(
                tenant_name=self._tenant_name,
                tenant_id=self._tenant_id,
                pipeline_run_id=calculation_run_id,
                stage="pipeline_run",
                operation="pipeline_run",
                outcome="completed_with_errors" if errors else "completed",
            ),
        )
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
