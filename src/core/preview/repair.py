from __future__ import annotations

import logging
import threading
import uuid
from collections.abc import Callable
from concurrent.futures import Executor, ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from enum import StrEnum
from typing import TYPE_CHECKING, Literal, Protocol, runtime_checkable

from core.preview.models import PreviewDiagnostic

if TYPE_CHECKING:
    from core.config.models import TenantConfig
    from core.preview.persistence import PreviewEvidenceStorageBackend
    from core.storage.backend_provider import TenantBackendProvider

logger = logging.getLogger(__name__)


def _aware(value: datetime | None) -> bool:
    return value is not None and value.tzinfo is not None and value.utcoffset() is not None


class PreviewRepairStatus(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    COMPLETED_WITH_FAILURES = "completed_with_failures"
    FAILED = "failed"


class PreviewRepairDateStatus(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    DAILY_VALIDATED = "daily_validated"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class PreviewRepairFailureStage(StrEnum):
    RETAINED_STATE = "retained_state"
    PROVIDER_SOURCE = "provider_source"
    CALCULATION = "calculation"
    EVIDENCE = "evidence"
    PREVIEW_VALIDATION = "preview_validation"
    WORKER = "worker"


class PreviewRepairWorkerConflictError(RuntimeError):
    """A guarded repair transition lost ownership to incompatible durable state."""


@dataclass(frozen=True)
class PreviewRepairDate:
    repair_id: str
    tracking_date: date
    status: PreviewRepairDateStatus
    started_at: datetime | None
    completed_at: datetime | None
    calculation_id: str | None
    calculation_completed_at: datetime | None
    rows_written: int | None
    failure_stage: PreviewRepairFailureStage | None
    diagnostic: PreviewDiagnostic | None

    def __post_init__(self) -> None:
        if not self.repair_id.strip() or not isinstance(self.status, PreviewRepairDateStatus):
            raise ValueError("invalid repair date identity")
        started = _aware(self.started_at)
        completed = _aware(self.completed_at)
        has_result = (
            bool(self.calculation_id)
            and _aware(self.calculation_completed_at)
            and self.rows_written is not None
            and self.rows_written >= 0
        )
        no_result = self.calculation_id is None and self.calculation_completed_at is None and self.rows_written is None
        no_failure = self.failure_stage is None and self.diagnostic is None
        if self.status is PreviewRepairDateStatus.QUEUED:
            valid = not started and not completed and no_result and no_failure
        elif self.status is PreviewRepairDateStatus.RUNNING:
            valid = started and not completed and no_result and no_failure
        elif self.status is PreviewRepairDateStatus.DAILY_VALIDATED:
            valid = started and not completed and has_result and no_failure
        elif self.status is PreviewRepairDateStatus.SUCCEEDED:
            valid = started and completed and has_result and no_failure
        else:
            valid = (
                completed
                and no_result
                and isinstance(self.failure_stage, PreviewRepairFailureStage)
                and self.diagnostic is not None
            )
        if not valid:
            raise ValueError("repair date fields do not match status")


@dataclass(frozen=True)
class PreviewRepair:
    repair_id: str
    tenant_name: str
    ecosystem: str
    tenant_id: str
    start_date: date
    end_date: date
    status: PreviewRepairStatus
    created_at: datetime
    started_at: datetime | None
    completed_at: datetime | None
    diagnostic: PreviewDiagnostic | None
    dates: tuple[PreviewRepairDate, ...]

    def __post_init__(self) -> None:
        if not all(value.strip() for value in (self.repair_id, self.tenant_name, self.ecosystem, self.tenant_id)):
            raise ValueError("repair identity must not be blank")
        if not isinstance(self.status, PreviewRepairStatus) or not _aware(self.created_at):
            raise ValueError("invalid repair status or creation time")
        span = (self.end_date - self.start_date).days
        if span < 1 or span > 364:
            raise ValueError("repair bounds must contain 1 to 364 dates")
        expected = tuple(self.start_date + timedelta(days=offset) for offset in range(span))
        if tuple(item.tracking_date for item in self.dates) != expected:
            raise ValueError("repair dates must exactly cover the requested interval")
        if any(item.repair_id != self.repair_id for item in self.dates):
            raise ValueError("repair date owner mismatch")
        terminal = {PreviewRepairDateStatus.SUCCEEDED, PreviewRepairDateStatus.FAILED}
        if self.status is PreviewRepairStatus.QUEUED:
            valid = (
                self.started_at is None
                and self.completed_at is None
                and self.diagnostic is None
                and all(item.status is PreviewRepairDateStatus.QUEUED for item in self.dates)
            )
        elif self.status is PreviewRepairStatus.RUNNING:
            valid = _aware(self.started_at) and self.completed_at is None and self.diagnostic is None
        elif self.status is PreviewRepairStatus.COMPLETED:
            valid = (
                _aware(self.started_at)
                and _aware(self.completed_at)
                and self.diagnostic is None
                and all(item.status is PreviewRepairDateStatus.SUCCEEDED for item in self.dates)
            )
        elif self.status is PreviewRepairStatus.COMPLETED_WITH_FAILURES:
            valid = (
                _aware(self.started_at)
                and _aware(self.completed_at)
                and self.diagnostic is None
                and all(item.status in terminal for item in self.dates)
                and any(item.status is PreviewRepairDateStatus.FAILED for item in self.dates)
            )
        else:
            valid = (
                _aware(self.completed_at)
                and self.diagnostic is not None
                and all(item.status in terminal for item in self.dates)
            )
        if not valid:
            raise ValueError("repair fields do not match status")


@dataclass(frozen=True)
class PreviewRepairPolicy:
    eligible_start_date: date
    eligible_end_date: date


def repair_policy_from_tenant_config(
    tenant_config: TenantConfig,
    *,
    created_at: datetime,
) -> PreviewRepairPolicy:
    if not tenant_config.focus_preview_enabled or tenant_config.focus_preview is None:
        raise ValueError("FOCUS Mapping Preview repair is unavailable")
    if not _aware(created_at):
        raise ValueError("created_at must be timezone-aware")
    now = created_at.astimezone(UTC)
    retention_cutoff = now - timedelta(days=tenant_config.retention_days)
    retained_start = retention_cutoff.date()
    if retention_cutoff.timetz().replace(tzinfo=None) != time.min:
        retained_start += timedelta(days=1)
    preview = tenant_config.focus_preview
    return PreviewRepairPolicy(
        eligible_start_date=max(
            preview.effective_start_date,
            now.date() - timedelta(days=tenant_config.lookback_days),
            retained_start,
        ),
        eligible_end_date=min(
            preview.effective_end_date,
            now.date() - timedelta(days=tenant_config.cutoff_days),
        ),
    )


def validate_repair_range(
    start_date: date,
    end_date: date,
    *,
    policy: PreviewRepairPolicy,
    created_at: datetime,
) -> None:
    if start_date >= end_date:
        raise ValueError("range_invalid")
    if start_date > created_at.astimezone(UTC).date() or end_date > created_at.astimezone(UTC).date():
        raise ValueError("future_range")
    if start_date < policy.eligible_start_date or end_date > policy.eligible_end_date:
        raise ValueError("range_ineligible")
    if (end_date - start_date).days > 364:
        raise ValueError("range_ineligible")


@runtime_checkable
class PreviewRepairRepository(Protocol):
    def create_queued(self, repair: PreviewRepair) -> PreviewRepair: ...
    def get_for_owner(self, repair_id: str, ecosystem: str, tenant_id: str) -> PreviewRepair | None: ...
    def find_active_for_owner(self, ecosystem: str, tenant_id: str) -> PreviewRepair | None: ...
    def mark_running(self, repair_id: str, *, started_at: datetime) -> PreviewRepair | None: ...
    def mark_date_running(
        self, repair_id: str, tracking_date: date, *, started_at: datetime
    ) -> PreviewRepairDate | None: ...
    def mark_date_daily_validated(
        self,
        repair_id: str,
        tracking_date: date,
        *,
        calculation_id: str,
        calculation_completed_at: datetime,
        rows_written: int,
    ) -> PreviewRepairDate | None: ...
    def mark_date_succeeded_from_running(
        self,
        repair_id: str,
        tracking_date: date,
        *,
        completed_at: datetime,
        calculation_id: str,
        calculation_completed_at: datetime,
        rows_written: int,
    ) -> PreviewRepairDate | None: ...
    def mark_date_failed_from_running(
        self,
        repair_id: str,
        tracking_date: date,
        *,
        completed_at: datetime,
        stage: PreviewRepairFailureStage,
        diagnostic: PreviewDiagnostic,
    ) -> PreviewRepairDate | None: ...
    def finalize_month_dates(
        self,
        repair_id: str,
        tracking_dates: tuple[date, ...],
        *,
        terminal_status: Literal[PreviewRepairDateStatus.SUCCEEDED, PreviewRepairDateStatus.FAILED],
        completed_at: datetime,
        stage: PreviewRepairFailureStage | None,
        diagnostic: PreviewDiagnostic | None,
    ) -> tuple[PreviewRepairDate, ...] | None: ...
    def fail_queued_before_execution(
        self, repair_id: str, *, completed_at: datetime, diagnostic: PreviewDiagnostic
    ) -> PreviewRepair | None: ...
    def fail_running_worker(
        self, repair_id: str, *, completed_at: datetime, diagnostic: PreviewDiagnostic
    ) -> PreviewRepair | None: ...
    def finalize_completed(self, repair_id: str, *, completed_at: datetime) -> PreviewRepair | None: ...
    def finalize_completed_with_failures(self, repair_id: str, *, completed_at: datetime) -> PreviewRepair | None: ...
    def fail_interrupted_before(
        self,
        ecosystem: str,
        tenant_id: str,
        process_started_at: datetime,
        *,
        completed_at: datetime,
        diagnostic: PreviewDiagnostic,
    ) -> int: ...


@runtime_checkable
class PreviewRepairRunner(Protocol):
    def run_focus_preview_repair(
        self,
        repair_id: str,
        tenant_name: str,
        tenant_config: TenantConfig,
    ) -> None: ...


class PreviewExecutor(Protocol):
    def submit(self, fn: Callable[[], None]) -> object: ...
    def shutdown(self, wait: bool = True) -> None: ...


class PreviewRepairRuntime:
    def __init__(
        self,
        *,
        runner: PreviewRepairRunner,
        backend_provider: TenantBackendProvider,
        max_workers: int,
        configured_owners: tuple[tuple[str, TenantConfig], ...],
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        repair_id_factory: Callable[[], str] = lambda: str(uuid.uuid4()),
        executor: Executor | None = None,
    ) -> None:
        self.runner = runner
        self.backend_provider = backend_provider
        self.clock = clock
        self.repair_id_factory = repair_id_factory
        self.configured_owners = configured_owners
        self.process_started_at = self.clock()
        self._executor = executor or ThreadPoolExecutor(max_workers=max_workers)
        self._lock = threading.Lock()

    def recover(self) -> None:
        from core.preview.persistence import PreviewEvidenceStorageBackend

        diagnostic = PreviewDiagnostic(
            code="focus_preview_repair_interrupted",
            message="The repair was interrupted; submit a new bounded repair to retry.",
            retryable=True,
        )
        for tenant_name, tenant_config in self.configured_owners:
            with self.backend_provider.acquire_backend(tenant_name, tenant_config) as backend:
                if not isinstance(backend, PreviewEvidenceStorageBackend):
                    continue
                completed_at = self.clock()
                with backend.create_preview_evidence_unit_of_work() as uow:
                    uow.repairs.fail_interrupted_before(
                        tenant_config.ecosystem,
                        tenant_config.tenant_id,
                        self.process_started_at,
                        completed_at=completed_at,
                        diagnostic=diagnostic,
                    )
                    uow.commit()

    def create_queued(
        self,
        *,
        backend: PreviewEvidenceStorageBackend,
        tenant_name: str,
        tenant_config: TenantConfig,
        start_date: date,
        end_date: date,
        created_at: datetime,
    ) -> PreviewRepair:
        repair_id = self.repair_id_factory()
        dates = tuple(
            PreviewRepairDate(
                repair_id=repair_id,
                tracking_date=start_date + timedelta(days=offset),
                status=PreviewRepairDateStatus.QUEUED,
                started_at=None,
                completed_at=None,
                calculation_id=None,
                calculation_completed_at=None,
                rows_written=None,
                failure_stage=None,
                diagnostic=None,
            )
            for offset in range((end_date - start_date).days)
        )
        operation = PreviewRepair(
            repair_id=repair_id,
            tenant_name=tenant_name,
            ecosystem=tenant_config.ecosystem,
            tenant_id=tenant_config.tenant_id,
            start_date=start_date,
            end_date=end_date,
            status=PreviewRepairStatus.QUEUED,
            created_at=created_at,
            started_at=None,
            completed_at=None,
            diagnostic=None,
            dates=dates,
        )
        with self._lock, backend.create_preview_evidence_unit_of_work() as uow:
            if uow.repairs.find_active_for_owner(operation.ecosystem, operation.tenant_id) is not None:
                raise RuntimeError("active_repair")
            created = uow.repairs.create_queued(operation)
            uow.commit()
        return created

    def schedule(
        self,
        repair: PreviewRepair,
        *,
        tenant_config: TenantConfig,
    ) -> None:
        def run() -> None:
            try:
                self.runner.run_focus_preview_repair(
                    repair.repair_id,
                    repair.tenant_name,
                    tenant_config,
                )
            except Exception as exc:
                logger.error(
                    "FOCUS Preview repair worker failed repair_id=%s error_type=%s",
                    repair.repair_id,
                    type(exc).__name__,
                )
                from core.preview.persistence import PreviewEvidenceStorageBackend

                diagnostic = PreviewDiagnostic(
                    code="focus_preview_repair_worker_unavailable",
                    message="The repair worker stopped; submit a new bounded repair to retry.",
                    retryable=True,
                )
                try:
                    with self.backend_provider.acquire_backend(
                        repair.tenant_name,
                        tenant_config,
                    ) as backend:
                        if not isinstance(backend, PreviewEvidenceStorageBackend):
                            return
                        with backend.create_preview_evidence_unit_of_work() as uow:
                            current = uow.repairs.get_for_owner(
                                repair.repair_id,
                                repair.ecosystem,
                                repair.tenant_id,
                            )
                            if current is None:
                                return
                            completed_at = self.clock()
                            if current.status is PreviewRepairStatus.QUEUED:
                                failed = uow.repairs.fail_queued_before_execution(
                                    repair.repair_id,
                                    completed_at=completed_at,
                                    diagnostic=diagnostic,
                                )
                            elif current.status is PreviewRepairStatus.RUNNING:
                                failed = uow.repairs.fail_running_worker(
                                    repair.repair_id,
                                    completed_at=completed_at,
                                    diagnostic=diagnostic,
                                )
                            else:
                                return
                            uow.commit()
                        if failed is None:
                            with backend.create_preview_generation_read_unit_of_work() as read_uow:
                                persisted = read_uow.repairs.get_for_owner(
                                    repair.repair_id,
                                    repair.ecosystem,
                                    repair.tenant_id,
                                )
                            if (
                                persisted is None
                                or persisted.status is not PreviewRepairStatus.FAILED
                                or persisted.completed_at != completed_at
                                or persisted.diagnostic != diagnostic
                            ):
                                raise PreviewRepairWorkerConflictError("repair worker failure transition conflicted")
                except Exception as recovery_exc:
                    logger.error(
                        "FOCUS Preview repair worker failure persistence failed repair_id=%s error_type=%s",
                        repair.repair_id,
                        type(recovery_exc).__name__,
                    )

        self._executor.submit(run)

    def close(self, *, wait: bool) -> None:
        self._executor.shutdown(wait=wait)
