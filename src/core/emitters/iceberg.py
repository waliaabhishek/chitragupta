"""Publish completed chargeback days to Iceberg and track their confirmation."""

from __future__ import annotations

import logging
from bisect import bisect_left
from contextlib import ExitStack
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from time import monotonic
from typing import TYPE_CHECKING, Literal, cast

from core.config.models import MAX_SOURCE_RETENTION_DAYS
from core.emitters.iceberg_spool import IcebergDaySpool, IcebergScratchError
from core.logging_context import safe_exception_context, safe_log_context
from core.storage.iceberg import (
    IcebergExportKey,
    IcebergExportStorageBackend,
    IcebergSourceWidthError,
    IcebergStorageDeadlineError,
    IcebergStorageUnsupportedError,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Mapping
    from contextlib import AbstractContextManager

    from core.config.models import IcebergCatalogConfig, IcebergEmitterSpec, TenantConfig
    from core.emitters.iceberg_transport import IcebergCatalog, IcebergTable
    from core.models import ChargebackRow, PipelineState
    from core.storage.iceberg import IcebergExportCheckpoint
    from core.storage.interface import StorageBackend

logger = logging.getLogger(__name__)

_DAY = timedelta(days=1)
_TERMINAL_AMOUNT_ERRORS = frozenset({"non_finite_amount", "unrepresentable_amount"})
_ROW_LIMIT = 100_000
_SOURCE_BATCH_BYTES = 8 * 1024 * 1024
_SCRATCH_BYTES = 256 * 1024 * 1024
_ARROW_BATCH_BYTES = 16 * 1024 * 1024
_ARROW_BATCH_ROWS = 16_384
_FILE_LIMIT = 128
_ATTEMPT_SECONDS = 10
_LOOKUP_SECONDS = 5
_SCHEDULED_PASS_SECONDS = 20
_HANDOFF_SECONDS = 1
_CLEANUP_SECONDS = 1
_CONFLICT_RETRIES = 1

type _AcquireBackend = Callable[[str], AbstractContextManager[StorageBackend | None]]
type _Location = tuple[IcebergCatalogConfig, str]
type _Source = tuple[str, str]
type _Position = tuple[datetime, date, str]
type _AttemptOutcome = Literal["confirmed", "failed", "unconfirmed"]
type _SourceState = Literal["unavailable", "pending", "unversioned", "completed", "invalid_revision"]


class IcebergExportError(RuntimeError):
    """An export failure with a code safe for status output and diagnostics."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


@dataclass(slots=True)
class IcebergPassSummary:
    attempted: int = 0
    confirmed: int = 0
    failed: int = 0
    unconfirmed: int = 0
    errors: int = 0


@dataclass(slots=True)
class IcebergSchedule:
    """Keep the starting positions for the running worker's next Iceberg pass."""

    discovery: _Source | None = None
    publication: _Position | None = None
    publication_fallback: _Position | None = None


@dataclass(frozen=True, slots=True)
class _Binding:
    name: str
    config: TenantConfig
    spec: IcebergEmitterSpec

    @property
    def source(self) -> _Source:
        return self.config.ecosystem, self.config.tenant_id

    @property
    def location(self) -> _Location:
        return self.spec.params.catalog, self.spec.params.table

    def window_start(self, today: date) -> date:
        lookback = self.spec.lookback_days or self.config.retention_days
        return today - timedelta(days=min(lookback, self.config.retention_days))


@dataclass(frozen=True, slots=True)
class _Candidate:
    binding: _Binding
    key: IcebergExportKey
    sort_time: datetime

    @property
    def position(self) -> _Position:
        return self.sort_time, self.key.date, self.binding.name


@dataclass(frozen=True, slots=True)
class _Attempt:
    outcome: _AttemptOutcome | None = None
    code: str | None = None
    retry: bool = False
    skip_tenant: bool = False
    skip_destination: bool = False


def _bindings(tenants: Mapping[str, TenantConfig]) -> list[_Binding]:
    return [
        _Binding(name, config, spec)
        for name, config in tenants.items()
        if (spec := config.plugin_settings.iceberg_emitter) is not None
    ]


def _source_state(state: PipelineState | None) -> _SourceState:
    if state is None:
        return "unavailable"
    if not state.chargeback_calculated:
        return "pending"
    if state.calculation_id is None and state.calculation_completed_at is None:
        return "unversioned"
    if state.has_usable_calculation:
        return "completed"
    return "invalid_revision"


def _amount_blocked(state: PipelineState, checkpoint: IcebergExportCheckpoint | None) -> bool:
    return bool(
        checkpoint
        and checkpoint.outcome == "failed"
        and checkpoint.attempted_revision == state.calculation_id
        and checkpoint.error_code in _TERMINAL_AMOUNT_ERRORS
        and not checkpoint.replay_requested
    )


def _eligible(state: PipelineState, checkpoint: IcebergExportCheckpoint | None) -> bool:
    if not state.chargeback_calculated:
        return False
    if _source_state(state) == "invalid_revision":
        return True
    if _amount_blocked(state, checkpoint):
        return False
    return (
        checkpoint is None
        or checkpoint.confirmed_at is None
        or checkpoint.confirmed_revision != state.calculation_id
        or checkpoint.replay_requested
    )


def _iceberg_backend(backend: StorageBackend) -> IcebergExportStorageBackend:
    if not isinstance(backend, IcebergExportStorageBackend):
        raise IcebergExportError("iceberg_storage_unavailable")
    return backend


def _log_failure(
    binding: _Binding,
    code: str,
    error: Exception,
    *,
    day: date | None = None,
    revision: str | None = None,
    outcome: str = "failed",
    attempt_number: int | None = None,
) -> None:
    logger.warning(
        "iceberg_export%s",
        safe_log_context(
            tenant_name=binding.name,
            tenant_id=binding.config.tenant_id,
            ecosystem=binding.config.ecosystem,
            tracking_date=day,
            calculation_id=revision,
            stage="emit",
            operation="iceberg_export",
            outcome=outcome,
            retryable=code not in _TERMINAL_AMOUNT_ERRORS,
            attempt_number=attempt_number,
            max_attempts=_CONFLICT_RETRIES + 1 if attempt_number is not None else None,
            **safe_exception_context(error, root_error_code=code),
        ),
    )


class _Catalogs:
    """Own catalog clients and discovery results for one pass or command."""

    def __init__(self, stack: ExitStack, now: datetime, shutdown_check: Callable[[], bool], seconds: float) -> None:
        self.started_at = monotonic()
        self.now = now.astimezone(UTC)
        self.deadline = self.started_at + seconds
        try:
            from core.emitters import iceberg_transport
        except ImportError as exc:
            raise IcebergExportError("iceberg_dependency_unavailable") from exc

        self.transport = iceberg_transport
        self.stack = stack
        self.shutdown_check = shutdown_check
        self._close_deadline: float | None = None
        self.clients: dict[IcebergCatalogConfig, IcebergCatalog | IcebergExportError] = {}
        self.destinations: dict[_Location, str | IcebergExportError] = {}

    def stopped(self, cutoff: float | None = None) -> bool:
        return self.shutdown_check() or monotonic() >= (self.deadline if cutoff is None else cutoff)

    def utc_now(self) -> datetime:
        return self.now + timedelta(seconds=monotonic() - self.started_at)

    def operation_deadline(self, cutoff: float | None = None) -> float:
        return min(self.deadline if cutoff is None else cutoff, monotonic() + _LOOKUP_SECONDS)

    def require_time(self, deadline: float, *, settling: bool = False) -> None:
        if not settling and self.shutdown_check():
            raise IcebergExportError("export_interrupted")
        if monotonic() >= deadline:
            raise IcebergExportError("export_deadline")

    def close_deadline(self) -> float:
        if self._close_deadline is None:
            self._close_deadline = min(self.deadline, monotonic() + _CLEANUP_SECONDS)
        return self._close_deadline

    def _load(self, location: _Location, deadline: float) -> IcebergTable:
        settings, identifier = location
        self.require_time(deadline)
        try:
            if settings not in self.clients:
                self.clients[settings] = self.stack.enter_context(
                    self.transport.open_iceberg_catalog(settings, deadline=deadline, close_deadline=self.close_deadline)
                )
        except Exception as exc:
            error = IcebergExportError(self.error_code(exc, "catalog_unavailable"))
            self.clients[settings] = error
            raise error from exc

        catalog = self.clients[settings]
        if isinstance(catalog, IcebergExportError):
            raise catalog
        self.require_time(deadline)
        return catalog.load_table(identifier, deadline=deadline)

    def resolve(self, binding: _Binding, cutoff: float | None = None) -> str:
        location = binding.location
        if location not in self.destinations:
            try:
                deadline = self.operation_deadline(cutoff)
                table_uuid = self._load(location, deadline).uuid
                self.require_time(deadline)
                self.destinations[location] = table_uuid
            except Exception as exc:
                error = IcebergExportError(self.error_code(exc, "destination_unavailable"))
                self.destinations[location] = error
                raise error from exc
        result = self.destinations[location]
        if isinstance(result, IcebergExportError):
            raise result
        return result

    def load_base(self, candidate: _Candidate, deadline: float) -> IcebergTable:
        base = self._load(candidate.binding.location, deadline)
        if base.uuid != candidate.key.table_uuid:
            raise IcebergExportError("destination_changed")
        return base

    def error_code(self, exc: Exception, fallback: str) -> str:
        if isinstance(exc, (IcebergExportError, IcebergScratchError, self.transport.IcebergDestinationError)):
            return exc.code
        if isinstance(exc, IcebergSourceWidthError):
            return "source_byte_limit"
        if isinstance(exc, IcebergStorageUnsupportedError):
            return "source_storage_unsupported"
        if isinstance(exc, IcebergStorageDeadlineError):
            return "source_deadline"
        return fallback


def _resolve_peers(
    catalogs: _Catalogs, bindings: list[_Binding], cutoff: float | None = None
) -> tuple[dict[str, str], dict[str, str], bool]:
    resolved: dict[str, str] = {}
    errors: dict[str, str] = {}
    owners: dict[str, list[_Binding]] = {}
    complete = True
    for binding in bindings:
        if catalogs.stopped(cutoff):
            code = "export_interrupted" if catalogs.shutdown_check() else "export_deadline"
            errors[binding.name] = code
            complete = False
            break
        try:
            table_uuid = catalogs.resolve(binding, cutoff)
        except IcebergExportError as exc:
            _log_failure(binding, exc.code, exc)
            errors[binding.name] = exc.code
            complete = complete and not catalogs.stopped(cutoff)
            continue
        resolved[binding.name] = table_uuid
        owners.setdefault(table_uuid, []).append(binding)

    unresolved = bool(errors)
    for owners_at_table in owners.values():
        for binding in owners_at_table:
            if len(owners_at_table) > 1:
                code = "duplicate_destination_owner"
            elif unresolved:
                code = "destination_owner_unresolved"
            else:
                continue
            del resolved[binding.name]
            errors[binding.name] = code
            _log_failure(binding, code, IcebergExportError(code))
    return resolved, errors, complete


def _project(row: ChargebackRow, key: IcebergExportKey, state: PipelineState) -> dict[str, object]:
    if row.timestamp.utcoffset() is None:
        raise IcebergExportError("invalid_source_fact")
    timestamp = row.timestamp.astimezone(UTC)
    if (row.ecosystem, row.tenant_id, timestamp.date()) != (key.ecosystem, key.tenant_id, key.date):
        raise IcebergExportError("source_scope_mismatch")
    return {
        "ecosystem": key.ecosystem,
        "tenant_id": key.tenant_id,
        "chargeback_date": key.date,
        "timestamp": timestamp,
        "identity_id": row.identity_id,
        "product_category": row.product_category,
        "product_type": row.product_type,
        "cost_type": row.cost_type.value,
        "resource_id": row.resource_id,
        "allocation_method": row.allocation_method,
        "allocation_detail": row.allocation_detail,
        "amount": row.amount,
        "tags": row.tags,
        "env_id": row.metadata.get("env_id"),
        "team": row.metadata.get("team"),
        "calculation_id": state.calculation_id,
        "calculation_completed_at": state.calculation_completed_at,
    }


def _capture(
    catalogs: _Catalogs,
    backend: StorageBackend,
    key: IcebergExportKey,
    attempted: PipelineState,
    spool: IcebergDaySpool,
    deadline: float,
) -> None:
    catalogs.require_time(deadline)
    with _iceberg_backend(backend).create_iceberg_read_unit_of_work(deadline=deadline) as uow:
        state = uow.pipeline_state.get(key.ecosystem, key.tenant_id, key.date)
        if _source_state(state) == "invalid_revision":
            raise IcebergExportError("invalid_calculation_revision")
        if (
            state is None
            or not state.chargeback_calculated
            or state.calculation_id != attempted.calculation_id
            or state.calculation_completed_at != attempted.calculation_completed_at
        ):
            raise IcebergExportError("source_revision_changed")
        start = datetime.combine(key.date, time.min, tzinfo=UTC)
        for fact in uow.iter_iceberg_chargebacks(
            key.ecosystem,
            key.tenant_id,
            start,
            start + _DAY,
            max_row_bytes=_SOURCE_BATCH_BYTES,
            max_batch_bytes=_SOURCE_BATCH_BYTES,
        ):
            catalogs.require_time(deadline)
            if spool.row_count >= _ROW_LIMIT:
                raise IcebergExportError("source_row_limit")
            if fact.dimension_id is None:
                raise IcebergExportError("invalid_source_fact")
            spool.add(_project(fact, key, state), dimension_id=fact.dimension_id)


def _discover(
    catalogs: _Catalogs,
    bindings: list[_Binding],
    resolved: Mapping[str, str],
    acquire_backend: _AcquireBackend,
    scope: tuple[date, date] | None = None,
    *,
    standalone: bool,
    cutoff: float | None = None,
) -> tuple[list[_Candidate], set[str], int, bool]:
    candidates: list[_Candidate] = []
    checked: set[str] = set()
    errors = 0
    complete = True
    today = catalogs.utc_now().date()
    for binding in bindings:
        table_uuid = resolved.get(binding.name)
        if table_uuid is None:
            continue
        if catalogs.stopped(cutoff):
            complete = False
            break
        start = binding.window_start(today)
        end = today + _DAY
        if scope is not None:
            start = max(start, scope[0])
            end = min(end, scope[1])
        if start >= end:
            checked.add(binding.name)
            continue
        deadline = catalogs.operation_deadline(cutoff)
        try:
            with acquire_backend(binding.name) as backend:
                if backend is None:
                    if standalone:
                        raise IcebergExportError("source_unavailable")
                    continue
                catalogs.require_time(deadline)
                with _iceberg_backend(backend).create_iceberg_read_unit_of_work(deadline=deadline) as uow:
                    checkpoints = {
                        item.key.date: item
                        for item in uow.iceberg_exports.find_by_range(table_uuid, *binding.source, start, end)
                    }
                    states = uow.pipeline_state.find_by_range(*binding.source, start, end)
                catalogs.require_time(deadline)
        except Exception as exc:
            _log_failure(binding, catalogs.error_code(exc, "source_discovery_failed"), exc)
            errors += 1
            complete = complete and not catalogs.stopped(cutoff)
            continue
        checked.add(binding.name)
        for state in states:
            checkpoint = checkpoints.get(state.tracking_date)
            if _eligible(state, checkpoint):
                key = IcebergExportKey(table_uuid, *binding.source, state.tracking_date)
                sort_time = checkpoint.attempted_at if checkpoint else None
                if sort_time is None:
                    sort_time = datetime.combine(key.date, time.min, tzinfo=UTC)
                candidates.append(_Candidate(binding, key, sort_time))
    return candidates, checked, errors, complete


def _discover_groups(
    catalogs: _Catalogs,
    bindings: list[_Binding],
    acquire_backend: _AcquireBackend,
    schedule: IcebergSchedule | None,
    cutoff: float,
) -> tuple[list[_Candidate], set[str], int]:
    groups: dict[_Source, list[_Binding]] = {}
    for binding in bindings:
        groups.setdefault(binding.source, []).append(binding)
    sources = list(groups)
    start = sources.index(schedule.discovery) if schedule is not None and schedule.discovery in sources else 0
    next_start = start + 1
    candidates: list[_Candidate] = []
    checked: set[str] = set()
    errors = 0
    for turn in range(len(sources)):
        index = (start + turn) % len(sources)
        if catalogs.stopped(cutoff):
            next_start = index
            break
        peers = groups[sources[index]]
        resolved, resolution_errors, resolved_all = _resolve_peers(catalogs, peers, cutoff)
        ready, reviewed, discovery_errors, read_all = _discover(
            catalogs, peers, resolved, acquire_backend, standalone=schedule is None, cutoff=cutoff
        )
        candidates.extend(ready)
        checked.update(reviewed)
        if resolved_all:
            checked.update(name for name, code in resolution_errors.items() if code == "duplicate_destination_owner")
        errors += len(resolution_errors) + discovery_errors
        if not (resolved_all and read_all):
            # Give a partial group a full turn. Rotate past a group that had the first turn already.
            next_start = index if turn else index + 1
            break
    if schedule is not None:
        schedule.discovery = sources[next_start % len(sources)]
    return candidates, checked, errors


def _record_failure(
    catalogs: _Catalogs,
    backend: StorageBackend,
    candidate: _Candidate,
    revision: str | None,
    exc: Exception,
    *,
    deadline: float,
    fallback: str,
    number: int,
) -> _Attempt:
    key = candidate.key
    code = catalogs.error_code(exc, fallback)
    try:
        catalogs.require_time(deadline, settling=True)
        with _iceberg_backend(backend).create_iceberg_write_unit_of_work(deadline=deadline) as uow:
            catalogs.require_time(deadline, settling=True)
            uow.iceberg_exports.fail_attempt(key, code, terminal=code in _TERMINAL_AMOUNT_ERRORS)
            catalogs.require_time(deadline, settling=True)
            uow.commit()
    except Exception as write_error:
        _log_failure(candidate.binding, code, exc, day=key.date, revision=revision, attempt_number=number)
        checkpoint_code = "checkpoint_failure_unconfirmed"
        _log_failure(
            candidate.binding, checkpoint_code, write_error, day=key.date, revision=revision, outcome="unconfirmed"
        )
        return _Attempt("unconfirmed", checkpoint_code, skip_tenant=True)
    _log_failure(candidate.binding, code, exc, day=key.date, revision=revision, attempt_number=number)
    return _Attempt("failed", code, retry=isinstance(exc, catalogs.transport.IcebergConflict))


def _arrow_batches(catalogs: _Catalogs, spool: IcebergDaySpool) -> Iterator[list[dict[str, object]]]:
    try:
        yield from spool.batches(_ARROW_BATCH_BYTES, max_rows=_ARROW_BATCH_ROWS)
    except (IcebergExportError, IcebergScratchError) as exc:
        raise catalogs.transport.IcebergDestinationError(exc.code) from exc


def _publish_attempt(
    catalogs: _Catalogs,
    backend: StorageBackend,
    base: IcebergTable,
    candidate: _Candidate,
    state: PipelineState,
    work_deadline: float,
    deadline: float,
    number: int,
) -> _Attempt:
    key = candidate.key
    binding = candidate.binding
    revision = state.calculation_id
    fallback = "source_projection_failed"
    try:
        with IcebergDaySpool(
            work_deadline, limit_bytes=_SCRATCH_BYTES, batch_bytes=_ARROW_BATCH_BYTES, check_time=catalogs.require_time
        ) as spool:
            _capture(catalogs, backend, key, state, spool, work_deadline)
            spool.finish()
            row_count = spool.row_count
            catalogs.require_time(work_deadline)
            fallback = "destination_write_failed"
            snapshot_id = base.overwrite_batches(
                _arrow_batches(catalogs, spool),
                ecosystem=key.ecosystem,
                tenant_id=key.tenant_id,
                chargeback_date=key.date,
                max_bytes=_ARROW_BATCH_BYTES,
                max_files=_FILE_LIMIT,
                deadline=work_deadline,
                check_shutdown=catalogs.shutdown_check,
            )
    except catalogs.transport.IcebergUnconfirmed as exc:
        code = "destination_commit_unconfirmed"
        _log_failure(binding, code, exc, day=key.date, revision=revision, outcome="unconfirmed", attempt_number=number)
        return _Attempt("unconfirmed", code)
    except Exception as exc:
        return _record_failure(
            catalogs, backend, candidate, revision, exc, deadline=deadline, fallback=fallback, number=number
        )

    try:
        catalogs.require_time(deadline, settling=True)
        with _iceberg_backend(backend).create_iceberg_write_unit_of_work(deadline=deadline) as uow:
            catalogs.require_time(deadline, settling=True)
            uow.iceberg_exports.confirm_attempt(
                key, revision, state.calculation_completed_at, snapshot_id, catalogs.utc_now(), row_count
            )
            catalogs.require_time(deadline, settling=True)
            uow.commit()
    except Exception as exc:
        code = "checkpoint_confirmation_unconfirmed"
        _log_failure(binding, code, exc, day=key.date, revision=revision, outcome="unconfirmed", attempt_number=number)
        return _Attempt("unconfirmed", code, skip_tenant=True)
    return _Attempt("confirmed")


def _attempt(
    catalogs: _Catalogs,
    candidate: _Candidate,
    acquire_backend: _AcquireBackend,
    deadline: float,
    number: int,
) -> _Attempt:
    binding, key = candidate.binding, candidate.key
    today = catalogs.utc_now().date()
    if key.date < binding.window_start(today) or key.date > today:
        return _Attempt(code="outside_window")
    # Leave a final SQL transaction even if a read, write, or commit uses its full budget.
    work_deadline = deadline - _ATTEMPT_SECONDS / 10
    try:
        base = catalogs.load_base(candidate, work_deadline)
    except Exception as exc:
        code = catalogs.error_code(exc, "destination_unavailable")
        _log_failure(binding, code, exc, day=key.date, attempt_number=number)
        return _Attempt(code=code, skip_destination=True)

    try:
        catalogs.require_time(work_deadline)
        with acquire_backend(binding.name) as backend:
            if backend is None:
                return _Attempt(code="source_busy_or_unavailable", skip_tenant=True)
            today = catalogs.utc_now().date()
            if key.date < binding.window_start(today) or key.date > today:
                return _Attempt(code="outside_window")
            state: PipelineState | None = None
            committing = durable = False
            try:
                catalogs.require_time(work_deadline)
                with _iceberg_backend(backend).create_iceberg_write_unit_of_work(deadline=work_deadline) as uow:
                    state = uow.pipeline_state.get(key.ecosystem, key.tenant_id, key.date)
                    if state is None or not _eligible(state, uow.iceberg_exports.get(key)):
                        return _Attempt()
                    catalogs.require_time(work_deadline)
                    uow.iceberg_exports.record_attempt(key, state.calculation_id, catalogs.utc_now())
                    catalogs.require_time(work_deadline)
                    committing = True
                    uow.commit()
                    durable = True
            except Exception as exc:
                code = (
                    "checkpoint_attempt_unconfirmed"
                    if committing
                    else catalogs.error_code(exc, "checkpoint_attempt_failed")
                )
                if code != "export_interrupted":
                    _log_failure(
                        binding,
                        code,
                        exc,
                        day=key.date,
                        revision=state.calculation_id if state else None,
                        outcome="unconfirmed" if committing else "failed",
                        attempt_number=number,
                    )
                return _Attempt("unconfirmed" if durable else None, code, skip_tenant=True)
            return _publish_attempt(catalogs, backend, base, candidate, state, work_deadline, deadline, number)
    except Exception as exc:
        code = catalogs.error_code(exc, "source_read_failed")
        if code != "export_interrupted":
            _log_failure(binding, code, exc, day=key.date, attempt_number=number)
        return _Attempt(code=code, skip_tenant=True)


def _run(
    catalogs: _Catalogs,
    candidates: list[_Candidate],
    acquire_backend: _AcquireBackend,
    *,
    limit: int | None,
    cutoff: float,
    schedule: IcebergSchedule | None = None,
    keep_position: bool = False,
    scoped: bool = False,
) -> tuple[IcebergPassSummary, dict[date, _Attempt]]:
    summary = IcebergPassSummary()
    scoped_attempts: dict[date, _Attempt] = {}
    skipped_tenants: set[str] = set()
    skipped_destinations: set[_Location] = set()
    routine_deferrals = {"source_busy_or_unavailable", "outside_window", "export_interrupted"}
    candidates.sort(key=lambda item: item.position)
    if schedule is not None and schedule.publication is not None:
        position = schedule.publication
        if keep_position and schedule.publication_fallback is not None:
            position = schedule.publication_fallback
        index = next((index for index, item in enumerate(candidates) if item.position[1:] == position[1:]), None)
        if index is None:
            index = bisect_left(candidates, position, key=lambda item: item.position)
        candidates = candidates[index:] + candidates[:index]

    waiting = None
    for candidate in candidates:
        if limit is not None and summary.attempted >= limit:
            break
        binding = candidate.binding
        if binding.name in skipped_tenants or binding.location in skipped_destinations:
            continue
        result = _Attempt()
        for number in range(1, _CONFLICT_RETRIES + 2):
            deadline = monotonic() + _ATTEMPT_SECONDS
            if catalogs.stopped(cutoff) or deadline > cutoff:
                if number == 1:
                    waiting = candidate.position
                break
            attempt = _attempt(catalogs, candidate, acquire_backend, deadline, number)
            if attempt.skip_tenant:
                skipped_tenants.add(binding.name)
            if attempt.skip_destination:
                skipped_destinations.add(binding.location)
            if (
                attempt.outcome is None
                and attempt.code is not None
                and (limit is None or attempt.code not in routine_deferrals)
            ):
                summary.errors += 1
            if result.outcome is None or attempt.outcome is not None:
                result = attempt
            if not attempt.retry:
                break
        if waiting is not None:
            break
        if scoped:
            scoped_attempts[candidate.key.date] = result
        if result.outcome is not None:
            summary.attempted += 1
        if result.outcome == "confirmed":
            summary.confirmed += 1
        elif result.outcome == "failed":
            summary.failed += 1
        elif result.outcome == "unconfirmed":
            summary.unconfirmed += 1
    if schedule is not None:
        continuation = waiting if summary.attempted == 0 else None
        if keep_position:
            # Hold the deferred date while letting the other ready dates take turns.
            schedule.publication_fallback = continuation
        else:
            schedule.publication = continuation
            schedule.publication_fallback = None
    return summary, scoped_attempts


def publish_iceberg(
    tenants: Mapping[str, TenantConfig],
    *,
    acquire_backend: _AcquireBackend,
    now: datetime,
    shutdown_check: Callable[[], bool] = lambda: False,
    limit: int | None = 10,
    schedule: IcebergSchedule | None = None,
) -> IcebergPassSummary:
    """Publish due days in fair order, with a shared scheduled admission limit."""
    bindings = _bindings(tenants)
    if not bindings or limit == 0 or shutdown_check():
        return IcebergPassSummary()
    with ExitStack() as stack:
        try:
            seconds = float("inf") if limit is None else _SCHEDULED_PASS_SECONDS
            catalogs = _Catalogs(stack, now, shutdown_check, seconds)
        except IcebergExportError as exc:
            for binding in bindings:
                _log_failure(binding, exc.code, exc)
            return IcebergPassSummary(errors=len(bindings))
        if limit is None:
            schedule = None
        elif schedule is None:
            schedule = IcebergSchedule()
        publication_cutoff = catalogs.deadline - _CLEANUP_SECONDS
        discovery_cutoff = publication_cutoff - _ATTEMPT_SECONDS - _HANDOFF_SECONDS
        candidates, checked, errors = _discover_groups(catalogs, bindings, acquire_backend, schedule, discovery_cutoff)
        keep_position = False
        if schedule is not None and schedule.publication is not None:
            name = schedule.publication[2]
            keep_position = name not in checked and any(binding.name == name for binding in bindings)
        summary, _ = _run(
            catalogs,
            candidates,
            acquire_backend,
            limit=limit,
            cutoff=publication_cutoff,
            schedule=schedule,
            keep_position=keep_position,
        )
        summary.errors += errors
        return summary


def _selected_binding(tenants: Mapping[str, TenantConfig], tenant_name: str, start: date, end: date) -> _Binding:
    if end < start or (end - start).days > MAX_SOURCE_RETENTION_DAYS:
        raise IcebergExportError("invalid_date_range")
    config = tenants.get(tenant_name)
    if config is None:
        raise IcebergExportError("unknown_tenant")
    spec = config.plugin_settings.iceberg_emitter
    if spec is None:
        raise IcebergExportError("iceberg_export_not_configured")
    return _Binding(tenant_name, config, spec)


def _status_rows(
    catalogs: _Catalogs,
    binding: _Binding,
    table_uuid: str,
    backend: StorageBackend,
    start: date,
    end: date,
    today: date,
    deadline: float,
) -> list[dict[str, object]]:
    catalogs.require_time(deadline)
    states: dict[date, PipelineState] = {}
    checkpoints: dict[date, IcebergExportCheckpoint] = {}
    query_end = min(end, today) + _DAY
    if start < query_end:
        with _iceberg_backend(backend).create_iceberg_read_unit_of_work(deadline=deadline) as uow:
            states = {
                item.tracking_date: item for item in uow.pipeline_state.find_by_range(*binding.source, start, query_end)
            }
            checkpoints = {
                item.key.date: item
                for item in uow.iceberg_exports.find_by_range(table_uuid, *binding.source, start, query_end)
            }
        catalogs.require_time(deadline)
    window_start = binding.window_start(today)
    result: list[dict[str, object]] = []
    for offset in range((end - start).days + 1):
        day = start + timedelta(days=offset)
        state = states.get(day)
        checkpoint = checkpoints.get(day)
        source_state = _source_state(state)
        in_window = window_start <= day <= today
        revision = state.calculation_id if state else None
        current = bool(
            in_window
            and source_state in ("unversioned", "completed")
            and checkpoint
            and checkpoint.confirmed_at is not None
            and checkpoint.confirmed_revision == revision
            and not checkpoint.replay_requested
            and checkpoint.outcome == "succeeded"
        )
        result.append(
            {
                "date": day,
                "source_state": source_state if in_window else "outside_window",
                "source_revision": revision,
                "source_completed_at": state.calculation_completed_at if state else None,
                "current": current,
                "confirmed_revision": checkpoint.confirmed_revision if checkpoint else None,
                "confirmed_unversioned": bool(
                    checkpoint and checkpoint.confirmed_at is not None and checkpoint.confirmed_revision is None
                ),
                "confirmed_source_completed_at": checkpoint.source_completed_at if checkpoint else None,
                "confirmed_at": checkpoint.confirmed_at if checkpoint else None,
                "snapshot_id": checkpoint.snapshot_id if checkpoint else None,
                "row_count": checkpoint.row_count if checkpoint else None,
                "attempted_revision": checkpoint.attempted_revision if checkpoint else None,
                "attempted_at": checkpoint.attempted_at if checkpoint else None,
                "outcome": checkpoint.outcome if checkpoint else None,
                "error_code": checkpoint.error_code if checkpoint else None,
                "replay_requested": bool(checkpoint and checkpoint.replay_requested),
                "blocked": bool(in_window and state and _amount_blocked(state, checkpoint)),
            }
        )
    return result


def iceberg_status(
    tenants: Mapping[str, TenantConfig],
    *,
    tenant_name: str,
    start: date,
    end: date,
    acquire_backend: _AcquireBackend,
    now: datetime,
) -> list[dict[str, object]]:
    """Read source and confirmation state for an inclusive range and the current destination."""
    binding = _selected_binding(tenants, tenant_name, start, end)
    with ExitStack() as stack:
        catalogs = _Catalogs(stack, now, lambda: False, _LOOKUP_SECONDS)
        table_uuid = catalogs.resolve(binding)
        catalogs.require_time(catalogs.deadline)
        with acquire_backend(tenant_name) as backend:
            if backend is None:
                raise IcebergExportError("source_unavailable")
            return _status_rows(
                catalogs, binding, table_uuid, backend, start, end, catalogs.utc_now().date(), catalogs.deadline
            )


def iceberg_replay(
    tenants: Mapping[str, TenantConfig],
    *,
    tenant_name: str,
    start: date,
    end: date,
    acquire_backend: _AcquireBackend,
    now: datetime,
) -> tuple[list[dict[str, object]], bool]:
    """Register scoped replay durably, then use normal publication for the requested dates."""
    binding = _selected_binding(tenants, tenant_name, start, end)
    with ExitStack() as stack:
        catalogs = _Catalogs(stack, now, lambda: False, float("inf"))
        peers = [peer for peer in _bindings(tenants) if peer.source == binding.source]
        resolved, errors, _ = _resolve_peers(catalogs, peers)
        if tenant_name not in resolved:
            raise IcebergExportError(errors[tenant_name])
        table_uuid = resolved[tenant_name]
        today = catalogs.utc_now().date()
        window_start = max(start, binding.window_start(today))
        window_end = min(end, today) + _DAY
        if window_start < window_end:
            deadline = catalogs.operation_deadline()
            with acquire_backend(tenant_name) as backend:
                if backend is None:
                    raise IcebergExportError("source_unavailable")
                committing = False
                try:
                    catalogs.require_time(deadline)
                    with _iceberg_backend(backend).create_iceberg_write_unit_of_work(deadline=deadline) as uow:
                        states = uow.pipeline_state.find_by_range(*binding.source, window_start, window_end)
                        if states:
                            catalogs.require_time(deadline)
                            uow.iceberg_exports.request_replay(
                                table_uuid, *binding.source, [state.tracking_date for state in states]
                            )
                            catalogs.require_time(deadline)
                            committing = True
                            uow.commit()
                except Exception as exc:
                    code = "replay_registration_unconfirmed" if committing else "replay_registration_failed"
                    raise IcebergExportError(code) from exc
        candidates, _, _, _ = _discover(
            catalogs, [binding], resolved, acquire_backend, (window_start, window_end), standalone=True
        )
        _, attempts = _run(catalogs, candidates, acquire_backend, limit=None, cutoff=catalogs.deadline, scoped=True)
        deadline = catalogs.operation_deadline()
        with acquire_backend(tenant_name) as backend:
            if backend is None:
                raise IcebergExportError("source_unavailable")
            rows = _status_rows(catalogs, binding, table_uuid, backend, start, end, catalogs.utc_now().date(), deadline)

    for row in rows:
        attempt = attempts.get(cast("date", row["date"]))
        if attempt and attempt.outcome == "confirmed" and row["current"]:
            row["replay_outcome"] = "confirmed"
        elif attempt and attempt.outcome is not None:
            row["replay_outcome"] = attempt.outcome if attempt.outcome != "confirmed" else "deferred"
        elif row["source_state"] in ("pending", "unavailable", "outside_window"):
            row["replay_outcome"] = row["source_state"]
        else:
            row["replay_outcome"] = "deferred"
        row["replay_error_code"] = attempt.code if attempt else None
    return rows, all(row["replay_outcome"] == "confirmed" for row in rows)
