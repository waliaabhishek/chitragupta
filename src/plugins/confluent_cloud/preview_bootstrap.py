from __future__ import annotations

import hashlib
import json
import logging
import uuid
from collections.abc import Callable, Iterable
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Protocol, runtime_checkable

from core.preview.evidence import (
    PreviewEvidenceBootstrapReason,
    PreviewEvidenceBootstrapResult,
    PreviewEvidenceBootstrapStatus,
    PreviewSourceReadiness,
    SourceAttemptFailureReason,
    SourceAttemptFinalStatus,
)
from core.preview.evidence_capture import PreviewEvidenceBootstrapConflictError

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from core.preview.evidence import PreviewSourceEvidence
    from core.preview.evidence_capture import NativeSourceWindow
    from core.preview.persistence import PreviewEvidenceStorageBackend, PreviewEvidenceWriteUnitOfWork


class PreviewEvidenceBootstrapError(RuntimeError):
    """Legacy Preview evidence bootstrap failed."""


@runtime_checkable
class CCloudBootstrappedLineageRefresher(Protocol):
    def refresh_bootstrapped_lineage(
        self,
        capture_ids: tuple[str, ...],
    ) -> None: ...


class _InvalidLegacyEvidenceError(ValueError):
    """Persisted legacy evidence is readable but violates the v18/v21 contract."""


@dataclass(frozen=True)
class _BootstrapWindowPlan:
    window: NativeSourceWindow
    source_count: int
    capture_id: str


def preview_utc_now() -> datetime:
    return datetime.now(UTC)


def new_preview_refresh_token() -> str:
    return str(uuid.uuid4())


def legacy_capture_id(
    ecosystem: str,
    tenant_id: str,
    window: NativeSourceWindow,
    records: Iterable[PreviewSourceEvidence],
) -> str:
    def encode_scalar(value: object) -> str:
        if isinstance(value, datetime):
            if value.tzinfo is None or value.utcoffset() is None:
                raise ValueError("legacy source timestamps must be timezone-aware")
            return value.astimezone(UTC).isoformat()
        if isinstance(value, Decimal):
            return str(value)
        raise TypeError(f"unsupported legacy source value: {type(value).__name__}")

    encoder = json.JSONEncoder(
        default=encode_scalar,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    digest = hashlib.sha256()

    def update(value: str) -> None:
        digest.update(value.encode())

    update('{"ecosystem":')
    update(encoder.encode(ecosystem))
    update(',"records":[')
    first = True
    for record in records:
        if not first:
            update(",")
        update(encoder.encode(asdict(record)))
        first = False
    update('],"tenant_id":')
    update(encoder.encode(tenant_id))
    update(',"window_end":')
    update(encoder.encode(window.end.astimezone(UTC)))
    update(',"window_start":')
    update(encoder.encode(window.start.astimezone(UTC)))
    update("}")
    return f"legacy:v1:{digest.hexdigest()}"


def _aware(value: datetime) -> bool:
    return value.tzinfo is not None and value.utcoffset() is not None


def _validate_legacy_record(
    record: PreviewSourceEvidence,
    *,
    ecosystem: str,
    tenant_id: str,
    window: NativeSourceWindow,
) -> tuple[str, str, datetime, datetime]:
    if record.ecosystem != ecosystem or record.tenant_id != tenant_id:
        raise _InvalidLegacyEvidenceError("legacy source owner does not match bootstrap owner")
    if not record.source_record_id.strip() or not record.identity_scheme.strip():
        raise _InvalidLegacyEvidenceError("legacy source identity is blank")
    timestamps = (
        record.collection_window_start,
        record.collection_window_end,
        record.evidence_scope_start,
        record.evidence_scope_end,
        record.allocation_timestamp,
    )
    if any(not _aware(value) for value in timestamps):
        raise _InvalidLegacyEvidenceError("legacy source timestamps must be timezone-aware")
    if record.retention_timestamp is None or not _aware(record.retention_timestamp):
        raise _InvalidLegacyEvidenceError("legacy source retention timestamp must be timezone-aware")
    if (
        record.collection_window_start >= record.collection_window_end
        or record.evidence_scope_start >= record.evidence_scope_end
        or record.collection_window_start != window.start
        or record.collection_window_end != window.end
    ):
        raise _InvalidLegacyEvidenceError("legacy source bounds are invalid")

    source_start = record.source_period_start
    source_end = record.source_period_end
    if (source_start is None) != (source_end is None):
        raise _InvalidLegacyEvidenceError("legacy source-period bounds must be paired")
    if source_start is not None and source_end is not None:
        if not _aware(source_start) or not _aware(source_end) or source_start >= source_end:
            raise _InvalidLegacyEvidenceError("legacy source-period bounds are invalid")
        if (
            record.evidence_scope_start != source_start
            or record.evidence_scope_end != source_end
            or record.allocation_timestamp != source_start
            or record.retention_timestamp != source_start
        ):
            raise _InvalidLegacyEvidenceError("dated legacy source scope is inconsistent")
    elif (
        record.evidence_scope_start != window.start
        or record.evidence_scope_end != window.end
        or record.retention_timestamp != window.end
    ):
        raise _InvalidLegacyEvidenceError("undated legacy source scope is inconsistent")

    association = (
        record.billing_timestamp,
        record.billing_env_id,
        record.billing_resource_id,
        record.billing_product_type,
        record.billing_product_category,
    )
    if any(value is None for value in association) and not all(value is None for value in association):
        raise _InvalidLegacyEvidenceError("legacy billing association must be complete or absent")
    if record.billing_timestamp is not None and not _aware(record.billing_timestamp):
        raise _InvalidLegacyEvidenceError("legacy billing timestamp must be timezone-aware")

    decimals = (
        record.amount,
        record.original_amount,
        record.discount_amount,
        record.price,
        record.quantity,
    )
    if any(value is not None and not value.is_finite() for value in decimals):
        raise _InvalidLegacyEvidenceError("legacy source decimals must be finite")
    if not all(isinstance(key, str) and isinstance(value, str) for key, value in record.native_tier_dimensions):
        raise _InvalidLegacyEvidenceError("legacy tier dimensions are invalid")
    if not all(isinstance(value, str) for value in record.diagnostics):
        raise _InvalidLegacyEvidenceError("legacy diagnostics are invalid")
    if record.raw_payload_json is None:
        raise _InvalidLegacyEvidenceError("legacy raw payload is unavailable")
    try:
        raw_payload = json.loads(record.raw_payload_json)
    except (json.JSONDecodeError, TypeError) as exc:
        raise _InvalidLegacyEvidenceError("legacy raw payload is invalid JSON") from exc
    if (
        not isinstance(raw_payload, dict)
        or json.dumps(
            raw_payload,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        != record.raw_payload_json
    ):
        raise _InvalidLegacyEvidenceError("legacy raw payload is not canonical JSON")
    return (
        record.source_record_id,
        record.identity_scheme,
        record.evidence_scope_start,
        record.evidence_scope_end,
    )


class CCloudPreviewEvidenceBootstrap:
    def __init__(
        self,
        backend: PreviewEvidenceStorageBackend,
        *,
        clock: Callable[[], datetime] = preview_utc_now,
        capture_id_factory: Callable[
            [str, str, NativeSourceWindow, Iterable[PreviewSourceEvidence]], str
        ] = legacy_capture_id,
        refresh_token_factory: Callable[[], str] = new_preview_refresh_token,
    ) -> None:
        self._backend = backend
        self._clock = clock
        self._capture_id_factory = capture_id_factory
        self._refresh_token_factory = refresh_token_factory

    def bootstrap_owner(
        self,
        *,
        ecosystem: str,
        tenant_id: str,
        policy_start: datetime,
        policy_end: datetime,
    ) -> PreviewEvidenceBootstrapResult:
        try:
            with self._backend.create_preview_evidence_unit_of_work() as uow:
                try:
                    result = self._bootstrap_in_uow(
                        uow,
                        ecosystem=ecosystem,
                        tenant_id=tenant_id,
                        policy_start=policy_start,
                        policy_end=policy_end,
                    )
                    if (
                        result.status
                        in {
                            PreviewEvidenceBootstrapStatus.BOOTSTRAPPED,
                            PreviewEvidenceBootstrapStatus.UNAVAILABLE,
                        }
                        and result.reason is not PreviewEvidenceBootstrapReason.NO_LEGACY_EVIDENCE
                    ):
                        uow.commit()
                    else:
                        uow.rollback()
                    return result
                except Exception:
                    uow.rollback()
                    raise
        except PreviewEvidenceBootstrapError:
            raise
        except Exception as exc:
            raise PreviewEvidenceBootstrapError("legacy Preview evidence bootstrap failed") from exc

    def _bootstrap_in_uow(
        self,
        uow: PreviewEvidenceWriteUnitOfWork,
        *,
        ecosystem: str,
        tenant_id: str,
        policy_start: datetime,
        policy_end: datetime,
    ) -> PreviewEvidenceBootstrapResult:
        source_readiness = uow.source_readiness
        source_windows = uow.source_windows
        if source_readiness.get_current_authority(ecosystem, tenant_id) is not None:
            return PreviewEvidenceBootstrapResult(
                status=PreviewEvidenceBootstrapStatus.ALREADY_CURRENT,
                bootstrapped_windows=0,
                bootstrapped_rows=0,
                reason=None,
            )
        windows = source_windows.list_unassociated_windows(ecosystem, tenant_id, policy_start, policy_end)
        if not windows:
            return PreviewEvidenceBootstrapResult(
                status=PreviewEvidenceBootstrapStatus.UNAVAILABLE,
                bootstrapped_windows=0,
                bootstrapped_rows=0,
                reason=PreviewEvidenceBootstrapReason.NO_LEGACY_EVIDENCE,
            )
        now = self._clock()
        attempt = source_readiness.begin_attempt(
            ecosystem,
            tenant_id,
            self._refresh_token_factory(),
            windows[0].start,
            windows[-1].end,
            now,
        )
        try:
            if any(left.end != right.start for left, right in zip(windows, windows[1:], strict=False)):
                raise _InvalidLegacyEvidenceError("legacy source windows are not contiguous")
            plans: list[_BootstrapWindowPlan] = []
            for window in windows:
                source_count = 0
                previous_key: tuple[str, str, datetime, datetime] | None = None

                def validated_records(current_window: NativeSourceWindow = window) -> Iterable[PreviewSourceEvidence]:
                    nonlocal previous_key, source_count
                    for record in source_windows.iter_unassociated_window(ecosystem, tenant_id, current_window):
                        primary_key = _validate_legacy_record(
                            record,
                            ecosystem=ecosystem,
                            tenant_id=tenant_id,
                            window=current_window,
                        )
                        if primary_key == previous_key:
                            raise _InvalidLegacyEvidenceError("legacy source primary key is duplicated")
                        previous_key = primary_key
                        source_count += 1
                        yield record

                if self._capture_id_factory is legacy_capture_id:
                    capture_id = self._capture_id_factory(
                        ecosystem,
                        tenant_id,
                        window,
                        validated_records(),
                    )
                else:
                    capture_id = self._capture_id_factory(
                        ecosystem,
                        tenant_id,
                        window,
                        tuple(validated_records()),
                    )
                if source_count == 0:
                    raise _InvalidLegacyEvidenceError("legacy source window is empty")
                plans.append(
                    _BootstrapWindowPlan(
                        window=window,
                        source_count=source_count,
                        capture_id=capture_id,
                    )
                )
        except _InvalidLegacyEvidenceError:
            source_readiness.finalize_attempt(
                attempt.attempt_sequence,
                SourceAttemptFinalStatus.FAILED,
                completed_at=self._clock(),
                reason=SourceAttemptFailureReason.BOOTSTRAP_INVALID,
            )
            return PreviewEvidenceBootstrapResult(
                status=PreviewEvidenceBootstrapStatus.UNAVAILABLE,
                bootstrapped_windows=0,
                bootstrapped_rows=0,
                reason=PreviewEvidenceBootstrapReason.INVALID_LEGACY_EVIDENCE,
            )
        readiness = tuple(
            PreviewSourceReadiness(
                ecosystem=ecosystem,
                tenant_id=tenant_id,
                window_start=plan.window.start,
                window_end=plan.window.end,
                capture_id=plan.capture_id,
                captured_at=now,
                source_count=plan.source_count,
                attempt_sequence=attempt.attempt_sequence,
            )
            for plan in plans
        )
        try:
            with uow.savepoint():
                for plan in plans:
                    source_windows.associate_legacy_window(
                        ecosystem,
                        tenant_id,
                        plan.window,
                        capture_id=plan.capture_id,
                        expected_source_count=plan.source_count,
                    )
                source_readiness.replace_overlapping(
                    ecosystem,
                    tenant_id,
                    windows[0].start,
                    windows[-1].end,
                    readiness,
                )
                lineage_refresher = uow.allocation_lineage
                if not isinstance(lineage_refresher, CCloudBootstrappedLineageRefresher):
                    raise PreviewEvidenceBootstrapError(
                        "legacy Preview evidence bootstrap lineage refresh capability is unavailable"
                    )
                lineage_refresher.refresh_bootstrapped_lineage(tuple(plan.capture_id for plan in plans))
        except PreviewEvidenceBootstrapConflictError:
            source_readiness.finalize_attempt(
                attempt.attempt_sequence,
                SourceAttemptFinalStatus.FAILED,
                completed_at=self._clock(),
                reason=SourceAttemptFailureReason.BOOTSTRAP_CONCURRENT_CHANGE,
            )
            return PreviewEvidenceBootstrapResult(
                status=PreviewEvidenceBootstrapStatus.UNAVAILABLE,
                bootstrapped_windows=0,
                bootstrapped_rows=0,
                reason=PreviewEvidenceBootstrapReason.CONCURRENT_CHANGE,
            )
        source_readiness.finalize_attempt(
            attempt.attempt_sequence,
            SourceAttemptFinalStatus.COMPLETE,
            completed_at=self._clock(),
            reason=None,
        )
        return PreviewEvidenceBootstrapResult(
            status=PreviewEvidenceBootstrapStatus.BOOTSTRAPPED,
            bootstrapped_windows=len(readiness),
            bootstrapped_rows=sum(item.source_count for item in readiness),
            reason=None,
        )
