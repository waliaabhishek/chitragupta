from __future__ import annotations

import logging
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Annotated, cast

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import Response, StreamingResponse
from starlette.background import BackgroundTask

from core.api.dependencies import get_backend_provider, get_settings
from core.api.schemas import (  # noqa: TC001  # FastAPI evaluates annotations
    FocusPreviewArtifactResponse,
    FocusPreviewCalculationCoverageEntryResponse,
    FocusPreviewDiagnosticResponse,
    FocusPreviewKnownGapResponse,
    FocusPreviewPackageResponse,
    FocusPreviewProfileResponse,
    FocusPreviewRepairDateResponse,
    FocusPreviewRepairRequestBody,
    FocusPreviewRepairResponse,
    FocusPreviewRequestBody,
    FocusPreviewRequestListResponse,
    FocusPreviewRevisionListResponse,
    FocusPreviewRevisionResponse,
    FocusPreviewRevisionSummaryResponse,
    FocusPreviewRevisionValidationSummaryResponse,
    FocusPreviewSourceSnapshotResponse,
    FocusPreviewStatusResponse,
)
from core.config.models import AppSettings, TenantConfig  # noqa: TC001  # FastAPI evaluates annotations
from core.logging_context import safe_exception_context, safe_log_context
from core.preview.artifacts import (  # noqa: TC001 - used by FastAPI route helpers
    PreviewArchiveStream,
    PreviewVerifiedArtifactStream,
    find_preview_artifact_metadata,
    preview_artifact_owner,
)
from core.preview.capability import FOCUS_PREVIEW_CAPABILITY
from core.preview.capacity import PreviewCapacityUnavailable
from core.preview.eligibility import COMMERCIAL_PROFILE_UNAVAILABLE, diagnostic_detail
from core.preview.mapping import (
    FOCUS_1_4_FULL_PROFILE_COLUMNS,
    FOCUS_1_4_SUMMARY_COLUMNS,
    preview_manifest_known_gaps,
)
from core.preview.models import (
    PreviewArtifactMetadata,
    PreviewDiagnostic,
    PreviewInterval,
    PreviewRequest,
    PreviewRequestStatus,
    PreviewRevision,
    preview_month,
    validate_preview_request_snapshot,
)
from core.preview.persistence import (
    PreviewEffectiveColumnsMetadataMissingError,
    PreviewEvidenceStorageBackend,
    PreviewRequestCursorError,
    PreviewRevisionCursorError,
    PreviewStorageBackend,
)
from core.preview.repair import (
    PreviewRepair,
    PreviewRepairAlreadyActiveError,
    PreviewRepairCapacityUnavailable,
    PreviewRepairRuntime,
    PreviewRepairWorkerUnavailableError,
    repair_policy_from_tenant_config,
    validate_repair_range,
)
from core.preview.request import (
    PreviewColumnSelectionEmptyError,
    PreviewRequestValidationError,
    canonicalize_daily_interval,
    canonicalize_monthly_interval,
    normalize_column_selection,
)
from core.preview.revisions import (
    PreviewRevisionArtifactUnavailableError,
    PreviewRevisionReader,
    masked_preview_owner,
)
from core.preview.service import (
    PreviewArtifactUnavailable,
    PreviewRecoveryUnavailable,
    PreviewRuntime,
    PreviewWorkerUnavailable,
)
from core.storage.backend_provider import TenantBackendProvider  # noqa: TC001

if TYPE_CHECKING:
    from contextlib import AbstractContextManager

    from core.storage.interface import StorageBackend

router = APIRouter(prefix="/tenants/{tenant_name}/focus-preview", tags=["focus-preview"])
_NO_REQUEST = cast("Request", None)
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class FocusPreviewRevisionScope:
    tenant_config: TenantConfig
    interval: PreviewInterval


def _revision_scope(
    tenant_name: str,
    month: str,
    settings: AppSettings,
) -> FocusPreviewRevisionScope:
    try:
        interval = canonicalize_monthly_interval(month=month)
    except PreviewRequestValidationError as exc:
        raise HTTPException(400, detail=exc.detail) from None
    tenant_config = settings.tenants.get(tenant_name)
    if tenant_config is None:
        raise HTTPException(404, detail=f"Tenant {tenant_name!r} not found")
    _check_ecosystem(tenant_config)
    _require_focus_preview_enabled(tenant_config)
    return FocusPreviewRevisionScope(tenant_config, interval)


def _revision_reader(request: Request) -> PreviewRevisionReader:
    reader = getattr(request.app.state, "preview_revision_reader", None)
    if not isinstance(reader, PreviewRevisionReader):
        raise HTTPException(503, detail="FOCUS Mapping Preview revision service is unavailable")
    return reader


def _close_revision_archive_safely(
    archive: PreviewArchiveStream,
    *,
    preserving: BaseException | None,
) -> None:
    try:
        archive.close()
    except BaseException as close_error:
        if preserving is not None:
            logger.error(
                "FOCUS Mapping Preview revision archive close failed while preserving stream error "
                "stream_error_type=%s close_error_type=%s",
                type(preserving).__name__,
                type(close_error).__name__,
            )
        else:
            logger.error(
                "FOCUS Mapping Preview revision archive close failed close_error_type=%s",
                type(close_error).__name__,
            )


def _runtime(request: Request) -> PreviewRuntime:
    runtime = getattr(request.app.state, "preview_runtime", None)
    if runtime is None:
        raise HTTPException(503, detail="FOCUS Mapping Preview runtime is unavailable")
    return cast("PreviewRuntime", runtime)


def _repair_runtime(request: Request) -> PreviewRepairRuntime:
    runtime = getattr(request.app.state, "preview_repair_runtime", None)
    if not isinstance(runtime, PreviewRepairRuntime):
        raise HTTPException(503, detail="FOCUS Mapping Preview repair worker is unavailable")
    return runtime


def _release_preview_backend_lease(
    lease: AbstractContextManager[StorageBackend],
    *,
    tenant_name: str,
    preserving: BaseException | None,
) -> None:
    try:
        if preserving is None:
            lease.__exit__(None, None, None)
        else:
            lease.__exit__(type(preserving), preserving, preserving.__traceback__)
    except BaseException as release_error:
        if preserving is None:
            raise
        logger.error(
            "FOCUS Mapping Preview backend lease release failed tenant=%s primary_error_type=%s release_error_type=%s",
            tenant_name,
            type(preserving).__name__,
            type(release_error).__name__,
        )


@contextmanager
def _preview_backend(
    provider: TenantBackendProvider,
    tenant_name: str,
    tenant_config: TenantConfig,
    *,
    unavailable_detail: str = "FOCUS Mapping Preview storage is unavailable",
    request_id: str | None = None,
    stage: str = "storage_acquisition",
    include_tenant_id: bool = True,
) -> Iterator[PreviewStorageBackend]:
    try:
        lease = provider.acquire_backend(tenant_name, tenant_config)
        backend = lease.__enter__()
    except Exception as exc:
        exception_context = safe_exception_context(exc)
        exception_context.pop("error_type", None)
        logger.error(
            "FOCUS Mapping Preview backend creation failed tenant=%s error_type=%s%s",
            tenant_name,
            type(exc).__name__,
            safe_log_context(
                tenant_name=tenant_name,
                tenant_id=tenant_config.tenant_id if include_tenant_id else None,
                request_id=request_id,
                stage=stage,
                operation="acquire_preview_storage",
                outcome="failed",
                retryable=True,
                **exception_context,
            ),
        )
        raise HTTPException(503, detail=unavailable_detail) from None

    if not isinstance(backend, PreviewStorageBackend):
        error = HTTPException(503, detail=unavailable_detail)
        logger.error(
            "FOCUS Mapping Preview backend protocol validation failed backend_type=%s%s",
            type(backend).__name__,
            safe_log_context(
                tenant_name=tenant_name,
                tenant_id=tenant_config.tenant_id if include_tenant_id else None,
                request_id=request_id,
                stage=stage,
                operation="validate_preview_storage",
                outcome="failed",
                retryable=True,
            ),
        )
        _release_preview_backend_lease(lease, tenant_name=tenant_name, preserving=error)
        raise error

    try:
        yield backend
    except BaseException as exc:
        _release_preview_backend_lease(lease, tenant_name=tenant_name, preserving=exc)
        raise
    else:
        _release_preview_backend_lease(lease, tenant_name=tenant_name, preserving=None)


@contextmanager
def _repair_backend(
    provider: TenantBackendProvider,
    tenant_name: str,
    tenant_config: TenantConfig,
    *,
    request_id: str | None = None,
) -> Iterator[PreviewEvidenceStorageBackend]:
    from core.preview.persistence import PreviewEvidenceStorageBackend
    from core.preview.storage_availability import PreviewEvidenceAvailabilityState

    try:
        lease = provider.acquire_backend(tenant_name, tenant_config)
        backend = lease.__enter__()
    except Exception as exc:
        logger.error(
            "FOCUS Mapping Preview repair backend failed tenant=%s error_type=%s%s",
            tenant_name,
            type(exc).__name__,
            safe_log_context(
                tenant_name=tenant_name,
                tenant_id=tenant_config.tenant_id,
                request_id=request_id,
                stage="repair_storage_acquisition",
                outcome="failed",
                retryable=True,
                **safe_exception_context(exc),
            ),
        )
        raise HTTPException(503, detail="FOCUS Mapping Preview repair storage is unavailable") from None

    if not isinstance(backend, PreviewEvidenceStorageBackend):
        error = HTTPException(503, detail="FOCUS Mapping Preview repair storage is unavailable")
        _release_preview_backend_lease(lease, tenant_name=tenant_name, preserving=error)
        raise error

    try:
        evidence_ready = backend.preview_evidence_availability.state is PreviewEvidenceAvailabilityState.READY
    except Exception as exc:
        error = HTTPException(503, detail="FOCUS Mapping Preview repair storage is unavailable")
        logger.error(
            "FOCUS Mapping Preview repair backend failed tenant=%s error_type=%s%s",
            tenant_name,
            type(exc).__name__,
            safe_log_context(
                tenant_name=tenant_name,
                tenant_id=tenant_config.tenant_id,
                request_id=request_id,
                stage="repair_storage_validation",
                outcome="failed",
                retryable=True,
                **safe_exception_context(exc),
            ),
        )
        _release_preview_backend_lease(lease, tenant_name=tenant_name, preserving=error)
        raise error from None

    if not evidence_ready:
        error = HTTPException(503, detail="FOCUS Mapping Preview repair storage is unavailable")
        _release_preview_backend_lease(lease, tenant_name=tenant_name, preserving=error)
        raise error

    try:
        yield backend
    except BaseException as exc:
        _release_preview_backend_lease(lease, tenant_name=tenant_name, preserving=exc)
        raise
    else:
        _release_preview_backend_lease(lease, tenant_name=tenant_name, preserving=None)


@contextmanager
def _revision_backend(
    provider: TenantBackendProvider,
    tenant_name: str,
    tenant_config: TenantConfig,
    request_id: str | None = None,
) -> Iterator[PreviewStorageBackend]:
    with _preview_backend(
        provider,
        tenant_name,
        tenant_config,
        unavailable_detail="FOCUS Mapping Preview revision storage is unavailable",
        request_id=request_id,
        stage="revision_storage_acquisition",
        include_tenant_id=False,
    ) as backend:
        yield backend


def _check_ecosystem(tenant_config: TenantConfig) -> None:
    if tenant_config.ecosystem != "confluent_cloud":
        raise HTTPException(400, detail="FOCUS Mapping Preview currently supports only Confluent Cloud tenants")


def _require_focus_preview_enabled(tenant_config: TenantConfig) -> None:
    if not tenant_config.focus_preview_enabled:
        raise HTTPException(409, detail=diagnostic_detail(COMMERCIAL_PROFILE_UNAVAILABLE))


def _get_preview_tenant(settings: AppSettings, tenant_name: str) -> TenantConfig:
    tenant_config = settings.tenants.get(tenant_name)
    if tenant_config is None:
        raise HTTPException(404, detail=f"Tenant {tenant_name!r} not found")
    _check_ecosystem(tenant_config)
    return tenant_config


def _artifact_response(
    artifact: PreviewArtifactMetadata,
    download_url: str,
) -> FocusPreviewArtifactResponse:
    return FocusPreviewArtifactResponse(
        name=artifact.name,
        media_type=artifact.media_type,
        size_bytes=artifact.size_bytes,
        sha256=artifact.sha256,
        order=artifact.order,
        download_url=download_url,
    )


def _serialize(request: PreviewRequest) -> FocusPreviewStatusResponse:
    validate_preview_request_snapshot(
        request=request,
        snapshot=request.source_snapshot,
        resulting_status=request.status,
        mode="strict_materialized",
    )
    base = f"/api/v1/tenants/{request.tenant_name}/focus-preview/requests/{request.request_id}"
    snapshot = None
    if request.source_snapshot is not None:
        effective_start = request.source_snapshot.effective_coverage_start_date
        effective_end = request.source_snapshot.effective_coverage_end_date
        assert effective_start is not None and effective_end is not None
        snapshot = FocusPreviewSourceSnapshotResponse(
            calculation_timestamp=request.source_snapshot.calculation_timestamp,
            calculation_coverage=[
                FocusPreviewCalculationCoverageEntryResponse(
                    tracking_date=item.tracking_date,
                    calculation_id=item.calculation_id,
                    calculation_completed_at=item.calculation_completed_at,
                    calculation_run_id=item.calculation_run_id,
                )
                for item in request.source_snapshot.calculation_coverage
            ],
            source_through=request.source_snapshot.source_through,
            effective_coverage_start_date=effective_start,
            effective_coverage_end_date=effective_end,
            evidence_through_date=(None if effective_start == effective_end else effective_end - timedelta(days=1)),
            availability_cutoff_end_date=request.source_snapshot.availability_cutoff_end_date,
            monthly_status=request.source_snapshot.monthly_status,
        )
    package = None
    if request.package is not None and request.status is PreviewRequestStatus.READY:
        package = FocusPreviewPackageResponse(
            manifest=_artifact_response(request.package.manifest, f"{base}/manifest"),
            files=[_artifact_response(item, f"{base}/files/{item.name}") for item in request.package.files],
            download_all_name=f"focus-mapping-preview-{request.request_id}.zip",
            download_all_url=f"{base}/archive",
        )
    diagnostic = None
    if request.diagnostic is not None:
        diagnostic = FocusPreviewDiagnosticResponse(
            code=request.diagnostic.code,
            message=request.diagnostic.message,
            retryable=request.diagnostic.retryable,
            source_correlation_ids=list(request.diagnostic.source_correlation_ids),
        )
    return FocusPreviewStatusResponse(
        request_id=request.request_id,
        tenant_name=request.tenant_name,
        target_focus_version=FOCUS_PREVIEW_CAPABILITY.target_focus_version,
        conformance_status=FOCUS_PREVIEW_CAPABILITY.conformance_status,
        grain=request.grain,
        start_date=request.start_date,
        end_date=request.end_date,
        month=preview_month(grain=request.grain, start_date=request.start_date, end_date=request.end_date),
        column_profile=request.column_profile,
        effective_columns=list(request.effective_columns),
        status=request.status.value,
        created_at=request.created_at,
        started_at=request.started_at,
        completed_at=request.completed_at,
        expires_at=request.expires_at,
        diagnostic=diagnostic,
        source_snapshot=snapshot,
        package=package,
    )


def _repair_diagnostic_response(
    diagnostic: PreviewDiagnostic | None,
) -> FocusPreviewDiagnosticResponse | None:
    if diagnostic is None:
        return None
    return FocusPreviewDiagnosticResponse(
        code=diagnostic.code,
        message=diagnostic.message,
        retryable=diagnostic.retryable,
        source_correlation_ids=list(diagnostic.source_correlation_ids),
    )


def _serialize_repair(repair: PreviewRepair) -> FocusPreviewRepairResponse:
    return FocusPreviewRepairResponse(
        repair_id=repair.repair_id,
        tenant_name=repair.tenant_name,
        start_date=repair.start_date,
        end_date=repair.end_date,
        status=repair.status.value,
        created_at=repair.created_at,
        started_at=repair.started_at,
        completed_at=repair.completed_at,
        diagnostic=_repair_diagnostic_response(repair.diagnostic),
        dates=[
            FocusPreviewRepairDateResponse(
                tracking_date=item.tracking_date,
                status=item.status.value,
                started_at=item.started_at,
                completed_at=item.completed_at,
                calculation_id=item.calculation_id,
                calculation_completed_at=item.calculation_completed_at,
                rows_written=item.rows_written,
                failure_stage=None if item.failure_stage is None else item.failure_stage.value,
                diagnostic=_repair_diagnostic_response(item.diagnostic),
            )
            for item in repair.dates
        ],
    )


def _snapshot_response(revision: PreviewRevision) -> FocusPreviewSourceSnapshotResponse:
    snapshot = revision.source_snapshot
    start = snapshot.effective_coverage_start_date
    end = snapshot.effective_coverage_end_date
    return FocusPreviewSourceSnapshotResponse(
        calculation_timestamp=snapshot.calculation_timestamp,
        calculation_coverage=[
            FocusPreviewCalculationCoverageEntryResponse(
                tracking_date=item.tracking_date,
                calculation_id=item.calculation_id,
                calculation_completed_at=item.calculation_completed_at,
                calculation_run_id=item.calculation_run_id,
            )
            for item in snapshot.calculation_coverage
        ],
        source_through=snapshot.source_through,
        effective_coverage_start_date=start,
        effective_coverage_end_date=end,
        evidence_through_date=None if start == end else end - timedelta(days=1),
        availability_cutoff_end_date=snapshot.availability_cutoff_end_date,
        monthly_status=snapshot.monthly_status,
    )


def _validation_response(
    reader: PreviewRevisionReader, revision: PreviewRevision
) -> FocusPreviewRevisionValidationSummaryResponse:
    summary = reader.validation_summary(revision=revision)
    return FocusPreviewRevisionValidationSummaryResponse(
        status=summary.status,
        mapping_profile_version=summary.mapping_profile_version,
        source_records=summary.source_records,
        rows=summary.rows,
        mapping_errors=summary.mapping_errors,
        artifact_integrity=summary.artifact_integrity,
    )


def _serialize_revision_summary(
    revision: PreviewRevision,
    *,
    tenant_name: str,
    reader: PreviewRevisionReader,
) -> FocusPreviewRevisionSummaryResponse:
    direct_base = f"/api/v1/tenants/{tenant_name}/focus-preview/revisions/{revision.revision_id}"
    return FocusPreviewRevisionSummaryResponse(
        revision_id=revision.revision_id,
        tenant_name=tenant_name,
        target_focus_version=FOCUS_PREVIEW_CAPABILITY.target_focus_version,
        conformance_status=FOCUS_PREVIEW_CAPABILITY.conformance_status,
        month=revision.month,
        start_date=revision.start_date,
        end_date=revision.end_date,
        monthly_status=revision.monthly_status,
        published_at=revision.published_at,
        supersedes_revision_id=revision.supersedes_revision_id,
        superseded_by_revision_id=revision.superseded_by_revision_id,
        lifecycle="current" if revision.is_current else "superseded",
        material_sha256=revision.material_sha256,
        source_snapshot=_snapshot_response(revision),
        validation=_validation_response(reader, revision),
        replacement_semantics="complete_replacement",
        consumer_action="replace_do_not_aggregate",
        detail_url=direct_base,
    )


def _serialize_revision(
    revision: PreviewRevision,
    *,
    tenant_name: str,
    reader: PreviewRevisionReader,
    direct: bool = False,
) -> FocusPreviewRevisionResponse:
    summary = _serialize_revision_summary(revision, tenant_name=tenant_name, reader=reader)
    if direct:
        base = summary.detail_url
        manifest_url = f"{base}/manifest"
        file_urls = [f"{base}/files/{item.name}" for item in revision.package.files]
        archive_url = f"{base}/archive"
        self_url = base
    else:
        base = f"/api/v1/tenants/{tenant_name}/focus-preview/revisions/current"
        guard = f"month={revision.month}&revision_id={revision.revision_id}"
        manifest_url = f"{base}/manifest?{guard}"
        file_urls = [f"{base}/files/{item.name}?{guard}" for item in revision.package.files]
        archive_url = f"{base}/archive?{guard}"
        self_url = f"{base}?{guard}"
    package = FocusPreviewPackageResponse(
        manifest=_artifact_response(revision.package.manifest, manifest_url),
        files=[_artifact_response(item, url) for item, url in zip(revision.package.files, file_urls, strict=True)],
        download_all_name=f"focus-mapping-preview-{revision.month}-{revision.revision_id}.zip",
        download_all_url=archive_url,
    )
    return FocusPreviewRevisionResponse(
        **summary.model_dump(),
        self_url=self_url,
        package=package,
    )


def _current_revision(
    tenant_name: str,
    scope: FocusPreviewRevisionScope,
    reader: PreviewRevisionReader,
    revision_id: str | None,
    backend: PreviewStorageBackend,
    request_id: str | None = None,
) -> PreviewRevision:
    tenant_config = scope.tenant_config
    try:
        revision = reader.get_current(
            backend=backend,
            ecosystem=tenant_config.ecosystem,
            tenant_id=tenant_config.tenant_id,
            month_start=scope.interval.start_date,
        )
    except PreviewRevisionArtifactUnavailableError as exc:
        raise _revision_artifact_unavailable(
            tenant_name=tenant_name,
            tenant_config=tenant_config,
            error=exc,
            request_id=request_id,
            revision_id=revision_id,
        ) from None
    except Exception as exc:
        logger.error(
            "FOCUS Mapping Preview revision storage read failed owner=%s%s",
            masked_preview_owner(ecosystem=tenant_config.ecosystem, tenant_id=tenant_config.tenant_id),
            safe_log_context(
                tenant_name=tenant_name,
                request_id=request_id,
                revision_id=revision_id,
                month=scope.interval.start_date.strftime("%Y-%m"),
                stage="revision_storage_read",
                operation="read_current_revision",
                outcome="unavailable",
                retryable=True,
                **safe_exception_context(exc),
            ),
        )
        raise HTTPException(503, detail="FOCUS Mapping Preview revision storage is unavailable") from None
    if revision is None:
        raise HTTPException(404, detail="Current FOCUS Mapping Preview revision not found")
    if revision_id is not None and revision.revision_id != revision_id:
        raise HTTPException(
            409,
            detail={
                "code": "focus_preview_current_changed",
                "message": (
                    "The current FOCUS Mapping Preview revision changed; fetch the current revision and retry."
                ),
                "retryable": True,
            },
        )
    return revision


def _revision_artifact_unavailable(
    *,
    tenant_name: str,
    tenant_config: TenantConfig,
    error: BaseException,
    request_id: str | None = None,
    revision_id: str | None = None,
) -> HTTPException:
    logger.error(
        "FOCUS Mapping Preview revision artifact unavailable owner=%s%s",
        masked_preview_owner(ecosystem=tenant_config.ecosystem, tenant_id=tenant_config.tenant_id),
        safe_log_context(
            tenant_name=tenant_name,
            request_id=request_id,
            revision_id=revision_id,
            stage="revision_artifact_read",
            operation="read_revision_artifact",
            outcome="unavailable",
            retryable=True,
            **safe_exception_context(error),
        ),
    )
    return HTTPException(500, detail="Stored FOCUS Mapping Preview revision artifact is unavailable")


def _lookup(
    runtime: PreviewRuntime,
    tenant_name: str,
    tenant_config: TenantConfig,
    request_id: str,
    backend: PreviewStorageBackend,
) -> tuple[PreviewRuntime, PreviewRequest]:
    _check_ecosystem(tenant_config)
    _require_focus_preview_enabled(tenant_config)
    try:
        runtime.ensure_owner_recovered(
            backend=backend,
            owner=preview_artifact_owner(tenant_name, tenant_config),
        )
        runtime.reconcile_expiry(
            backend=backend,
            ecosystem=tenant_config.ecosystem,
            tenant_id=tenant_config.tenant_id,
            request_id=request_id,
        )
    except PreviewRecoveryUnavailable:
        raise HTTPException(503, detail="FOCUS Mapping Preview recovery is unavailable") from None
    try:
        preview = runtime.get_request(
            backend=backend,
            request_id=request_id,
            ecosystem=tenant_config.ecosystem,
            tenant_id=tenant_config.tenant_id,
        )
    except PreviewEffectiveColumnsMetadataMissingError:
        raise HTTPException(503, detail="FOCUS Mapping Preview storage is unavailable") from None
    if preview is None:
        raise HTTPException(404, detail=f"Preview request {request_id!r} not found")
    return runtime, preview


def _log_ignored_columns(
    *,
    tenant_name: str,
    request_id: str | None,
    unknown: tuple[str, ...],
    duplicates: tuple[str, ...],
) -> None:
    if unknown:
        logger.warning(
            "FOCUS Mapping Preview ignored unsupported Custom columns count=%d%s",
            len(unknown),
            safe_log_context(
                tenant_name=tenant_name,
                request_id=request_id,
                stage="preview_request_validation",
                operation="normalize_custom_columns",
                outcome="ignored_unknown_columns",
                retryable=False,
            ),
        )
    if duplicates:
        logger.warning(
            "FOCUS Mapping Preview ignored duplicate Custom columns count=%d%s",
            len(duplicates),
            safe_log_context(
                tenant_name=tenant_name,
                request_id=request_id,
                stage="preview_request_validation",
                operation="normalize_custom_columns",
                outcome="ignored_duplicate_columns",
                retryable=False,
            ),
        )


@router.get("/profile", response_model=FocusPreviewProfileResponse)
def get_profile(
    tenant_name: str,
    settings: Annotated[AppSettings, Depends(get_settings)],
) -> FocusPreviewProfileResponse:
    tenant_config = _get_preview_tenant(settings, tenant_name)
    _require_focus_preview_enabled(tenant_config)
    return FocusPreviewProfileResponse(
        mapping_profile_version=FOCUS_PREVIEW_CAPABILITY.mapping_profile_version,
        target_focus_version=FOCUS_PREVIEW_CAPABILITY.target_focus_version,
        conformance_status=FOCUS_PREVIEW_CAPABILITY.conformance_status,
        full_columns=list(FOCUS_1_4_FULL_PROFILE_COLUMNS),
        summary_columns=list(FOCUS_1_4_SUMMARY_COLUMNS),
        known_gaps=[FocusPreviewKnownGapResponse.model_validate(gap) for gap in preview_manifest_known_gaps()],
    )


@router.post("/repairs", status_code=202, response_model=FocusPreviewRepairResponse)
def submit_repair(
    request: Request,
    response: Response,
    tenant_name: str,
    body: FocusPreviewRepairRequestBody,
    settings: Annotated[AppSettings, Depends(get_settings)],
    provider: Annotated[TenantBackendProvider, Depends(get_backend_provider)],
) -> FocusPreviewRepairResponse:
    tenant_config = _get_preview_tenant(settings, tenant_name)
    _require_focus_preview_enabled(tenant_config)
    created_at = datetime.now(UTC)
    policy = repair_policy_from_tenant_config(tenant_config, created_at=created_at)
    try:
        validate_repair_range(
            body.start_date,
            body.end_date,
            policy=policy,
            created_at=created_at,
        )
    except ValueError as exc:
        error = str(exc)
        if error == "range_invalid":
            detail = {
                "code": "focus_preview_repair_range_invalid",
                "message": (
                    "FOCUS Mapping Preview repair requires an inclusive start date before the exclusive end date."
                ),
                "retryable": False,
            }
        elif error == "future_range":
            detail = {
                "code": "focus_preview_repair_future_range",
                "message": "FOCUS Mapping Preview repair cannot include future UTC dates.",
                "retryable": False,
            }
        else:
            detail = {
                "code": "focus_preview_repair_range_ineligible",
                "message": (
                    "The requested repair range is outside the tenant's complete "
                    "Preview eligibility and retained-data interval."
                ),
                "retryable": False,
            }
        raise HTTPException(400, detail=detail) from None
    runtime = _repair_runtime(request)
    with _repair_backend(
        provider,
        tenant_name,
        tenant_config,
        request_id=getattr(request.state, "request_id", None),
    ) as backend:
        workflow_runner = getattr(request.app.state, "workflow_runner", None)
        is_tenant_running = getattr(workflow_runner, "is_tenant_running", None)
        if callable(is_tenant_running) and is_tenant_running(tenant_name):
            raise HTTPException(
                409,
                detail={
                    "code": "focus_preview_repair_tenant_busy",
                    "message": ("The tenant pipeline is busy; wait for it to finish and retry the repair."),
                    "retryable": True,
                },
            )
        try:
            repair = runtime.submit(
                backend=backend,
                tenant_name=tenant_name,
                tenant_config=tenant_config,
                start_date=body.start_date,
                end_date=body.end_date,
                created_at=created_at,
            )
        except PreviewRepairCapacityUnavailable:
            raise HTTPException(
                429,
                detail={
                    "code": "focus_preview_repair_capacity_exhausted",
                    "message": "FOCUS Mapping Preview repair capacity is exhausted.",
                    "retryable": True,
                },
            ) from None
        except PreviewRepairWorkerUnavailableError:
            raise HTTPException(
                503,
                detail="FOCUS Mapping Preview repair worker is unavailable",
            ) from None
        except PreviewRepairAlreadyActiveError:
            raise HTTPException(
                409,
                detail={
                    "code": "focus_preview_repair_in_progress",
                    "message": ("A FOCUS Mapping Preview repair is already queued or running for this tenant."),
                    "retryable": True,
                },
            ) from None
    response.headers["Location"] = f"/api/v1/tenants/{tenant_name}/focus-preview/repairs/{repair.repair_id}"
    logger.info(
        "FOCUS Mapping Preview repair accepted%s",
        safe_log_context(
            tenant_name=tenant_name,
            tenant_id=tenant_config.tenant_id,
            request_id=getattr(request.state, "request_id", None),
            repair_id=repair.repair_id,
            stage="repair_submission",
            outcome="accepted",
        ),
    )
    return _serialize_repair(repair)


@router.get("/repairs/{repair_id}", response_model=FocusPreviewRepairResponse)
def get_repair(
    tenant_name: str,
    repair_id: str,
    settings: Annotated[AppSettings, Depends(get_settings)],
    provider: Annotated[TenantBackendProvider, Depends(get_backend_provider)],
    request: Request = _NO_REQUEST,
) -> FocusPreviewRepairResponse:
    tenant_config = _get_preview_tenant(settings, tenant_name)
    _require_focus_preview_enabled(tenant_config)
    with (
        _repair_backend(
            provider,
            tenant_name,
            tenant_config,
            request_id=getattr(getattr(request, "state", None), "request_id", None),
        ) as backend,
        backend.create_preview_generation_read_unit_of_work() as uow,
    ):
        repair = uow.repairs.get_for_owner(
            repair_id,
            tenant_config.ecosystem,
            tenant_config.tenant_id,
        )
    if repair is None:
        raise HTTPException(
            404,
            detail=f"FOCUS Mapping Preview repair {repair_id!r} not found",
        )
    return _serialize_repair(repair)


@router.post("/requests", status_code=202, response_model=FocusPreviewStatusResponse)
def submit_preview(
    request: Request,
    tenant_name: str,
    body: FocusPreviewRequestBody,
    settings: Annotated[AppSettings, Depends(get_settings)],
    provider: Annotated[TenantBackendProvider, Depends(get_backend_provider)],
) -> FocusPreviewStatusResponse:
    tenant_config = _get_preview_tenant(settings, tenant_name)
    try:
        interval = (
            canonicalize_daily_interval(start_date=body.start_date, end_date=body.end_date)
            if body.grain == "daily"
            else canonicalize_monthly_interval(month=body.month)
        )
        selection = normalize_column_selection(
            profile=body.column_profile,
            requested_columns=body.columns,
        )
    except PreviewColumnSelectionEmptyError as exc:
        _log_ignored_columns(
            tenant_name=tenant_name,
            request_id=getattr(request.state, "request_id", None),
            unknown=exc.ignored_unknown,
            duplicates=exc.ignored_duplicates,
        )
        raise HTTPException(400, detail=exc.detail) from None
    except PreviewRequestValidationError as exc:
        raise HTTPException(400, detail=exc.detail) from None
    _log_ignored_columns(
        tenant_name=tenant_name,
        request_id=getattr(request.state, "request_id", None),
        unknown=selection.ignored_unknown,
        duplicates=selection.ignored_duplicates,
    )
    _require_focus_preview_enabled(tenant_config)
    runtime = _runtime(request)
    owner = preview_artifact_owner(tenant_name, tenant_config)
    try:
        reservation = runtime.reserve_requested(owner=owner)
    except PreviewCapacityUnavailable:
        raise HTTPException(
            429,
            detail={
                "code": "preview_capacity_exhausted",
                "message": "FOCUS Mapping Preview generation capacity is exhausted.",
                "retryable": True,
            },
        ) from None
    attached = False
    try:
        with _preview_backend(
            provider,
            tenant_name,
            tenant_config,
            request_id=getattr(request.state, "request_id", None),
        ) as backend:
            try:
                runtime.ensure_owner_recovered(
                    backend=backend,
                    owner=owner,
                )
                preview = runtime.submit(
                    tenant_name=tenant_name,
                    tenant_config=tenant_config,
                    backend=backend,
                    start_date=interval.start_date,
                    end_date=interval.end_date,
                    grain=interval.grain,
                    column_profile=body.column_profile,
                    effective_columns=selection.effective_columns,
                    reservation=reservation,
                )
                attached = True
            except PreviewRecoveryUnavailable:
                raise HTTPException(
                    503,
                    detail="FOCUS Mapping Preview recovery is unavailable",
                ) from None
            except PreviewWorkerUnavailable:
                raise HTTPException(
                    503,
                    detail="FOCUS Mapping Preview worker is unavailable",
                ) from None
    finally:
        if not attached:
            reservation.cancel()
    logger.info(
        "FOCUS Mapping Preview request accepted%s",
        safe_log_context(
            tenant_name=tenant_name,
            tenant_id=tenant_config.tenant_id,
            request_id=getattr(request.state, "request_id", None),
            stage="preview_submission",
            outcome="accepted",
        ),
    )
    return _serialize(preview)


@router.get("/requests", response_model=FocusPreviewRequestListResponse)
def list_previews(
    request: Request,
    tenant_name: str,
    settings: Annotated[AppSettings, Depends(get_settings)],
    provider: Annotated[TenantBackendProvider, Depends(get_backend_provider)],
    limit: Annotated[int, Query(ge=1, le=100)] = 20,
    cursor: Annotated[str | None, Query(min_length=1)] = None,
) -> FocusPreviewRequestListResponse:
    tenant_config = _get_preview_tenant(settings, tenant_name)
    _require_focus_preview_enabled(tenant_config)
    runtime = _runtime(request)
    with _preview_backend(
        provider,
        tenant_name,
        tenant_config,
        request_id=getattr(getattr(request, "state", None), "request_id", None),
    ) as backend:
        try:
            runtime.ensure_owner_recovered(
                backend=backend,
                owner=preview_artifact_owner(tenant_name, tenant_config),
            )
            page = runtime.list_recent_requests(
                backend=backend,
                ecosystem=tenant_config.ecosystem,
                tenant_id=tenant_config.tenant_id,
                limit=limit,
                cursor_request_id=cursor,
            )
        except PreviewRecoveryUnavailable:
            raise HTTPException(503, detail="FOCUS Mapping Preview recovery is unavailable") from None
        except PreviewRequestCursorError:
            raise HTTPException(400, detail="Preview request cursor is invalid") from None
    return FocusPreviewRequestListResponse(
        items=[_serialize(item) for item in page.items], next_cursor=page.next_cursor
    )


@router.get("/revisions/current", response_model=FocusPreviewRevisionResponse)
def get_current_revision(
    request: Request,
    tenant_name: str,
    month: str,
    settings: Annotated[AppSettings, Depends(get_settings)],
    provider: Annotated[TenantBackendProvider, Depends(get_backend_provider)],
    revision_id: str | None = None,
) -> FocusPreviewRevisionResponse:
    scope = _revision_scope(tenant_name, month, settings)
    reader = _revision_reader(request)
    with _revision_backend(
        provider,
        tenant_name,
        scope.tenant_config,
        request_id=getattr(request.state, "request_id", None),
    ) as backend:
        revision = _current_revision(
            tenant_name,
            scope,
            reader,
            revision_id,
            backend,
            request_id=getattr(request.state, "request_id", None),
        )
        try:
            return _serialize_revision(revision, tenant_name=tenant_name, reader=reader)
        except Exception as exc:
            raise _revision_artifact_unavailable(
                tenant_name=tenant_name,
                tenant_config=scope.tenant_config,
                error=exc,
                request_id=getattr(request.state, "request_id", None),
                revision_id=revision.revision_id,
            ) from None


@router.get("/revisions/current/manifest")
def get_current_revision_manifest(
    request: Request,
    tenant_name: str,
    month: str,
    revision_id: str,
    settings: Annotated[AppSettings, Depends(get_settings)],
    provider: Annotated[TenantBackendProvider, Depends(get_backend_provider)],
) -> Response:
    scope = _revision_scope(tenant_name, month, settings)
    reader = _revision_reader(request)
    with _revision_backend(
        provider,
        tenant_name,
        scope.tenant_config,
        request_id=getattr(request.state, "request_id", None),
    ) as backend:
        revision = _current_revision(
            tenant_name,
            scope,
            reader,
            revision_id,
            backend,
            request_id=getattr(request.state, "request_id", None),
        )
        try:
            stream = reader.open_manifest_stream(revision=revision)
            return _artifact_streaming_response(stream, media_type="application/json")
        except Exception as exc:
            raise _revision_artifact_unavailable(
                tenant_name=tenant_name,
                tenant_config=scope.tenant_config,
                error=exc,
                request_id=getattr(request.state, "request_id", None),
                revision_id=revision.revision_id,
            ) from None


@router.get("/revisions/current/files/{file_name}")
def get_current_revision_file(
    request: Request,
    tenant_name: str,
    file_name: str,
    month: str,
    revision_id: str,
    settings: Annotated[AppSettings, Depends(get_settings)],
    provider: Annotated[TenantBackendProvider, Depends(get_backend_provider)],
) -> Response:
    scope = _revision_scope(tenant_name, month, settings)
    reader = _revision_reader(request)
    with _revision_backend(
        provider,
        tenant_name,
        scope.tenant_config,
        request_id=getattr(request.state, "request_id", None),
    ) as backend:
        revision = _current_revision(
            tenant_name,
            scope,
            reader,
            revision_id,
            backend,
            request_id=getattr(request.state, "request_id", None),
        )
        if find_preview_artifact_metadata(revision.package.files, file_name) is None:
            raise HTTPException(404, detail="FOCUS Mapping Preview file not found for current revision")
        try:
            metadata, stream = reader.open_file_stream(revision=revision, file_name=file_name)
            return _artifact_streaming_response(stream, media_type=metadata.media_type)
        except Exception as exc:
            raise _revision_artifact_unavailable(
                tenant_name=tenant_name,
                tenant_config=scope.tenant_config,
                error=exc,
                request_id=getattr(request.state, "request_id", None),
                revision_id=revision.revision_id,
            ) from None


@router.get("/revisions/current/archive")
def get_current_revision_archive(
    request: Request,
    tenant_name: str,
    month: str,
    revision_id: str,
    settings: Annotated[AppSettings, Depends(get_settings)],
    provider: Annotated[TenantBackendProvider, Depends(get_backend_provider)],
) -> StreamingResponse:
    scope = _revision_scope(tenant_name, month, settings)
    reader = _revision_reader(request)
    with _revision_backend(
        provider,
        tenant_name,
        scope.tenant_config,
        getattr(getattr(request, "state", None), "request_id", None),
    ) as backend:
        revision = _current_revision(
            tenant_name,
            scope,
            reader,
            revision_id,
            backend,
            getattr(getattr(request, "state", None), "request_id", None),
        )
        try:
            archive = reader.open_archive(revision=revision)
        except Exception as exc:
            raise _revision_artifact_unavailable(
                tenant_name=tenant_name,
                tenant_config=scope.tenant_config,
                error=exc,
                request_id=getattr(getattr(request, "state", None), "request_id", None),
                revision_id=revision.revision_id,
            ) from None

    def chunks() -> Iterator[bytes]:
        try:
            yield from archive.iter_chunks()
        except BaseException as exc:
            _close_revision_archive_safely(archive, preserving=exc)
            raise
        else:
            _close_revision_archive_safely(archive, preserving=None)

    filename = f"focus-mapping-preview-{revision.month}-{revision.revision_id}.zip"
    return StreamingResponse(
        chunks(),
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        background=BackgroundTask(_close_revision_archive_safely, archive, preserving=None),
    )


def _direct_revision(
    tenant_name: str,
    tenant_config: TenantConfig,
    reader: PreviewRevisionReader,
    revision_id: str,
    backend: PreviewStorageBackend,
    request_id: str | None = None,
) -> PreviewRevision:
    try:
        revision = reader.get_for_owner(
            backend=backend,
            ecosystem=tenant_config.ecosystem,
            tenant_id=tenant_config.tenant_id,
            revision_id=revision_id,
        )
    except Exception as exc:
        logger.error(
            "FOCUS Mapping Preview revision storage read failed owner=%s%s",
            masked_preview_owner(ecosystem=tenant_config.ecosystem, tenant_id=tenant_config.tenant_id),
            safe_log_context(
                tenant_name=tenant_name,
                request_id=request_id,
                revision_id=revision_id,
                stage="revision_storage_read",
                operation="read_revision",
                outcome="unavailable",
                retryable=True,
                **safe_exception_context(exc),
            ),
        )
        raise HTTPException(503, detail="FOCUS Mapping Preview revision storage is unavailable") from None
    if revision is None:
        raise HTTPException(404, detail="FOCUS Mapping Preview revision not found")
    return revision


@router.get("/revisions", response_model=FocusPreviewRevisionListResponse)
def list_revisions(
    request: Request,
    tenant_name: str,
    month: str,
    settings: Annotated[AppSettings, Depends(get_settings)],
    provider: Annotated[TenantBackendProvider, Depends(get_backend_provider)],
    limit: Annotated[int, Query(ge=1, le=100)] = 20,
    cursor: Annotated[str | None, Query(min_length=1)] = None,
) -> FocusPreviewRevisionListResponse:
    scope = _revision_scope(tenant_name, month, settings)
    reader = _revision_reader(request)
    tenant_config = scope.tenant_config
    with _revision_backend(
        provider,
        tenant_name,
        tenant_config,
        request_id=getattr(request.state, "request_id", None),
    ) as backend:
        artifact_revision_id: str | None = None
        try:
            page = reader.list_for_owner_month(
                backend=backend,
                ecosystem=tenant_config.ecosystem,
                tenant_id=tenant_config.tenant_id,
                month_start=scope.interval.start_date,
                limit=limit,
                cursor_revision_id=cursor,
            )
            items = []
            for item in page.items:
                artifact_revision_id = item.revision_id
                items.append(_serialize_revision_summary(item, tenant_name=tenant_name, reader=reader))
        except PreviewRevisionCursorError:
            raise HTTPException(400, detail="FOCUS Mapping Preview revision cursor is invalid") from None
        except PreviewRevisionArtifactUnavailableError as exc:
            raise _revision_artifact_unavailable(
                tenant_name=tenant_name,
                tenant_config=tenant_config,
                error=exc,
                request_id=getattr(request.state, "request_id", None),
                revision_id=artifact_revision_id,
            ) from None
        except Exception as exc:
            logger.error(
                "FOCUS Mapping Preview revision history read failed owner=%s%s",
                masked_preview_owner(ecosystem=tenant_config.ecosystem, tenant_id=tenant_config.tenant_id),
                safe_log_context(
                    tenant_name=tenant_name,
                    request_id=getattr(request.state, "request_id", None),
                    month=month,
                    stage="revision_storage_read",
                    operation="list_revisions",
                    outcome="unavailable",
                    retryable=True,
                    **safe_exception_context(exc),
                ),
            )
            raise HTTPException(503, detail="FOCUS Mapping Preview revision storage is unavailable") from None
    return FocusPreviewRevisionListResponse(
        items=items,
        next_cursor=page.next_cursor,
        replacement_semantics="complete_replacement",
        consumer_action="replace_do_not_aggregate",
    )


@router.get("/revisions/{revision_id}", response_model=FocusPreviewRevisionResponse)
def get_revision(
    request: Request,
    tenant_name: str,
    revision_id: str,
    settings: Annotated[AppSettings, Depends(get_settings)],
    provider: Annotated[TenantBackendProvider, Depends(get_backend_provider)],
) -> FocusPreviewRevisionResponse:
    tenant_config = _get_preview_tenant(settings, tenant_name)
    _require_focus_preview_enabled(tenant_config)
    reader = _revision_reader(request)
    with _revision_backend(
        provider,
        tenant_name,
        tenant_config,
        request_id=getattr(request.state, "request_id", None),
    ) as backend:
        revision = _direct_revision(
            tenant_name,
            tenant_config,
            reader,
            revision_id,
            backend,
            request_id=getattr(request.state, "request_id", None),
        )
        try:
            return _serialize_revision(
                revision,
                tenant_name=tenant_name,
                reader=reader,
                direct=True,
            )
        except Exception as exc:
            raise _revision_artifact_unavailable(
                tenant_name=tenant_name,
                tenant_config=tenant_config,
                error=exc,
                request_id=getattr(request.state, "request_id", None),
                revision_id=revision_id,
            ) from None


@router.get("/revisions/{revision_id}/manifest")
def get_revision_manifest(
    request: Request,
    tenant_name: str,
    revision_id: str,
    settings: Annotated[AppSettings, Depends(get_settings)],
    provider: Annotated[TenantBackendProvider, Depends(get_backend_provider)],
) -> Response:
    tenant_config = _get_preview_tenant(settings, tenant_name)
    _require_focus_preview_enabled(tenant_config)
    reader = _revision_reader(request)
    with _revision_backend(
        provider,
        tenant_name,
        tenant_config,
        request_id=getattr(request.state, "request_id", None),
    ) as backend:
        revision = _direct_revision(
            tenant_name,
            tenant_config,
            reader,
            revision_id,
            backend,
            request_id=getattr(request.state, "request_id", None),
        )
        try:
            stream = reader.open_manifest_stream(revision=revision)
            return _artifact_streaming_response(stream, media_type="application/json")
        except Exception as exc:
            raise _revision_artifact_unavailable(
                tenant_name=tenant_name,
                tenant_config=tenant_config,
                error=exc,
                request_id=getattr(request.state, "request_id", None),
                revision_id=revision_id,
            ) from None


@router.get("/revisions/{revision_id}/files/{file_name}")
def get_revision_file(
    request: Request,
    tenant_name: str,
    revision_id: str,
    file_name: str,
    settings: Annotated[AppSettings, Depends(get_settings)],
    provider: Annotated[TenantBackendProvider, Depends(get_backend_provider)],
) -> Response:
    tenant_config = _get_preview_tenant(settings, tenant_name)
    _require_focus_preview_enabled(tenant_config)
    reader = _revision_reader(request)
    with _revision_backend(
        provider,
        tenant_name,
        tenant_config,
        request_id=getattr(request.state, "request_id", None),
    ) as backend:
        revision = _direct_revision(
            tenant_name,
            tenant_config,
            reader,
            revision_id,
            backend,
            request_id=getattr(request.state, "request_id", None),
        )
        try:
            metadata, stream = reader.open_file_stream(revision=revision, file_name=file_name)
            return _artifact_streaming_response(stream, media_type=metadata.media_type)
        except FileNotFoundError, StopIteration:
            raise HTTPException(404, detail="FOCUS Mapping Preview file not found for revision") from None
        except Exception as exc:
            raise _revision_artifact_unavailable(
                tenant_name=tenant_name,
                tenant_config=tenant_config,
                error=exc,
                request_id=getattr(request.state, "request_id", None),
                revision_id=revision_id,
            ) from None


@router.get("/revisions/{revision_id}/archive")
def get_revision_archive(
    request: Request,
    tenant_name: str,
    revision_id: str,
    settings: Annotated[AppSettings, Depends(get_settings)],
    provider: Annotated[TenantBackendProvider, Depends(get_backend_provider)],
) -> StreamingResponse:
    tenant_config = _get_preview_tenant(settings, tenant_name)
    _require_focus_preview_enabled(tenant_config)
    reader = _revision_reader(request)
    with _revision_backend(
        provider,
        tenant_name,
        tenant_config,
        getattr(getattr(request, "state", None), "request_id", None),
    ) as backend:
        revision = _direct_revision(
            tenant_name,
            tenant_config,
            reader,
            revision_id,
            backend,
            getattr(getattr(request, "state", None), "request_id", None),
        )
        try:
            archive = reader.open_archive(revision=revision)
        except Exception as exc:
            raise _revision_artifact_unavailable(
                tenant_name=tenant_name,
                tenant_config=tenant_config,
                error=exc,
                request_id=getattr(getattr(request, "state", None), "request_id", None),
                revision_id=revision_id,
            ) from None

    def chunks() -> Iterator[bytes]:
        try:
            yield from archive.iter_chunks()
        except BaseException as exc:
            _close_revision_archive_safely(archive, preserving=exc)
            raise
        else:
            _close_revision_archive_safely(archive, preserving=None)

    filename = f"focus-mapping-preview-{revision.month}-{revision.revision_id}.zip"
    return StreamingResponse(
        chunks(),
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        background=BackgroundTask(_close_revision_archive_safely, archive, preserving=None),
    )


@router.get("/requests/{request_id}", response_model=FocusPreviewStatusResponse)
def get_preview(
    request: Request,
    tenant_name: str,
    request_id: str,
    settings: Annotated[AppSettings, Depends(get_settings)],
    provider: Annotated[TenantBackendProvider, Depends(get_backend_provider)],
) -> FocusPreviewStatusResponse:
    tenant_config = _get_preview_tenant(settings, tenant_name)
    _require_focus_preview_enabled(tenant_config)
    runtime = _runtime(request)
    with _preview_backend(
        provider,
        tenant_name,
        tenant_config,
        request_id=getattr(getattr(request, "state", None), "request_id", None),
    ) as backend:
        _runtime_value, preview = _lookup(runtime, tenant_name, tenant_config, request_id, backend)
    return _serialize(preview)


def _require_ready(preview: PreviewRequest) -> None:
    if preview.status in {PreviewRequestStatus.QUEUED, PreviewRequestStatus.RUNNING}:
        raise HTTPException(
            409,
            detail=f"Preview request {preview.request_id!r} is not ready (status: {preview.status.value})",
        )
    if preview.status is PreviewRequestStatus.FAILED:
        raise HTTPException(409, detail=f"Preview request {preview.request_id!r} failed; inspect diagnostics")
    if preview.status is PreviewRequestStatus.EXPIRED:
        assert preview.expires_at is not None
        expires = preview.expires_at.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        raise HTTPException(410, detail=f"Preview request {preview.request_id!r} expired at {expires}")


@router.get("/requests/{request_id}/manifest")
def get_manifest(
    request: Request,
    tenant_name: str,
    request_id: str,
    settings: Annotated[AppSettings, Depends(get_settings)],
    provider: Annotated[TenantBackendProvider, Depends(get_backend_provider)],
) -> StreamingResponse:
    tenant_config = _get_preview_tenant(settings, tenant_name)
    _require_focus_preview_enabled(tenant_config)
    runtime = _runtime(request)
    with _preview_backend(
        provider,
        tenant_name,
        tenant_config,
        request_id=getattr(getattr(request, "state", None), "request_id", None),
    ) as backend:
        runtime, preview = _lookup(runtime, tenant_name, tenant_config, request_id, backend)
        _require_ready(preview)
        try:
            stream = runtime.open_manifest_stream(preview)
        except PreviewArtifactUnavailable, OSError:
            raise HTTPException(500, detail="Stored preview artifact is unavailable") from None
    return _artifact_streaming_response(stream, media_type="application/json")


@router.get("/requests/{request_id}/files/{file_name}")
def get_file(
    request: Request,
    tenant_name: str,
    request_id: str,
    file_name: str,
    settings: Annotated[AppSettings, Depends(get_settings)],
    provider: Annotated[TenantBackendProvider, Depends(get_backend_provider)],
) -> StreamingResponse:
    tenant_config = _get_preview_tenant(settings, tenant_name)
    _require_focus_preview_enabled(tenant_config)
    runtime = _runtime(request)
    with _preview_backend(
        provider,
        tenant_name,
        tenant_config,
        request_id=getattr(getattr(request, "state", None), "request_id", None),
    ) as backend:
        runtime, preview = _lookup(runtime, tenant_name, tenant_config, request_id, backend)
        _require_ready(preview)
        if preview.package is None:
            raise HTTPException(404, detail=f"Preview file {file_name!r} not found for request {request_id!r}")
        metadata = find_preview_artifact_metadata(preview.package.files, file_name)
        if metadata is None:
            raise HTTPException(404, detail=f"Preview file {file_name!r} not found for request {request_id!r}")
        try:
            stream = runtime.open_file_stream(preview, file_name)
        except PreviewArtifactUnavailable, OSError:
            raise HTTPException(500, detail="Stored preview artifact is unavailable") from None
    return _artifact_streaming_response(stream, media_type=metadata.media_type)


def _artifact_streaming_response(
    stream: PreviewVerifiedArtifactStream,
    *,
    media_type: str,
) -> StreamingResponse:
    def chunks() -> Iterator[bytes]:
        try:
            yield from stream.iter_chunks()
        finally:
            stream.close()

    return StreamingResponse(
        chunks(),
        media_type=media_type,
        headers={"Content-Length": str(stream.size_bytes)},
        background=BackgroundTask(stream.close),
    )


@router.get("/requests/{request_id}/archive")
def get_archive(
    request: Request,
    tenant_name: str,
    request_id: str,
    settings: Annotated[AppSettings, Depends(get_settings)],
    provider: Annotated[TenantBackendProvider, Depends(get_backend_provider)],
) -> StreamingResponse:
    tenant_config = _get_preview_tenant(settings, tenant_name)
    _require_focus_preview_enabled(tenant_config)
    runtime = _runtime(request)
    with _preview_backend(
        provider,
        tenant_name,
        tenant_config,
        request_id=getattr(getattr(request, "state", None), "request_id", None),
    ) as backend:
        runtime, preview = _lookup(runtime, tenant_name, tenant_config, request_id, backend)
        _require_ready(preview)
        try:
            archive = runtime.open_archive(preview)
        except PreviewArtifactUnavailable, OSError:
            raise HTTPException(500, detail="Stored preview artifact is unavailable") from None

    def chunks() -> Iterator[bytes]:
        try:
            yield from archive.iter_chunks()
        finally:
            archive.close()

    filename = f"focus-mapping-preview-{request_id}.zip"
    return StreamingResponse(
        chunks(),
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        background=BackgroundTask(archive.close),
    )
