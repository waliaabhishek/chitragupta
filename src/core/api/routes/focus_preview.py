from __future__ import annotations

import logging
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, timedelta
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import Response, StreamingResponse
from starlette.background import BackgroundTask

from core.api.dependencies import get_or_create_backend, get_settings, get_tenant_config
from core.api.schemas import (  # noqa: TC001  # FastAPI evaluates annotations
    FocusPreviewArtifactResponse,
    FocusPreviewCalculationCoverageEntryResponse,
    FocusPreviewDiagnosticResponse,
    FocusPreviewPackageResponse,
    FocusPreviewProfileResponse,
    FocusPreviewRequestBody,
    FocusPreviewRequestListResponse,
    FocusPreviewRevisionResponse,
    FocusPreviewSourceSnapshotResponse,
    FocusPreviewStatusResponse,
)
from core.config.models import AppSettings, TenantConfig  # noqa: TC001  # FastAPI evaluates annotations
from core.preview.artifacts import PreviewArchiveStream  # noqa: TC001 - used by FastAPI route helpers
from core.preview.mapping import (
    FOCUS_1_4_FULL_PROFILE_COLUMNS,
    FOCUS_1_4_SUMMARY_COLUMNS,
    MAPPING_PROFILE_VERSION,
)
from core.preview.models import (
    PreviewArtifactMetadata,
    PreviewInterval,
    PreviewRequest,
    PreviewRequestStatus,
    PreviewRevision,
    preview_month,
    validate_preview_request_snapshot,
)
from core.preview.persistence import PreviewRequestCursorError, PreviewStorageBackend
from core.preview.request import (
    PreviewColumnSelectionEmptyError,
    PreviewRequestValidationError,
    canonicalize_daily_interval,
    canonicalize_monthly_interval,
    normalize_column_selection,
)
from core.preview.revisions import (
    PreviewCurrentRevisionReader,
    PreviewRevisionArtifactUnavailableError,
    masked_preview_owner,
)
from core.preview.service import (
    PreviewArtifactUnavailable,
    PreviewRecoveryUnavailable,
    PreviewRuntime,
    PreviewWorkerUnavailable,
)

router = APIRouter(prefix="/tenants/{tenant_name}/focus-preview", tags=["focus-preview"])
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class FocusPreviewRevisionScope:
    tenant_config: TenantConfig
    interval: PreviewInterval


def _revision_scope(
    tenant_name: str,
    month: str,
    settings: Annotated[AppSettings, Depends(get_settings)],
) -> FocusPreviewRevisionScope:
    try:
        interval = canonicalize_monthly_interval(month=month)
    except PreviewRequestValidationError as exc:
        raise HTTPException(400, detail=exc.detail) from None
    tenant_config = settings.tenants.get(tenant_name)
    if tenant_config is None:
        raise HTTPException(404, detail=f"Tenant {tenant_name!r} not found")
    _check_ecosystem(tenant_config)
    return FocusPreviewRevisionScope(tenant_config, interval)


def _revision_reader(request: Request) -> PreviewCurrentRevisionReader:
    reader = getattr(request.app.state, "preview_revision_reader", None)
    if not isinstance(reader, PreviewCurrentRevisionReader):
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
    if not isinstance(runtime, PreviewRuntime):
        raise HTTPException(503, detail="FOCUS Mapping Preview runtime is unavailable")
    return runtime


def _backend(request: Request, tenant_name: str, tenant_config: TenantConfig) -> PreviewStorageBackend:
    try:
        backend = get_or_create_backend(
            request.app.state.backends,
            tenant_name,
            tenant_config.storage,
            tenant_config.ecosystem,
        )
    except Exception as exc:
        logger.error(
            "FOCUS Mapping Preview backend creation failed tenant=%s error_type=%s",
            tenant_name,
            type(exc).__name__,
        )
        raise HTTPException(503, detail="FOCUS Mapping Preview storage is unavailable") from None
    if not isinstance(backend, PreviewStorageBackend):
        raise HTTPException(503, detail="FOCUS Mapping Preview storage is unavailable")
    return backend


def _check_ecosystem(tenant_config: TenantConfig) -> None:
    if tenant_config.ecosystem != "confluent_cloud":
        raise HTTPException(400, detail="FOCUS Mapping Preview currently supports only Confluent Cloud tenants")


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


def _serialize_revision(revision: PreviewRevision, *, tenant_name: str) -> FocusPreviewRevisionResponse:
    base = f"/api/v1/tenants/{tenant_name}/focus-preview/revisions/current"
    guard = f"month={revision.month}&revision_id={revision.revision_id}"
    package = FocusPreviewPackageResponse(
        manifest=_artifact_response(revision.package.manifest, f"{base}/manifest?{guard}"),
        files=[_artifact_response(item, f"{base}/files/{item.name}?{guard}") for item in revision.package.files],
        download_all_name=f"focus-mapping-preview-{revision.month}-{revision.revision_id}.zip",
        download_all_url=f"{base}/archive?{guard}",
    )
    return FocusPreviewRevisionResponse(
        revision_id=revision.revision_id,
        tenant_name=tenant_name,
        month=revision.month,
        start_date=revision.start_date,
        end_date=revision.end_date,
        monthly_status=revision.monthly_status,
        published_at=revision.published_at,
        supersedes_revision_id=revision.supersedes_revision_id,
        material_sha256=revision.material_sha256,
        source_snapshot=_snapshot_response(revision),
        self_url=f"{base}?{guard}",
        package=package,
    )


def _current_revision(
    request: Request,
    tenant_name: str,
    scope: FocusPreviewRevisionScope,
    reader: PreviewCurrentRevisionReader,
    revision_id: str | None,
) -> PreviewRevision:
    tenant_config = scope.tenant_config
    try:
        backend = _backend(request, tenant_name, tenant_config)
    except HTTPException as exc:
        if exc.status_code == 503:
            raise HTTPException(503, detail="FOCUS Mapping Preview revision storage is unavailable") from None
        raise
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
        ) from None
    except Exception as exc:
        logger.error(
            "FOCUS Mapping Preview revision storage read failed tenant=%s owner=%s error_type=%s",
            tenant_name,
            masked_preview_owner(ecosystem=tenant_config.ecosystem, tenant_id=tenant_config.tenant_id),
            type(exc).__name__,
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
) -> HTTPException:
    logger.error(
        "FOCUS Mapping Preview revision artifact unavailable tenant=%s owner=%s error_type=%s",
        tenant_name,
        masked_preview_owner(ecosystem=tenant_config.ecosystem, tenant_id=tenant_config.tenant_id),
        type(error).__name__,
    )
    return HTTPException(500, detail="Stored FOCUS Mapping Preview revision artifact is unavailable")


def _lookup(
    request: Request,
    tenant_name: str,
    tenant_config: TenantConfig,
    request_id: str,
) -> tuple[PreviewRuntime, PreviewRequest]:
    _check_ecosystem(tenant_config)
    runtime = _runtime(request)
    backend = _backend(request, tenant_name, tenant_config)
    try:
        runtime.ensure_owner_recovered(
            backend=backend,
            tenant_name=tenant_name,
            ecosystem=tenant_config.ecosystem,
            tenant_id=tenant_config.tenant_id,
        )
        runtime.reconcile_expiry(
            backend=backend,
            ecosystem=tenant_config.ecosystem,
            tenant_id=tenant_config.tenant_id,
            request_id=request_id,
        )
    except PreviewRecoveryUnavailable:
        raise HTTPException(503, detail="FOCUS Mapping Preview recovery is unavailable") from None
    preview = runtime.get_request(
        backend=backend,
        request_id=request_id,
        ecosystem=tenant_config.ecosystem,
        tenant_id=tenant_config.tenant_id,
    )
    if preview is None:
        raise HTTPException(404, detail=f"Preview request {request_id!r} not found")
    return runtime, preview


def _log_ignored_columns(
    tenant_name: str,
    unknown: tuple[str, ...],
    duplicates: tuple[str, ...],
) -> None:
    for column in unknown:
        logger.warning(
            "FOCUS Mapping Preview ignored unsupported Custom column tenant=%s column=%r",
            tenant_name,
            column,
        )
    for column in duplicates:
        logger.warning(
            "FOCUS Mapping Preview ignored duplicate Custom column tenant=%s column=%r",
            tenant_name,
            column,
        )


@router.get("/profile", response_model=FocusPreviewProfileResponse)
def get_profile(
    tenant_config: Annotated[TenantConfig, Depends(get_tenant_config)],
) -> FocusPreviewProfileResponse:
    _check_ecosystem(tenant_config)
    return FocusPreviewProfileResponse(
        mapping_profile_version=MAPPING_PROFILE_VERSION,
        full_columns=list(FOCUS_1_4_FULL_PROFILE_COLUMNS),
        summary_columns=list(FOCUS_1_4_SUMMARY_COLUMNS),
    )


@router.post("/requests", status_code=202, response_model=FocusPreviewStatusResponse)
def submit_preview(
    request: Request,
    tenant_name: str,
    body: FocusPreviewRequestBody,
    tenant_config: Annotated[TenantConfig, Depends(get_tenant_config)],
) -> FocusPreviewStatusResponse:
    _check_ecosystem(tenant_config)
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
        _log_ignored_columns(tenant_name, exc.ignored_unknown, exc.ignored_duplicates)
        raise HTTPException(400, detail=exc.detail) from None
    except PreviewRequestValidationError as exc:
        raise HTTPException(400, detail=exc.detail) from None
    _log_ignored_columns(tenant_name, selection.ignored_unknown, selection.ignored_duplicates)
    runtime = _runtime(request)
    backend = _backend(request, tenant_name, tenant_config)
    try:
        runtime.ensure_owner_recovered(
            backend=backend,
            tenant_name=tenant_name,
            ecosystem=tenant_config.ecosystem,
            tenant_id=tenant_config.tenant_id,
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
        )
    except PreviewRecoveryUnavailable:
        raise HTTPException(503, detail="FOCUS Mapping Preview recovery is unavailable") from None
    except PreviewWorkerUnavailable:
        raise HTTPException(503, detail="FOCUS Mapping Preview worker is unavailable") from None
    return _serialize(preview)


@router.get("/requests", response_model=FocusPreviewRequestListResponse)
def list_previews(
    request: Request,
    tenant_name: str,
    tenant_config: Annotated[TenantConfig, Depends(get_tenant_config)],
    limit: Annotated[int, Query(ge=1, le=100)] = 20,
    cursor: Annotated[str | None, Query(min_length=1)] = None,
) -> FocusPreviewRequestListResponse:
    _check_ecosystem(tenant_config)
    runtime = _runtime(request)
    backend = _backend(request, tenant_name, tenant_config)
    try:
        runtime.ensure_owner_recovered(
            backend=backend,
            tenant_name=tenant_name,
            ecosystem=tenant_config.ecosystem,
            tenant_id=tenant_config.tenant_id,
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
    scope: Annotated[FocusPreviewRevisionScope, Depends(_revision_scope)],
    revision_id: str | None = None,
) -> FocusPreviewRevisionResponse:
    reader = _revision_reader(request)
    revision = _current_revision(request, tenant_name, scope, reader, revision_id)
    return _serialize_revision(revision, tenant_name=tenant_name)


@router.get("/revisions/current/manifest")
def get_current_revision_manifest(
    request: Request,
    tenant_name: str,
    revision_id: str,
    scope: Annotated[FocusPreviewRevisionScope, Depends(_revision_scope)],
) -> Response:
    reader = _revision_reader(request)
    revision = _current_revision(request, tenant_name, scope, reader, revision_id)
    try:
        body = reader.read_manifest(revision)
    except Exception as exc:
        raise _revision_artifact_unavailable(
            tenant_name=tenant_name,
            tenant_config=scope.tenant_config,
            error=exc,
        ) from None
    return Response(body, media_type="application/json")


@router.get("/revisions/current/files/{file_name}")
def get_current_revision_file(
    request: Request,
    tenant_name: str,
    file_name: str,
    revision_id: str,
    scope: Annotated[FocusPreviewRevisionScope, Depends(_revision_scope)],
) -> Response:
    reader = _revision_reader(request)
    revision = _current_revision(request, tenant_name, scope, reader, revision_id)
    if file_name not in {item.name for item in revision.package.files}:
        raise HTTPException(404, detail="FOCUS Mapping Preview file not found for current revision")
    try:
        metadata, body = reader.read_file(revision, file_name)
    except Exception as exc:
        raise _revision_artifact_unavailable(
            tenant_name=tenant_name,
            tenant_config=scope.tenant_config,
            error=exc,
        ) from None
    return Response(body, media_type=metadata.media_type)


@router.get("/revisions/current/archive")
def get_current_revision_archive(
    request: Request,
    tenant_name: str,
    revision_id: str,
    scope: Annotated[FocusPreviewRevisionScope, Depends(_revision_scope)],
) -> StreamingResponse:
    reader = _revision_reader(request)
    revision = _current_revision(request, tenant_name, scope, reader, revision_id)
    try:
        archive = reader.open_archive(revision)
    except Exception as exc:
        raise _revision_artifact_unavailable(
            tenant_name=tenant_name,
            tenant_config=scope.tenant_config,
            error=exc,
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
    tenant_config: Annotated[TenantConfig, Depends(get_tenant_config)],
) -> FocusPreviewStatusResponse:
    _runtime_value, preview = _lookup(request, tenant_name, tenant_config, request_id)
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
    tenant_config: Annotated[TenantConfig, Depends(get_tenant_config)],
) -> Response:
    runtime, preview = _lookup(request, tenant_name, tenant_config, request_id)
    _require_ready(preview)
    try:
        body = runtime.read_manifest_bytes(preview)
    except PreviewArtifactUnavailable, OSError:
        raise HTTPException(500, detail="Stored preview artifact is unavailable") from None
    return Response(body, media_type="application/json")


@router.get("/requests/{request_id}/files/{file_name}")
def get_file(
    request: Request,
    tenant_name: str,
    request_id: str,
    file_name: str,
    tenant_config: Annotated[TenantConfig, Depends(get_tenant_config)],
) -> Response:
    runtime, preview = _lookup(request, tenant_name, tenant_config, request_id)
    _require_ready(preview)
    if preview.package is None or file_name not in {item.name for item in preview.package.files}:
        raise HTTPException(404, detail=f"Preview file {file_name!r} not found for request {request_id!r}")
    metadata = next(item for item in preview.package.files if item.name == file_name)
    try:
        body = runtime.read_file_bytes(preview, file_name)
    except PreviewArtifactUnavailable, OSError:
        raise HTTPException(500, detail="Stored preview artifact is unavailable") from None
    return Response(body, media_type=metadata.media_type)


@router.get("/requests/{request_id}/archive")
def get_archive(
    request: Request,
    tenant_name: str,
    request_id: str,
    tenant_config: Annotated[TenantConfig, Depends(get_tenant_config)],
) -> StreamingResponse:
    runtime, preview = _lookup(request, tenant_name, tenant_config, request_id)
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
