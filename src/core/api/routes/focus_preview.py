from __future__ import annotations

import logging
from datetime import timedelta
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import Response

from core.api.dependencies import get_or_create_backend, get_tenant_config
from core.api.schemas import (  # noqa: TC001  # FastAPI evaluates annotations
    FocusPreviewArtifactResponse,
    FocusPreviewCalculationCoverageEntryResponse,
    FocusPreviewDiagnosticResponse,
    FocusPreviewPackageResponse,
    FocusPreviewProfileResponse,
    FocusPreviewRequestBody,
    FocusPreviewSourceSnapshotResponse,
    FocusPreviewStatusResponse,
)
from core.config.models import TenantConfig  # noqa: TC001  # FastAPI evaluates annotations
from core.preview.mapping import (
    FOCUS_1_4_FULL_PROFILE_COLUMNS,
    FOCUS_1_4_SUMMARY_COLUMNS,
    MAPPING_PROFILE_VERSION,
)
from core.preview.models import (
    PreviewArtifactMetadata,
    PreviewRequest,
    PreviewRequestStatus,
    preview_month,
    validate_preview_request_snapshot,
)
from core.preview.persistence import PreviewStorageBackend
from core.preview.request import (
    PreviewColumnSelectionEmptyError,
    PreviewRequestValidationError,
    canonicalize_daily_interval,
    canonicalize_monthly_interval,
    normalize_column_selection,
)
from core.preview.service import PreviewArtifactUnavailable, PreviewRuntime, PreviewWorkerUnavailable

router = APIRouter(prefix="/tenants/{tenant_name}/focus-preview", tags=["focus-preview"])
logger = logging.getLogger(__name__)


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
    except Exception:
        logger.exception("FOCUS Mapping Preview backend creation failed")
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
    if request.package is not None:
        package = FocusPreviewPackageResponse(
            manifest=_artifact_response(request.package.manifest, f"{base}/manifest"),
            files=[_artifact_response(item, f"{base}/files/{item.name}") for item in request.package.files],
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
        diagnostic=diagnostic,
        source_snapshot=snapshot,
        package=package,
    )


def _lookup(
    request: Request,
    tenant_name: str,
    tenant_config: TenantConfig,
    request_id: str,
) -> tuple[PreviewRuntime, PreviewRequest]:
    _check_ecosystem(tenant_config)
    runtime = _runtime(request)
    backend = _backend(request, tenant_name, tenant_config)
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
    except PreviewWorkerUnavailable:
        raise HTTPException(503, detail="FOCUS Mapping Preview worker is unavailable") from None
    return _serialize(preview)


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
