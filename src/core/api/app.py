from __future__ import annotations

import asyncio
import logging
import time
import uuid
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

from fastapi import FastAPI, Request
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.responses import JSONResponse, Response

from core.api import API_VERSION
from core.api.exception_handler import global_exception_handler
from core.config.models import TenantConfig  # noqa: TC001  # resolved by get_type_hints contract tests
from core.logging_context import safe_exception_context, safe_log_context
from core.preview.service import PreviewRuntime
from core.storage.backend_provider import ApiTenantBackendProvider, TenantBackendProvider

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from starlette.types import ASGIApp

    from core.config.models import AppSettings
    from core.plugin.registry import PluginRegistry
    from workflow_runner import WorkflowRunner


class RequestTimeoutMiddleware(BaseHTTPMiddleware):
    """Return 504 to client if request exceeds timeout_seconds.

    Note: for sync def endpoints, the threadpool thread continues running after
    timeout — this provides client-side backpressure only, not threadpool relief.
    """

    def __init__(self, app: ASGIApp, timeout_seconds: int) -> None:
        super().__init__(app)
        self.timeout_seconds = timeout_seconds

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        try:
            return await asyncio.wait_for(call_next(request), timeout=float(self.timeout_seconds))
        except TimeoutError as exc:
            logger.warning(
                "request_timeout%s",
                safe_log_context(
                    request_id=getattr(request.state, "request_id", None),
                    stage="api_request",
                    outcome="timeout",
                    retryable=True,
                    **safe_exception_context(exc),
                ),
            )
            return JSONResponse(
                {"detail": f"Request exceeded {self.timeout_seconds}s timeout"},
                status_code=504,
            )


class RequestContextMiddleware(BaseHTTPMiddleware):
    """Attach a process-local request correlation id and log bounded lifecycle events."""

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        request_id = uuid.uuid4().hex
        request.state.request_id = request_id
        started_at = time.perf_counter()
        logger.debug(
            "request_started method=%s path=%s%s",
            request.method,
            request.url.path,
            safe_log_context(request_id=request_id, stage="api_request", outcome="started"),
        )
        response = await call_next(request)
        elapsed_ms = int((time.perf_counter() - started_at) * 1000)
        logger.debug(
            "request_completed method=%s path=%s status=%d elapsed_ms=%d%s",
            request.method,
            request.url.path,
            response.status_code,
            elapsed_ms,
            safe_log_context(request_id=request_id, stage="api_request", outcome="completed"),
        )
        return response


logger = logging.getLogger(__name__)


def recover_preview_owner(
    tenant_name: str,
    tenant_config: TenantConfig,
    backend_provider: TenantBackendProvider,
    preview_runtime: PreviewRuntime,
) -> None:
    from core.preview.artifacts import preview_artifact_owner
    from core.preview.persistence import PreviewStorageBackend
    from core.preview.service import PreviewRecoveryUnavailable

    if not isinstance(preview_runtime, PreviewRuntime):
        raise PreviewRecoveryUnavailable("FOCUS Mapping Preview recovery is unavailable")
    with backend_provider.acquire_backend(tenant_name, tenant_config) as backend:
        if not isinstance(backend, PreviewStorageBackend):
            raise PreviewRecoveryUnavailable("FOCUS Mapping Preview recovery is unavailable")
        preview_runtime.ensure_owner_recovered(
            backend=backend,
            owner=preview_artifact_owner(tenant_name, tenant_config),
        )


def create_app(
    settings: AppSettings | None = None,
    *,
    workflow_runner: WorkflowRunner | None = None,
    mode: str = "api",
    plugin_registry: PluginRegistry | None = None,
) -> FastAPI:
    """Factory function for creating the FastAPI application."""
    if settings is None:
        from core.config.models import AppSettings as _AppSettings

        settings = _AppSettings()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        logger.info("Chitragupta API starting up version=%s", API_VERSION)
        app.state.settings = settings
        backend_provider: TenantBackendProvider
        owns_backend_provider = False
        if isinstance(workflow_runner, TenantBackendProvider):
            backend_provider = workflow_runner
        else:
            from core.plugin.loader import build_plugin_registry

            backend_provider = ApiTenantBackendProvider(plugin_registry or build_plugin_registry(settings))
            owns_backend_provider = True
        app.state.backend_provider = backend_provider
        app.state.preview_artifact_store = None
        app.state.preview_runtime = None
        app.state.preview_repair_runtime = None
        app.state.preview_revision_reader = None
        app.state.workflow_runner = workflow_runner
        app.state.mode = mode
        from core.preview.artifacts import LocalPreviewArtifactStore, preview_artifact_owner
        from core.preview.repair import PreviewRepairRunner, PreviewRepairRuntime
        from core.preview.revisions import PreviewRevisionReadService
        from core.preview.service import PreviewRecoveryUnavailable, PreviewRuntime

        preview_artifact_store: LocalPreviewArtifactStore | None = None
        preview_runtime: PreviewRuntime | None = None
        preview_repair_runtime: PreviewRepairRuntime | None = None
        original_error: BaseException | None = None
        try:
            if settings.focus_preview_enabled:
                preview_artifact_store = LocalPreviewArtifactStore(settings.preview.artifact_root)
                enabled_owners = tuple(
                    preview_artifact_owner(tenant_name, tenant)
                    for tenant_name, tenant in settings.tenants.items()
                    if tenant.focus_preview_enabled
                )
                shared_scheduler = (
                    workflow_runner.preview_generation_scheduler
                    if mode == "both" and workflow_runner is not None
                    else None
                )
                preview_runtime = PreviewRuntime(
                    artifact_store=preview_artifact_store,
                    backend_provider=backend_provider,
                    max_workers=settings.preview.max_workers,
                    max_queued_generations=settings.preview.max_queued_generations,
                    max_running_generations_per_tenant=(settings.preview.max_running_generations_per_tenant),
                    max_queued_generations_per_tenant=(settings.preview.max_queued_generations_per_tenant),
                    max_generation_spool_bytes=settings.preview.max_generation_spool_bytes,
                    max_csv_file_bytes=settings.preview.max_csv_file_bytes,
                    configured_owners=enabled_owners,
                    scheduler=shared_scheduler,
                )
                app.state.preview_artifact_store = preview_artifact_store
                app.state.preview_runtime = preview_runtime
                app.state.preview_revision_reader = PreviewRevisionReadService(
                    artifact_store=preview_artifact_store,
                )
                for tenant_name, tenant_config in settings.tenants.items():
                    if not tenant_config.focus_preview_enabled:
                        continue
                    try:
                        recover_preview_owner(
                            tenant_name,
                            tenant_config,
                            backend_provider,
                            preview_runtime,
                        )
                    except PreviewRecoveryUnavailable:
                        pass
                    except Exception as exc:
                        logger.error(
                            "preview_owner_recovery_unavailable%s",
                            safe_log_context(
                                tenant_name=tenant_name,
                                stage="preview_owner_recovery",
                                outcome="unavailable",
                                retryable=True,
                                **safe_exception_context(exc),
                            ),
                        )
                if mode == "both" and isinstance(workflow_runner, PreviewRepairRunner):
                    preview_repair_runtime = PreviewRepairRuntime(
                        runner=workflow_runner,
                        backend_provider=backend_provider,
                        max_workers=settings.preview.max_workers,
                        max_queued_repairs=settings.preview.max_queued_repairs,
                        configured_owners=tuple(
                            (tenant_name, tenant_config)
                            for tenant_name, tenant_config in settings.tenants.items()
                            if tenant_config.focus_preview_enabled
                        ),
                    )
                    preview_repair_runtime.recover()
                    app.state.preview_repair_runtime = preview_repair_runtime
            if workflow_runner is None:
                for tenant_name, tenant_config in settings.tenants.items():
                    try:
                        with backend_provider.acquire_backend(tenant_name, tenant_config):
                            pass
                    except Exception as exc:
                        logger.warning(
                            "backend_prepare_failed%s",
                            safe_log_context(
                                tenant_name=tenant_name,
                                stage="backend_prepare",
                                outcome="failed",
                                retryable=True,
                                **safe_exception_context(exc),
                            ),
                        )
            yield
        except BaseException as exc:
            original_error = exc
            raise
        finally:
            logger.info("Chitragupta API shutting down — disposing backends")
            cleanup_errors: list[BaseException] = []

            def record_cleanup_error(step: str, exc: BaseException) -> None:
                cleanup_errors.append(exc)
                logger.error(
                    "api_cleanup_failed%s",
                    safe_log_context(
                        stage=step,
                        operation="api_cleanup",
                        outcome="failed",
                        retryable=False,
                        **safe_exception_context(exc),
                    ),
                )

            if preview_repair_runtime is not None:
                try:
                    preview_repair_runtime.close(wait=True)
                except BaseException as exc:
                    record_cleanup_error("preview_repair_runtime", exc)
            if mode == "both" and workflow_runner is not None:
                logger.debug("Draining workflow runner")
                try:
                    workflow_runner.drain(30)
                except BaseException as exc:
                    record_cleanup_error("workflow_runner", exc)
            if preview_runtime is not None:
                try:
                    preview_runtime.close(wait=True)
                except BaseException as exc:
                    record_cleanup_error("preview_runtime", exc)
            if preview_artifact_store is not None:
                try:
                    preview_artifact_store.close()
                except BaseException as exc:
                    record_cleanup_error("preview_artifact_store", exc)
            if owns_backend_provider:
                try:
                    backend_provider.close()
                except BaseException as exc:
                    record_cleanup_error("backend_provider", exc)
            if mode != "both" and workflow_runner is not None:
                logger.debug("Draining workflow runner")
                try:
                    workflow_runner.drain(30)
                except BaseException as exc:
                    record_cleanup_error("workflow_runner", exc)
            logger.info("Chitragupta API shutdown complete")
            if cleanup_errors and original_error is None:
                raise cleanup_errors[0]

    app = FastAPI(
        title="Chitragupta API",
        version=API_VERSION,
        lifespan=lifespan,
    )

    app.add_exception_handler(Exception, global_exception_handler)

    if settings.api.enable_cors:
        from fastapi.middleware.cors import CORSMiddleware

        app.add_middleware(
            CORSMiddleware,
            allow_origins=settings.api.cors_origins,
            allow_methods=["GET", "POST", "PATCH", "DELETE"],
            allow_headers=["*"],
        )

    app.add_middleware(
        RequestTimeoutMiddleware,
        timeout_seconds=settings.api.request_timeout_seconds,
    )
    app.add_middleware(RequestContextMiddleware)

    from core.api.routes import (
        aggregation,
        billing,
        chargebacks,
        export,
        focus_preview,
        graph,
        health,
        identities,
        inventory,
        pipeline,
        readiness,
        resources,
        tags,
        tenants,
        topic_attributions,
    )

    app.include_router(health.router)
    app.include_router(readiness.router, prefix="/api/v1")
    app.include_router(tenants.router, prefix="/api/v1")
    app.include_router(billing.router, prefix="/api/v1")
    # aggregation must be registered before chargebacks so static /chargebacks/aggregate
    # takes precedence over the dynamic /chargebacks/{dimension_id} GET route
    app.include_router(aggregation.router, prefix="/api/v1")
    app.include_router(chargebacks.router, prefix="/api/v1")
    app.include_router(resources.router, prefix="/api/v1")
    app.include_router(identities.router, prefix="/api/v1")
    app.include_router(inventory.router, prefix="/api/v1")
    app.include_router(tags.router, prefix="/api/v1")
    app.include_router(pipeline.router, prefix="/api/v1")
    app.include_router(export.router, prefix="/api/v1")
    if mode in {"api", "both"}:
        app.include_router(focus_preview.router, prefix="/api/v1")
    app.include_router(topic_attributions.router, prefix="/api/v1")
    app.include_router(graph.router, prefix="/api/v1")

    return app
