from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

from fastapi import FastAPI, Request
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.responses import JSONResponse, Response

from core.api import API_VERSION
from core.api.exception_handler import global_exception_handler
from core.config.models import TenantConfig  # noqa: TC001  # resolved by get_type_hints contract tests
from core.preview.service import PreviewRuntime
from core.storage.interface import StorageBackend  # noqa: TC001  # resolved by get_type_hints contract tests

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from starlette.types import ASGIApp

    from core.config.models import AppSettings
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
        except TimeoutError:
            return JSONResponse(
                {"detail": f"Request exceeded {self.timeout_seconds}s timeout"},
                status_code=504,
            )


logger = logging.getLogger(__name__)


def recover_preview_owner(
    tenant_name: str,
    tenant_config: TenantConfig,
    cache: dict[str, StorageBackend],
    preview_runtime: PreviewRuntime,
) -> None:
    from core.api.dependencies import get_or_create_backend
    from core.preview.persistence import PreviewStorageBackend
    from core.preview.service import PreviewRecoveryUnavailable

    if not isinstance(preview_runtime, PreviewRuntime):
        raise PreviewRecoveryUnavailable("FOCUS Mapping Preview recovery is unavailable")
    backend = get_or_create_backend(
        cache,
        tenant_name,
        tenant_config.storage,
        tenant_config.ecosystem,
    )
    if not isinstance(backend, PreviewStorageBackend):
        raise PreviewRecoveryUnavailable("FOCUS Mapping Preview recovery is unavailable")
    preview_runtime.ensure_owner_recovered(
        backend=backend,
        tenant_name=tenant_name,
        ecosystem=tenant_config.ecosystem,
        tenant_id=tenant_config.tenant_id,
    )


def create_app(
    settings: AppSettings | None = None, workflow_runner: WorkflowRunner | None = None, mode: str = "api"
) -> FastAPI:
    """Factory function for creating the FastAPI application."""
    if settings is None:
        from core.config.models import AppSettings as _AppSettings

        settings = _AppSettings()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        logger.info("Chitragupta API starting up version=%s", API_VERSION)
        app.state.settings = settings
        app.state.backends = {}
        app.state.workflow_runner = workflow_runner
        app.state.mode = mode
        from core.preview.artifacts import LocalPreviewArtifactStore
        from core.preview.service import PreviewRecoveryUnavailable, PreviewRuntime

        preview_artifact_store: LocalPreviewArtifactStore | None = None
        preview_runtime: PreviewRuntime | None = None
        original_error: BaseException | None = None
        try:
            preview_artifact_store = LocalPreviewArtifactStore(settings.preview.artifact_root)
            preview_runtime = PreviewRuntime(
                artifact_store=preview_artifact_store,
                max_workers=settings.preview.max_workers,
                max_csv_file_bytes=settings.preview.max_csv_file_bytes,
                configured_owner_keys=tuple(
                    (tenant_name, tenant.ecosystem, tenant.tenant_id)
                    for tenant_name, tenant in settings.tenants.items()
                    if tenant.ecosystem == "confluent_cloud"
                ),
            )
            app.state.preview_artifact_store = preview_artifact_store
            app.state.preview_runtime = preview_runtime
            staging_recovered = False
            try:
                await asyncio.to_thread(preview_runtime.ensure_staging_recovered)
                staging_recovered = True
            except PreviewRecoveryUnavailable as exc:
                logger.error(
                    "FOCUS Mapping Preview staging recovery unavailable error_type=%s",
                    type(exc).__name__,
                )
            if staging_recovered:
                for tenant_name, tenant_config in settings.tenants.items():
                    if tenant_config.ecosystem != "confluent_cloud":
                        continue
                    try:
                        await asyncio.to_thread(
                            recover_preview_owner,
                            tenant_name,
                            tenant_config,
                            app.state.backends,
                            preview_runtime,
                        )
                    except Exception as exc:
                        logger.error(
                            "FOCUS Mapping Preview owner recovery unavailable tenant=%s error_type=%s",
                            tenant_name,
                            type(exc).__name__,
                        )
            if workflow_runner is None:
                from workflow_runner import cleanup_orphaned_runs_for_all_tenants

                await asyncio.to_thread(cleanup_orphaned_runs_for_all_tenants, settings, swallow_errors=True)
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
                    "Chitragupta API cleanup failed step=%s error_type=%s",
                    step,
                    type(exc).__name__,
                )

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
            for backend in tuple(app.state.backends.values()):
                try:
                    backend.dispose()
                except BaseException as exc:
                    record_cleanup_error("backend", exc)
            if workflow_runner is not None:
                logger.debug("Draining workflow runner")
                try:
                    await asyncio.to_thread(workflow_runner.drain, 30)
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
    app.include_router(focus_preview.router, prefix="/api/v1")
    app.include_router(topic_attributions.router, prefix="/api/v1")
    app.include_router(graph.router, prefix="/api/v1")

    return app
