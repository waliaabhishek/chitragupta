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


def create_app(settings: AppSettings, workflow_runner: WorkflowRunner | None = None, mode: str = "api") -> FastAPI:
    """Factory function for creating the FastAPI application."""

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        logger.info("Chitragupt API starting up version=%s", API_VERSION)
        app.state.settings = settings
        app.state.backends = {}
        app.state.workflow_runner = workflow_runner
        app.state.mode = mode
        if workflow_runner is None:
            from workflow_runner import cleanup_orphaned_runs_for_all_tenants

            await asyncio.to_thread(cleanup_orphaned_runs_for_all_tenants, settings, swallow_errors=True)
        yield
        logger.info("Chitragupt API shutting down — disposing backends")
        for backend in app.state.backends.values():
            backend.dispose()
        if workflow_runner is not None:
            logger.debug("Draining workflow runner")
            await asyncio.to_thread(workflow_runner.drain, 30)
        logger.info("Chitragupt API shutdown complete")

    app = FastAPI(
        title="Chitragupt API",
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
        health,
        identities,
        inventory,
        pipeline,
        readiness,
        resources,
        tags,
        tenants,
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

    return app
