from __future__ import annotations

from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

from fastapi import FastAPI

from core.api import API_VERSION

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from core.config.models import AppSettings


def create_app(settings: AppSettings) -> FastAPI:
    """Factory function for creating the FastAPI application."""

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        app.state.settings = settings
        app.state.backends = {}
        yield
        for backend in app.state.backends.values():
            backend.dispose()

    app = FastAPI(
        title="Chargeback Engine API",
        version=API_VERSION,
        lifespan=lifespan,
    )

    if settings.api.enable_cors:
        from fastapi.middleware.cors import CORSMiddleware

        app.add_middleware(
            CORSMiddleware,
            allow_origins=settings.api.cors_origins,
            allow_methods=["GET"],
            allow_headers=["*"],
        )

    from core.api.routes import billing, chargebacks, health, identities, resources, tenants

    app.include_router(health.router)
    app.include_router(tenants.router, prefix="/api/v1")
    app.include_router(billing.router, prefix="/api/v1")
    app.include_router(chargebacks.router, prefix="/api/v1")
    app.include_router(resources.router, prefix="/api/v1")
    app.include_router(identities.router, prefix="/api/v1")

    return app
