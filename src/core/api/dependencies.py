from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, date, datetime
from typing import Annotated, cast

from fastapi import Depends, HTTPException, Path, Request

from core.config.models import AppSettings, TenantConfig  # noqa: TC001  # FastAPI evaluates annotations at runtime
from core.storage.interface import StorageBackend, UnitOfWork  # noqa: TC001


def utc_today() -> date:
    """Return today's date in UTC. Use instead of date.today() in all API routes."""
    return datetime.now(UTC).date()


def get_settings(request: Request) -> AppSettings:
    """Get application settings from app state."""
    return cast("AppSettings", request.app.state.settings)


def get_tenant_config(
    tenant_name: Annotated[str, Path(description="Tenant name from config")],
    settings: Annotated[AppSettings, Depends(get_settings)],
) -> TenantConfig:
    """Get tenant configuration by name. Raises 404 if not found."""
    if tenant_name not in settings.tenants:
        raise HTTPException(status_code=404, detail=f"Tenant {tenant_name!r} not found")
    return settings.tenants[tenant_name]


def get_or_create_backend(
    backends: dict[str, StorageBackend], tenant_name: str, connection_string: str
) -> StorageBackend:
    """Get cached backend or create and cache a new one.

    Args:
        backends: The backends cache dict (typically app.state.backends).
        tenant_name: Key for caching.
        connection_string: Database connection string for creating new backend.

    Returns:
        The cached or newly created StorageBackend.
    """
    if tenant_name not in backends:
        from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend

        backends[tenant_name] = SQLModelBackend(connection_string, use_migrations=False)
    return backends[tenant_name]


def get_storage_backend(
    request: Request,
    tenant_name: Annotated[str, Path(description="Tenant name from config")],
    tenant_config: Annotated[TenantConfig, Depends(get_tenant_config)],
) -> StorageBackend:
    """Get or create shared storage backend for tenant (cached in app.state)."""
    # Lazy-init backends dict
    if not hasattr(request.app.state, "backends"):
        request.app.state.backends = {}

    return get_or_create_backend(request.app.state.backends, tenant_name, tenant_config.storage.connection_string)


def get_unit_of_work(
    backend: Annotated[StorageBackend, Depends(get_storage_backend)],
) -> Iterator[UnitOfWork]:
    """Yield a UoW from the shared backend.

    No cleanup needed: read-only endpoints don't commit, and backends
    are shared in app.state (disposed on shutdown, not per-request).
    """
    yield backend.create_unit_of_work()


def validate_datetime_param(dt: datetime | None, param_name: str) -> datetime | None:
    """Validate that a datetime parameter has timezone info.

    Args:
        dt: The datetime to validate (may be None).
        param_name: Name of the parameter for error messages.

    Returns:
        The datetime converted to UTC, or None if input was None.

    Raises:
        HTTPException: If datetime is naive (no timezone).
    """
    if dt is None:
        return None
    if dt.tzinfo is None:
        raise HTTPException(
            status_code=400,
            detail=f"{param_name} must include timezone (e.g., 2026-02-24T00:00:00Z)",
        )
    return dt.astimezone(UTC)
