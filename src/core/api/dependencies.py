from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING, Annotated, cast

from fastapi import Depends, HTTPException, Path, Request

from core.config.models import AppSettings, TenantConfig  # noqa: TC001  # FastAPI evaluates annotations at runtime
from core.storage.interface import StorageBackend, UnitOfWork  # noqa: TC001
from core.storage.registry import create_storage_backend

if TYPE_CHECKING:
    from core.config.models import StorageConfig


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
    backends: dict[str, StorageBackend], tenant_name: str, storage_config: StorageConfig
) -> StorageBackend:
    """Get cached backend or create and cache a new one."""
    if tenant_name not in backends:
        backends[tenant_name] = create_storage_backend(storage_config, use_migrations=False)
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

    return get_or_create_backend(request.app.state.backends, tenant_name, tenant_config.storage)


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


@dataclass(frozen=True)
class TemporalParams:
    active_at: datetime | None
    period_start: datetime | None
    period_end: datetime | None


def validate_temporal_params(
    active_at: datetime | None,
    period_start: datetime | None,
    period_end: datetime | None,
) -> TemporalParams:
    """Validate and normalise temporal query parameters.

    Validates timezone presence, checks mutual exclusivity of active_at vs
    period range, and checks period ordering.

    Args:
        active_at: Point-in-time filter.
        period_start: Start of period range filter.
        period_end: End of period range filter.

    Returns:
        TemporalParams with all values converted to UTC.

    Raises:
        HTTPException 400: If any datetime is naive, active_at is combined with
            a period param, or period_start > period_end.
    """
    active_at = validate_datetime_param(active_at, "active_at")
    period_start = validate_datetime_param(period_start, "period_start")
    period_end = validate_datetime_param(period_end, "period_end")

    if active_at and (period_start or period_end):
        raise HTTPException(400, detail="Cannot combine active_at with period_start/period_end")
    if period_start and period_end and period_start > period_end:
        raise HTTPException(400, detail="period_start must be <= period_end")

    return TemporalParams(active_at=active_at, period_start=period_start, period_end=period_end)
