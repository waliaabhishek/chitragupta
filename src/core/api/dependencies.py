from __future__ import annotations

import logging
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta, tzinfo
from typing import TYPE_CHECKING, Annotated, cast
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from fastapi import Depends, HTTPException, Path, Request

from core.config.models import AppSettings, TenantConfig  # noqa: TC001  # FastAPI evaluates annotations at runtime
from core.storage.interface import ReadOnlyUnitOfWork, StorageBackend, UnitOfWork  # noqa: TC001
from core.storage.registry import create_storage_backend
from plugins.storage_modules import get_storage_module_for_ecosystem

if TYPE_CHECKING:
    from core.config.models import StorageConfig
logger = logging.getLogger(__name__)


def utc_today() -> date:
    """Return today's date in UTC. Use instead of date.today() in all API routes."""
    return datetime.now(UTC).date()


def resolve_date_range(
    start_date: date | None,
    end_date: date | None,
    timezone: str | None = None,
) -> tuple[datetime, datetime]:
    """Resolve optional date params to UTC datetime bounds with a 30-day default window.

    Applies defaults (today-30d to today), validates ordering, and converts to
    UTC-aware datetimes. end_dt is exclusive (midnight of the day after end_date)
    so that records timestamped anywhere on end_date are included.

    When timezone is provided, midnight boundaries are computed in that timezone
    before converting to UTC. This ensures a user selecting Dec 31 in America/Denver
    gets end_dt=2026-01-01T07:00:00 UTC rather than 2026-01-01T00:00:00 UTC.

    Args:
        start_date: Inclusive start date, or None to default to today - 30 days.
        end_date: Inclusive end date, or None to default to today.
        timezone: IANA timezone string (e.g. "America/Denver"), or None for UTC.

    Returns:
        (start_dt, end_dt) as UTC-aware datetimes.

    Raises:
        HTTPException 400: If effective start_date > end_date.
        HTTPException 400: If timezone is an unrecognised IANA string.
    """
    today = utc_today()
    effective_end = end_date or today
    effective_start = start_date or (today - timedelta(days=30))

    if effective_start > effective_end:
        raise HTTPException(400, detail="start_date must be <= end_date")

    tz: tzinfo = UTC
    if timezone:
        try:
            tz = ZoneInfo(timezone)
        except ZoneInfoNotFoundError:
            raise HTTPException(400, detail=f"Unknown timezone: {timezone!r}") from None

    start_dt = datetime(effective_start.year, effective_start.month, effective_start.day, tzinfo=tz).astimezone(UTC)
    end_dt = (
        datetime(effective_end.year, effective_end.month, effective_end.day, tzinfo=tz) + timedelta(days=1)
    ).astimezone(UTC)
    return start_dt, end_dt


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
    backends: dict[str, StorageBackend], tenant_name: str, storage_config: StorageConfig, ecosystem: str
) -> StorageBackend:
    """Get cached backend or create and cache a new one."""
    if tenant_name not in backends:
        storage_module = get_storage_module_for_ecosystem(ecosystem)
        backend = create_storage_backend(storage_config, storage_module=storage_module, use_migrations=False)
        backend.create_tables()
        backends[tenant_name] = backend
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

    return get_or_create_backend(
        request.app.state.backends, tenant_name, tenant_config.storage, tenant_config.ecosystem
    )


def get_unit_of_work(
    backend: Annotated[StorageBackend, Depends(get_storage_backend)],
) -> Iterator[ReadOnlyUnitOfWork]:
    """Yield a read-only UoW. Default for all API read endpoints.

    Uses backend.create_read_only_unit_of_work() so connections never
    acquire a RESERVED lock — WAL readers and the pipeline writer are
    fully concurrent with zero contention.
    """
    with backend.create_read_only_unit_of_work() as uow:
        yield uow


def get_write_unit_of_work(
    backend: Annotated[StorageBackend, Depends(get_storage_backend)],
) -> Iterator[UnitOfWork]:
    """Yield a read-write UoW. Explicit opt-in for endpoints that call commit().

    Used by: tag write endpoints (create/update/delete/bulk), chargeback PATCH.
    """
    with backend.create_unit_of_work() as uow:
        yield uow


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
