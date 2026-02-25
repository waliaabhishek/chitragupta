from __future__ import annotations

import math
from datetime import datetime
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query

from core.api.dependencies import get_tenant_config, get_unit_of_work, validate_datetime_param
from core.api.schemas import IdentityResponse, PaginatedResponse
from core.config.models import TenantConfig  # noqa: TC001  # FastAPI evaluates annotations at runtime
from core.storage.interface import UnitOfWork  # noqa: TC001

router = APIRouter(tags=["identities"])


@router.get("/tenants/{tenant_name}/identities", response_model=PaginatedResponse[IdentityResponse])
async def list_identities(
    tenant_config: Annotated[TenantConfig, Depends(get_tenant_config)],
    uow: Annotated[UnitOfWork, Depends(get_unit_of_work)],
    identity_type: Annotated[str | None, Query()] = None,
    active_at: Annotated[datetime | None, Query()] = None,
    period_start: Annotated[datetime | None, Query()] = None,
    period_end: Annotated[datetime | None, Query()] = None,
    page: Annotated[int, Query(ge=1)] = 1,
    page_size: Annotated[int, Query(ge=1, le=1000)] = 100,
) -> PaginatedResponse[IdentityResponse]:
    active_at = validate_datetime_param(active_at, "active_at")
    period_start = validate_datetime_param(period_start, "period_start")
    period_end = validate_datetime_param(period_end, "period_end")

    if active_at and (period_start or period_end):
        raise HTTPException(400, detail="Cannot combine active_at with period_start/period_end")
    if period_start and period_end and period_start > period_end:
        raise HTTPException(400, detail="period_start must be <= period_end")

    eco = tenant_config.ecosystem
    tid = tenant_config.tenant_id
    offset = (page - 1) * page_size

    with uow:
        if active_at:
            all_items = uow.identities.find_active_at(eco, tid, active_at)
            if identity_type:
                all_items = [i for i in all_items if i.identity_type == identity_type]
            total = len(all_items)
            items = all_items[offset : offset + page_size]
        elif period_start and period_end:
            all_items = uow.identities.find_by_period(eco, tid, period_start, period_end)
            if identity_type:
                all_items = [i for i in all_items if i.identity_type == identity_type]
            total = len(all_items)
            items = all_items[offset : offset + page_size]
        else:
            items, total = uow.identities.find_paginated(
                eco, tid, limit=page_size, offset=offset, identity_type=identity_type
            )

    pages = math.ceil(total / page_size) if total > 0 else 0
    return PaginatedResponse[IdentityResponse](
        items=[
            IdentityResponse(
                ecosystem=i.ecosystem,
                tenant_id=i.tenant_id,
                identity_id=i.identity_id,
                identity_type=i.identity_type,
                display_name=i.display_name,
                created_at=i.created_at,
                deleted_at=i.deleted_at,
                last_seen_at=i.last_seen_at,
                metadata=i.metadata,
            )
            for i in items
        ],
        total=total,
        page=page,
        page_size=page_size,
        pages=pages,
    )
