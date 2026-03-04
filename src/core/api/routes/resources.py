from __future__ import annotations

import math
from datetime import datetime
from typing import Annotated

from fastapi import APIRouter, Depends, Query

from core.api.dependencies import get_tenant_config, get_unit_of_work, validate_temporal_params
from core.api.schemas import PaginatedResponse, ResourceResponse
from core.config.models import TenantConfig  # noqa: TC001  # FastAPI evaluates annotations at runtime
from core.storage.interface import UnitOfWork  # noqa: TC001

router = APIRouter(tags=["resources"])


@router.get("/tenants/{tenant_name}/resources", response_model=PaginatedResponse[ResourceResponse])
async def list_resources(
    tenant_config: Annotated[TenantConfig, Depends(get_tenant_config)],
    uow: Annotated[UnitOfWork, Depends(get_unit_of_work)],
    resource_type: Annotated[str | None, Query()] = None,
    status: Annotated[str | None, Query()] = None,
    active_at: Annotated[datetime | None, Query()] = None,
    period_start: Annotated[datetime | None, Query()] = None,
    period_end: Annotated[datetime | None, Query()] = None,
    page: Annotated[int, Query(ge=1)] = 1,
    page_size: Annotated[int, Query(ge=1, le=1000)] = 100,
) -> PaginatedResponse[ResourceResponse]:
    tp = validate_temporal_params(active_at, period_start, period_end)

    eco = tenant_config.ecosystem
    tid = tenant_config.tenant_id
    offset = (page - 1) * page_size

    with uow:
        if tp.active_at:
            items, total = uow.resources.find_active_at(
                eco,
                tid,
                tp.active_at,
                resource_type=resource_type,
                status=status,
                limit=page_size,
                offset=offset,
            )
        elif tp.period_start and tp.period_end:
            items, total = uow.resources.find_by_period(
                eco,
                tid,
                tp.period_start,
                tp.period_end,
                resource_type=resource_type,
                status=status,
                limit=page_size,
                offset=offset,
            )
        else:
            items, total = uow.resources.find_paginated(
                eco, tid, limit=page_size, offset=offset, resource_type=resource_type, status=status
            )

    pages = math.ceil(total / page_size) if total > 0 else 0
    return PaginatedResponse[ResourceResponse](
        items=[
            ResourceResponse(
                ecosystem=r.ecosystem,
                tenant_id=r.tenant_id,
                resource_id=r.resource_id,
                resource_type=r.resource_type,
                display_name=r.display_name,
                parent_id=r.parent_id,
                owner_id=r.owner_id,
                status=r.status.value if hasattr(r.status, "value") else str(r.status),
                created_at=r.created_at,
                deleted_at=r.deleted_at,
                last_seen_at=r.last_seen_at,
                metadata=r.metadata,
            )
            for r in items
        ],
        total=total,
        page=page,
        page_size=page_size,
        pages=pages,
    )
