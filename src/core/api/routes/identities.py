from __future__ import annotations

import logging
import math
from datetime import datetime
from typing import Annotated

from fastapi import APIRouter, Depends, Query

from core.api.dependencies import get_tenant_config, get_unit_of_work, validate_temporal_params
from core.api.schemas import IdentityResponse, PaginatedResponse
from core.config.models import TenantConfig  # noqa: TC001  # FastAPI evaluates annotations at runtime
from core.storage.interface import ReadOnlyUnitOfWork  # noqa: TC001

logger = logging.getLogger(__name__)

router = APIRouter(tags=["identities"])


@router.get("/tenants/{tenant_name}/identities", response_model=PaginatedResponse[IdentityResponse])
async def list_identities(
    tenant_config: Annotated[TenantConfig, Depends(get_tenant_config)],
    uow: Annotated[ReadOnlyUnitOfWork, Depends(get_unit_of_work)],
    identity_type: Annotated[str | None, Query()] = None,
    active_at: Annotated[datetime | None, Query()] = None,
    period_start: Annotated[datetime | None, Query()] = None,
    period_end: Annotated[datetime | None, Query()] = None,
    page: Annotated[int, Query(ge=1)] = 1,
    page_size: Annotated[int, Query(ge=1, le=1000)] = 100,
    search: Annotated[str | None, Query(description="ILIKE on identity_id and display_name")] = None,
    sort_by: Annotated[str | None, Query(description="identity_id | display_name | identity_type")] = None,
    sort_order: Annotated[str, Query(description="asc or desc")] = "asc",
    tag_key: Annotated[str | None, Query(description="Filter by entity tag key")] = None,
    tag_value: Annotated[str | None, Query(description="Filter by entity tag value (requires tag_key)")] = None,
) -> PaginatedResponse[IdentityResponse]:
    logger.debug("GET /identities tenant=%s page=%d page_size=%d", tenant_config.tenant_id, page, page_size)
    tp = validate_temporal_params(active_at, period_start, period_end)

    eco = tenant_config.ecosystem
    tid = tenant_config.tenant_id
    offset = (page - 1) * page_size

    if tp.active_at:
        items, total = uow.identities.find_active_at(
            eco,
            tid,
            tp.active_at,
            identity_type=identity_type,
            limit=page_size,
            offset=offset,
        )
    elif tp.period_start and tp.period_end:
        items, total = uow.identities.find_by_period(
            eco,
            tid,
            tp.period_start,
            tp.period_end,
            identity_type=identity_type,
            limit=page_size,
            offset=offset,
        )
    else:
        items, total = uow.identities.find_paginated(
            eco,
            tid,
            limit=page_size,
            offset=offset,
            identity_type=identity_type,
            search=search,
            sort_by=sort_by,
            sort_order=sort_order,
            tag_key=tag_key,
            tag_value=tag_value,
            tags_repo=uow.tags if tag_key is not None else None,
        )
    pages = math.ceil(total / page_size) if total > 0 else 0
    logger.info("Listed identities tenant=%s returned=%d total=%d", tenant_config.tenant_id, len(items), total)
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
