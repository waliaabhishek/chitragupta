from __future__ import annotations

import logging
import math
from collections.abc import Sequence
from datetime import datetime
from typing import Annotated

from fastapi import APIRouter, Depends, Query

from core.api.dependencies import get_tenant_config, get_unit_of_work, validate_temporal_params
from core.api.schemas import PaginatedResponse, ResourceResponse
from core.config.models import TenantConfig  # noqa: TC001  # FastAPI evaluates annotations at runtime
from core.storage.interface import ReadOnlyUnitOfWork  # noqa: TC001

logger = logging.getLogger(__name__)

router = APIRouter(tags=["resources"])


@router.get("/tenants/{tenant_name}/resources", response_model=PaginatedResponse[ResourceResponse])
async def list_resources(
    tenant_config: Annotated[TenantConfig, Depends(get_tenant_config)],
    uow: Annotated[ReadOnlyUnitOfWork, Depends(get_unit_of_work)],
    resource_type: Annotated[str | None, Query()] = None,
    status: Annotated[str | None, Query()] = None,
    active_at: Annotated[datetime | None, Query()] = None,
    period_start: Annotated[datetime | None, Query()] = None,
    period_end: Annotated[datetime | None, Query()] = None,
    page: Annotated[int, Query(ge=1)] = 1,
    page_size: Annotated[int, Query(ge=1, le=1000)] = 100,
    search: Annotated[str | None, Query(description="ILIKE on resource_id and display_name")] = None,
    sort_by: Annotated[str | None, Query(description="resource_id | display_name | resource_type | status")] = None,
    sort_order: Annotated[str, Query(description="asc or desc")] = "asc",
    tag_key: Annotated[str | None, Query(description="Filter by entity tag key")] = None,
    tag_value: Annotated[str | None, Query(description="Filter by entity tag value (requires tag_key)")] = None,
) -> PaginatedResponse[ResourceResponse]:
    logger.debug("GET /resources tenant=%s page=%d page_size=%d", tenant_config.tenant_id, page, page_size)
    tp = validate_temporal_params(active_at, period_start, period_end)

    eco = tenant_config.ecosystem
    tid = tenant_config.tenant_id
    offset = (page - 1) * page_size

    # Resolve effective type filter before any repo branch.
    # When the user provides a type, use it directly.
    # When no type is specified, pass all types present in the DB (show-all semantics).
    # Empty DB → empty list → _apply_resource_type_filter emits literal(False) → zero rows. Correct.
    if resource_type is not None:
        effective_rt: str | Sequence[str] = resource_type
    else:
        type_counts = uow.resources.count_by_type(eco, tid)
        effective_rt = list(type_counts.keys())

    if tp.active_at:
        items, total = uow.resources.find_active_at(
            eco,
            tid,
            tp.active_at,
            resource_type=effective_rt,
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
            resource_type=effective_rt,
            status=status,
            limit=page_size,
            offset=offset,
        )
    else:
        items, total = uow.resources.find_paginated(
            eco,
            tid,
            limit=page_size,
            offset=offset,
            resource_type=effective_rt,
            status=status,
            search=search,
            sort_by=sort_by,
            sort_order=sort_order,
            tag_key=tag_key,
            tag_value=tag_value,
            tags_repo=uow.tags if tag_key is not None else None,
        )
    pages = math.ceil(total / page_size) if total > 0 else 0
    logger.info("Listed resources tenant=%s returned=%d total=%d", tenant_config.tenant_id, len(items), total)
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
