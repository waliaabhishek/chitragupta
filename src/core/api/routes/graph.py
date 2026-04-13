from __future__ import annotations

import logging
from datetime import UTC, date, datetime
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query

from core.api.dependencies import get_tenant_config, get_unit_of_work, resolve_date_range, validate_datetime_param
from core.api.schemas import GraphEdge, GraphNode, GraphResponse
from core.config.models import TenantConfig  # noqa: TC001  # FastAPI evaluates annotations at runtime
from core.storage.interface import ReadOnlyUnitOfWork  # noqa: TC001

logger = logging.getLogger(__name__)

router = APIRouter(tags=["graph"])


@router.get("/tenants/{tenant_name}/graph", response_model=GraphResponse)
async def get_graph_neighborhood(
    tenant_config: Annotated[TenantConfig, Depends(get_tenant_config)],
    uow: Annotated[ReadOnlyUnitOfWork, Depends(get_unit_of_work)],
    focus: Annotated[str | None, Query(description="Entity ID to focus on; omit for root (tenant) view")] = None,
    depth: Annotated[int, Query(ge=1, le=3, description="Hierarchy hops from focus")] = 1,
    at: Annotated[datetime | None, Query(description="Point-in-time filter (ISO, must include tz)")] = None,
    start_date: Annotated[date | None, Query(description="Cost period start date")] = None,
    end_date: Annotated[date | None, Query(description="Cost period end date")] = None,
    timezone: Annotated[str | None, Query(description="IANA timezone for date boundaries")] = None,
) -> GraphResponse:
    at_dt = validate_datetime_param(at, "at")
    if at_dt is None:
        at_dt = datetime.now(UTC)

    # Billing period: explicit dates override; otherwise derive month boundaries from `at`
    if start_date is None and end_date is None:
        period_start = at_dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        # First day of next month
        if period_start.month == 12:
            period_end = period_start.replace(year=period_start.year + 1, month=1)
        else:
            period_end = period_start.replace(month=period_start.month + 1)
    else:
        period_start, period_end = resolve_date_range(start_date, end_date, timezone=timezone)

    logger.debug(
        "GET /graph tenant=%s focus=%s depth=%d at=%s period=[%s, %s)",
        tenant_config.tenant_id,
        focus,
        depth,
        at_dt,
        period_start,
        period_end,
    )

    try:
        neighborhood = uow.graph.find_neighborhood(
            ecosystem=tenant_config.ecosystem,
            tenant_id=tenant_config.tenant_id,
            focus_id=focus,
            depth=depth,
            at=at_dt,
            period_start=period_start,
            period_end=period_end,
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    return GraphResponse(
        nodes=[
            GraphNode(
                id=n.id,
                resource_type=n.resource_type,
                display_name=n.display_name,
                cost=n.cost,
                created_at=n.created_at,
                deleted_at=n.deleted_at,
                tags=n.tags,
                parent_id=n.parent_id,
                cloud=n.cloud,
                region=n.region,
                status=n.status,
                cross_references=n.cross_references,
            )
            for n in neighborhood.nodes
        ],
        edges=[
            GraphEdge(
                source=e.source,
                target=e.target,
                relationship_type=e.relationship_type.value,
                cost=e.cost,
            )
            for e in neighborhood.edges
        ],
    )
