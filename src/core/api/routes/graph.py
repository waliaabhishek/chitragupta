from __future__ import annotations

import logging
from datetime import UTC, date, datetime
from typing import Annotated, Literal

from fastapi import APIRouter, Depends, HTTPException, Query

from core.api.dependencies import get_tenant_config, get_unit_of_work, resolve_date_range, validate_datetime_param
from core.api.schemas import (
    GraphDiffNode,
    GraphDiffResponse,
    GraphEdge,
    GraphNode,
    GraphResponse,
    GraphSearchResponse,
    GraphSearchResult,
    GraphTimelinePoint,
    GraphTimelineResponse,
)
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
    expand: Annotated[
        Literal["topics", "identities", "resources", "clusters"] | None,
        Query(description='Expand a group: "topics", "identities", "resources", or "clusters"'),
    ] = None,
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
            expand=expand,
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
                child_count=n.child_count,
                child_total_cost=n.child_total_cost,
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


@router.get("/tenants/{tenant_name}/graph/search", response_model=GraphSearchResponse)
async def search_graph(
    tenant_config: Annotated[TenantConfig, Depends(get_tenant_config)],
    uow: Annotated[ReadOnlyUnitOfWork, Depends(get_unit_of_work)],
    q: Annotated[
        str,
        Query(
            min_length=1, description="Case-insensitive partial match against resource_id, display_name, identity_id"
        ),
    ],
) -> GraphSearchResponse:
    logger.debug("GET /graph/search tenant=%s q=%r", tenant_config.tenant_id, q)
    results = uow.graph.search_entities(
        ecosystem=tenant_config.ecosystem,
        tenant_id=tenant_config.tenant_id,
        query=q,
    )
    return GraphSearchResponse(
        results=[
            GraphSearchResult(
                id=r.id,
                resource_type=r.resource_type,
                display_name=r.display_name,
                parent_id=r.parent_id,
                parent_display_name=r.parent_display_name,
                status=r.status,
            )
            for r in results
        ]
    )


@router.get("/tenants/{tenant_name}/graph/diff", response_model=GraphDiffResponse)
async def diff_graph(
    tenant_config: Annotated[TenantConfig, Depends(get_tenant_config)],
    uow: Annotated[ReadOnlyUnitOfWork, Depends(get_unit_of_work)],
    from_start: Annotated[date, Query(description="Start of 'before' period")],
    from_end: Annotated[date, Query(description="End of 'before' period")],
    to_start: Annotated[date, Query(description="Start of 'after' period")],
    to_end: Annotated[date, Query(description="End of 'after' period")],
    focus: Annotated[str | None, Query(description="Entity ID to focus on; omit for root (tenant) view")] = None,
    depth: Annotated[int, Query(ge=1, le=3, description="Hierarchy hops from focus")] = 1,
    timezone: Annotated[str | None, Query(description="IANA timezone for date boundaries")] = None,
) -> GraphDiffResponse:
    from_start_dt, from_end_dt = resolve_date_range(from_start, from_end, timezone=timezone)
    to_start_dt, to_end_dt = resolve_date_range(to_start, to_end, timezone=timezone)

    logger.debug(
        "GET /graph/diff tenant=%s focus=%s depth=%d from=[%s,%s) to=[%s,%s)",
        tenant_config.tenant_id,
        focus,
        depth,
        from_start_dt,
        from_end_dt,
        to_start_dt,
        to_end_dt,
    )

    try:
        diff_nodes = uow.graph.diff_neighborhood(
            ecosystem=tenant_config.ecosystem,
            tenant_id=tenant_config.tenant_id,
            focus_id=focus,
            depth=depth,
            from_start=from_start_dt,
            from_end=from_end_dt,
            to_start=to_start_dt,
            to_end=to_end_dt,
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    return GraphDiffResponse(
        nodes=[
            GraphDiffNode(
                id=n.id,
                resource_type=n.resource_type,
                display_name=n.display_name,
                parent_id=n.parent_id,
                cost_before=n.cost_before,
                cost_after=n.cost_after,
                cost_delta=n.cost_delta,
                pct_change=n.pct_change,
                status=n.status,
            )
            for n in diff_nodes
        ]
    )


@router.get("/tenants/{tenant_name}/graph/timeline", response_model=GraphTimelineResponse)
async def get_graph_timeline(
    tenant_config: Annotated[TenantConfig, Depends(get_tenant_config)],
    uow: Annotated[ReadOnlyUnitOfWork, Depends(get_unit_of_work)],
    entity_id: Annotated[str, Query(description="Entity ID (resource_id or identity_id)")],
    start: Annotated[date, Query(description="Start date (inclusive)")],
    end: Annotated[date, Query(description="End date (inclusive)")],
    timezone: Annotated[str | None, Query(description="IANA timezone for date boundaries")] = None,
) -> GraphTimelineResponse:
    start_dt, end_dt = resolve_date_range(start, end, timezone=timezone)

    logger.debug(
        "GET /graph/timeline tenant=%s entity=%s period=[%s,%s)",
        tenant_config.tenant_id,
        entity_id,
        start_dt,
        end_dt,
    )

    try:
        points = uow.graph.get_timeline(
            ecosystem=tenant_config.ecosystem,
            tenant_id=tenant_config.tenant_id,
            entity_id=entity_id,
            start=start_dt,
            end=end_dt,
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    return GraphTimelineResponse(
        entity_id=entity_id,
        points=[GraphTimelinePoint(date=p.date, cost=p.cost) for p in points],
    )
