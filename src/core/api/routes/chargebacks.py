from __future__ import annotations

import logging
import math
from datetime import date
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Path, Query

from core.api.dependencies import get_tenant_config, get_unit_of_work, get_write_unit_of_work, resolve_date_range
from core.api.schemas import (
    AllocationIssueResponse,
    ChargebackDatesResponse,
    ChargebackDimensionResponse,
    ChargebackDimensionUpdateRequest,
    ChargebackResponse,
    PaginatedResponse,
    TagResponse,
)
from core.config.models import TenantConfig  # noqa: TC001  # FastAPI evaluates annotations at runtime
from core.storage.interface import ReadOnlyUnitOfWork, UnitOfWork  # noqa: TC001

logger = logging.getLogger(__name__)

router = APIRouter(tags=["chargebacks"])


@router.get("/tenants/{tenant_name}/chargebacks", response_model=PaginatedResponse[ChargebackResponse])
async def list_chargebacks(
    tenant_config: Annotated[TenantConfig, Depends(get_tenant_config)],
    uow: Annotated[ReadOnlyUnitOfWork, Depends(get_unit_of_work)],
    start_date: Annotated[date | None, Query()] = None,
    end_date: Annotated[date | None, Query()] = None,
    identity_id: Annotated[str | None, Query()] = None,
    product_type: Annotated[str | None, Query()] = None,
    resource_id: Annotated[str | None, Query()] = None,
    cost_type: Annotated[str | None, Query()] = None,
    page: Annotated[int, Query(ge=1)] = 1,
    page_size: Annotated[int, Query(ge=1, le=1000)] = 100,
) -> PaginatedResponse[ChargebackResponse]:
    logger.debug(
        "GET /chargebacks tenant=%s page=%d page_size=%d",
        tenant_config.tenant_id,
        page,
        page_size,
    )
    start_dt, end_dt = resolve_date_range(start_date, end_date)

    offset = (page - 1) * page_size
    items, total = uow.chargebacks.find_by_filters(
        ecosystem=tenant_config.ecosystem,
        tenant_id=tenant_config.tenant_id,
        start=start_dt,
        end=end_dt,
        identity_id=identity_id,
        product_type=product_type,
        resource_id=resource_id,
        cost_type=cost_type,
        limit=page_size,
        offset=offset,
    )
    pages = math.ceil(total / page_size) if total > 0 else 0
    logger.info(
        "Listed chargebacks tenant=%s returned=%d total=%d",
        tenant_config.tenant_id,
        len(items),
        total,
    )
    return PaginatedResponse[ChargebackResponse](
        items=[
            ChargebackResponse(
                dimension_id=c.dimension_id,
                ecosystem=c.ecosystem,
                tenant_id=c.tenant_id,
                timestamp=c.timestamp,
                resource_id=c.resource_id,
                product_category=c.product_category,
                product_type=c.product_type,
                identity_id=c.identity_id,
                cost_type=c.cost_type.value,
                amount=c.amount,
                allocation_method=c.allocation_method,
                allocation_detail=c.allocation_detail,
                tags=c.tags,
                metadata=c.metadata,
            )
            for c in items
        ],
        total=total,
        page=page,
        page_size=page_size,
        pages=pages,
    )


def _build_dimension_response(uow: ReadOnlyUnitOfWork, dimension_id: int) -> ChargebackDimensionResponse:
    """Build ChargebackDimensionResponse with tags for a given dimension."""
    dim = uow.chargebacks.get_dimension(dimension_id)
    if dim is None:
        raise RuntimeError(f"Dimension {dimension_id} disappeared between existence check and fetch")
    tags = uow.tags.get_tags(dimension_id)
    return ChargebackDimensionResponse(
        dimension_id=dim.dimension_id,
        ecosystem=dim.ecosystem,
        tenant_id=dim.tenant_id,
        resource_id=dim.resource_id,
        product_category=dim.product_category,
        product_type=dim.product_type,
        identity_id=dim.identity_id,
        cost_type=dim.cost_type,
        allocation_method=dim.allocation_method,
        allocation_detail=dim.allocation_detail,
        tags=[
            TagResponse(
                tag_id=t.tag_id,  # type: ignore[arg-type]  # tag_id always set after persistence
                dimension_id=t.dimension_id,
                tag_key=t.tag_key,
                tag_value=t.tag_value,
                display_name=t.display_name,
                created_by=t.created_by,
                created_at=t.created_at,
            )
            for t in tags
        ],
    )


@router.get(
    "/tenants/{tenant_name}/chargebacks/dates",
    response_model=ChargebackDatesResponse,
)
async def get_chargeback_dates(
    tenant_config: Annotated[TenantConfig, Depends(get_tenant_config)],
    uow: Annotated[ReadOnlyUnitOfWork, Depends(get_unit_of_work)],
) -> ChargebackDatesResponse:
    """Return all distinct dates that have chargeback facts for the tenant."""
    dates = uow.chargebacks.get_distinct_dates(
        ecosystem=tenant_config.ecosystem,
        tenant_id=tenant_config.tenant_id,
    )
    return ChargebackDatesResponse(dates=dates)


@router.get(
    "/tenants/{tenant_name}/chargebacks/allocation-issues",
    response_model=PaginatedResponse[AllocationIssueResponse],
)
async def list_allocation_issues(
    tenant_config: Annotated[TenantConfig, Depends(get_tenant_config)],
    uow: Annotated[ReadOnlyUnitOfWork, Depends(get_unit_of_work)],
    start_date: Annotated[date | None, Query()] = None,
    end_date: Annotated[date | None, Query()] = None,
    identity_id: Annotated[str | None, Query()] = None,
    product_type: Annotated[str | None, Query()] = None,
    resource_id: Annotated[str | None, Query()] = None,
    page: Annotated[int, Query(ge=1)] = 1,
    page_size: Annotated[int, Query(ge=1, le=1000)] = 100,
) -> PaginatedResponse[AllocationIssueResponse]:
    logger.debug(
        "GET /chargebacks/allocation-issues tenant=%s page=%d",
        tenant_config.tenant_id,
        page,
    )
    start_dt, end_dt = resolve_date_range(start_date, end_date)
    offset = (page - 1) * page_size
    items, total = uow.chargebacks.find_allocation_issues(
        ecosystem=tenant_config.ecosystem,
        tenant_id=tenant_config.tenant_id,
        start=start_dt,
        end=end_dt,
        identity_id=identity_id,
        product_type=product_type,
        resource_id=resource_id,
        limit=page_size,
        offset=offset,
    )
    pages = math.ceil(total / page_size) if total > 0 else 0
    logger.info(
        "Listed allocation issues tenant=%s returned=%d total=%d",
        tenant_config.tenant_id,
        len(items),
        total,
    )
    return PaginatedResponse[AllocationIssueResponse](
        items=[
            AllocationIssueResponse(
                ecosystem=r.ecosystem,
                resource_id=r.resource_id,
                product_type=r.product_type,
                identity_id=r.identity_id,
                allocation_detail=r.allocation_detail,
                row_count=r.row_count,
                usage_cost=r.usage_cost,
                shared_cost=r.shared_cost,
                total_cost=r.total_cost,
            )
            for r in items
        ],
        total=total,
        page=page,
        page_size=page_size,
        pages=pages,
    )


@router.patch(
    "/tenants/{tenant_name}/chargebacks/{dimension_id}",
    response_model=ChargebackDimensionResponse,
)
async def update_chargeback_dimension(
    tenant_config: Annotated[TenantConfig, Depends(get_tenant_config)],
    uow: Annotated[UnitOfWork, Depends(get_write_unit_of_work)],
    dimension_id: Annotated[int, Path(description="Chargeback dimension ID")],
    body: ChargebackDimensionUpdateRequest,
) -> ChargebackDimensionResponse:
    """Update tags/annotations on a chargeback dimension."""
    logger.debug("PATCH /chargebacks/%s tenant=%s", dimension_id, tenant_config.tenant_id)
    dim = uow.chargebacks.get_dimension(dimension_id)
    if dim is None or dim.ecosystem != tenant_config.ecosystem or dim.tenant_id != tenant_config.tenant_id:
        raise HTTPException(status_code=404, detail=f"Dimension {dimension_id} not found")

    # Replace all tags
    if body.tags is not None:
        existing = uow.tags.get_tags(dimension_id)
        for existing_tag in existing:
            if existing_tag.tag_id is not None:
                uow.tags.delete_tag(existing_tag.tag_id)
        for new_tag in body.tags:
            uow.tags.add_tag(dimension_id, new_tag.tag_key, new_tag.display_name, new_tag.created_by)

    # Add tags
    if body.add_tags is not None:
        for add_tag in body.add_tags:
            uow.tags.add_tag(dimension_id, add_tag.tag_key, add_tag.display_name, add_tag.created_by)

    # Remove specific tags
    if body.remove_tag_ids is not None:
        for tag_id in body.remove_tag_ids:
            tag = uow.tags.get_tag(tag_id)
            if tag is None:
                raise HTTPException(status_code=404, detail=f"Tag {tag_id} not found")
            if tag.dimension_id != dimension_id:
                raise HTTPException(status_code=400, detail=f"Tag {tag_id} does not belong to dimension {dimension_id}")
            uow.tags.delete_tag(tag_id)

    uow.commit()
    logger.info("Updated chargeback dimension %s tenant=%s", dimension_id, tenant_config.tenant_id)
    return _build_dimension_response(uow, dimension_id)


@router.get(
    "/tenants/{tenant_name}/chargebacks/{dimension_id}",
    response_model=ChargebackDimensionResponse,
)
async def get_chargeback_dimension(
    tenant_config: Annotated[TenantConfig, Depends(get_tenant_config)],
    uow: Annotated[ReadOnlyUnitOfWork, Depends(get_unit_of_work)],
    dimension_id: Annotated[int, Path(description="Chargeback dimension ID")],
) -> ChargebackDimensionResponse:
    """Get a single chargeback dimension with its tags."""
    dim = uow.chargebacks.get_dimension(dimension_id)
    if dim is None or dim.ecosystem != tenant_config.ecosystem or dim.tenant_id != tenant_config.tenant_id:
        raise HTTPException(status_code=404, detail=f"Dimension {dimension_id} not found")
    return _build_dimension_response(uow, dimension_id)
