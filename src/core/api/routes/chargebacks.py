from __future__ import annotations

import math
from datetime import UTC, date, datetime, timedelta
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Path, Query

from core.api.dependencies import get_tenant_config, get_unit_of_work
from core.api.schemas import (
    ChargebackDimensionResponse,
    ChargebackDimensionUpdateRequest,
    ChargebackResponse,
    PaginatedResponse,
    TagResponse,
)
from core.config.models import TenantConfig  # noqa: TC001  # FastAPI evaluates annotations at runtime
from core.storage.interface import UnitOfWork  # noqa: TC001

router = APIRouter(tags=["chargebacks"])


@router.get("/tenants/{tenant_name}/chargebacks", response_model=PaginatedResponse[ChargebackResponse])
async def list_chargebacks(
    tenant_config: Annotated[TenantConfig, Depends(get_tenant_config)],
    uow: Annotated[UnitOfWork, Depends(get_unit_of_work)],
    start_date: Annotated[date | None, Query()] = None,
    end_date: Annotated[date | None, Query()] = None,
    identity_id: Annotated[str | None, Query()] = None,
    product_type: Annotated[str | None, Query()] = None,
    resource_id: Annotated[str | None, Query()] = None,
    cost_type: Annotated[str | None, Query()] = None,
    page: Annotated[int, Query(ge=1)] = 1,
    page_size: Annotated[int, Query(ge=1, le=1000)] = 100,
) -> PaginatedResponse[ChargebackResponse]:
    today = datetime.now(UTC).date()
    effective_end = end_date or today
    effective_start = start_date or (today - timedelta(days=30))

    if effective_start > effective_end:
        raise HTTPException(400, detail="start_date must be <= end_date")

    # Convert dates to datetimes with UTC timezone
    # end_dt is exclusive (start of next day) to include all records on end_date
    start_dt = datetime(effective_start.year, effective_start.month, effective_start.day, tzinfo=UTC)
    end_dt = datetime(effective_end.year, effective_end.month, effective_end.day, tzinfo=UTC) + timedelta(days=1)

    offset = (page - 1) * page_size
    with uow:
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


def _build_dimension_response(uow: UnitOfWork, dimension_id: int) -> ChargebackDimensionResponse:
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
                created_by=t.created_by,
                created_at=t.created_at,
            )
            for t in tags
        ],
    )


@router.get(
    "/tenants/{tenant_name}/chargebacks/{dimension_id}",
    response_model=ChargebackDimensionResponse,
)
async def get_chargeback_dimension(
    tenant_config: Annotated[TenantConfig, Depends(get_tenant_config)],
    uow: Annotated[UnitOfWork, Depends(get_unit_of_work)],
    dimension_id: Annotated[int, Path(description="Chargeback dimension ID")],
) -> ChargebackDimensionResponse:
    """Get a single chargeback dimension with its tags."""
    with uow:
        dim = uow.chargebacks.get_dimension(dimension_id)
        if dim is None or dim.ecosystem != tenant_config.ecosystem or dim.tenant_id != tenant_config.tenant_id:
            raise HTTPException(status_code=404, detail=f"Dimension {dimension_id} not found")
        return _build_dimension_response(uow, dimension_id)


@router.patch(
    "/tenants/{tenant_name}/chargebacks/{dimension_id}",
    response_model=ChargebackDimensionResponse,
)
async def update_chargeback_dimension(
    tenant_config: Annotated[TenantConfig, Depends(get_tenant_config)],
    uow: Annotated[UnitOfWork, Depends(get_unit_of_work)],
    dimension_id: Annotated[int, Path(description="Chargeback dimension ID")],
    body: ChargebackDimensionUpdateRequest,
) -> ChargebackDimensionResponse:
    """Update tags/annotations on a chargeback dimension."""
    with uow:
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
                uow.tags.add_tag(dimension_id, new_tag.tag_key, new_tag.tag_value, new_tag.created_by)

        # Add tags
        if body.add_tags is not None:
            for add_tag in body.add_tags:
                uow.tags.add_tag(dimension_id, add_tag.tag_key, add_tag.tag_value, add_tag.created_by)

        # Remove specific tags
        if body.remove_tag_ids is not None:
            for tag_id in body.remove_tag_ids:
                tag = uow.tags.get_tag(tag_id)
                if tag is None:
                    raise HTTPException(status_code=404, detail=f"Tag {tag_id} not found")
                if tag.dimension_id != dimension_id:
                    raise HTTPException(
                        status_code=400, detail=f"Tag {tag_id} does not belong to dimension {dimension_id}"
                    )
                uow.tags.delete_tag(tag_id)

        uow.commit()
        return _build_dimension_response(uow, dimension_id)
