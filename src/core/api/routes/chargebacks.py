from __future__ import annotations

import math
from datetime import UTC, date, datetime, timedelta
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query

from core.api.dependencies import get_tenant_config, get_unit_of_work
from core.api.schemas import ChargebackResponse, PaginatedResponse
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
    today = date.today()
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
                ecosystem=c.ecosystem,
                tenant_id=c.tenant_id,
                timestamp=c.timestamp,
                resource_id=c.resource_id,
                product_category=c.product_category,
                product_type=c.product_type,
                identity_id=c.identity_id,
                cost_type=c.cost_type.value if hasattr(c.cost_type, "value") else str(c.cost_type),
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
