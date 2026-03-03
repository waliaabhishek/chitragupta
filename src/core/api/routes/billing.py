from __future__ import annotations

import math
from datetime import UTC, date, datetime, timedelta
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query

from core.api.dependencies import get_tenant_config, get_unit_of_work, utc_today
from core.api.schemas import BillingLineResponse, PaginatedResponse
from core.config.models import TenantConfig  # noqa: TC001  # FastAPI evaluates annotations at runtime
from core.storage.interface import UnitOfWork  # noqa: TC001

router = APIRouter(tags=["billing"])


@router.get("/tenants/{tenant_name}/billing", response_model=PaginatedResponse[BillingLineResponse])
async def list_billing(
    tenant_config: Annotated[TenantConfig, Depends(get_tenant_config)],
    uow: Annotated[UnitOfWork, Depends(get_unit_of_work)],
    start_date: Annotated[date | None, Query()] = None,
    end_date: Annotated[date | None, Query()] = None,
    product_type: Annotated[str | None, Query()] = None,
    resource_id: Annotated[str | None, Query()] = None,
    page: Annotated[int, Query(ge=1)] = 1,
    page_size: Annotated[int, Query(ge=1, le=1000)] = 100,
) -> PaginatedResponse[BillingLineResponse]:
    today = utc_today()
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
        items, total = uow.billing.find_by_filters(
            ecosystem=tenant_config.ecosystem,
            tenant_id=tenant_config.tenant_id,
            start=start_dt,
            end=end_dt,
            product_type=product_type,
            resource_id=resource_id,
            limit=page_size,
            offset=offset,
        )

    pages = math.ceil(total / page_size) if total > 0 else 0
    return PaginatedResponse[BillingLineResponse](
        items=[
            BillingLineResponse(
                ecosystem=b.ecosystem,
                tenant_id=b.tenant_id,
                timestamp=b.timestamp,
                resource_id=b.resource_id,
                product_category=b.product_category,
                product_type=b.product_type,
                quantity=b.quantity,
                unit_price=b.unit_price,
                total_cost=b.total_cost,
                currency=b.currency,
                granularity=b.granularity,
                metadata=b.metadata,
            )
            for b in items
        ],
        total=total,
        page=page,
        page_size=page_size,
        pages=pages,
    )
