from __future__ import annotations

import logging
import math
from datetime import date
from typing import Annotated

from fastapi import APIRouter, Depends, Query

from core.api.dependencies import get_tenant_config, get_unit_of_work, resolve_date_range
from core.api.schemas import BillingLineResponse, PaginatedResponse
from core.config.models import TenantConfig  # noqa: TC001  # FastAPI evaluates annotations at runtime
from core.storage.interface import ReadOnlyUnitOfWork  # noqa: TC001

logger = logging.getLogger(__name__)

router = APIRouter(tags=["billing"])


@router.get("/tenants/{tenant_name}/billing", response_model=PaginatedResponse[BillingLineResponse])
async def list_billing(
    tenant_config: Annotated[TenantConfig, Depends(get_tenant_config)],
    uow: Annotated[ReadOnlyUnitOfWork, Depends(get_unit_of_work)],
    start_date: Annotated[date | None, Query()] = None,
    end_date: Annotated[date | None, Query()] = None,
    timezone: Annotated[
        str | None, Query(description="IANA timezone for date boundaries (e.g. America/Denver)")
    ] = None,
    product_type: Annotated[str | None, Query()] = None,
    resource_id: Annotated[str | None, Query()] = None,
    page: Annotated[int, Query(ge=1)] = 1,
    page_size: Annotated[int, Query(ge=1, le=1000)] = 100,
) -> PaginatedResponse[BillingLineResponse]:
    logger.debug(
        "GET /billing tenant=%s page=%d page_size=%d",
        tenant_config.tenant_id,
        page,
        page_size,
    )
    start_dt, end_dt = resolve_date_range(start_date, end_date, timezone=timezone)

    offset = (page - 1) * page_size
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
    logger.info("Listed billing tenant=%s returned=%d total=%d", tenant_config.tenant_id, len(items), total)
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
