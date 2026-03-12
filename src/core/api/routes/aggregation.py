from __future__ import annotations

import logging
from datetime import date
from decimal import Decimal
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query

from core.api.dependencies import get_tenant_config, get_unit_of_work, resolve_date_range
from core.api.schemas import AggregationBucket, AggregationResponse
from core.config.models import TenantConfig  # noqa: TC001  # FastAPI evaluates annotations at runtime
from core.storage.interface import UnitOfWork  # noqa: TC001

logger = logging.getLogger(__name__)

router = APIRouter(tags=["aggregation"])

_VALID_GROUP_BY = frozenset(
    {
        "identity_id",
        "resource_id",
        "product_type",
        "product_category",
        "cost_type",
        "allocation_method",
        "environment_id",
    }
)

_VALID_TIME_BUCKETS = frozenset({"hour", "day", "week", "month"})


@router.get(
    "/tenants/{tenant_name}/chargebacks/aggregate",
    response_model=AggregationResponse,
)
async def aggregate_chargebacks(
    tenant_config: Annotated[TenantConfig, Depends(get_tenant_config)],
    uow: Annotated[UnitOfWork, Depends(get_unit_of_work)],
    group_by: Annotated[list[str] | None, Query(description="Dimension columns to group by")] = None,
    time_bucket: Annotated[str, Query(description="Time bucket: hour, day, week, month")] = "day",
    start_date: Annotated[date | None, Query()] = None,
    end_date: Annotated[date | None, Query()] = None,
    identity_id: Annotated[str | None, Query()] = None,
    product_type: Annotated[str | None, Query()] = None,
    resource_id: Annotated[str | None, Query()] = None,
    cost_type: Annotated[str | None, Query()] = None,
) -> AggregationResponse:
    logger.debug(
        "GET /chargebacks/aggregate tenant=%s group_by=%s time_bucket=%s",
        tenant_config.tenant_id,
        group_by,
        time_bucket,
    )
    if group_by is None:
        group_by = ["identity_id"]

    invalid_cols = set(group_by) - _VALID_GROUP_BY
    if invalid_cols:
        raise HTTPException(
            status_code=400,
            detail=f"group_by must be from {sorted(_VALID_GROUP_BY)}, got invalid: {sorted(invalid_cols)}",
        )

    if not group_by:
        raise HTTPException(status_code=400, detail="group_by must contain at least one column")

    if time_bucket not in _VALID_TIME_BUCKETS:
        raise HTTPException(
            status_code=400,
            detail=f"time_bucket must be one of {sorted(_VALID_TIME_BUCKETS)}, got {time_bucket!r}",
        )

    start_dt, end_dt = resolve_date_range(start_date, end_date)

    with uow:
        rows = uow.chargebacks.aggregate(
            ecosystem=tenant_config.ecosystem,
            tenant_id=tenant_config.tenant_id,
            group_by=group_by,
            time_bucket=time_bucket,
            start=start_dt,
            end=end_dt,
            identity_id=identity_id,
            product_type=product_type,
            resource_id=resource_id,
            cost_type=cost_type,
            limit=10000,
        )

    buckets = [
        AggregationBucket(
            dimensions=r.dimensions,
            time_bucket=r.time_bucket,
            total_amount=r.total_amount,
            usage_amount=r.usage_amount,
            shared_amount=r.shared_amount,
            row_count=r.row_count,
        )
        for r in rows
    ]

    total_amount = sum((b.total_amount for b in buckets), Decimal(0))
    usage_amount = sum((b.usage_amount for b in buckets), Decimal(0))
    shared_amount = sum((b.shared_amount for b in buckets), Decimal(0))
    total_rows = sum(b.row_count for b in buckets)

    logger.info(
        "Aggregated chargebacks tenant=%s buckets=%d",
        tenant_config.tenant_id,
        len(buckets),
    )
    return AggregationResponse(
        buckets=buckets,
        total_amount=total_amount,
        usage_amount=usage_amount,
        shared_amount=shared_amount,
        total_rows=total_rows,
    )
