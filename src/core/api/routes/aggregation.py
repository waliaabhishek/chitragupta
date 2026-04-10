from __future__ import annotations

import logging
from datetime import date
from decimal import Decimal
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, Request

from core.api.dependencies import get_tenant_config, get_unit_of_work, resolve_date_range
from core.api.schemas import AggregationBucket, AggregationResponse
from core.config.models import TenantConfig  # noqa: TC001  # FastAPI evaluates annotations at runtime
from core.storage.interface import ReadOnlyUnitOfWork  # noqa: TC001
from core.utils.tag_validation import is_valid_tag_key

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
    request: Request,
    tenant_config: Annotated[TenantConfig, Depends(get_tenant_config)],
    uow: Annotated[ReadOnlyUnitOfWork, Depends(get_unit_of_work)],
    group_by: Annotated[list[str] | None, Query(description="Dimension columns or tag:{key} to group by")] = None,
    time_bucket: Annotated[str, Query(description="Time bucket: hour, day, week, month")] = "day",
    start_date: Annotated[date | None, Query()] = None,
    end_date: Annotated[date | None, Query()] = None,
    timezone: Annotated[
        str | None, Query(description="IANA timezone for date boundaries (e.g. America/Denver)")
    ] = None,
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
    # Parse tag filters from raw query params (FastAPI can't express dynamic tag:{key}={value})
    tag_filters: dict[str, list[str]] = {}
    for param_name, param_value in request.query_params.multi_items():
        if param_name.startswith("tag:"):
            tag_key = param_name[4:]
            if not is_valid_tag_key(tag_key):
                raise HTTPException(status_code=400, detail=f"Invalid tag key format: {tag_key!r}")
            values = [v.strip() for v in param_value.split(",") if v.strip()]
            if values:
                tag_filters.setdefault(tag_key, []).extend(values)

    # Split group_by into dimension keys and tag keys
    if group_by is None:
        group_by = ["identity_id"]

    dim_group_by: list[str] = []
    tag_group_by: list[str] = []
    for gb in group_by:
        if gb.startswith("tag:"):
            key = gb[4:]
            if not is_valid_tag_key(key):
                raise HTTPException(status_code=400, detail=f"Invalid tag key format in group_by: {key!r}")
            tag_group_by.append(key)
        else:
            dim_group_by.append(gb)

    if not dim_group_by and not tag_group_by:
        raise HTTPException(status_code=400, detail="group_by must contain at least one column")

    if dim_group_by:
        invalid_cols = set(dim_group_by) - _VALID_GROUP_BY
        if invalid_cols:
            raise HTTPException(
                status_code=400,
                detail=f"group_by must be from {sorted(_VALID_GROUP_BY)}, got invalid: {sorted(invalid_cols)}",
            )

    if time_bucket not in _VALID_TIME_BUCKETS:
        raise HTTPException(status_code=400, detail=f"time_bucket must be one of {sorted(_VALID_TIME_BUCKETS)}")

    start_dt, end_dt = resolve_date_range(start_date, end_date, timezone=timezone)

    rows = uow.chargebacks.aggregate(
        ecosystem=tenant_config.ecosystem,
        tenant_id=tenant_config.tenant_id,
        group_by=dim_group_by,
        time_bucket=time_bucket,
        start=start_dt,
        end=end_dt,
        identity_id=identity_id,
        product_type=product_type,
        resource_id=resource_id,
        cost_type=cost_type,
        tag_group_by=tag_group_by or None,
        tag_filters=tag_filters or None,
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
