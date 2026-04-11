from __future__ import annotations

import csv
import io
import logging
import math
from collections.abc import Iterator
from datetime import date
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from core.api.dependencies import get_tenant_config, get_unit_of_work, resolve_date_range
from core.api.schemas import (
    PaginatedResponse,
    TopicAttributionAggregationResponse,
    TopicAttributionDatesResponse,
    TopicAttributionResponse,
)
from core.api.schemas import (
    TopicAttributionAggregationBucket as TopicAttributionAggregationBucketSchema,
)
from core.config.models import TenantConfig  # noqa: TC001
from core.storage.interface import ReadOnlyUnitOfWork  # noqa: TC001
from core.utils.tag_validation import is_valid_tag_key

logger = logging.getLogger(__name__)

router = APIRouter(tags=["topic-attributions"])


@router.get(
    "/tenants/{tenant_name}/topic-attributions",
    response_model=PaginatedResponse[TopicAttributionResponse],
)
async def list_topic_attributions(
    tenant_config: Annotated[TenantConfig, Depends(get_tenant_config)],
    uow: Annotated[ReadOnlyUnitOfWork, Depends(get_unit_of_work)],
    start_date: Annotated[date | None, Query()] = None,
    end_date: Annotated[date | None, Query()] = None,
    timezone: Annotated[str | None, Query()] = None,
    cluster_resource_id: Annotated[str | None, Query()] = None,
    topic_name: Annotated[str | None, Query()] = None,
    product_type: Annotated[str | None, Query()] = None,
    attribution_method: Annotated[str | None, Query()] = None,
    page: Annotated[int, Query(ge=1)] = 1,
    page_size: Annotated[int, Query(ge=1, le=1000)] = 100,
) -> PaginatedResponse[TopicAttributionResponse]:
    start_dt, end_dt = resolve_date_range(start_date, end_date, timezone=timezone)
    offset = (page - 1) * page_size
    items, total = uow.topic_attributions.find_by_filters(
        ecosystem=tenant_config.ecosystem,
        tenant_id=tenant_config.tenant_id,
        start=start_dt,
        end=end_dt,
        cluster_resource_id=cluster_resource_id,
        topic_name=topic_name,
        product_type=product_type,
        attribution_method=attribution_method,
        limit=page_size,
        offset=offset,
    )
    pages = math.ceil(total / page_size) if total > 0 else 0
    return PaginatedResponse[TopicAttributionResponse](
        items=[
            TopicAttributionResponse(
                dimension_id=r.dimension_id,
                ecosystem=r.ecosystem,
                tenant_id=r.tenant_id,
                timestamp=r.timestamp,
                env_id=r.env_id,
                cluster_resource_id=r.cluster_resource_id,
                topic_name=r.topic_name,
                product_category=r.product_category,
                product_type=r.product_type,
                attribution_method=r.attribution_method,
                amount=r.amount,
            )
            for r in items
        ],
        total=total,
        page=page,
        page_size=page_size,
        pages=pages,
    )


@router.get(
    "/tenants/{tenant_name}/topic-attributions/aggregate",
    response_model=TopicAttributionAggregationResponse,
)
async def aggregate_topic_attributions(
    request: Request,
    tenant_config: Annotated[TenantConfig, Depends(get_tenant_config)],
    uow: Annotated[ReadOnlyUnitOfWork, Depends(get_unit_of_work)],
    group_by: Annotated[list[str] | None, Query(description="Dimension columns or tag:{key} to group by")] = None,
    time_bucket: Annotated[str, Query()] = "day",
    start_date: Annotated[date | None, Query()] = None,
    end_date: Annotated[date | None, Query()] = None,
    timezone: Annotated[str | None, Query()] = None,
    cluster_resource_id: Annotated[str | None, Query()] = None,
    topic_name: Annotated[str | None, Query()] = None,
    product_type: Annotated[str | None, Query()] = None,
) -> TopicAttributionAggregationResponse:
    start_dt, end_dt = resolve_date_range(start_date, end_date, timezone=timezone)

    # Parse tag filters from raw query params
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
        group_by = ["topic_name"]

    dim_group_by: list[str] = []
    tag_group_by: list[str] = []
    for gb in group_by:
        if gb.startswith("tag:"):
            key = gb[4:]
            if not is_valid_tag_key(key):
                raise HTTPException(status_code=400, detail=f"Invalid tag key format in group_by: {key!r}")
            tag_group_by.append(key)
        else:
            dim_group_by.append(gb)  # invalid dim names silently filtered by repo (preserves existing behavior)

    result = uow.topic_attributions.aggregate(
        ecosystem=tenant_config.ecosystem,
        tenant_id=tenant_config.tenant_id,
        group_by=dim_group_by,
        time_bucket=time_bucket,
        start=start_dt,
        end=end_dt,
        cluster_resource_id=cluster_resource_id,
        topic_name=topic_name,
        product_type=product_type,
        tag_group_by=tag_group_by or None,
        tag_filters=tag_filters or None,
    )
    return TopicAttributionAggregationResponse(
        buckets=[
            TopicAttributionAggregationBucketSchema(
                dimensions=b.dimensions,
                time_bucket=b.time_bucket,
                total_amount=b.total_amount,
                row_count=b.row_count,
            )
            for b in result.buckets
        ],
        total_amount=result.total_amount,
        total_rows=result.total_rows,
    )


@router.get(
    "/tenants/{tenant_name}/topic-attributions/dates",
    response_model=TopicAttributionDatesResponse,
)
async def list_topic_attribution_dates(
    tenant_config: Annotated[TenantConfig, Depends(get_tenant_config)],
    uow: Annotated[ReadOnlyUnitOfWork, Depends(get_unit_of_work)],
) -> TopicAttributionDatesResponse:
    dates = uow.topic_attributions.get_distinct_dates(
        tenant_config.ecosystem,
        tenant_config.tenant_id,
    )
    return TopicAttributionDatesResponse(dates=dates)


@router.post("/tenants/{tenant_name}/topic-attributions/export")
async def export_topic_attributions(
    tenant_config: Annotated[TenantConfig, Depends(get_tenant_config)],
    uow: Annotated[ReadOnlyUnitOfWork, Depends(get_unit_of_work)],
    start_date: Annotated[date | None, Query()] = None,
    end_date: Annotated[date | None, Query()] = None,
    timezone: Annotated[str | None, Query()] = None,
) -> StreamingResponse:
    start_dt, end_dt = resolve_date_range(start_date, end_date, timezone=timezone)
    rows = uow.topic_attributions.iter_by_filters(
        ecosystem=tenant_config.ecosystem,
        tenant_id=tenant_config.tenant_id,
        start=start_dt,
        end=end_dt,
    )

    def generate() -> Iterator[str]:
        buf = io.StringIO()
        writer = csv.writer(buf, lineterminator="\n")
        writer.writerow(
            [
                "ecosystem",
                "tenant_id",
                "timestamp",
                "env_id",
                "cluster_resource_id",
                "topic_name",
                "product_category",
                "product_type",
                "attribution_method",
                "amount",
            ]
        )
        yield buf.getvalue()
        for row in rows:
            buf.seek(0)
            buf.truncate(0)
            writer.writerow(
                [
                    row.ecosystem,
                    row.tenant_id,
                    row.timestamp.isoformat(),
                    row.env_id,
                    row.cluster_resource_id,
                    row.topic_name,
                    row.product_category,
                    row.product_type,
                    row.attribution_method,
                    str(row.amount),
                ]
            )
            yield buf.getvalue()

    return StreamingResponse(
        generate(),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=topic_attributions.csv"},
    )
