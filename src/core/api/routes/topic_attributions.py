from __future__ import annotations

import csv
import io
import logging
import math
from collections.abc import Iterator
from datetime import date
from typing import Annotated

from fastapi import APIRouter, Depends, Query
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
    tenant_config: Annotated[TenantConfig, Depends(get_tenant_config)],
    uow: Annotated[ReadOnlyUnitOfWork, Depends(get_unit_of_work)],
    group_by: Annotated[list[str] | None, Query()] = None,
    time_bucket: Annotated[str, Query()] = "day",
    start_date: Annotated[date | None, Query()] = None,
    end_date: Annotated[date | None, Query()] = None,
    timezone: Annotated[str | None, Query()] = None,
    cluster_resource_id: Annotated[str | None, Query()] = None,
    topic_name: Annotated[str | None, Query()] = None,
    product_type: Annotated[str | None, Query()] = None,
) -> TopicAttributionAggregationResponse:
    start_dt, end_dt = resolve_date_range(start_date, end_date, timezone=timezone)
    if group_by is None:
        group_by = ["topic_name"]
    result = uow.topic_attributions.aggregate(
        ecosystem=tenant_config.ecosystem,
        tenant_id=tenant_config.tenant_id,
        group_by=group_by,
        time_bucket=time_bucket,
        start=start_dt,
        end=end_dt,
        cluster_resource_id=cluster_resource_id,
        topic_name=topic_name,
        product_type=product_type,
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
    rows, _ = uow.topic_attributions.find_by_filters(
        ecosystem=tenant_config.ecosystem,
        tenant_id=tenant_config.tenant_id,
        start=start_dt,
        end=end_dt,
        limit=100_000,
        offset=0,
    )

    def generate() -> Iterator[str]:
        buf = io.StringIO()
        writer = csv.writer(buf)
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
