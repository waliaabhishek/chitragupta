from __future__ import annotations

import csv
import io
from collections.abc import Iterator
from datetime import UTC, date, datetime, timedelta
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse

from core.api.dependencies import get_tenant_config, get_unit_of_work
from core.api.schemas import ExportRequest  # noqa: TC001  # FastAPI evaluates annotations at runtime
from core.config.models import TenantConfig  # noqa: TC001  # FastAPI evaluates annotations at runtime
from core.storage.interface import UnitOfWork  # noqa: TC001

router = APIRouter(tags=["export"])

_DEFAULT_COLUMNS = [
    "timestamp",
    "resource_id",
    "product_category",
    "product_type",
    "identity_id",
    "cost_type",
    "amount",
    "allocation_method",
    "tags",
]

_ALL_COLUMNS = frozenset(
    {
        "ecosystem",
        "tenant_id",
        "timestamp",
        "resource_id",
        "product_category",
        "product_type",
        "identity_id",
        "cost_type",
        "amount",
        "allocation_method",
        "allocation_detail",
        "tags",
        "metadata",
    }
)

_VALID_FILTER_KEYS = frozenset({"identity_id", "product_type", "resource_id", "cost_type"})


def _stream_csv(
    uow: UnitOfWork,
    ecosystem: str,
    tenant_id: str,
    start_dt: datetime,
    end_dt: datetime,
    columns: list[str],
    filters: dict[str, str] | None,
) -> Iterator[str]:
    """Generate CSV rows as a streaming iterator."""
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n")

    # Header row
    writer.writerow(columns)
    yield buf.getvalue()
    buf.seek(0)
    buf.truncate(0)

    # Build filter kwargs
    filter_kwargs: dict[str, str | None] = {}
    if filters:
        for k, v in filters.items():
            filter_kwargs[k] = v

    with uow:
        items, _ = uow.chargebacks.find_by_filters(
            ecosystem=ecosystem,
            tenant_id=tenant_id,
            start=start_dt,
            end=end_dt,
            limit=100000,
            offset=0,
            **filter_kwargs,
        )

        for row in items:
            values = []
            for col_name in columns:
                if col_name == "cost_type":
                    values.append(row.cost_type.value if hasattr(row.cost_type, "value") else str(row.cost_type))
                elif col_name == "tags":
                    values.append(";".join(row.tags))
                elif col_name == "metadata":
                    values.append(str(row.metadata))
                else:
                    values.append(str(getattr(row, col_name, "")))
            writer.writerow(values)
            yield buf.getvalue()
            buf.seek(0)
            buf.truncate(0)


@router.post("/tenants/{tenant_name}/export")
async def export_chargebacks(
    tenant_config: Annotated[TenantConfig, Depends(get_tenant_config)],
    uow: Annotated[UnitOfWork, Depends(get_unit_of_work)],
    body: ExportRequest,
) -> StreamingResponse:
    today = date.today()
    effective_start = body.start_date or (today - timedelta(days=30))
    effective_end = body.end_date or today

    if effective_start > effective_end:
        raise HTTPException(400, detail="start_date must be <= end_date")

    columns = body.columns or list(_DEFAULT_COLUMNS)
    invalid = set(columns) - _ALL_COLUMNS
    if invalid:
        raise HTTPException(400, detail=f"Invalid columns: {sorted(invalid)}")

    if body.filters:
        invalid_filters = set(body.filters.keys()) - _VALID_FILTER_KEYS
        if invalid_filters:
            raise HTTPException(400, detail=f"Invalid filter keys: {sorted(invalid_filters)}")

    start_dt = datetime(effective_start.year, effective_start.month, effective_start.day, tzinfo=UTC)
    end_dt = datetime(effective_end.year, effective_end.month, effective_end.day, tzinfo=UTC) + timedelta(days=1)

    return StreamingResponse(
        _stream_csv(
            uow=uow,
            ecosystem=tenant_config.ecosystem,
            tenant_id=tenant_config.tenant_id,
            start_dt=start_dt,
            end_dt=end_dt,
            columns=columns,
            filters=body.filters,
        ),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=chargebacks.csv"},
    )
