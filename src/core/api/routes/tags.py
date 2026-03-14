from __future__ import annotations

import logging
import math
from typing import TYPE_CHECKING, Annotated

from fastapi import APIRouter, Depends, HTTPException, Path, Query

from core.api.dependencies import get_tenant_config, get_unit_of_work, get_write_unit_of_work, resolve_date_range
from core.api.schemas import (
    BulkTagByFilterRequest,
    BulkTagRequest,
    BulkTagResponse,
    PaginatedResponse,
    TagCreateRequest,
    TagResponse,
    TagUpdateRequest,
    TagWithDimensionResponse,
)
from core.config.models import TenantConfig  # noqa: TC001  # FastAPI evaluates annotations at runtime
from core.storage.interface import ReadOnlyUnitOfWork, UnitOfWork  # noqa: TC001

if TYPE_CHECKING:
    from core.models.chargeback import CustomTag
logger = logging.getLogger(__name__)

router = APIRouter(tags=["tags"])


def _validate_dimension_ownership(
    uow: ReadOnlyUnitOfWork,
    dimension_id: int,
    tenant_config: TenantConfig,
) -> None:
    """Validate that a dimension belongs to the given tenant. Raises 404 if not."""
    dim = uow.chargebacks.get_dimension(dimension_id)
    if dim is None or dim.ecosystem != tenant_config.ecosystem or dim.tenant_id != tenant_config.tenant_id:
        raise HTTPException(status_code=404, detail=f"Dimension {dimension_id} not found")


def _tag_response(tag: CustomTag) -> TagResponse:
    """Convert domain CustomTag to API response. tag_id is always set after persistence."""
    assert tag.tag_id is not None, "tag_id must be set after persistence"
    return TagResponse(
        tag_id=tag.tag_id,
        dimension_id=tag.dimension_id,
        tag_key=tag.tag_key,
        tag_value=tag.tag_value,
        display_name=tag.display_name,
        created_by=tag.created_by,
        created_at=tag.created_at,
    )


@router.get(
    "/tenants/{tenant_name}/chargebacks/{dimension_id}/tags",
    response_model=list[TagResponse],
)
async def list_tags(
    tenant_config: Annotated[TenantConfig, Depends(get_tenant_config)],
    uow: Annotated[ReadOnlyUnitOfWork, Depends(get_unit_of_work)],
    dimension_id: Annotated[int, Path(description="Chargeback dimension ID")],
) -> list[TagResponse]:
    logger.debug("GET /chargebacks/%s/tags tenant=%s", dimension_id, tenant_config.tenant_id)
    _validate_dimension_ownership(uow, dimension_id, tenant_config)
    tags = uow.tags.get_tags(dimension_id)
    return [_tag_response(t) for t in tags]


@router.post(
    "/tenants/{tenant_name}/chargebacks/{dimension_id}/tags",
    response_model=TagResponse,
    status_code=201,
)
async def create_tag(
    tenant_config: Annotated[TenantConfig, Depends(get_tenant_config)],
    uow: Annotated[UnitOfWork, Depends(get_write_unit_of_work)],
    dimension_id: Annotated[int, Path(description="Chargeback dimension ID")],
    body: TagCreateRequest,
) -> TagResponse:
    _validate_dimension_ownership(uow, dimension_id, tenant_config)
    tag = uow.tags.add_tag(
        dimension_id=dimension_id,
        tag_key=body.tag_key,
        display_name=body.display_name,
        created_by=body.created_by,
    )
    uow.commit()
    return _tag_response(tag)


@router.get(
    "/tenants/{tenant_name}/tags",
    response_model=PaginatedResponse[TagWithDimensionResponse],
)
async def list_tags_for_tenant(
    tenant_config: Annotated[TenantConfig, Depends(get_tenant_config)],
    uow: Annotated[ReadOnlyUnitOfWork, Depends(get_unit_of_work)],
    page: Annotated[int, Query(ge=1)] = 1,
    page_size: Annotated[int, Query(ge=1, le=1000)] = 100,
    search: Annotated[str | None, Query()] = None,
) -> PaginatedResponse[TagWithDimensionResponse]:
    offset = (page - 1) * page_size
    tags, total = uow.tags.find_tags_for_tenant(
        ecosystem=tenant_config.ecosystem,
        tenant_id=tenant_config.tenant_id,
        limit=page_size,
        offset=offset,
        search=search or None,
    )
    # Batch fetch dimensions for denormalized context
    dimension_ids = list({t.dimension_id for t in tags})
    dims = uow.chargebacks.get_dimensions_batch(dimension_ids)
    pages = math.ceil(total / page_size) if total > 0 else 0
    items = []
    for tag in tags:
        dim = dims.get(tag.dimension_id)
        items.append(
            TagWithDimensionResponse(
                tag_id=tag.tag_id,  # type: ignore[arg-type]  # always set after persistence
                dimension_id=tag.dimension_id,
                tag_key=tag.tag_key,
                tag_value=tag.tag_value,
                display_name=tag.display_name,
                created_by=tag.created_by,
                created_at=tag.created_at,
                identity_id=dim.identity_id if dim else "",
                product_type=dim.product_type if dim else "",
                resource_id=dim.resource_id if dim else None,
            )
        )
    return PaginatedResponse[TagWithDimensionResponse](
        items=items,
        total=total,
        page=page,
        page_size=page_size,
        pages=pages,
    )


@router.patch(
    "/tenants/{tenant_name}/tags/{tag_id}",
    response_model=TagResponse,
)
async def update_tag(
    tenant_config: Annotated[TenantConfig, Depends(get_tenant_config)],
    uow: Annotated[UnitOfWork, Depends(get_write_unit_of_work)],
    tag_id: Annotated[int, Path(description="Tag ID to update")],
    body: TagUpdateRequest,
) -> TagResponse:
    tag = uow.tags.get_tag(tag_id)
    if tag is None:
        raise HTTPException(status_code=404, detail=f"Tag {tag_id} not found")
    _validate_dimension_ownership(uow, tag.dimension_id, tenant_config)
    updated = uow.tags.update_display_name(tag_id, body.display_name)
    uow.commit()
    return _tag_response(updated)


@router.delete(
    "/tenants/{tenant_name}/tags/{tag_id}",
    status_code=204,
)
async def delete_tag(
    tenant_config: Annotated[TenantConfig, Depends(get_tenant_config)],
    uow: Annotated[UnitOfWork, Depends(get_write_unit_of_work)],
    tag_id: Annotated[int, Path(description="Tag ID to delete")],
) -> None:
    tag = uow.tags.get_tag(tag_id)
    if tag is None:
        raise HTTPException(status_code=404, detail=f"Tag {tag_id} not found")
    # Validate tenant ownership via dimension
    _validate_dimension_ownership(uow, tag.dimension_id, tenant_config)
    uow.tags.delete_tag(tag_id)
    uow.commit()


# Matches _CHUNK_SIZE in repositories.py — both bound by SQLite 32K param limit
_BULK_CHUNK_SIZE = 500


def _run_bulk_tag(
    uow: UnitOfWork,
    tenant_config: TenantConfig,
    dimension_ids: list[int],
    tag_key: str,
    display_name: str,
    created_by: str,
    override_existing: bool,
) -> BulkTagResponse:
    """Core logic for bulk tagging. Batch-fetches to avoid N+1 queries."""
    created_count = 0
    updated_count = 0
    skipped_count = 0
    errors: list[str] = []

    for i in range(0, len(dimension_ids), _BULK_CHUNK_SIZE):
        chunk = dimension_ids[i : i + _BULK_CHUNK_SIZE]

        # 1 query for all dimensions in chunk
        dims_map = uow.chargebacks.get_dimensions_batch(chunk)
        # 1 query for all existing tags in chunk
        existing_tags = uow.tags.find_tags_by_dimensions_and_key(chunk, tag_key)

        for dim_id in chunk:
            dim = dims_map.get(dim_id)
            if dim is None or dim.ecosystem != tenant_config.ecosystem or dim.tenant_id != tenant_config.tenant_id:
                errors.append(str(dim_id))
                continue

            existing = existing_tags.get(dim_id)
            if existing is not None:
                if override_existing:
                    uow.tags.update_display_name(existing.tag_id, display_name)  # type: ignore[arg-type]  # always set after DB fetch
                    updated_count += 1
                else:
                    skipped_count += 1
            else:
                uow.tags.add_tag(dim_id, tag_key, display_name, created_by)
                created_count += 1

    uow.commit()
    return BulkTagResponse(
        created_count=created_count,
        updated_count=updated_count,
        skipped_count=skipped_count,
        errors=errors,
    )


@router.post(
    "/tenants/{tenant_name}/tags/bulk",
    response_model=BulkTagResponse,
)
async def bulk_add_tags(
    tenant_config: Annotated[TenantConfig, Depends(get_tenant_config)],
    uow: Annotated[UnitOfWork, Depends(get_write_unit_of_work)],
    body: BulkTagRequest,
) -> BulkTagResponse:
    return _run_bulk_tag(
        uow=uow,
        tenant_config=tenant_config,
        dimension_ids=body.dimension_ids,
        tag_key=body.tag_key,
        display_name=body.display_name,
        created_by=body.created_by,
        override_existing=body.override_existing,
    )


@router.post(
    "/tenants/{tenant_name}/tags/bulk-by-filter",
    response_model=BulkTagResponse,
)
async def bulk_add_tags_by_filter(
    tenant_config: Annotated[TenantConfig, Depends(get_tenant_config)],
    uow: Annotated[UnitOfWork, Depends(get_write_unit_of_work)],
    body: BulkTagByFilterRequest,
) -> BulkTagResponse:
    start_dt, end_dt = resolve_date_range(body.start_date, body.end_date)
    dimension_ids = uow.chargebacks.find_dimension_ids_by_filters(
        ecosystem=tenant_config.ecosystem,
        tenant_id=tenant_config.tenant_id,
        start=start_dt,
        end=end_dt,
        identity_id=body.identity_id,
        product_type=body.product_type,
        resource_id=body.resource_id,
        cost_type=body.cost_type,
    )
    return _run_bulk_tag(
        uow=uow,
        tenant_config=tenant_config,
        dimension_ids=dimension_ids,
        tag_key=body.tag_key,
        display_name=body.display_name,
        created_by=body.created_by,
        override_existing=body.override_existing,
    )
