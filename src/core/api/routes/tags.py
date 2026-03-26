from __future__ import annotations

import logging
import math
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Path, Query
from sqlalchemy.exc import IntegrityError

from core.api.dependencies import get_tenant_config, get_unit_of_work, get_write_unit_of_work, resolve_date_range
from core.api.schemas import (
    BulkEntityTagRequest,
    BulkEntityTagResponse,
    BulkTagByFilterRequest,
    BulkTagByFilterResponse,
    EntityTagCreateRequest,
    EntityTagResponse,
    EntityTagUpdateRequest,
    PaginatedResponse,
)
from core.config.models import TenantConfig  # noqa: TC001
from core.storage.interface import ReadOnlyUnitOfWork, UnitOfWork  # noqa: TC001

logger = logging.getLogger(__name__)

router = APIRouter(tags=["tags"])

_ALLOWED_ENTITY_TYPES = frozenset({"resource", "identity"})


def _validate_entity_type(entity_type: str) -> None:
    if entity_type not in _ALLOWED_ENTITY_TYPES:
        raise HTTPException(
            status_code=422,
            detail=f"entity_type must be one of {sorted(_ALLOWED_ENTITY_TYPES)}",
        )


def _validate_entity_ownership(
    uow: ReadOnlyUnitOfWork,
    entity_type: str,
    entity_id: str,
    tenant_config: TenantConfig,
) -> None:
    """Verify entity_id belongs to tenant. Raises 404 if not found."""
    if entity_type == "resource":
        if uow.resources.get(tenant_config.ecosystem, tenant_config.tenant_id, entity_id) is None:
            raise HTTPException(status_code=404, detail=f"Resource {entity_id} not found")
    else:  # "identity"
        if uow.identities.get(tenant_config.ecosystem, tenant_config.tenant_id, entity_id) is None:
            raise HTTPException(status_code=404, detail=f"Identity {entity_id} not found")


@router.get(
    "/tenants/{tenant_name}/entities/{entity_type}/{entity_id}/tags",
    response_model=list[EntityTagResponse],
)
async def list_entity_tags(
    tenant_config: Annotated[TenantConfig, Depends(get_tenant_config)],
    uow: Annotated[ReadOnlyUnitOfWork, Depends(get_unit_of_work)],
    entity_type: Annotated[str, Path()],
    entity_id: Annotated[str, Path()],
) -> list[EntityTagResponse]:
    _validate_entity_type(entity_type)
    tags = uow.tags.get_tags(tenant_config.tenant_id, entity_type, entity_id)
    return [EntityTagResponse.model_validate(t.__dict__) for t in tags]


@router.post(
    "/tenants/{tenant_name}/entities/{entity_type}/{entity_id}/tags",
    response_model=EntityTagResponse,
    status_code=201,
)
async def create_entity_tag(
    tenant_config: Annotated[TenantConfig, Depends(get_tenant_config)],
    uow: Annotated[UnitOfWork, Depends(get_write_unit_of_work)],
    entity_type: Annotated[str, Path()],
    entity_id: Annotated[str, Path()],
    body: EntityTagCreateRequest,
) -> EntityTagResponse:
    _validate_entity_type(entity_type)
    _validate_entity_ownership(uow, entity_type, entity_id, tenant_config)
    try:
        tag = uow.tags.add_tag(
            tenant_id=tenant_config.tenant_id,
            entity_type=entity_type,
            entity_id=entity_id,
            tag_key=body.tag_key,
            tag_value=body.tag_value,
            created_by=body.created_by,
        )
        uow.commit()
    except IntegrityError:
        raise HTTPException(
            status_code=409, detail=f"Tag key '{body.tag_key}' already exists on {entity_type} {entity_id}"
        ) from None
    return EntityTagResponse.model_validate(tag.__dict__)


@router.put(
    "/tenants/{tenant_name}/entities/{entity_type}/{entity_id}/tags/{tag_key}",
    response_model=EntityTagResponse,
)
async def update_entity_tag(
    tenant_config: Annotated[TenantConfig, Depends(get_tenant_config)],
    uow: Annotated[UnitOfWork, Depends(get_write_unit_of_work)],
    entity_type: Annotated[str, Path()],
    entity_id: Annotated[str, Path()],
    tag_key: Annotated[str, Path()],
    body: EntityTagUpdateRequest,
) -> EntityTagResponse:
    _validate_entity_type(entity_type)
    _validate_entity_ownership(uow, entity_type, entity_id, tenant_config)
    tags = uow.tags.get_tags(tenant_config.tenant_id, entity_type, entity_id)
    match = next((t for t in tags if t.tag_key == tag_key), None)
    if match is None:
        raise HTTPException(
            status_code=404,
            detail=f"Tag key '{tag_key}' not found on {entity_type} {entity_id}",
        )
    if match.tag_id is None:
        raise RuntimeError(f"tag_id is None for persisted tag with key '{tag_key}'")
    updated = uow.tags.update_tag(match.tag_id, body.tag_value)
    uow.commit()
    return EntityTagResponse.model_validate(updated.__dict__)


@router.delete(
    "/tenants/{tenant_name}/entities/{entity_type}/{entity_id}/tags/{tag_key}",
    status_code=204,
)
async def delete_entity_tag(
    tenant_config: Annotated[TenantConfig, Depends(get_tenant_config)],
    uow: Annotated[UnitOfWork, Depends(get_write_unit_of_work)],
    entity_type: Annotated[str, Path()],
    entity_id: Annotated[str, Path()],
    tag_key: Annotated[str, Path()],
) -> None:
    _validate_entity_type(entity_type)
    _validate_entity_ownership(uow, entity_type, entity_id, tenant_config)
    tags = uow.tags.get_tags(tenant_config.tenant_id, entity_type, entity_id)
    match = next((t for t in tags if t.tag_key == tag_key), None)
    if match is None:
        raise HTTPException(
            status_code=404,
            detail=f"Tag key '{tag_key}' not found on {entity_type} {entity_id}",
        )
    if match.tag_id is None:
        raise RuntimeError(f"tag_id is None for persisted tag with key '{tag_key}'")
    uow.tags.delete_tag(match.tag_id)
    uow.commit()


@router.get(
    "/tenants/{tenant_name}/tags",
    response_model=PaginatedResponse[EntityTagResponse],
)
async def list_tags_for_tenant(
    tenant_config: Annotated[TenantConfig, Depends(get_tenant_config)],
    uow: Annotated[ReadOnlyUnitOfWork, Depends(get_unit_of_work)],
    page: Annotated[int, Query(ge=1)] = 1,
    page_size: Annotated[int, Query(ge=1, le=1000)] = 100,
    entity_type: Annotated[str | None, Query()] = None,
    tag_key: Annotated[str | None, Query()] = None,
) -> PaginatedResponse[EntityTagResponse]:
    if entity_type is not None:
        _validate_entity_type(entity_type)
    offset = (page - 1) * page_size
    tags, total = uow.tags.find_tags_for_tenant(
        tenant_id=tenant_config.tenant_id,
        limit=page_size,
        offset=offset,
        entity_type=entity_type,
        tag_key=tag_key,
    )
    pages = math.ceil(total / page_size) if total > 0 else 0
    return PaginatedResponse[EntityTagResponse](
        items=[EntityTagResponse.model_validate(t.__dict__) for t in tags],
        total=total,
        page=page,
        page_size=page_size,
        pages=pages,
    )


@router.post(
    "/tenants/{tenant_name}/tags/bulk",
    response_model=BulkEntityTagResponse,
)
async def bulk_add_entity_tags(
    tenant_config: Annotated[TenantConfig, Depends(get_tenant_config)],
    uow: Annotated[UnitOfWork, Depends(get_write_unit_of_work)],
    body: BulkEntityTagRequest,
) -> BulkEntityTagResponse:
    for item in body.items:
        if item.entity_type not in _ALLOWED_ENTITY_TYPES:
            raise HTTPException(status_code=422, detail=f"Invalid entity_type '{item.entity_type}'")
    items_dicts = [i.model_dump() for i in body.items]
    created, updated, skipped = uow.tags.bulk_add_tags(
        tenant_id=tenant_config.tenant_id,
        items=items_dicts,
        override_existing=body.override_existing,
        created_by=body.created_by,
    )
    uow.commit()
    return BulkEntityTagResponse(created_count=created, updated_count=updated, skipped_count=skipped)


@router.post(
    "/tenants/{tenant_name}/tags/bulk-by-filter",
    response_model=BulkTagByFilterResponse,
)
async def bulk_add_entity_tags_by_filter(
    tenant_config: Annotated[TenantConfig, Depends(get_tenant_config)],
    uow: Annotated[UnitOfWork, Depends(get_write_unit_of_work)],
    body: BulkTagByFilterRequest,
) -> BulkTagByFilterResponse:
    start_dt, end_dt = resolve_date_range(body.start_date, body.end_date, timezone=body.timezone)

    seen: set[tuple[str, str]] = set()
    items: list[dict[str, str]] = []
    for row in uow.chargebacks.iter_by_filters(
        ecosystem=tenant_config.ecosystem,
        tenant_id=tenant_config.tenant_id,
        start=start_dt,
        end=end_dt,
        identity_id=body.identity_id,
    ):
        entity_type = "resource" if row.resource_id is not None else "identity"
        entity_id = row.resource_id if row.resource_id is not None else row.identity_id
        key = (entity_type, entity_id)
        if key not in seen:
            seen.add(key)
            items.append(
                {
                    "entity_type": entity_type,
                    "entity_id": entity_id,
                    "tag_key": body.tag_key,
                    "tag_value": body.display_name,
                }
            )

    if not items:
        return BulkTagByFilterResponse(created_count=0, updated_count=0, skipped_count=0)

    created, updated, skipped = uow.tags.bulk_add_tags(
        tenant_id=tenant_config.tenant_id,
        items=items,
        override_existing=body.override_existing,
        created_by=body.created_by,
    )
    uow.commit()
    return BulkTagByFilterResponse(created_count=created, updated_count=updated, skipped_count=skipped)
