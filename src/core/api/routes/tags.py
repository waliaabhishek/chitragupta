from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Path

from core.api.dependencies import get_tenant_config, get_unit_of_work
from core.api.schemas import TagCreateRequest, TagResponse
from core.config.models import TenantConfig  # noqa: TC001  # FastAPI evaluates annotations at runtime
from core.storage.interface import UnitOfWork  # noqa: TC001

router = APIRouter(tags=["tags"])


def _validate_dimension_ownership(
    uow: UnitOfWork,
    dimension_id: int,
    tenant_config: TenantConfig,
) -> None:
    """Validate that a dimension belongs to the given tenant. Raises 404 if not."""
    dim = uow.chargebacks.get_dimension(dimension_id)
    if dim is None or dim.ecosystem != tenant_config.ecosystem or dim.tenant_id != tenant_config.tenant_id:
        raise HTTPException(status_code=404, detail=f"Dimension {dimension_id} not found")


@router.get(
    "/tenants/{tenant_name}/chargebacks/{dimension_id}/tags",
    response_model=list[TagResponse],
)
async def list_tags(
    tenant_config: Annotated[TenantConfig, Depends(get_tenant_config)],
    uow: Annotated[UnitOfWork, Depends(get_unit_of_work)],
    dimension_id: Annotated[int, Path(description="Chargeback dimension ID")],
) -> list[TagResponse]:
    with uow:
        _validate_dimension_ownership(uow, dimension_id, tenant_config)
        tags = uow.tags.get_tags(dimension_id)

    return [
        TagResponse(
            tag_id=t.tag_id,  # type: ignore[arg-type]  # tag_id is always set after persistence
            dimension_id=t.dimension_id,
            tag_key=t.tag_key,
            tag_value=t.tag_value,
            created_by=t.created_by,
            created_at=t.created_at,
        )
        for t in tags
    ]


@router.post(
    "/tenants/{tenant_name}/chargebacks/{dimension_id}/tags",
    response_model=TagResponse,
    status_code=201,
)
async def create_tag(
    tenant_config: Annotated[TenantConfig, Depends(get_tenant_config)],
    uow: Annotated[UnitOfWork, Depends(get_unit_of_work)],
    dimension_id: Annotated[int, Path(description="Chargeback dimension ID")],
    body: TagCreateRequest,
) -> TagResponse:
    with uow:
        _validate_dimension_ownership(uow, dimension_id, tenant_config)
        tag = uow.tags.add_tag(
            dimension_id=dimension_id,
            tag_key=body.tag_key,
            tag_value=body.tag_value,
            created_by=body.created_by,
        )
        uow.commit()

    return TagResponse(
        tag_id=tag.tag_id,  # type: ignore[arg-type]  # tag_id is always set after persistence
        dimension_id=tag.dimension_id,
        tag_key=tag.tag_key,
        tag_value=tag.tag_value,
        created_by=tag.created_by,
        created_at=tag.created_at,
    )


@router.delete(
    "/tenants/{tenant_name}/tags/{tag_id}",
    status_code=204,
)
async def delete_tag(
    tenant_config: Annotated[TenantConfig, Depends(get_tenant_config)],
    uow: Annotated[UnitOfWork, Depends(get_unit_of_work)],
    tag_id: Annotated[int, Path(description="Tag ID to delete")],
) -> None:
    with uow:
        tag = uow.tags.get_tag(tag_id)
        if tag is None:
            raise HTTPException(status_code=404, detail=f"Tag {tag_id} not found")
        # Validate tenant ownership via dimension
        _validate_dimension_ownership(uow, tag.dimension_id, tenant_config)
        uow.tags.delete_tag(tag_id)
        uow.commit()
