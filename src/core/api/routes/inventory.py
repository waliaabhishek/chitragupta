from __future__ import annotations

import logging
from typing import Annotated

from fastapi import APIRouter, Depends

from core.api.dependencies import get_tenant_config, get_unit_of_work
from core.api.schemas import InventorySummaryResponse
from core.config.models import TenantConfig  # noqa: TC001
from core.storage.interface import UnitOfWork  # noqa: TC001

logger = logging.getLogger(__name__)

router = APIRouter(tags=["inventory"])


@router.get(
    "/tenants/{tenant_name}/inventory/summary",
    response_model=InventorySummaryResponse,
)
async def get_inventory_summary(
    tenant_config: Annotated[TenantConfig, Depends(get_tenant_config)],
    uow: Annotated[UnitOfWork, Depends(get_unit_of_work)],
) -> InventorySummaryResponse:
    logger.debug("GET /inventory/summary tenant=%s", tenant_config.tenant_id)
    eco = tenant_config.ecosystem
    tid = tenant_config.tenant_id
    with uow:
        resource_counts = uow.resources.count_by_type(eco, tid)
        identity_counts = uow.identities.count_by_type(eco, tid)
    logger.info(
        "Inventory summary tenant=%s resource_types=%d identity_types=%d",
        tid,
        len(resource_counts),
        len(identity_counts),
    )
    return InventorySummaryResponse(resource_counts=resource_counts, identity_counts=identity_counts)
