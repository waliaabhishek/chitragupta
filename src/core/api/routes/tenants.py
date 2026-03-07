from __future__ import annotations

import logging
from datetime import date
from typing import Annotated

from fastapi import APIRouter, Depends, Query, Request

from core.api.dependencies import get_or_create_backend, get_settings, get_tenant_config, get_unit_of_work
from core.api.schemas import (
    PipelineStateResponse,
    TenantListResponse,
    TenantStatusDetailResponse,
    TenantStatusSummary,
)
from core.config.models import AppSettings, TenantConfig  # noqa: TC001  # FastAPI evaluates annotations at runtime
from core.storage.interface import UnitOfWork  # noqa: TC001

logger = logging.getLogger(__name__)

router = APIRouter(tags=["tenants"])


@router.get("/tenants", response_model=TenantListResponse)
async def list_tenants(
    request: Request,
    settings: Annotated[AppSettings, Depends(get_settings)],
) -> TenantListResponse:
    logger.debug("GET /tenants")
    summaries: list[TenantStatusSummary] = []
    for tenant_name, tenant_config in settings.tenants.items():
        backend = get_or_create_backend(request.app.state.backends, tenant_name, tenant_config.storage, tenant_config.ecosystem)
        uow = backend.create_unit_of_work()
        with uow:
            pending = uow.pipeline_state.count_pending(tenant_config.ecosystem, tenant_config.tenant_id)
            calculated = uow.pipeline_state.count_calculated(tenant_config.ecosystem, tenant_config.tenant_id)
            last_date = uow.pipeline_state.get_last_calculated_date(tenant_config.ecosystem, tenant_config.tenant_id)
        summaries.append(
            TenantStatusSummary(
                tenant_name=tenant_name,
                tenant_id=tenant_config.tenant_id,
                ecosystem=tenant_config.ecosystem,
                dates_pending=pending,
                dates_calculated=calculated,
                last_calculated_date=last_date,
            )
        )
    logger.info("Listed tenants count=%d", len(summaries))
    return TenantListResponse(tenants=summaries)


@router.get("/tenants/{tenant_name}/status", response_model=TenantStatusDetailResponse)
async def get_tenant_status(
    tenant_name: str,
    tenant_config: Annotated[TenantConfig, Depends(get_tenant_config)],
    uow: Annotated[UnitOfWork, Depends(get_unit_of_work)],
    start_date: Annotated[date | None, Query()] = None,
    end_date: Annotated[date | None, Query()] = None,
) -> TenantStatusDetailResponse:
    with uow:
        if start_date and end_date:
            states = uow.pipeline_state.find_by_range(
                tenant_config.ecosystem, tenant_config.tenant_id, start_date, end_date
            )
        elif start_date:
            # From start to far future
            states = uow.pipeline_state.find_by_range(
                tenant_config.ecosystem, tenant_config.tenant_id, start_date, date(9999, 12, 31)
            )
        elif end_date:
            # From far past to end
            states = uow.pipeline_state.find_by_range(
                tenant_config.ecosystem, tenant_config.tenant_id, date(2000, 1, 1), end_date
            )
        else:
            # All states — use a very wide range
            states = uow.pipeline_state.find_by_range(
                tenant_config.ecosystem, tenant_config.tenant_id, date(2000, 1, 1), date(9999, 12, 31)
            )

    return TenantStatusDetailResponse(
        tenant_name=tenant_name,
        tenant_id=tenant_config.tenant_id,
        ecosystem=tenant_config.ecosystem,
        states=[
            PipelineStateResponse(
                tracking_date=s.tracking_date,
                billing_gathered=s.billing_gathered,
                resources_gathered=s.resources_gathered,
                chargeback_calculated=s.chargeback_calculated,
            )
            for s in states
        ],
    )
