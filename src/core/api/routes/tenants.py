from __future__ import annotations

import logging
from datetime import date
from typing import TYPE_CHECKING, Annotated

from fastapi import APIRouter, Depends, Query, Request

from core.api.dependencies import get_or_create_backend, get_settings, get_tenant_config, get_unit_of_work
from core.api.schemas import (
    PipelineStateResponse,
    TenantListResponse,
    TenantStatusDetailResponse,
    TenantStatusSummary,
)
from core.config.models import AppSettings, TenantConfig  # noqa: TC001  # FastAPI evaluates annotations at runtime
from core.storage.interface import ReadOnlyUnitOfWork  # noqa: TC001

if TYPE_CHECKING:
    from core.config.models import PluginSettingsBase

logger = logging.getLogger(__name__)

router = APIRouter(tags=["tenants"])


def _is_topic_attribution_enabled(plugin_settings: PluginSettingsBase) -> bool:
    """Return True if topic attribution is enabled in the tenant's plugin config."""
    ta = getattr(plugin_settings, "topic_attribution", None)
    return bool(ta and getattr(ta, "enabled", False))


@router.get("/tenants", response_model=TenantListResponse)
async def list_tenants(
    request: Request,
    settings: Annotated[AppSettings, Depends(get_settings)],
) -> TenantListResponse:
    logger.debug("GET /tenants")
    summaries: list[TenantStatusSummary] = []
    for tenant_name, tenant_config in settings.tenants.items():
        backend = get_or_create_backend(
            request.app.state.backends, tenant_name, tenant_config.storage, tenant_config.ecosystem
        )
        with backend.create_read_only_unit_of_work() as uow:
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
                topic_attribution_enabled=_is_topic_attribution_enabled(tenant_config.plugin_settings),
            )
        )
    logger.info("Listed tenants count=%d", len(summaries))
    return TenantListResponse(tenants=summaries)


@router.get("/tenants/{tenant_name}/status", response_model=TenantStatusDetailResponse)
async def get_tenant_status(
    tenant_name: str,
    tenant_config: Annotated[TenantConfig, Depends(get_tenant_config)],
    uow: Annotated[ReadOnlyUnitOfWork, Depends(get_unit_of_work)],
    start_date: Annotated[date | None, Query()] = None,
    end_date: Annotated[date | None, Query()] = None,
) -> TenantStatusDetailResponse:
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
        topic_attribution_enabled=_is_topic_attribution_enabled(tenant_config.plugin_settings),
        states=[
            PipelineStateResponse(
                tracking_date=s.tracking_date,
                billing_gathered=s.billing_gathered,
                resources_gathered=s.resources_gathered,
                chargeback_calculated=s.chargeback_calculated,
                topic_overlay_gathered=s.topic_overlay_gathered,
                topic_attribution_calculated=s.topic_attribution_calculated,
            )
            for s in states
        ],
    )
