from __future__ import annotations

import logging

from fastapi import APIRouter

from core.api import API_VERSION
from core.api.schemas import HealthResponse

logger = logging.getLogger(__name__)

router = APIRouter(tags=["health"])


@router.get("/health", response_model=HealthResponse)
async def health_check() -> HealthResponse:
    logger.debug("GET /health")
    return HealthResponse(status="ok", version=API_VERSION)
