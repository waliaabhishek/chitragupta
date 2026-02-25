from __future__ import annotations

from fastapi import APIRouter

from core.api import API_VERSION
from core.api.schemas import HealthResponse

router = APIRouter(tags=["health"])


@router.get("/health", response_model=HealthResponse)
async def health_check() -> HealthResponse:
    return HealthResponse(status="ok", version=API_VERSION)
