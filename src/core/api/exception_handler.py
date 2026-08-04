from __future__ import annotations

import logging
import uuid
from typing import TYPE_CHECKING

from fastapi.responses import JSONResponse

from core.logging_context import safe_exception_context, safe_log_context

if TYPE_CHECKING:
    from fastapi import Request
logger = logging.getLogger(__name__)


async def global_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    error_id = str(uuid.uuid4())
    logger.error(
        "Unhandled exception method=%s path=%s%s",
        request.method,
        request.url.path,
        safe_log_context(
            request_id=getattr(request.state, "request_id", None),
            error_id=error_id,
            stage="api_request",
            outcome="failed",
            retryable=False,
            **safe_exception_context(exc),
        ),
    )
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal server error", "error_id": error_id},
    )
