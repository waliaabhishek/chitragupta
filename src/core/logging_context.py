from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from types import TracebackType
from urllib.parse import quote
from uuid import UUID

SAFE_LOG_CONTEXT_KEYS = (
    "tenant_name",
    "tenant_id",
    "ecosystem",
    "request_id",
    "error_id",
    "pipeline_run_id",
    "calculation_id",
    "revision_id",
    "repair_id",
    "tracking_date",
    "month",
    "stage",
    "operation",
    "outcome",
    "retryable",
    "attempt_number",
    "max_attempts",
    "error_type",
    "root_error_type",
    "root_error_code",
    "traceback_frames",
    "diagnostic_code",
    "resource_id",
    "product_type",
    "service_type",
    "emitter_name",
    "pipeline",
)

_SCALAR_TYPES = (str, int, float, Decimal, UUID)
_SAFE_URL_CHARACTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789._-:/"
_MAX_VALUE_LENGTH = 160
_MAX_TRACEBACK_FRAMES = 8


def _normalize_value(value: object) -> str:
    try:
        if isinstance(value, bool):
            normalized = "true" if value else "false"
        elif isinstance(value, Enum):
            normalized = _normalize_value(value.value)
        elif isinstance(value, datetime | date):
            normalized = value.isoformat()
        elif isinstance(value, _SCALAR_TYPES):
            normalized = str(value)
        else:
            normalized = f"<{type(value).__name__}>"
    except BaseException:
        normalized = f"<{type(value).__name__}>"
    if len(normalized) > _MAX_VALUE_LENGTH:
        normalized = f"{normalized[: _MAX_VALUE_LENGTH - 3]}..."
    return normalized


def safe_log_context(**fields: object) -> str:
    """Render bounded, allow-listed correlation context without raising."""
    try:
        parts = [
            f"{key}={quote(_normalize_value(fields[key]), safe=_SAFE_URL_CHARACTERS)}"
            for key in SAFE_LOG_CONTEXT_KEYS
            if key in fields and fields[key] is not None
        ]
        return f" {' '.join(parts)}" if parts else ""
    except BaseException:
        return ""


def _deepest_exception(exc: BaseException) -> BaseException:
    current = exc
    seen: set[int] = set()
    while id(current) not in seen:
        seen.add(id(current))
        next_error = current.__cause__ or current.__context__
        if next_error is None:
            break
        current = next_error
    return current


def _safe_error_code(exc: BaseException) -> object | None:
    for attribute in ("code", "sqlstate", "status_code"):
        try:
            value = getattr(exc, attribute, None)
        except BaseException:
            continue
        if value is None or isinstance(value, bool):
            continue
        if isinstance(value, (*_SCALAR_TYPES, Enum)):
            return value
    return None


def _traceback_frames(traceback: TracebackType | None) -> str:
    frames: list[str] = []
    while traceback is not None:
        frame = traceback.tb_frame
        module = frame.f_globals.get("__name__", "<unknown>")
        frames.append(f"{module}:{frame.f_code.co_name}:{traceback.tb_lineno}")
        traceback = traceback.tb_next
    return ">".join(frames[-_MAX_TRACEBACK_FRAMES:])


def safe_exception_context(
    exc: BaseException,
    *,
    root_error_code: str | None = None,
) -> dict[str, object]:
    """Return exception type, bounded frame lineage, and a safe scalar code."""
    try:
        root = _deepest_exception(exc)
        context: dict[str, object] = {"error_type": type(exc).__name__}
        if root is not exc:
            context["root_error_type"] = type(root).__name__
        code: object | None = root_error_code
        if code is None:
            code = _safe_error_code(root)
        if code is not None:
            context["root_error_code"] = code
        frames = _traceback_frames(exc.__traceback__)
        if frames:
            context["traceback_frames"] = frames
        return context
    except BaseException:
        return {"error_type": type(exc).__name__}
