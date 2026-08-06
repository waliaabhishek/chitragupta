from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from enum import Enum
from uuid import UUID

import pytest

EXPECTED_SAFE_KEYS = (
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


class _Status(Enum):
    READY = "ready"


@dataclass
class _ExplodingStringer:
    def __str__(self) -> str:
        raise RuntimeError("secret-rendering-path")


class _SqlStateError(RuntimeError):
    def __init__(self, message: str, *, sqlstate: str) -> None:
        super().__init__(message)
        self.sqlstate = sqlstate


def _secret_bearing_exception() -> BaseException:
    try:
        raise _SqlStateError(
            "duplicate key value violates unique constraint; token=super-secret",
            sqlstate="23505",
        )
    except _SqlStateError as root:
        raise ValueError("database payload leaked: select * from billing") from root


def test_safe_log_context_exports_exact_canonical_key_order() -> None:
    from core.logging_context import SAFE_LOG_CONTEXT_KEYS

    assert tuple(SAFE_LOG_CONTEXT_KEYS) == EXPECTED_SAFE_KEYS


def test_safe_log_context_formats_order_escaping_truncation_and_supported_scalars() -> None:
    from core.logging_context import safe_log_context

    rendered = safe_log_context(
        tenant_id="tenant 1",
        tracking_date=date(2026, 8, 4),
        retryable=False,
        attempt_number=2,
        max_attempts=5,
        outcome=_Status.READY,
        calculation_id=UUID("12345678-1234-5678-1234-567812345678"),
        product_type=Decimal("10.50"),
        emitter_name="line1\nline2",
        stage="x" * 161,
        ignored_key="must-drop",
    )

    assert rendered == (
        " tenant_id=tenant%201"
        " calculation_id=12345678-1234-5678-1234-567812345678"
        " tracking_date=2026-08-04"
        " stage=" + ("x" * 157) + "..."
        " outcome=ready"
        " retryable=false"
        " attempt_number=2"
        " max_attempts=5"
        " product_type=10.50"
        " emitter_name=line1%0Aline2"
    )


def test_safe_log_context_never_raises_and_uses_class_name_for_unsupported_objects() -> None:
    from core.logging_context import safe_log_context

    rendered = safe_log_context(resource_id=_ExplodingStringer(), request_id="")

    assert rendered == " request_id= resource_id=%3C_ExplodingStringer%3E"


def test_safe_exception_context_omits_secret_messages_and_keeps_sanitized_metadata() -> None:
    from core.logging_context import safe_exception_context

    with pytest.raises(ValueError) as exc_info:
        _secret_bearing_exception()

    context = safe_exception_context(exc_info.value)

    assert context["error_type"] == "ValueError"
    assert context["root_error_type"] == "_SqlStateError"
    assert context["root_error_code"] == "23505"
    assert "duplicate key value" not in str(context)
    assert "token=super-secret" not in str(context)
    assert "select * from billing" not in str(context)
    traceback_frames = str(context["traceback_frames"])
    assert "test_logging_context" in traceback_frames
    assert ">" in traceback_frames


def test_safe_exception_context_prefers_explicit_root_error_code_override() -> None:
    from core.logging_context import safe_exception_context

    try:
        raise RuntimeError("provider body contained secret token")
    except RuntimeError as exc:
        context = safe_exception_context(exc, root_error_code="RATE_LIMITED")

    assert context["error_type"] == "RuntimeError"
    assert context["root_error_code"] == "RATE_LIMITED"
    assert "secret token" not in str(context)
