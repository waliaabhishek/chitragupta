from __future__ import annotations

import logging
from datetime import date
from typing import TYPE_CHECKING, Any

from core.emitters.models import EmitManifest, EmitOutcome
from core.emitters.protocols import RowProvider  # module-level import — safe with PEP 695  # noqa: TC001
from core.logging_context import safe_exception_context, safe_log_context

if TYPE_CHECKING:
    from core.emitters.protocols import LifecycleEmitter
    from core.plugin.protocols import Emitter

logger = logging.getLogger(__name__)


class PerDateDriver:
    """Drives plain Emitter protocol — calls __call__ per date, tracks per-date outcomes."""

    def __init__(self, emitter: Emitter) -> None:
        self._emitter = emitter

    def run(
        self,
        tenant_id: str,
        manifest: EmitManifest,
        row_provider: RowProvider[Any],
    ) -> dict[date, EmitOutcome]:
        outcomes: dict[date, EmitOutcome] = {}
        for dt in manifest.pending_dates:
            rows = row_provider(tenant_id, dt)
            if not rows:
                outcomes[dt] = EmitOutcome.SKIPPED
                continue
            try:
                self._emitter(tenant_id, dt, rows)
                outcomes[dt] = EmitOutcome.EMITTED
            except Exception as exc:
                logger.error(
                    "emitter_date_failed%s",
                    safe_log_context(
                        tenant_id=tenant_id,
                        tracking_date=dt,
                        stage="emit",
                        operation="per_date_emit",
                        outcome="failed",
                        retryable=True,
                        emitter_name=type(self._emitter).__name__,
                        **safe_exception_context(exc),
                    ),
                )
                outcomes[dt] = EmitOutcome.FAILED
        return outcomes


class LifecycleDriver:
    """Drives LifecycleEmitter protocol — open → N×emit → close."""

    def __init__(self, emitter: LifecycleEmitter[Any]) -> None:
        self._emitter = emitter

    def run(
        self,
        tenant_id: str,
        manifest: EmitManifest,
        row_provider: RowProvider[Any],
    ) -> dict[date, EmitOutcome]:
        try:
            self._emitter.open(tenant_id, manifest)
        except Exception as exc:
            logger.error(
                "emitter_open_failed%s",
                safe_log_context(
                    tenant_id=tenant_id,
                    stage="emit_open",
                    outcome="failed",
                    retryable=True,
                    emitter_name=type(self._emitter).__name__,
                    **safe_exception_context(exc),
                ),
            )
            return {d: EmitOutcome.FAILED for d in manifest.pending_dates}

        for dt in manifest.pending_dates:
            rows = row_provider(tenant_id, dt)
            if not rows:
                continue
            try:
                self._emitter.emit(tenant_id, dt, rows)
            except Exception as exc:
                logger.error(
                    "emitter_date_failed%s",
                    safe_log_context(
                        tenant_id=tenant_id,
                        tracking_date=dt,
                        stage="emit",
                        outcome="failed",
                        retryable=True,
                        emitter_name=type(self._emitter).__name__,
                        **safe_exception_context(exc),
                    ),
                )

        try:
            result = self._emitter.close(tenant_id)
            return result.outcomes
        except Exception as exc:
            logger.error(
                "emitter_close_failed%s",
                safe_log_context(
                    tenant_id=tenant_id,
                    stage="emit_close",
                    outcome="failed",
                    retryable=True,
                    emitter_name=type(self._emitter).__name__,
                    **safe_exception_context(exc),
                ),
            )
            return {d: EmitOutcome.FAILED for d in manifest.pending_dates}
