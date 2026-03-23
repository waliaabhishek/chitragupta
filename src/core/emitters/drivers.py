from __future__ import annotations

import logging
from datetime import date
from typing import TYPE_CHECKING

from core.emitters.models import EmitManifest, EmitOutcome

if TYPE_CHECKING:
    from core.emitters.protocols import LifecycleEmitter, RowProvider
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
        row_provider: RowProvider,
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
            except Exception:
                logger.exception(
                    "PerDateDriver: emitter %r failed for tenant=%s date=%s",
                    self._emitter,
                    tenant_id,
                    dt,
                )
                outcomes[dt] = EmitOutcome.FAILED
        return outcomes


class LifecycleDriver:
    """Drives LifecycleEmitter protocol — open → N×emit → close."""

    def __init__(self, emitter: LifecycleEmitter) -> None:
        self._emitter = emitter

    def run(
        self,
        tenant_id: str,
        manifest: EmitManifest,
        row_provider: RowProvider,
    ) -> dict[date, EmitOutcome]:
        try:
            self._emitter.open(tenant_id, manifest)
        except Exception:
            logger.exception("LifecycleDriver: open() failed for tenant=%s", tenant_id)
            return {d: EmitOutcome.FAILED for d in manifest.pending_dates}

        for dt in manifest.pending_dates:
            rows = row_provider(tenant_id, dt)
            if not rows:
                continue
            try:
                self._emitter.emit(tenant_id, dt, rows)
            except Exception:
                logger.exception(
                    "LifecycleDriver: emit() failed for tenant=%s date=%s",
                    tenant_id,
                    dt,
                )

        try:
            result = self._emitter.close(tenant_id)
            return result.outcomes
        except Exception:
            logger.exception("LifecycleDriver: close() failed for tenant=%s", tenant_id)
            return {d: EmitOutcome.FAILED for d in manifest.pending_dates}
