from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
from datetime import date
from typing import TYPE_CHECKING, Protocol, runtime_checkable

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from core.emitters.models import EmitManifest, EmitResult
    from core.models.chargeback import ChargebackRow

# Single canonical definition — imported by runner.py and drivers.py.
# Uses PEP 695 lazy type alias (Python 3.12+) to avoid NameError:
# ChargebackRow is TYPE_CHECKING-only, so an eager assignment would fail at runtime.
type RowProvider = Callable[[str, date], Sequence[ChargebackRow]]


@runtime_checkable
class LifecycleEmitter(Protocol):
    """Open/emit-per-date/close lifecycle. For CSV, bulk-API, batch sinks."""

    def open(self, tenant_id: str, manifest: EmitManifest) -> None: ...
    def emit(self, tenant_id: str, date: date, rows: Sequence[ChargebackRow]) -> None: ...
    def close(self, tenant_id: str) -> EmitResult: ...


@runtime_checkable
class ExpositionEmitter(Protocol):
    """Pull-based exposition sink (e.g. Prometheus scrape). Load + get_consumed."""

    def load(self, tenant_id: str, manifest: EmitManifest, rows: RowProvider) -> None: ...
    def get_consumed(self, tenant_id: str) -> set[date]: ...
