from __future__ import annotations

import logging
from datetime import date
from typing import TYPE_CHECKING, Protocol, runtime_checkable

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from core.emitters.models import EmissionRecord


@runtime_checkable
class EmissionRepository(Protocol):
    """Tracks per-(tenant, emitter, pipeline, date) emission state."""

    def upsert(self, record: EmissionRecord) -> None: ...
    def get_emitted_dates(self, ecosystem: str, tenant_id: str, emitter_name: str, pipeline: str) -> set[date]: ...
    def get_failed_dates(self, ecosystem: str, tenant_id: str, emitter_name: str, pipeline: str) -> set[date]: ...
