from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import date
from enum import Enum

logger = logging.getLogger(__name__)


class EmitOutcome(Enum):
    EMITTED = "emitted"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class EmitManifest:
    pending_dates: Sequence[date]
    total_rows_estimate: int | None
    is_reemission: bool


@dataclass
class EmitResult:
    outcomes: dict[date, EmitOutcome] = field(default_factory=dict)


@dataclass
class EmissionRecord:
    """Domain model for emission state. No ORM dependency."""

    ecosystem: str
    tenant_id: str
    emitter_name: str
    pipeline: str
    date: date
    status: str  # "emitted" | "failed"
    attempt_count: int = 1
