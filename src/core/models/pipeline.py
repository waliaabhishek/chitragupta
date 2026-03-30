from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Literal

PIPELINE_STAGES = ("gathering", "calculating", "topic_overlay", "emitting")

logger = logging.getLogger(__name__)


@dataclass
class PipelineState:
    """Tracks per-date pipeline execution state for a tenant."""

    ecosystem: str
    tenant_id: str
    tracking_date: date
    billing_gathered: bool = False
    resources_gathered: bool = False
    chargeback_calculated: bool = False
    topic_overlay_gathered: bool = False
    topic_attribution_calculated: bool = False


@dataclass
class PipelineRun:
    """Tracks a single pipeline execution for a tenant."""

    tenant_name: str
    started_at: datetime
    status: Literal["running", "completed", "failed", "skipped"]
    id: int | None = field(default=None)
    ended_at: datetime | None = None
    stage: str | None = None
    current_date: date | None = None
    dates_gathered: int = 0
    dates_calculated: int = 0
    rows_written: int = 0
    error_message: str | None = None
