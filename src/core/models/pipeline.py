from __future__ import annotations

from dataclasses import dataclass
from datetime import date


@dataclass
class PipelineState:
    """Tracks per-date pipeline execution state for a tenant."""

    ecosystem: str
    tenant_id: str
    tracking_date: date
    billing_gathered: bool = False
    resources_gathered: bool = False
    chargeback_calculated: bool = False
