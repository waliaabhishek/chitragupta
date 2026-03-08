from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class MetricQuery:
    """An immutable metric query definition."""

    key: str
    query_expression: str
    label_keys: tuple[str, ...]
    resource_label: str | None
    query_mode: Literal["instant", "range"] = "range"
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class MetricRow:
    """An immutable metric data point."""

    timestamp: datetime
    metric_key: str
    value: float
    labels: dict[str, str] = field(default_factory=dict)
