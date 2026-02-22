from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass(frozen=True)
class MetricQuery:
    """An immutable metric query definition."""

    key: str
    query_expression: str
    label_keys: tuple[str, ...]
    resource_label: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class MetricRow:
    """An immutable metric data point."""

    timestamp: datetime
    metric_key: str
    value: float
    labels: dict[str, str] = field(default_factory=dict)
