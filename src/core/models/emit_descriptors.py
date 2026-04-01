from __future__ import annotations

import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class MetricDescriptor:
    """Declares how a row type maps to one Prometheus metric.

    A row type may carry multiple MetricDescriptors (one per metric family).
    value_field: attribute name on the row for the gauge value.
    label_fields: attribute names on the row for label values (order matters).
    """

    name: str
    value_field: str
    label_fields: tuple[str, ...]
    documentation: str = ""
    metric_type: str = "gauge"
