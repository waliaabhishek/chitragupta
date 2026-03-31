"""CCloud-specific MetricQuery definitions for topic attribution discovery."""

from __future__ import annotations

import logging
from dataclasses import replace
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from core.models.metrics import MetricQuery

logger = logging.getLogger(__name__)

# Attribution key → discovery key mapping (disc_ prefix distinguishes discovery
# queries from attribution queries in logs).
_DISC_KEY_MAP: dict[str, str] = {
    "topic_bytes_in": "disc_bytes_in",
    "topic_bytes_out": "disc_bytes_out",
    "topic_retained_bytes": "disc_retained",
}


def build_discovery_queries(overrides: dict[str, str]) -> list[MetricQuery]:
    """Build discovery MetricQuery list using the same metric name resolution as attribution.

    Delegates to build_metric_queries (single source of truth for _DEFAULT_METRIC_NAMES
    and _QUERY_TEMPLATES), then renames keys with disc_ prefix to distinguish discovery
    queries from attribution queries.
    """
    from core.engine.topic_attribution import build_metric_queries

    attribution_queries = build_metric_queries(overrides)
    return [replace(q, key=_DISC_KEY_MAP[q.key]) for q in attribution_queries]
