from __future__ import annotations

import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class TypeStatusCounts:
    """Breakdown of total, active, and deleted counts for a single type."""

    total: int
    active: int
    deleted: int
