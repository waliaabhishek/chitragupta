from __future__ import annotations

import logging
from datetime import UTC, datetime

logger = logging.getLogger(__name__)


def canonical_utc_second(
    value: datetime,
    *,
    field: str = "timestamp",
) -> datetime:
    """Normalize an aware instant to UTC and persisted second precision."""
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must be timezone-aware")
    return value.astimezone(UTC).replace(microsecond=0)
