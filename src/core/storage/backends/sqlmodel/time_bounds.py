from __future__ import annotations

import logging
from datetime import datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import String, literal

from core.storage.backends.sqlmodel.timestamps import canonical_utc_second

if TYPE_CHECKING:
    from sqlmodel import Session


logger = logging.getLogger(__name__)


def exact_utc_half_open_bounds(
    session: Session,
    start: datetime,
    end: datetime,
) -> tuple[Any, Any]:
    """Return indexed timestamp bounds compatible with the active SQL dialect."""
    normalized_start = canonical_utc_second(start, field="start")
    normalized_end = canonical_utc_second(end, field="end")
    if session.get_bind().dialect.name != "sqlite":
        return normalized_start, normalized_end

    def sqlite_value(value: datetime) -> str:
        return value.replace(tzinfo=None).isoformat(" ", timespec="seconds")

    return (
        literal(sqlite_value(normalized_start), type_=String()),
        literal(sqlite_value(normalized_end), type_=String()),
    )
