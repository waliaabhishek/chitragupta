from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import String, literal

if TYPE_CHECKING:
    from sqlmodel import Session


def exact_utc_half_open_bounds(
    session: Session,
    start: datetime,
    end: datetime,
) -> tuple[Any, Any]:
    """Return indexed timestamp bounds compatible with the active SQL dialect."""
    normalized_start = start.astimezone(UTC)
    normalized_end = end.astimezone(UTC)
    if session.get_bind().dialect.name != "sqlite":
        return normalized_start, normalized_end

    def sqlite_value(value: datetime) -> str:
        naive = value.replace(tzinfo=None)
        timespec = "seconds" if naive.microsecond == 0 else "microseconds"
        return naive.isoformat(" ", timespec=timespec)

    return (
        literal(sqlite_value(normalized_start), type_=String()),
        literal(sqlite_value(normalized_end), type_=String()),
    )
