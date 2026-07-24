from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, cast

from sqlalchemy import DateTime
from sqlalchemy.dialects.sqlite import DATETIME
from sqlalchemy.types import TypeDecorator

from core.time_precision import canonical_utc_second as canonical_utc_second

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlalchemy.engine import Dialect
    from sqlalchemy.sql.type_api import TypeEngine

logger = logging.getLogger(__name__)

_SQLITE_SECOND_DATETIME = cast(
    "Callable[..., DateTime]",
    DATETIME,
)(truncate_microseconds=True)


def exclusive_utc_second_upper_bound(
    value: datetime,
    *,
    field: str = "before",
) -> datetime:
    """Preserve ``timestamp < value`` semantics for second-precision rows."""
    canonical = canonical_utc_second(value, field=field)
    if value.astimezone(UTC).microsecond:
        return canonical + timedelta(seconds=1)
    return canonical


class UTCSecondDateTime(TypeDecorator[datetime]):
    """Persist aware UTC datetimes with an explicit second-precision contract."""

    impl = DateTime(timezone=True)
    cache_ok = True

    def load_dialect_impl(self, dialect: Dialect) -> TypeEngine[datetime]:
        if dialect.name == "sqlite":
            return dialect.type_descriptor(_SQLITE_SECOND_DATETIME)
        return dialect.type_descriptor(DateTime(timezone=True))

    def process_bind_param(
        self,
        value: datetime | None,
        dialect: Dialect,
    ) -> datetime | None:
        del dialect
        return None if value is None else canonical_utc_second(value)

    def process_result_value(
        self,
        value: datetime | None,
        dialect: Dialect,
    ) -> datetime | None:
        del dialect
        if value is None:
            return None
        if value.tzinfo is None:
            value = value.replace(tzinfo=UTC)
        return canonical_utc_second(value)
