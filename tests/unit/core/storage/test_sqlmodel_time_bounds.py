from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any

from sqlalchemy.dialects import postgresql, sqlite
from sqlmodel import col, select

from core.storage.backends.sqlmodel.time_bounds import exact_utc_half_open_bounds
from plugins.confluent_cloud.storage.tables import CCloudBillingTable

START = datetime(2026, 7, 1, tzinfo=UTC)
END = START + timedelta(days=1)


class _DialectSession:
    def __init__(self, dialect: object) -> None:
        self._bind = SimpleNamespace(dialect=dialect)

    def get_bind(self) -> object:
        return self._bind


def _statement(session: Any):
    start, end = exact_utc_half_open_bounds(session, START, END)
    return (
        select(CCloudBillingTable).where(
            col(CCloudBillingTable.timestamp) >= start,
            col(CCloudBillingTable.timestamp) < end,
        ),
        start,
        end,
    )


def test_sqlite_bounds_are_second_precision_half_open_and_leave_timestamp_indexable() -> None:
    statement, _start, _end = _statement(_DialectSession(sqlite.dialect()))
    compiled = statement.compile(dialect=sqlite.dialect())
    sql = str(compiled).lower()

    assert "date(" not in sql
    assert "timestamp >= ?" in sql
    assert "timestamp < ?" in sql
    assert "2026-07-01 00:00:00" in compiled.params.values()
    assert "2026-07-02 00:00:00" in compiled.params.values()


def test_postgresql_bounds_remain_aware_utc_half_open_without_session_date_extraction() -> None:
    statement, start, end = _statement(_DialectSession(postgresql.dialect()))
    compiled = statement.compile(dialect=postgresql.dialect())
    sql = str(compiled).lower()

    assert start == START and start.tzinfo is UTC
    assert end == END and end.tzinfo is UTC
    assert "date(" not in sql
    assert "timestamp >=" in sql
    assert "timestamp <" in sql
