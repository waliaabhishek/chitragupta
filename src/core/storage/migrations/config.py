from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING

from sqlalchemy.engine import make_url
from sqlalchemy.exc import ArgumentError

if TYPE_CHECKING:
    from alembic.config import Config

SQLALCHEMY_URL_X_ARGUMENT = "sqlalchemy.url"


def set_alembic_database_url(config: Config, database_url: str) -> None:
    """Store a database URL without changing the value Alembic reads."""
    config.set_main_option("sqlalchemy.url", database_url.replace("%", "%%"))


def apply_database_url_x_argument(config: Config, x_arguments: Mapping[str, str]) -> None:
    """Validate and apply a manual Alembic database URL override."""
    database_url = x_arguments.get(SQLALCHEMY_URL_X_ARGUMENT)
    if database_url is None:
        return
    if not database_url:
        raise ValueError("invalid sqlalchemy.url override; provide a non-empty SQLAlchemy database URL")
    try:
        parsed = make_url(database_url)
        _ = parsed.port
    except ArgumentError, ValueError:
        raise ValueError("invalid sqlalchemy.url override; provide a valid SQLAlchemy database URL") from None
    set_alembic_database_url(config, database_url)
