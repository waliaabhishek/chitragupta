from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from alembic.config import Config


def set_alembic_database_url(config: Config, database_url: str) -> None:
    """Store a database URL without changing the value Alembic reads."""
    config.set_main_option("sqlalchemy.url", database_url.replace("%", "%%"))
