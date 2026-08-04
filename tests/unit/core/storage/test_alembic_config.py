from __future__ import annotations

import pytest
from alembic.config import Config
from sqlalchemy import engine_from_config, pool
from sqlalchemy.exc import ArgumentError

from core.storage.backends.sqlmodel.module import CoreStorageModule
from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend

ENCODED_POSTGRESQL_URL = (
    "postgresql+psycopg://user:p%40ss@localhost/testdb?options=-csearch_path%3Dtenant"  # pragma: allowlist secret
)


def _set_alembic_database_url(config: Config, database_url: str) -> None:
    from core.storage.migrations.config import set_alembic_database_url

    set_alembic_database_url(config, database_url)


def test_raw_alembic_config_rejects_percent_encoded_url_without_helper() -> None:
    config = Config()

    with pytest.raises(ValueError, match="invalid interpolation syntax"):
        config.set_main_option("sqlalchemy.url", ENCODED_POSTGRESQL_URL)


def test_set_alembic_database_url_preserves_encoded_url_for_alembic_and_sqlalchemy() -> None:
    config = Config()

    _set_alembic_database_url(config, ENCODED_POSTGRESQL_URL)

    section = config.get_section(config.config_ini_section, {})
    assert config.get_main_option("sqlalchemy.url") == ENCODED_POSTGRESQL_URL
    assert section["sqlalchemy.url"] == ENCODED_POSTGRESQL_URL

    engine = engine_from_config(section, prefix="sqlalchemy.", poolclass=pool.NullPool)
    try:
        assert engine.url.password == "p@ss"  # pragma: allowlist secret
        assert engine.url.query["options"] == "-csearch_path=tenant"
        assert engine.url.render_as_string(hide_password=False) == ENCODED_POSTGRESQL_URL
    finally:
        engine.dispose()


def test_set_alembic_database_url_handles_sqlite_without_semantic_change() -> None:
    config = Config()
    sqlite_url = "sqlite:///tmp/test-task-254-49.db"

    _set_alembic_database_url(config, sqlite_url)

    assert config.get_main_option("sqlalchemy.url") == sqlite_url
    assert config.get_section(config.config_ini_section, {})["sqlalchemy.url"] == sqlite_url


def test_invalid_database_url_still_fails_from_sqlalchemy() -> None:
    config = Config()

    _set_alembic_database_url(config, "not-a-sqlalchemy-url")

    with pytest.raises(ArgumentError, match="Could not parse SQLAlchemy URL from given URL string"):
        engine_from_config(
            config.get_section(config.config_ini_section, {}),
            prefix="sqlalchemy.",
            poolclass=pool.NullPool,
        )


def test_production_migration_bootstrap_uses_helper_for_encoded_url() -> None:
    backend = SQLModelBackend(
        ENCODED_POSTGRESQL_URL,
        CoreStorageModule(),
        use_migrations=True,
    )
    captured: list[tuple[Config, str]] = []

    def capture_upgrade(config: Config, revision: str) -> None:
        captured.append((config, revision))

    try:
        with pytest.MonkeyPatch.context() as monkeypatch:
            monkeypatch.setattr("alembic.command.upgrade", capture_upgrade)
            backend._run_migrations()
    finally:
        backend.dispose()

    assert len(captured) == 1
    config, revision = captured[0]
    assert revision == "head"
    assert config.get_main_option("sqlalchemy.url") == ENCODED_POSTGRESQL_URL
