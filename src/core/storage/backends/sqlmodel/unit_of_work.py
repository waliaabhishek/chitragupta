from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Self

from sqlmodel import Session, SQLModel

from core.storage.backends.sqlmodel.engine import get_or_create_engine
from core.storage.backends.sqlmodel.repositories import (
    SQLModelBillingRepository,
    SQLModelChargebackRepository,
    SQLModelIdentityRepository,
    SQLModelPipelineRunRepository,
    SQLModelPipelineStateRepository,
    SQLModelResourceRepository,
    SQLModelTagRepository,
)

if TYPE_CHECKING:
    from core.storage.interface import (
        BillingRepository,
        ChargebackRepository,
        IdentityRepository,
        PipelineRunRepository,
        PipelineStateRepository,
        ResourceRepository,
        TagRepository,
    )

logger = logging.getLogger(__name__)


class SQLModelUnitOfWork:
    """SQLModel implementation of UnitOfWork protocol."""

    def __init__(self, connection_string: str) -> None:
        self._engine = get_or_create_engine(connection_string)
        self._session: Session | None = None
        # Repo attributes initialized in __enter__
        self.resources: ResourceRepository
        self.identities: IdentityRepository
        self.billing: BillingRepository
        self.chargebacks: ChargebackRepository
        self.pipeline_state: PipelineStateRepository
        self.pipeline_runs: PipelineRunRepository
        self.tags: TagRepository

    def __enter__(self) -> Self:
        self._session = Session(self._engine)
        self._committed = False
        self.resources = SQLModelResourceRepository(self._session)
        self.identities = SQLModelIdentityRepository(self._session)
        self.billing = SQLModelBillingRepository(self._session)
        self.chargebacks = SQLModelChargebackRepository(self._session)
        self.pipeline_state = SQLModelPipelineStateRepository(self._session)
        self.pipeline_runs = SQLModelPipelineRunRepository(self._session)
        self.tags = SQLModelTagRepository(self._session)
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> None:
        if self._session is None:
            return
        try:
            if not self._committed:
                self._session.rollback()
        finally:
            self._session.close()
            self._session = None

    def commit(self) -> None:
        if self._session is None:
            raise RuntimeError("Cannot commit outside of a transaction")
        self._session.commit()
        self._committed = True

    def rollback(self) -> None:
        if self._session is None:
            raise RuntimeError("Cannot rollback outside of a transaction")
        self._session.rollback()


class SQLModelBackend:
    """SQLModel implementation of StorageBackend protocol."""

    def __init__(self, connection_string: str, *, use_migrations: bool = True) -> None:
        self._connection_string = connection_string
        self._use_migrations = use_migrations
        self._engine = get_or_create_engine(connection_string)

    def create_unit_of_work(self) -> SQLModelUnitOfWork:
        return SQLModelUnitOfWork(self._connection_string)

    def create_tables(self) -> None:
        if self._use_migrations:
            self._run_migrations()
        else:
            SQLModel.metadata.create_all(self._engine)

    def _run_migrations(self) -> None:
        import pathlib

        from alembic import command
        from alembic.config import Config

        # Locate alembic.ini relative to this package
        migrations_dir = pathlib.Path(__file__).resolve().parent.parent.parent / "migrations"
        alembic_ini = migrations_dir / "alembic.ini"

        cfg = Config(str(alembic_ini))
        cfg.set_main_option("script_location", str(migrations_dir))
        cfg.set_main_option("sqlalchemy.url", self._connection_string)
        command.upgrade(cfg, "head")

    def dispose(self) -> None:
        self._engine.dispose()
