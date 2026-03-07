from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Self

from sqlmodel import Session

from core.storage.backends.sqlmodel.engine import get_or_create_engine
from core.storage.backends.sqlmodel.repositories import (
    SQLModelChargebackRepository,
    SQLModelPipelineRunRepository,
    SQLModelPipelineStateRepository,
    SQLModelTagRepository,
)

if TYPE_CHECKING:
    from core.plugin.protocols import StorageModule
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

    def __init__(self, connection_string: str, storage_module: StorageModule) -> None:
        self._engine = get_or_create_engine(connection_string)
        self._storage_module = storage_module
        self._session: Session | None = None
        # Initialized to None; overridden in __enter__ with real repo instances.
        # Must be assigned (not just annotated) so isinstance(self, UnitOfWork) works
        # outside a context block (UnitOfWork is @runtime_checkable Protocol).
        self.resources: ResourceRepository = None  # type: ignore[assignment]
        self.identities: IdentityRepository = None  # type: ignore[assignment]
        self.billing: BillingRepository = None  # type: ignore[assignment]
        self.chargebacks: ChargebackRepository = None  # type: ignore[assignment]
        self.pipeline_state: PipelineStateRepository = None  # type: ignore[assignment]
        self.pipeline_runs: PipelineRunRepository = None  # type: ignore[assignment]
        self.tags: TagRepository = None  # type: ignore[assignment]

    def __enter__(self) -> Self:
        self._session = Session(self._engine)
        self._committed = False
        self.resources = self._storage_module.create_resource_repository(self._session)
        self.identities = self._storage_module.create_identity_repository(self._session)
        self.billing = self._storage_module.create_billing_repository(self._session)
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

    def __init__(
        self,
        connection_string: str,
        storage_module: StorageModule,
        *,
        use_migrations: bool = True,
    ) -> None:
        self._connection_string = connection_string
        self._storage_module = storage_module
        self._use_migrations = use_migrations
        self._engine = get_or_create_engine(connection_string)

    def create_unit_of_work(self) -> SQLModelUnitOfWork:
        return SQLModelUnitOfWork(self._connection_string, self._storage_module)

    def create_tables(self) -> None:
        if self._use_migrations:
            self._run_migrations()
        else:
            from core.storage.backends.sqlmodel.module import CoreStorageModule

            # Always create core orchestration tables (chargeback, pipeline, etc.)
            # before the plugin registers its own tables.
            CoreStorageModule().register_tables(self._engine)
            self._storage_module.register_tables(self._engine)

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
