from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from sqlmodel import SQLModel

if TYPE_CHECKING:
    from sqlalchemy import Engine
    from sqlmodel import Session

    from core.storage.interface import BillingRepository, IdentityRepository, ResourceRepository

logger = logging.getLogger(__name__)


class CoreStorageModule:
    """StorageModule using core SQLModel repositories and tables.

    Used by generic ecosystems that don't need plugin-specific billing columns.
    """

    def create_billing_repository(self, session: Session) -> BillingRepository:
        from core.storage.backends.sqlmodel.repositories import SQLModelBillingRepository

        return SQLModelBillingRepository(session)

    def create_resource_repository(self, session: Session) -> ResourceRepository:
        from core.storage.backends.sqlmodel.repositories import SQLModelResourceRepository

        return SQLModelResourceRepository(session)

    def create_identity_repository(self, session: Session) -> IdentityRepository:
        from core.storage.backends.sqlmodel.repositories import SQLModelIdentityRepository

        return SQLModelIdentityRepository(session)

    def register_tables(self, engine: Engine) -> None:
        """Ensure core tables are created (idempotent)."""
        from core.storage.backends.sqlmodel.base_tables import BillingTable, IdentityTable, ResourceTable
        from core.storage.backends.sqlmodel.tables import (
            ChargebackDimensionTable,
            ChargebackFactTable,
            CustomTagTable,
            PipelineRunTable,
            PipelineStateTable,
        )

        core_tables = [
            ResourceTable.__table__,
            IdentityTable.__table__,
            BillingTable.__table__,
            ChargebackDimensionTable.__table__,
            ChargebackFactTable.__table__,
            PipelineStateTable.__table__,
            PipelineRunTable.__table__,
            CustomTagTable.__table__,
        ]
        SQLModel.metadata.create_all(engine, tables=core_tables)
