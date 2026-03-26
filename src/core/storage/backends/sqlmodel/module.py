from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from sqlmodel import SQLModel

if TYPE_CHECKING:
    from sqlalchemy import Engine
    from sqlmodel import Session

    from core.storage.interface import BillingRepository, ChargebackRepository, IdentityRepository, ResourceRepository

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

    def create_chargeback_repository(self, session: Session) -> ChargebackRepository:
        from core.storage.backends.sqlmodel.repositories import SQLModelChargebackRepository

        return SQLModelChargebackRepository(session)

    def register_tables(self, engine: Engine) -> None:
        """Ensure core tables are created (idempotent)."""
        from core.storage.backends.sqlmodel.base_tables import BillingTable, IdentityTable, ResourceTable
        from core.storage.backends.sqlmodel.tables import (
            ChargebackDimensionTable,
            ChargebackFactTable,
            EmissionRecordTable,
            EntityTagTable,
            PipelineRunTable,
            PipelineStateTable,
        )

        core_tables = [
            ResourceTable.__table__,  # type: ignore[attr-defined]  # SQLModel tables have __table__ at runtime via SQLAlchemy metaclass
            IdentityTable.__table__,  # type: ignore[attr-defined]  # SQLModel tables have __table__ at runtime via SQLAlchemy metaclass
            BillingTable.__table__,  # type: ignore[attr-defined]  # SQLModel tables have __table__ at runtime via SQLAlchemy metaclass
            ChargebackDimensionTable.__table__,  # type: ignore[attr-defined]  # SQLModel tables have __table__ at runtime via SQLAlchemy metaclass
            ChargebackFactTable.__table__,  # type: ignore[attr-defined]  # SQLModel tables have __table__ at runtime via SQLAlchemy metaclass
            PipelineStateTable.__table__,  # type: ignore[attr-defined]  # SQLModel tables have __table__ at runtime via SQLAlchemy metaclass
            PipelineRunTable.__table__,  # type: ignore[attr-defined]  # SQLModel tables have __table__ at runtime via SQLAlchemy metaclass
            EntityTagTable.__table__,  # type: ignore[attr-defined]  # SQLModel tables have __table__ at runtime via SQLAlchemy metaclass
            EmissionRecordTable.__table__,  # type: ignore[attr-defined]  # SQLModel tables have __table__ at runtime via SQLAlchemy metaclass
        ]
        SQLModel.metadata.create_all(engine, tables=core_tables)
