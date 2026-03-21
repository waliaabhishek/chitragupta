from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from sqlmodel import SQLModel

from plugins.confluent_cloud.storage import tables as _tables  # noqa: F401 — registers CCloudBillingTable
from plugins.confluent_cloud.storage.repositories import CCloudBillingRepository, CCloudChargebackRepository

if TYPE_CHECKING:
    from sqlalchemy import Engine
    from sqlmodel import Session

    from core.storage.interface import BillingRepository, ChargebackRepository, IdentityRepository, ResourceRepository

logger = logging.getLogger(__name__)


class CCloudStorageModule:
    """StorageModule for Confluent Cloud.

    Provides a plugin-specific CCloudBillingRepository with env_id in the PK,
    and delegates resource/identity repos to the core SQLModel implementations.
    """

    def create_billing_repository(self, session: Session) -> BillingRepository:
        return CCloudBillingRepository(session)  # type: ignore[return-value]

    def create_resource_repository(self, session: Session) -> ResourceRepository:
        from core.storage.backends.sqlmodel.repositories import SQLModelResourceRepository

        return SQLModelResourceRepository(session)

    def create_identity_repository(self, session: Session) -> IdentityRepository:
        from core.storage.backends.sqlmodel.repositories import SQLModelIdentityRepository

        return SQLModelIdentityRepository(session)

    def create_chargeback_repository(self, session: Session) -> ChargebackRepository:
        return CCloudChargebackRepository(session)

    def register_tables(self, engine: Engine) -> None:
        """Ensure CCloud plugin tables are created (idempotent)."""
        from core.storage.backends.sqlmodel.base_tables import IdentityTable, ResourceTable
        from plugins.confluent_cloud.storage.tables import CCloudBillingTable

        ccloud_tables = [
            ResourceTable.__table__,  # type: ignore[attr-defined]  # SQLModel tables have __table__ at runtime via SQLAlchemy metaclass
            IdentityTable.__table__,  # type: ignore[attr-defined]  # SQLModel tables have __table__ at runtime via SQLAlchemy metaclass
            CCloudBillingTable.__table__,  # type: ignore[attr-defined]  # SQLModel tables have __table__ at runtime via SQLAlchemy metaclass
        ]
        SQLModel.metadata.create_all(engine, tables=ccloud_tables)
