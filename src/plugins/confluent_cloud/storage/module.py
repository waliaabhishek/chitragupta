from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from sqlmodel import SQLModel

from plugins.confluent_cloud.storage import tables as _tables  # noqa: F401 — registers CCloudBillingTable
from plugins.confluent_cloud.storage.repositories import CCloudBillingRepository, CCloudChargebackRepository

if TYPE_CHECKING:
    from sqlalchemy import Connection, Engine
    from sqlmodel import Session

    from core.preview.persistence import (
        PreviewEvidenceBootstrap,
        PreviewEvidenceStorageBackend,
        PreviewEvidenceWriteUnitOfWork,
        PreviewGenerationReadUnitOfWork,
        PreviewSourceAttemptFallbackWriter,
    )
    from core.preview.storage_availability import PreviewEvidenceAvailability
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
        """Ensure ordinary CCloud plugin tables are created (idempotent)."""
        from core.storage.backends.sqlmodel.base_tables import IdentityTable, ResourceTable
        from plugins.confluent_cloud.storage.tables import CCloudBillingTable

        ccloud_tables = [
            ResourceTable.__table__,  # type: ignore[attr-defined]  # SQLModel tables have __table__ at runtime via SQLAlchemy metaclass
            IdentityTable.__table__,  # type: ignore[attr-defined]  # SQLModel tables have __table__ at runtime via SQLAlchemy metaclass
            CCloudBillingTable.__table__,  # type: ignore[attr-defined]  # SQLModel tables have __table__ at runtime via SQLAlchemy metaclass
        ]
        SQLModel.metadata.create_all(engine, tables=ccloud_tables)

    def register_preview_evidence_tables(self, engine: Engine) -> None:
        from plugins.confluent_cloud.storage.preview_tables import (
            CCloudAllocationLineagePortionTable,
            CCloudAllocationLineageRunTable,
            CCloudCostSourceRecordTable,
            CCloudFocusPreviewRepairDateTable,
            CCloudFocusPreviewRepairHeadTable,
            CCloudFocusPreviewRepairTable,
            CCloudOrganizationAuthorityAttemptTable,
            CCloudSourceCaptureReadinessHistoryTable,
            CCloudSourceCaptureReadinessTable,
            CCloudSourceEvidenceAttemptTable,
        )

        tables = [
            CCloudCostSourceRecordTable.__table__,  # type: ignore[attr-defined]
            CCloudSourceEvidenceAttemptTable.__table__,  # type: ignore[attr-defined]
            CCloudSourceCaptureReadinessTable.__table__,  # type: ignore[attr-defined]
            CCloudSourceCaptureReadinessHistoryTable.__table__,  # type: ignore[attr-defined]
            CCloudFocusPreviewRepairTable.__table__,  # type: ignore[attr-defined]
            CCloudFocusPreviewRepairDateTable.__table__,  # type: ignore[attr-defined]
            CCloudFocusPreviewRepairHeadTable.__table__,  # type: ignore[attr-defined]
            CCloudOrganizationAuthorityAttemptTable.__table__,  # type: ignore[attr-defined]
            CCloudAllocationLineageRunTable.__table__,  # type: ignore[attr-defined]
            CCloudAllocationLineagePortionTable.__table__,  # type: ignore[attr-defined]
        ]
        SQLModel.metadata.create_all(engine, tables=tables)

    def prepare_preview_evidence_migration(
        self,
        connection: Connection,
        *,
        target_revision: str,
    ) -> None:
        from plugins.confluent_cloud.storage.preview_schema import CCloudPreviewSchemaManager

        CCloudPreviewSchemaManager().prepare(connection, target_revision=target_revision)

    def downgrade_preview_evidence_migration(
        self,
        connection: Connection,
        *,
        target_revision: str,
    ) -> None:
        from plugins.confluent_cloud.storage.preview_schema import CCloudPreviewSchemaManager

        CCloudPreviewSchemaManager().downgrade(connection, target_revision=target_revision)

    def create_preview_evidence_unit_of_work(
        self,
        connection_string: str,
        availability: PreviewEvidenceAvailability,
    ) -> PreviewEvidenceWriteUnitOfWork:
        from plugins.confluent_cloud.storage.preview_unit_of_work import (
            CCloudPreviewEvidenceSQLModelUnitOfWork,
        )

        return CCloudPreviewEvidenceSQLModelUnitOfWork(connection_string, availability)

    def create_preview_source_attempt_fallback_repository(self, session: Session) -> PreviewSourceAttemptFallbackWriter:
        from plugins.confluent_cloud.storage.preview_repositories import (
            SQLModelPreviewSourceReadinessRepository,
        )

        return SQLModelPreviewSourceReadinessRepository(session)

    def create_preview_generation_read_unit_of_work(
        self,
        connection_string: str,
        availability: PreviewEvidenceAvailability,
    ) -> PreviewGenerationReadUnitOfWork:
        from plugins.confluent_cloud.storage.preview_unit_of_work import (
            CCloudPreviewGenerationReadSQLModelUnitOfWork,
        )

        return CCloudPreviewGenerationReadSQLModelUnitOfWork(connection_string, availability)

    def create_preview_evidence_bootstrap(
        self,
        backend: PreviewEvidenceStorageBackend,
    ) -> PreviewEvidenceBootstrap:
        from plugins.confluent_cloud.preview_bootstrap import CCloudPreviewEvidenceBootstrap

        return CCloudPreviewEvidenceBootstrap(backend)
