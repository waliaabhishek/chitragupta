from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import TYPE_CHECKING, Self

from sqlmodel import Session

from core.preview.storage_availability import (
    PreviewEvidenceAvailability,
    PreviewEvidenceAvailabilityState,
    PreviewEvidenceUnavailableError,
)
from core.storage.backends.sqlmodel.engine import get_or_create_engine, get_or_create_read_only_engine
from core.storage.backends.sqlmodel.repositories import SQLModelEntityTagRepository
from plugins.confluent_cloud.storage.preview_repositories import (
    SQLModelPreviewAllocationLineageRepository,
    SQLModelPreviewOrganizationAuthorityRepository,
    SQLModelPreviewRepairRepository,
    SQLModelPreviewSourceReadinessRepository,
    SQLModelPreviewSourceWindowRepository,
)
from plugins.confluent_cloud.storage.repositories import CCloudBillingRepository, CCloudChargebackRepository

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from collections.abc import Iterator

    from core.preview.evidence import PreviewAllocationEvidenceReader, PreviewCostEvidenceReader
    from core.preview.evidence_capture import PreviewSourceWindowWriter
    from core.preview.organization_authority import (
        PreviewOrganizationAuthorityReader,
        PreviewOrganizationAuthorityWriter,
    )
    from core.preview.persistence import (
        PreviewAllocationLineageWriter,
        PreviewCalculationRepository,
        PreviewSourceReadinessReader,
        PreviewSourceReadinessWriter,
    )
    from core.preview.repair import PreviewRepairRepository
    from core.storage.interface import EntityTagRepository, IdentityRepository, ResourceRepository


def _require_ready(availability: PreviewEvidenceAvailability) -> None:
    if availability.state is not PreviewEvidenceAvailabilityState.READY:
        raise PreviewEvidenceUnavailableError("FOCUS Mapping Preview evidence storage is unavailable")


class CCloudPreviewEvidenceSQLModelUnitOfWork:
    source_windows: PreviewSourceWindowWriter
    allocation_lineage: PreviewAllocationLineageWriter
    source_readiness: PreviewSourceReadinessWriter
    organization_authority: PreviewOrganizationAuthorityWriter
    repairs: PreviewRepairRepository

    def __init__(self, connection_string: str, availability: PreviewEvidenceAvailability) -> None:
        _require_ready(availability)
        self._engine = get_or_create_engine(connection_string)
        self._session: Session | None = None

    def __enter__(self) -> Self:
        self._session = Session(self._engine)
        self._finished = False
        self.source_windows = SQLModelPreviewSourceWindowRepository(self._session)
        self.allocation_lineage = SQLModelPreviewAllocationLineageRepository(self._session)
        self.source_readiness = SQLModelPreviewSourceReadinessRepository(self._session)
        self.organization_authority = SQLModelPreviewOrganizationAuthorityRepository(self._session)
        self.repairs = SQLModelPreviewRepairRepository(self._session)
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: object,
    ) -> None:
        if self._session is None:
            return
        try:
            if not self._finished:
                self._session.rollback()
        finally:
            self._session.close()
            self._session = None

    def commit(self) -> None:
        if self._session is None:
            raise RuntimeError("Cannot commit outside of a transaction")
        self._session.commit()
        self._finished = True

    def rollback(self) -> None:
        if self._session is None:
            raise RuntimeError("Cannot rollback outside of a transaction")
        self._session.rollback()
        self._finished = True

    @contextmanager
    def savepoint(self) -> Iterator[None]:
        if self._session is None:
            raise RuntimeError("Cannot create a savepoint outside of a transaction")
        with self._session.begin_nested():
            yield


class CCloudPreviewGenerationReadSQLModelUnitOfWork:
    calculations: PreviewCalculationRepository
    cost_evidence: PreviewCostEvidenceReader
    allocation_evidence: PreviewAllocationEvidenceReader
    source_readiness: PreviewSourceReadinessReader
    organization_authority: PreviewOrganizationAuthorityReader
    resources: ResourceRepository
    identities: IdentityRepository
    tags: EntityTagRepository
    repairs: PreviewRepairRepository

    def __init__(self, connection_string: str, availability: PreviewEvidenceAvailability) -> None:
        _require_ready(availability)
        self._engine = get_or_create_read_only_engine(connection_string)
        self._session: Session | None = None

    def __enter__(self) -> Self:
        from core.preview.persistence import SQLModelPreviewCalculationRepository
        from core.storage.backends.sqlmodel.repositories import (
            SQLModelIdentityRepository,
            SQLModelResourceRepository,
        )

        self._session = Session(self._engine)
        self.calculations = SQLModelPreviewCalculationRepository(self._session)
        self.cost_evidence = CCloudBillingRepository(self._session)
        self.allocation_evidence = CCloudChargebackRepository(self._session)
        self.source_readiness = SQLModelPreviewSourceReadinessRepository(self._session)
        self.organization_authority = SQLModelPreviewOrganizationAuthorityRepository(self._session)
        self.repairs = SQLModelPreviewRepairRepository(self._session)
        self.resources = SQLModelResourceRepository(self._session)
        self.identities = SQLModelIdentityRepository(self._session)
        self.tags = SQLModelEntityTagRepository(self._session)
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: object,
    ) -> None:
        if self._session is not None:
            self._session.close()
            self._session = None
