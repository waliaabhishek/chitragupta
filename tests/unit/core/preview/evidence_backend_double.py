from __future__ import annotations

from unittest.mock import MagicMock

from core.preview.persistence import PreviewEvidenceStorageBackend
from core.preview.storage_availability import (
    PreviewEvidenceAvailability,
    PreviewEvidenceAvailabilityState,
)
from core.storage.interface import StorageBackend


class PreviewEvidenceBackendDouble:
    """Call-spy double for the complete generic and preview storage intersection."""

    def __init__(self) -> None:
        self.preview_evidence_availability = PreviewEvidenceAvailability(
            state=PreviewEvidenceAvailabilityState.READY,
        )
        self.create_unit_of_work = MagicMock()
        self.create_read_only_unit_of_work = MagicMock()
        self.create_tables = MagicMock()
        self.dispose = MagicMock()
        self.create_preview_evidence_unit_of_work = MagicMock()
        self.create_preview_generation_read_unit_of_work = MagicMock()
        self.create_preview_evidence_bootstrap = MagicMock()
        self.mark_preview_evidence_bootstrap_unavailable = MagicMock()


def preview_evidence_backend_double() -> PreviewEvidenceBackendDouble:
    backend = PreviewEvidenceBackendDouble()
    assert isinstance(backend, StorageBackend)
    assert isinstance(backend, PreviewEvidenceStorageBackend)
    return backend
