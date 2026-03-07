from __future__ import annotations

import logging

from core.storage.backends.sqlmodel.module import CoreStorageModule

logger = logging.getLogger(__name__)


class GenericMetricsOnlyStorageModule(CoreStorageModule):
    """StorageModule for generic metrics-only ecosystems. Uses core schema (no plugin-specific columns)."""
