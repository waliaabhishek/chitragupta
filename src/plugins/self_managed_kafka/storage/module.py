from __future__ import annotations

import logging

from core.storage.backends.sqlmodel.module import CoreStorageModule

logger = logging.getLogger(__name__)


class SelfManagedKafkaStorageModule(CoreStorageModule):
    """StorageModule for self-managed Kafka. Uses core schema (no plugin-specific columns)."""
