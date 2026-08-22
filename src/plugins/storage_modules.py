"""Plugin storage module registry.

This module provides ecosystem → StorageModule mapping for contexts that need
a storage module without instantiating the full plugin (e.g., API dependencies).

Lives in plugins/ (not core/) to maintain proper dependency direction:
plugins depend on core, not vice versa.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from core.plugin.protocols import StorageModule


def get_storage_module_for_ecosystem(ecosystem: str) -> StorageModule:
    """Return the appropriate StorageModule for an ecosystem.

    This allows API and other contexts to get the correct storage module
    without having to instantiate the full plugin.
    """
    if ecosystem == "confluent_cloud":
        from plugins.confluent_cloud.storage.module import CCloudStorageModule

        return CCloudStorageModule()

    if ecosystem == "self_managed_kafka":
        from plugins.self_managed_kafka.storage.module import SelfManagedKafkaStorageModule

        return SelfManagedKafkaStorageModule()

    # Generic ecosystems use core storage.
    from core.storage.backends.sqlmodel.module import CoreStorageModule

    return CoreStorageModule()
