from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from core.models import Resource
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SMKSharedContext:
    """Pre-built shared context for a single SMK gather cycle.

    Created once per gather cycle by SelfManagedKafkaPlugin.build_shared_context().
    Contains the cluster resource constructed from plugin config. Unlike CCloud,
    SMK has exactly one cluster (single Resource, not a collection), so no tuple
    wrapping is needed.

    Note: gather_cluster_resource() is a pure config-to-Resource constructor
    (no API call). Moving it to Phase 1 provides structural consistency with the
    CCloud pattern rather than eliminating an API round-trip.
    """

    cluster_resource: Resource
