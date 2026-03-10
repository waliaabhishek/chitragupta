"""Connect allocators for CCloud cost distribution.

Two composable models cover all Kafka Connect product types:

CONNECTOR_TASKS_MODEL (CONNECT_NUM_TASKS, CONNECT_THROUGHPUT, CUSTOM variants):
  Tier 0: EvenSplit over merged_active (USAGE)
  Tier 1: Terminal to resource_id (SHARED, no_identities_located)

CONNECTOR_CAPACITY_MODEL (CONNECT_CAPACITY, CUSTOM_CONNECT_PLUGIN):
  Tier 0: EvenSplit over merged_active (SHARED)
  Tier 1: Terminal to resource_id (SHARED, no_identities_located)

No tenant_period fallback — resource-local terminal preserves GAP-24 behavior.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

from plugins.confluent_cloud.allocation_models import (
    CONNECTOR_CAPACITY_MODEL,
    CONNECTOR_TASKS_MODEL,
)

connect_capacity_allocator = CONNECTOR_CAPACITY_MODEL
connect_tasks_allocator = CONNECTOR_TASKS_MODEL
connect_throughput_allocator = CONNECTOR_TASKS_MODEL
