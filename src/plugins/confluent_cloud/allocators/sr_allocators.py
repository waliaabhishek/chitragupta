"""Schema Registry allocators for CCloud cost distribution.

Schema Registry costs use a 3-tier composable model (SR_MODEL):
  Tier 0: EvenSplit over merged_active identities (USAGE)
  Tier 1: EvenSplit over tenant_period owners (SHARED, no_active_identities_located)
  Tier 2: Terminal to resource_id (SHARED, no_identities_located)

No metrics needed -- SR doesn't track per-principal usage.
"""

from __future__ import annotations

import logging

from plugins.confluent_cloud.allocation_models import SR_MODEL

logger = logging.getLogger(__name__)

schema_registry_allocator = SR_MODEL
