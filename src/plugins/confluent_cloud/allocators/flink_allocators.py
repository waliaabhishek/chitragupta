"""Flink allocators for CCloud cost distribution.

Flink CFU costs use usage-ratio allocation by statement owner CFU consumption.
CFU (Confluent Flink Units) represent compute capacity per statement.

Fallback chain:
1. Usage-ratio by statement owner (from IdentityResolution.context["stmt_owner_cfu"])
2. Even split across merged_active -- zero-CFU fallback (USAGE)
3. Even split across tenant_period owners (SHARED)
4. Terminal to resource_id / compute_pool_id (SHARED)
"""

from __future__ import annotations

import logging

from plugins.confluent_cloud.allocation_models import FLINK_MODEL

logger = logging.getLogger(__name__)

# Direct assignment — flink_cfu_allocator is an alias for FLINK_MODEL.
# Tier 0: Usage-ratio by statement owner CFU (USAGE)
# Tier 1: Even split across merged_active, CFU=0 fallback (USAGE)
# Tier 2: Even split across tenant_period owners (SHARED)
# Tier 3: Terminal to compute_pool_id / resource_id (SHARED)
flink_cfu_allocator = FLINK_MODEL
