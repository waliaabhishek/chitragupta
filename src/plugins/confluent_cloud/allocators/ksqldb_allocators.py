"""ksqlDB allocators for CCloud cost distribution.

ksqlDB CSU costs use a 3-tier composable model (KSQLDB_MODEL):
  Tier 0: EvenSplit over merged_active identities (USAGE)
  Tier 1: EvenSplit over tenant_period owners (SHARED, no_active_identities_located)
  Tier 2: Terminal to resource_id (SHARED, no_identities_located)

No metrics needed -- ksqlDB doesn't track per-principal usage beyond active SA.
"""

from __future__ import annotations

import logging

from plugins.confluent_cloud.allocation_models import KSQLDB_MODEL

logger = logging.getLogger(__name__)

ksqldb_csu_allocator = KSQLDB_MODEL
