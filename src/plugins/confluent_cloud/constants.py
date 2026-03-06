from __future__ import annotations

import logging

logger = logging.getLogger(__name__)
# Allocation detail reason codes specific to the Confluent Cloud plugin.
# Stored as VARCHAR in the DB — no enum membership required.
# Core AllocationDetail is intentionally not used here (DIP: plugin must not
# push ecosystem-specific values into core).

CLUSTER_LINKING_COST: str = "cluster_linking_cost"
NO_FLINK_STMT_NAME_TO_OWNER_MAP: str = "no_flink_stmt_name_to_owner_map"
