from __future__ import annotations

import logging

from core.storage.backends.sqlmodel.base_tables import BillingTable, IdentityTable, ResourceTable

logger = logging.getLogger(__name__)

# Self-managed Kafka uses the same billing/resource/identity schema as core.
SMKBillingTable = BillingTable
SMKResourceTable = ResourceTable
SMKIdentityTable = IdentityTable
