from __future__ import annotations

import logging

from core.storage.backends.sqlmodel.base_tables import BillingTable, IdentityTable, ResourceTable

logger = logging.getLogger(__name__)

# Generic metrics-only plugin uses the same billing/resource/identity schema as core.
GMOBillingTable = BillingTable
GMOResourceTable = ResourceTable
GMOIdentityTable = IdentityTable
