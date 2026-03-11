"""Org-wide allocators for CCloud cost distribution."""

from __future__ import annotations

import logging

from core.engine.allocation_models import ChainModel
from plugins.confluent_cloud.allocation_models import ORG_WIDE_MODEL

logger = logging.getLogger(__name__)

org_wide_allocator: ChainModel = ORG_WIDE_MODEL
