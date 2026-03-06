"""Schema Registry allocators for CCloud cost distribution.

Schema Registry costs use simple even split across active identities.
No metrics needed — SR doesn't track per-principal usage.
"""

from __future__ import annotations

import logging

from core.engine.helpers import allocate_evenly_with_fallback

logger = logging.getLogger(__name__)

schema_registry_allocator = allocate_evenly_with_fallback
