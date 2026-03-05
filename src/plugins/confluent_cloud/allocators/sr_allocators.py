"""Schema Registry allocators for CCloud cost distribution.

Schema Registry costs use simple even split across active identities.
No metrics needed — SR doesn't track per-principal usage.
"""

from __future__ import annotations

from core.engine.helpers import allocate_evenly_with_fallback

schema_registry_allocator = allocate_evenly_with_fallback
