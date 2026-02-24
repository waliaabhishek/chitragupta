from __future__ import annotations

import logging

LOGGER = logging.getLogger(__name__)


def parse_ccloud_crn(crn: str) -> dict[str, str]:
    """Parse a CCloud CRN into key-value pairs.

    Example: 'crn://confluent.cloud/organization=abc/environment=env-xyz'
    Returns: {'organization': 'abc', 'environment': 'env-xyz'}
    """
    if not crn:
        return {}

    # Strip crn:// prefix and authority
    path = crn
    if path.startswith("crn://"):
        # Remove "crn://confluent.cloud" prefix
        parts = path.split("/", 3)  # ['crn:', '', 'confluent.cloud', 'rest...']
        path = "/" + parts[3] if len(parts) > 3 else ""

    result: dict[str, str] = {}
    for segment in path.strip("/").split("/"):
        if "=" in segment:
            key, _, value = segment.partition("=")
            if key and value:
                result[key] = value
        # Skip segments without '=' (like authority)
    return result
