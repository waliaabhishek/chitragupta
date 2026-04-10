from __future__ import annotations

import logging
import re

logger = logging.getLogger(__name__)

_TAG_KEY_RE = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9_-]{0,62}$")


def is_valid_tag_key(key: str) -> bool:
    return bool(_TAG_KEY_RE.match(key))
