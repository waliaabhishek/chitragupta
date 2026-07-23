from __future__ import annotations

import hashlib
import json
import logging
from collections.abc import Mapping
from typing import Any

from pydantic import BaseModel, SecretStr

logger = logging.getLogger(__name__)


def _fingerprint_value(value: Any) -> Any:
    if isinstance(value, SecretStr):
        return value.get_secret_value()
    if isinstance(value, BaseModel):
        fields = {name: _fingerprint_value(getattr(value, name)) for name in type(value).model_fields}
        if value.model_extra:
            fields.update((name, _fingerprint_value(field_value)) for name, field_value in value.model_extra.items())
        return fields
    if isinstance(value, Mapping):
        return {str(key): _fingerprint_value(field_value) for key, field_value in value.items()}
    if isinstance(value, list | tuple):
        return [_fingerprint_value(item) for item in value]
    if isinstance(value, set | frozenset):
        return sorted((_fingerprint_value(item) for item in value), key=repr)
    return value


def tenant_config_fingerprint(config: BaseModel) -> str:
    """Return a stable digest that includes the values behind every SecretStr."""
    payload = json.dumps(
        _fingerprint_value(config),
        default=str,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(payload.encode()).hexdigest()
