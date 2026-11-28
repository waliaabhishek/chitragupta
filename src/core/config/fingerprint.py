from __future__ import annotations

import hashlib
import json
import logging
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, SecretStr

if TYPE_CHECKING:
    from core.config.models import StorageConfig, TenantConfig

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


def tenant_config_fingerprint(config: TenantConfig) -> str:
    """Return a stable digest of tenant settings used by the source runtime."""
    runtime_settings = dict(config)
    runtime_settings["plugin_settings"] = config.plugin_settings.plugin_payload()
    payload = json.dumps(
        _fingerprint_value(runtime_settings),
        default=str,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(payload.encode()).hexdigest()


def storage_backend_fingerprint(config: StorageConfig) -> str:
    """Return a stable digest for the configured storage backend identity."""
    payload = json.dumps(
        {
            "backend": "sqlmodel",
            "connection_string": config.connection_string.get_secret_value(),
        },
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(payload.encode()).hexdigest()
