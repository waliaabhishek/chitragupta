from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from fastapi import FastAPI

    from core.config.models import TenantConfig
    from core.storage.interface import StorageBackend


class FixedTenantBackendProvider:
    """Test provider that exercises the production lease protocol without owning backends."""

    def __init__(self, backends: dict[str, StorageBackend] | None = None) -> None:
        self.backends = dict(backends or {})
        self.acquisitions: list[str] = []
        self.lease_events: list[tuple[str, str]] = []

    @contextmanager
    def acquire_backend(
        self,
        tenant_name: str,
        tenant_config: TenantConfig,
    ) -> Iterator[StorageBackend]:
        del tenant_config
        self.acquisitions.append(tenant_name)
        self.lease_events.append(("enter", tenant_name))
        try:
            yield self.backends[tenant_name]
        finally:
            self.lease_events.append(("exit", tenant_name))

    def close(self) -> None:
        return None


def install_backend(app: FastAPI, tenant_name: str, backend: StorageBackend) -> FixedTenantBackendProvider:
    provider = getattr(app.state, "backend_provider", None)
    if not isinstance(provider, FixedTenantBackendProvider):
        provider = FixedTenantBackendProvider()
        app.state.backend_provider = provider
    provider.backends[tenant_name] = backend
    runtime = getattr(app.state, "preview_runtime", None)
    if runtime is not None:
        runtime._backend_provider = provider
    return provider
