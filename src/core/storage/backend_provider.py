from __future__ import annotations

import logging
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from threading import Condition, RLock
from typing import TYPE_CHECKING, Protocol, runtime_checkable

from core.config.fingerprint import tenant_config_fingerprint
from core.storage.registry import create_storage_backend
from core.storage.tenant_lifecycle import prepare_tenant_backend

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from contextlib import AbstractContextManager

    from core.config.models import TenantConfig
    from core.plugin.protocols import EcosystemPlugin
    from core.plugin.registry import PluginRegistry
    from core.preview.evidence import PreviewEvidenceBootstrapResult
    from core.preview.storage_availability import PreviewEvidenceBootstrapUnavailable
    from core.storage.interface import StorageBackend


@runtime_checkable
class TenantBackendProvider(Protocol):
    def acquire_backend(
        self,
        tenant_name: str,
        tenant_config: TenantConfig,
    ) -> AbstractContextManager[StorageBackend]: ...

    def close(self) -> None: ...


@dataclass
class _ApiTenantBackendEntry:
    config_fingerprint: str
    plugin: EcosystemPlugin
    backend: StorageBackend
    bootstrap_result: PreviewEvidenceBootstrapResult | PreviewEvidenceBootstrapUnavailable | None = None
    leases: int = 0
    retiring: bool = False


class ApiTenantBackendProvider:
    """Own initialized API-only plugins and their leased storage backends."""

    def __init__(self, plugin_registry: PluginRegistry) -> None:
        self._plugin_registry = plugin_registry
        self._condition = Condition(RLock())
        self._entries: dict[str, _ApiTenantBackendEntry] = {}
        self._closed = False

    @staticmethod
    def _fingerprint(config: TenantConfig) -> str:
        return tenant_config_fingerprint(config)

    def _construct_entry(
        self,
        tenant_name: str,
        tenant_config: TenantConfig,
        fingerprint: str,
    ) -> _ApiTenantBackendEntry:
        plugin = self._plugin_registry.create(tenant_config.ecosystem)
        backend: StorageBackend | None = None
        try:
            plugin.initialize(tenant_config.plugin_settings.model_dump())
            storage_module = plugin.get_storage_module()
            backend = create_storage_backend(
                tenant_config.storage,
                storage_module=storage_module,
                focus_preview_enabled=tenant_config.focus_preview_enabled,
            )
            bootstrap_result = prepare_tenant_backend(backend, tenant_name, tenant_config)
            return _ApiTenantBackendEntry(fingerprint, plugin, backend, bootstrap_result)
        except BaseException:
            if backend is not None:
                try:
                    backend.dispose()
                except BaseException as cleanup_error:
                    logger.error(
                        "API backend construction cleanup failed step=storage error_type=%s",
                        type(cleanup_error).__name__,
                    )
                try:
                    plugin.close()
                except BaseException as cleanup_error:
                    logger.error(
                        "API backend construction cleanup failed step=plugin error_type=%s",
                        type(cleanup_error).__name__,
                    )
            else:
                try:
                    plugin.close()
                except BaseException as cleanup_error:
                    logger.error(
                        "API backend construction cleanup failed step=plugin error_type=%s",
                        type(cleanup_error).__name__,
                    )
            raise

    @staticmethod
    def _close_entry(entry: _ApiTenantBackendEntry) -> list[BaseException]:
        failures: list[BaseException] = []
        try:
            entry.backend.dispose()
        except BaseException as exc:
            failures.append(exc)
        try:
            entry.plugin.close()
        except BaseException as exc:
            failures.append(exc)
        return failures

    @contextmanager
    def acquire_backend(
        self,
        tenant_name: str,
        tenant_config: TenantConfig,
    ) -> Iterator[StorageBackend]:
        fingerprint = self._fingerprint(tenant_config)
        with self._condition:
            while True:
                if self._closed:
                    raise RuntimeError("tenant backend provider is closed")
                entry = self._entries.get(tenant_name)
                if entry is None:
                    entry = self._construct_entry(tenant_name, tenant_config, fingerprint)
                    self._entries[tenant_name] = entry
                    entry.leases = 1
                    break
                if entry.retiring:
                    self._condition.wait()
                    continue
                if entry.config_fingerprint == fingerprint:
                    entry.leases += 1
                    break

                # The thread that flips this flag is the sole retirement owner.
                # Other acquirers and close() wait for it to remove and dispose
                # this exact entry before re-evaluating the current cache state.
                entry.retiring = True
                while entry.leases:
                    self._condition.wait()
                if self._entries.get(tenant_name) is entry:
                    del self._entries[tenant_name]
                try:
                    failures = self._close_entry(entry)
                finally:
                    self._condition.notify_all()
                if failures:
                    raise failures[0]
        try:
            yield entry.backend
        finally:
            with self._condition:
                entry.leases -= 1
                self._condition.notify_all()

    def close(self) -> None:
        with self._condition:
            if self._closed:
                return
            self._closed = True
            failures: list[BaseException] = []
            while self._entries:
                tenant_name, entry = next(iter(self._entries.items()))
                if entry.retiring:
                    self._condition.wait()
                    continue
                entry.retiring = True
                while entry.leases:
                    self._condition.wait()
                if self._entries.get(tenant_name) is entry:
                    del self._entries[tenant_name]
                try:
                    failures.extend(self._close_entry(entry))
                finally:
                    self._condition.notify_all()
            if failures:
                raise failures[0]
