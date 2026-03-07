from __future__ import annotations

import logging
from unittest.mock import MagicMock

from core.plugin.protocols import StorageModule
from core.storage.interface import BillingRepository, IdentityRepository, ResourceRepository
from plugins.generic_metrics_only.plugin import GenericMetricsOnlyPlugin
from plugins.generic_metrics_only.storage.module import GenericMetricsOnlyStorageModule

logger = logging.getLogger(__name__)


class TestGMOPluginGetStorageModule:
    def test_get_storage_module_returns_storage_module(self) -> None:
        """GenericMetricsOnlyPlugin.get_storage_module() returns a StorageModule."""
        plugin = GenericMetricsOnlyPlugin()
        module = plugin.get_storage_module()
        assert isinstance(module, StorageModule)

    def test_get_storage_module_returns_gmo_module_type(self) -> None:
        """get_storage_module() returns GenericMetricsOnlyStorageModule specifically."""
        plugin = GenericMetricsOnlyPlugin()
        module = plugin.get_storage_module()
        assert isinstance(module, GenericMetricsOnlyStorageModule)

    def test_gmo_storage_module_satisfies_protocol(self) -> None:
        """GenericMetricsOnlyStorageModule satisfies the StorageModule Protocol."""
        module = GenericMetricsOnlyStorageModule()
        assert isinstance(module, StorageModule)

    def test_create_billing_repository_returns_billing_repository(self) -> None:
        """create_billing_repository() returns a BillingRepository."""
        module = GenericMetricsOnlyStorageModule()
        session = MagicMock()
        repo = module.create_billing_repository(session)
        assert isinstance(repo, BillingRepository)

    def test_create_resource_repository_returns_resource_repository(self) -> None:
        """create_resource_repository() returns a ResourceRepository."""
        module = GenericMetricsOnlyStorageModule()
        session = MagicMock()
        repo = module.create_resource_repository(session)
        assert isinstance(repo, ResourceRepository)

    def test_create_identity_repository_returns_identity_repository(self) -> None:
        """create_identity_repository() returns an IdentityRepository."""
        module = GenericMetricsOnlyStorageModule()
        session = MagicMock()
        repo = module.create_identity_repository(session)
        assert isinstance(repo, IdentityRepository)
