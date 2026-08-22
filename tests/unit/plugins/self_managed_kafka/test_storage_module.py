from __future__ import annotations

import logging
from unittest.mock import MagicMock

from sqlalchemy import create_engine, inspect

from core.plugin.protocols import StorageModule
from core.storage.interface import BillingRepository, ChargebackRepository, IdentityRepository, ResourceRepository
from plugins.self_managed_kafka.plugin import SelfManagedKafkaPlugin
from plugins.self_managed_kafka.storage.module import SelfManagedKafkaStorageModule

logger = logging.getLogger(__name__)


class TestSMKPluginGetStorageModule:
    def test_get_storage_module_returns_storage_module(self) -> None:
        """SelfManagedKafkaPlugin.get_storage_module() returns a StorageModule."""
        plugin = SelfManagedKafkaPlugin()
        module = plugin.get_storage_module()
        assert isinstance(module, StorageModule)

    def test_get_storage_module_returns_smk_module_type(self) -> None:
        """get_storage_module() returns SelfManagedKafkaStorageModule specifically."""
        plugin = SelfManagedKafkaPlugin()
        module = plugin.get_storage_module()
        assert isinstance(module, SelfManagedKafkaStorageModule)

    def test_smk_storage_module_satisfies_protocol(self) -> None:
        """SelfManagedKafkaStorageModule satisfies the StorageModule Protocol."""
        module = SelfManagedKafkaStorageModule()
        assert isinstance(module, StorageModule)

    def test_create_billing_repository_returns_billing_repository(self) -> None:
        """create_billing_repository() returns a BillingRepository."""
        module = SelfManagedKafkaStorageModule()
        session = MagicMock()
        repo = module.create_billing_repository(session)
        assert isinstance(repo, BillingRepository)

    def test_create_resource_repository_returns_resource_repository(self) -> None:
        """create_resource_repository() returns a ResourceRepository."""
        module = SelfManagedKafkaStorageModule()
        session = MagicMock()
        repo = module.create_resource_repository(session)
        assert isinstance(repo, ResourceRepository)

    def test_create_identity_repository_returns_identity_repository(self) -> None:
        """create_identity_repository() returns an IdentityRepository."""
        module = SelfManagedKafkaStorageModule()
        session = MagicMock()
        repo = module.create_identity_repository(session)
        assert isinstance(repo, IdentityRepository)

    def test_create_chargeback_repository_returns_the_plugin_owned_repository(self) -> None:
        module = SelfManagedKafkaStorageModule()
        repository = module.create_chargeback_repository(MagicMock())

        assert isinstance(repository, ChargebackRepository)
        assert type(repository).__name__ == "SelfManagedKafkaChargebackRepository"

    def test_storage_module_exposes_plugin_migration_capability(self) -> None:
        module = SelfManagedKafkaStorageModule()

        assert callable(getattr(module, "prepare_plugin_storage_migration", None))
        assert callable(getattr(module, "downgrade_plugin_storage_migration", None))

    def test_direct_registration_creates_both_plugin_owned_tables(self, tmp_path) -> None:
        url = f"sqlite:///{tmp_path / 'direct-plugin-tables.db'}"
        engine = create_engine(url)
        try:
            SelfManagedKafkaStorageModule().register_tables(engine)
            assert {"self_managed_kafka_scope_state", "self_managed_kafka_principal_team_snapshots"} <= set(
                inspect(engine).get_table_names()
            )
        finally:
            engine.dispose()
