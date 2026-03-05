"""Tests for BaseServiceHandler convenience base class (TASK-023)."""

from __future__ import annotations

from collections.abc import Iterable
from unittest.mock import MagicMock

import pytest


class TestBaseServiceHandlerInit:
    """BaseServiceHandler.__init__ stores attributes correctly."""

    def test_stores_connection(self) -> None:
        """_connection is stored from constructor argument."""
        from core.plugin.base import BaseServiceHandler

        conn = object()

        class ConcreteHandler(BaseServiceHandler):
            pass

        handler = ConcreteHandler(connection=conn, config=None, ecosystem="test")
        assert handler._connection is conn

    def test_stores_config(self) -> None:
        """_config is stored from constructor argument."""
        from core.plugin.base import BaseServiceHandler

        cfg = object()

        class ConcreteHandler(BaseServiceHandler):
            pass

        handler = ConcreteHandler(connection=None, config=cfg, ecosystem="test")
        assert handler._config is cfg

    def test_stores_ecosystem(self) -> None:
        """_ecosystem is stored from constructor argument."""
        from core.plugin.base import BaseServiceHandler

        class ConcreteHandler(BaseServiceHandler):
            pass

        handler = ConcreteHandler(connection=None, config=None, ecosystem="confluent_cloud")
        assert handler._ecosystem == "confluent_cloud"

    def test_stores_none_connection(self) -> None:
        """_connection may be None (common in tests)."""
        from core.plugin.base import BaseServiceHandler

        class ConcreteHandler(BaseServiceHandler):
            pass

        handler = ConcreteHandler(connection=None, config=None, ecosystem="test")
        assert handler._connection is None


class TestBaseServiceHandlerGatherIdentities:
    """BaseServiceHandler.gather_identities returns empty iterable."""

    def test_returns_empty_iterable(self) -> None:
        """gather_identities returns an empty iterable by default."""
        from core.plugin.base import BaseServiceHandler

        class ConcreteHandler(BaseServiceHandler):
            pass

        mock_uow = MagicMock()
        handler = ConcreteHandler(connection=None, config=None, ecosystem="test")
        result = list(handler.gather_identities("tenant-1", mock_uow))
        assert result == []

    def test_return_type_is_iterable(self) -> None:
        """gather_identities return value is iterable."""
        from core.plugin.base import BaseServiceHandler

        class ConcreteHandler(BaseServiceHandler):
            pass

        mock_uow = MagicMock()
        handler = ConcreteHandler(connection=None, config=None, ecosystem="test")
        result = handler.gather_identities("tenant-1", mock_uow)
        assert isinstance(result, Iterable)


class TestBaseServiceHandlerGetAllocator:
    """BaseServiceHandler.get_allocator raises ValueError for empty _ALLOCATOR_MAP."""

    def test_raises_value_error_when_map_empty(self) -> None:
        """get_allocator raises ValueError when _ALLOCATOR_MAP has no entries."""
        from core.plugin.base import BaseServiceHandler

        class ConcreteHandler(BaseServiceHandler):
            pass

        handler = ConcreteHandler(connection=None, config=None, ecosystem="test")
        with pytest.raises(ValueError, match="Unknown product type"):
            handler.get_allocator("SOME_TYPE")

    def test_returns_allocator_from_map(self) -> None:
        """get_allocator returns correct allocator when product type is in _ALLOCATOR_MAP."""
        from core.plugin.base import BaseServiceHandler
        from core.plugin.protocols import CostAllocator

        mock_allocator = MagicMock(spec=CostAllocator)

        class ConcreteHandler(BaseServiceHandler):
            _ALLOCATOR_MAP = {"MY_PRODUCT": mock_allocator}  # type: ignore[assignment]

        handler = ConcreteHandler(connection=None, config=None, ecosystem="test")
        assert handler.get_allocator("MY_PRODUCT") is mock_allocator

    def test_raises_for_unknown_type_with_populated_map(self) -> None:
        """get_allocator raises ValueError for product type not in _ALLOCATOR_MAP."""
        from core.plugin.base import BaseServiceHandler
        from core.plugin.protocols import CostAllocator

        mock_allocator = MagicMock(spec=CostAllocator)

        class ConcreteHandler(BaseServiceHandler):
            _ALLOCATOR_MAP = {"KNOWN_TYPE": mock_allocator}  # type: ignore[assignment]

        handler = ConcreteHandler(connection=None, config=None, ecosystem="test")
        with pytest.raises(ValueError, match="Unknown product type"):
            handler.get_allocator("UNKNOWN_TYPE")

    def test_error_message_includes_product_type(self) -> None:
        """ValueError message includes the unknown product type string."""
        from core.plugin.base import BaseServiceHandler

        class ConcreteHandler(BaseServiceHandler):
            pass

        handler = ConcreteHandler(connection=None, config=None, ecosystem="test")
        with pytest.raises(ValueError, match="BOGUS_PRODUCT"):
            handler.get_allocator("BOGUS_PRODUCT")


class TestProtocolComplianceAfterRefactor:
    """Handlers refactored to use BaseServiceHandler still satisfy ServiceHandler protocol."""

    def test_schema_registry_handler_is_service_handler(self) -> None:
        """SchemaRegistryHandler satisfies the ServiceHandler protocol."""
        from core.plugin.protocols import ServiceHandler
        from plugins.confluent_cloud.handlers.schema_registry import SchemaRegistryHandler

        handler = SchemaRegistryHandler(connection=None, config=None, ecosystem="confluent_cloud")
        assert isinstance(handler, ServiceHandler)

    def test_kafka_handler_is_service_handler(self) -> None:
        """KafkaHandler satisfies the ServiceHandler protocol."""
        from core.plugin.protocols import ServiceHandler
        from plugins.confluent_cloud.handlers.kafka import KafkaHandler

        handler = KafkaHandler(connection=None, config=None, ecosystem="confluent_cloud")
        assert isinstance(handler, ServiceHandler)

    def test_flink_handler_is_service_handler(self) -> None:
        """FlinkHandler satisfies the ServiceHandler protocol."""
        from core.plugin.protocols import ServiceHandler
        from plugins.confluent_cloud.handlers.flink import FlinkHandler

        handler = FlinkHandler(connection=None, config=None, ecosystem="confluent_cloud")
        assert isinstance(handler, ServiceHandler)

    def test_connector_handler_is_service_handler(self) -> None:
        """ConnectorHandler satisfies the ServiceHandler protocol."""
        from core.plugin.protocols import ServiceHandler
        from plugins.confluent_cloud.handlers.connectors import ConnectorHandler

        handler = ConnectorHandler(connection=None, config=None, ecosystem="confluent_cloud")
        assert isinstance(handler, ServiceHandler)

    def test_ksqldb_handler_is_service_handler(self) -> None:
        """KsqldbHandler satisfies the ServiceHandler protocol."""
        from core.plugin.protocols import ServiceHandler
        from plugins.confluent_cloud.handlers.ksqldb import KsqldbHandler

        handler = KsqldbHandler(connection=None, config=None, ecosystem="confluent_cloud")
        assert isinstance(handler, ServiceHandler)


class TestSchemaRegistryHandlerViaBaseClass:
    """Verify SchemaRegistryHandler uses BaseServiceHandler after refactor."""

    def test_get_allocator_schema_registry(self) -> None:
        """SCHEMA_REGISTRY returns schema_registry_allocator via inherited get_allocator."""
        from plugins.confluent_cloud.allocators.sr_allocators import schema_registry_allocator
        from plugins.confluent_cloud.handlers.schema_registry import SchemaRegistryHandler

        handler = SchemaRegistryHandler(connection=None, config=None, ecosystem="confluent_cloud")
        assert handler.get_allocator("SCHEMA_REGISTRY") is schema_registry_allocator

    def test_get_allocator_unknown_raises(self) -> None:
        """Unknown product type raises ValueError via inherited get_allocator."""
        from plugins.confluent_cloud.handlers.schema_registry import SchemaRegistryHandler

        handler = SchemaRegistryHandler(connection=None, config=None, ecosystem="confluent_cloud")
        with pytest.raises(ValueError, match="Unknown product type"):
            handler.get_allocator("UNKNOWN")

    def test_gather_identities_returns_empty(self) -> None:
        """gather_identities returns empty via inherited BaseServiceHandler."""
        from plugins.confluent_cloud.handlers.schema_registry import SchemaRegistryHandler

        mock_uow = MagicMock()
        handler = SchemaRegistryHandler(connection=None, config=None, ecosystem="confluent_cloud")
        result = list(handler.gather_identities("t1", mock_uow))
        assert result == []

    def test_inherits_from_base_service_handler(self) -> None:
        """SchemaRegistryHandler is an instance of BaseServiceHandler after refactor."""
        from core.plugin.base import BaseServiceHandler
        from plugins.confluent_cloud.handlers.schema_registry import SchemaRegistryHandler

        handler = SchemaRegistryHandler(connection=None, config=None, ecosystem="confluent_cloud")
        assert isinstance(handler, BaseServiceHandler)


class TestFlinkHandlerViaBaseClass:
    """Verify FlinkHandler uses BaseServiceHandler after refactor."""

    def test_inherits_from_base_service_handler(self) -> None:
        """FlinkHandler is an instance of BaseServiceHandler after refactor."""
        from core.plugin.base import BaseServiceHandler
        from plugins.confluent_cloud.handlers.flink import FlinkHandler

        handler = FlinkHandler(connection=None, config=None, ecosystem="confluent_cloud")
        assert isinstance(handler, BaseServiceHandler)

    def test_gather_identities_returns_empty(self) -> None:
        """FlinkHandler.gather_identities still returns empty via base class."""
        from plugins.confluent_cloud.handlers.flink import FlinkHandler

        mock_uow = MagicMock()
        handler = FlinkHandler(connection=None, config=None, ecosystem="confluent_cloud")
        result = list(handler.gather_identities("t1", mock_uow))
        assert result == []

    def test_get_allocator_unknown_raises(self) -> None:
        """FlinkHandler raises ValueError for unknown type via inherited get_allocator."""
        from plugins.confluent_cloud.handlers.flink import FlinkHandler

        handler = FlinkHandler(connection=None, config=None, ecosystem="confluent_cloud")
        with pytest.raises(ValueError, match="Unknown product type"):
            handler.get_allocator("UNKNOWN")

    def test_flink_regions_populated_from_config(self) -> None:
        """FlinkHandler._flink_regions is built from config.flink entries."""
        from plugins.confluent_cloud.handlers.flink import FlinkHandler

        mock_region = MagicMock()
        mock_region.region_id = "us-east-1"
        mock_region.key = "MY_API_KEY"
        mock_region.secret.get_secret_value.return_value = "MY_SECRET"

        mock_config = MagicMock()
        mock_config.flink = [mock_region]

        handler = FlinkHandler(connection=None, config=mock_config, ecosystem="confluent_cloud")
        assert "us-east-1" in handler._flink_regions
        assert handler._flink_regions["us-east-1"] == ("MY_API_KEY", "MY_SECRET")


class TestKafkaHandlerViaBaseClass:
    """Verify KafkaHandler uses BaseServiceHandler after refactor."""

    def test_inherits_from_base_service_handler(self) -> None:
        """KafkaHandler is an instance of BaseServiceHandler after refactor."""
        from core.plugin.base import BaseServiceHandler
        from plugins.confluent_cloud.handlers.kafka import KafkaHandler

        handler = KafkaHandler(connection=None, config=None, ecosystem="confluent_cloud")
        assert isinstance(handler, BaseServiceHandler)

    def test_gather_identities_non_empty(self) -> None:
        """KafkaHandler.gather_identities override is preserved (non-empty)."""
        from unittest.mock import patch

        from plugins.confluent_cloud.handlers.kafka import KafkaHandler

        mock_uow = MagicMock()
        mock_uow.identities = MagicMock()
        mock_uow.identities.find_by_period.return_value = ([], 0)

        # KafkaHandler overrides gather_identities — it must NOT return empty
        # even when the base class default would. We verify the override is preserved
        # by confirming the method exists and the base default is NOT used.
        with (
            patch(
                "plugins.confluent_cloud.gathering.gather_service_accounts",
                return_value=[],
            ) as mock_sa,
            patch(
                "plugins.confluent_cloud.gathering.gather_users",
                return_value=[],
            ) as mock_users,
            patch(
                "plugins.confluent_cloud.gathering.gather_api_keys",
                return_value=[],
            ) as mock_keys,
            patch(
                "plugins.confluent_cloud.gathering.gather_identity_providers",
                return_value=[],
            ) as mock_providers,
            patch(
                "plugins.confluent_cloud.gathering.gather_identity_pools",
                return_value=[],
            ) as mock_pools,
        ):
            handler = KafkaHandler(connection=MagicMock(), config=None, ecosystem="confluent_cloud")
            result = list(handler.gather_identities("t1", mock_uow))
            assert result == []
            # Verify override is active: gathering functions were called, not base's iter(())
            mock_sa.assert_called_once()
            mock_users.assert_called_once()
            mock_keys.assert_called_once()
            mock_providers.assert_called_once()
            mock_pools.assert_called_once()


class TestConnectorHandlerViaBaseClass:
    """ConnectorHandler.get_allocator returns connect_capacity_allocator for CONNECT_CAPACITY."""

    def test_get_allocator_connect_capacity(self) -> None:
        """CONNECT_CAPACITY returns connect_capacity_allocator."""
        from plugins.confluent_cloud.allocators.connector_allocators import connect_capacity_allocator
        from plugins.confluent_cloud.handlers.connectors import ConnectorHandler

        handler = ConnectorHandler(connection=None, config=None, ecosystem="confluent_cloud")
        assert handler.get_allocator("CONNECT_CAPACITY") is connect_capacity_allocator

    def test_inherits_from_base_service_handler(self) -> None:
        """ConnectorHandler is an instance of BaseServiceHandler after refactor."""
        from core.plugin.base import BaseServiceHandler
        from plugins.confluent_cloud.handlers.connectors import ConnectorHandler

        handler = ConnectorHandler(connection=None, config=None, ecosystem="confluent_cloud")
        assert isinstance(handler, BaseServiceHandler)


class TestKsqldbHandlerViaBaseClass:
    """KsqldbHandler.get_allocator returns ksqldb_csu_allocator for KSQL_NUM_CSU."""

    def test_get_allocator_ksql_num_csu(self) -> None:
        """KSQL_NUM_CSU returns ksqldb_csu_allocator."""
        from plugins.confluent_cloud.allocators.ksqldb_allocators import ksqldb_csu_allocator
        from plugins.confluent_cloud.handlers.ksqldb import KsqldbHandler

        handler = KsqldbHandler(connection=None, config=None, ecosystem="confluent_cloud")
        assert handler.get_allocator("KSQL_NUM_CSU") is ksqldb_csu_allocator

    def test_inherits_from_base_service_handler(self) -> None:
        """KsqldbHandler is an instance of BaseServiceHandler after refactor."""
        from core.plugin.base import BaseServiceHandler
        from plugins.confluent_cloud.handlers.ksqldb import KsqldbHandler

        handler = KsqldbHandler(connection=None, config=None, ecosystem="confluent_cloud")
        assert isinstance(handler, BaseServiceHandler)


class TestOrgWideAndDefaultHandlerUnchanged:
    """OrgWideCostHandler and DefaultHandler do NOT use BaseServiceHandler."""

    def test_org_wide_handler_not_base_service_handler(self) -> None:
        """OrgWideCostHandler does not inherit from BaseServiceHandler."""
        from core.plugin.base import BaseServiceHandler
        from plugins.confluent_cloud.handlers.org_wide import OrgWideCostHandler

        handler = OrgWideCostHandler(ecosystem="confluent_cloud")
        assert not isinstance(handler, BaseServiceHandler)

    def test_default_handler_not_base_service_handler(self) -> None:
        """DefaultHandler does not inherit from BaseServiceHandler."""
        from core.plugin.base import BaseServiceHandler
        from plugins.confluent_cloud.handlers.default import DefaultHandler

        handler = DefaultHandler(ecosystem="confluent_cloud")
        assert not isinstance(handler, BaseServiceHandler)

    def test_org_wide_handler_no_base_import(self) -> None:
        """org_wide module does not import BaseServiceHandler."""
        import importlib.util
        import ast
        import pathlib

        src = pathlib.Path("src/plugins/confluent_cloud/handlers/org_wide.py").read_text()
        tree = ast.parse(src)
        imports = [node for node in ast.walk(tree) if isinstance(node, ast.ImportFrom | ast.Import)]
        import_strs = [ast.dump(n) for n in imports]
        assert not any("base" in s.lower() for s in import_strs), "org_wide.py should not import from core.plugin.base"

    def test_default_handler_no_base_import(self) -> None:
        """default module does not import BaseServiceHandler."""
        import ast
        import pathlib

        src = pathlib.Path("src/plugins/confluent_cloud/handlers/default.py").read_text()
        tree = ast.parse(src)
        imports = [node for node in ast.walk(tree) if isinstance(node, ast.ImportFrom | ast.Import)]
        import_strs = [ast.dump(n) for n in imports]
        assert not any("base" in s.lower() for s in import_strs), "default.py should not import from core.plugin.base"
