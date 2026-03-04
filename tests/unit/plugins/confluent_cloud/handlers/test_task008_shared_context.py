"""TASK-008: Two-Phase Handler Gather — CCloud handler tests.

Tests verify that each CCloud handler's gather_resources() uses shared_ctx
instead of querying UoW or calling gathering functions directly.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from core.models import Resource, ResourceStatus


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_uow() -> MagicMock:
    """Mock UnitOfWork — should NOT be queried in TASK-008 handlers."""
    uow = MagicMock()
    uow.resources = MagicMock()
    uow.resources.find_by_period.return_value = ([], 0)
    return uow


def _make_env_resource(env_id: str = "env-1") -> Resource:
    return Resource(
        ecosystem="confluent_cloud",
        tenant_id="org-123",
        resource_id=env_id,
        resource_type="environment",
        status=ResourceStatus.ACTIVE,
        metadata={},
    )


def _make_cluster_resource(cluster_id: str = "lkc-1", parent_id: str = "env-1") -> Resource:
    return Resource(
        ecosystem="confluent_cloud",
        tenant_id="org-123",
        resource_id=cluster_id,
        resource_type="kafka_cluster",
        status=ResourceStatus.ACTIVE,
        parent_id=parent_id,
        metadata={},
    )


@pytest.fixture
def ccloud_shared_ctx():
    """A CCloudSharedContext with one environment and one cluster."""
    from plugins.confluent_cloud.shared_context import CCloudSharedContext

    env = _make_env_resource("env-1")
    cluster = _make_cluster_resource("lkc-1", parent_id="env-1")
    return CCloudSharedContext(
        environment_resources=(env,),
        kafka_cluster_resources=(cluster,),
    )


# ---------------------------------------------------------------------------
# CCloudSharedContext dataclass
# ---------------------------------------------------------------------------


class TestCCloudSharedContext:
    """Unit tests for the CCloudSharedContext dataclass itself."""

    def test_env_ids_property(self) -> None:
        """env_ids returns resource_id for each environment_resource."""
        from plugins.confluent_cloud.shared_context import CCloudSharedContext

        env1 = _make_env_resource("env-aaa")
        env2 = _make_env_resource("env-bbb")
        ctx = CCloudSharedContext(
            environment_resources=(env1, env2),
            kafka_cluster_resources=(),
        )
        assert ctx.env_ids == ["env-aaa", "env-bbb"]

    def test_kafka_cluster_pairs_property(self) -> None:
        """kafka_cluster_pairs returns (parent_id, resource_id) for each cluster."""
        from plugins.confluent_cloud.shared_context import CCloudSharedContext

        cluster = _make_cluster_resource("lkc-1", parent_id="env-1")
        ctx = CCloudSharedContext(
            environment_resources=(),
            kafka_cluster_resources=(cluster,),
        )
        assert ctx.kafka_cluster_pairs == [("env-1", "lkc-1")]

    def test_fields_are_tuples_not_lists(self) -> None:
        """environment_resources and kafka_cluster_resources are tuples (frozen=True enforces immutability)."""
        from plugins.confluent_cloud.shared_context import CCloudSharedContext

        ctx = CCloudSharedContext(environment_resources=(), kafka_cluster_resources=())
        assert isinstance(ctx.environment_resources, tuple)
        assert isinstance(ctx.kafka_cluster_resources, tuple)

    def test_is_frozen(self) -> None:
        """CCloudSharedContext is frozen — assignment raises AttributeError."""
        from plugins.confluent_cloud.shared_context import CCloudSharedContext

        ctx = CCloudSharedContext(environment_resources=(), kafka_cluster_resources=())
        with pytest.raises(AttributeError):
            ctx.environment_resources = ()  # type: ignore[misc]


# ---------------------------------------------------------------------------
# KafkaHandler.gather_resources — shared_ctx path
# ---------------------------------------------------------------------------


class TestKafkaHandlerGatherResourcesSharedContext:
    """TASK-008: KafkaHandler yields from shared_ctx without calling API."""

    def test_yields_environment_then_cluster_resources(self, mock_uow: MagicMock, ccloud_shared_ctx: object) -> None:
        """gather_resources yields environment_resources then kafka_cluster_resources."""
        from plugins.confluent_cloud.handlers.kafka import KafkaHandler

        mock_conn = MagicMock()
        handler = KafkaHandler(connection=mock_conn, config=None, ecosystem="confluent_cloud")

        result = list(handler.gather_resources("org-123", mock_uow, ccloud_shared_ctx))

        assert len(result) == 2
        assert result[0].resource_type == "environment"
        assert result[1].resource_type == "kafka_cluster"
        assert result[0].resource_id == "env-1"
        assert result[1].resource_id == "lkc-1"

    def test_does_not_call_gather_environments_directly(self, mock_uow: MagicMock, ccloud_shared_ctx: object) -> None:
        """gather_resources must NOT call gather_environments() — data comes from shared_ctx."""
        from plugins.confluent_cloud.handlers.kafka import KafkaHandler

        mock_conn = MagicMock()
        handler = KafkaHandler(connection=mock_conn, config=None, ecosystem="confluent_cloud")

        with patch("plugins.confluent_cloud.gathering.gather_environments") as mock_envs:
            list(handler.gather_resources("org-123", mock_uow, ccloud_shared_ctx))

        mock_envs.assert_not_called()

    def test_does_not_call_gather_kafka_clusters_directly(self, mock_uow: MagicMock, ccloud_shared_ctx: object) -> None:
        """gather_resources must NOT call gather_kafka_clusters() — data comes from shared_ctx."""
        from plugins.confluent_cloud.handlers.kafka import KafkaHandler

        mock_conn = MagicMock()
        handler = KafkaHandler(connection=mock_conn, config=None, ecosystem="confluent_cloud")

        with patch("plugins.confluent_cloud.gathering.gather_kafka_clusters") as mock_clusters:
            list(handler.gather_resources("org-123", mock_uow, ccloud_shared_ctx))

        mock_clusters.assert_not_called()

    def test_returns_empty_when_shared_ctx_wrong_type(self, mock_uow: MagicMock) -> None:
        """gather_resources yields nothing when shared_ctx is not CCloudSharedContext."""
        from plugins.confluent_cloud.handlers.kafka import KafkaHandler

        mock_conn = MagicMock()
        handler = KafkaHandler(connection=mock_conn, config=None, ecosystem="confluent_cloud")

        result = list(handler.gather_resources("org-123", mock_uow, shared_ctx=None))
        assert result == []

    def test_returns_empty_when_connection_is_none(self, mock_uow: MagicMock, ccloud_shared_ctx: object) -> None:
        """gather_resources yields nothing when connection is None (even with valid shared_ctx)."""
        from plugins.confluent_cloud.handlers.kafka import KafkaHandler

        handler = KafkaHandler(connection=None, config=None, ecosystem="confluent_cloud")
        result = list(handler.gather_resources("org-123", mock_uow, ccloud_shared_ctx))
        assert result == []


# ---------------------------------------------------------------------------
# ConnectorHandler.gather_resources — shared_ctx path
# ---------------------------------------------------------------------------


class TestConnectorHandlerGatherResourcesSharedContext:
    """TASK-008: ConnectorHandler uses shared_ctx.kafka_cluster_pairs instead of UoW query."""

    def test_calls_gather_connectors_with_cluster_pairs_from_shared_ctx(self, mock_uow: MagicMock) -> None:
        """gather_connectors receives (env_id, cluster_id) pairs from shared_ctx."""
        from plugins.confluent_cloud.handlers.connectors import ConnectorHandler
        from plugins.confluent_cloud.shared_context import CCloudSharedContext

        cluster = _make_cluster_resource("lkc-1", parent_id="env-1")
        ctx = CCloudSharedContext(
            environment_resources=(),
            kafka_cluster_resources=(cluster,),
        )
        mock_conn = MagicMock()
        connector = MagicMock()

        with patch(
            "plugins.confluent_cloud.gathering.gather_connectors",
            return_value=[connector],
        ) as mock_gather:
            handler = ConnectorHandler(connection=mock_conn, config=None, ecosystem="confluent_cloud")
            result = list(handler.gather_resources("org-123", mock_uow, ctx))

        mock_gather.assert_called_once()
        clusters_arg = list(mock_gather.call_args[0][3])
        assert clusters_arg == [("env-1", "lkc-1")]
        assert result == [connector]

    def test_no_uow_find_by_period_called(self, mock_uow: MagicMock) -> None:
        """UoW.resources.find_by_period must NOT be called — clusters come from shared_ctx."""
        from plugins.confluent_cloud.handlers.connectors import ConnectorHandler
        from plugins.confluent_cloud.shared_context import CCloudSharedContext

        cluster = _make_cluster_resource("lkc-1", parent_id="env-1")
        ctx = CCloudSharedContext(
            environment_resources=(),
            kafka_cluster_resources=(cluster,),
        )
        mock_conn = MagicMock()

        with patch("plugins.confluent_cloud.gathering.gather_connectors", return_value=[]):
            handler = ConnectorHandler(connection=mock_conn, config=None, ecosystem="confluent_cloud")
            list(handler.gather_resources("org-123", mock_uow, ctx))

        mock_uow.resources.find_by_period.assert_not_called()

    def test_returns_empty_when_shared_ctx_wrong_type(self, mock_uow: MagicMock) -> None:
        """gather_resources yields nothing when shared_ctx is not CCloudSharedContext."""
        from plugins.confluent_cloud.handlers.connectors import ConnectorHandler

        mock_conn = MagicMock()
        handler = ConnectorHandler(connection=mock_conn, config=None, ecosystem="confluent_cloud")

        result = list(handler.gather_resources("org-123", mock_uow, shared_ctx=None))
        assert result == []

    def test_multiple_clusters_all_passed_to_gather_connectors(self, mock_uow: MagicMock) -> None:
        """All clusters from shared_ctx are passed to gather_connectors."""
        from plugins.confluent_cloud.handlers.connectors import ConnectorHandler
        from plugins.confluent_cloud.shared_context import CCloudSharedContext

        cluster1 = _make_cluster_resource("lkc-001", parent_id="env-1")
        cluster2 = _make_cluster_resource("lkc-002", parent_id="env-2")
        ctx = CCloudSharedContext(
            environment_resources=(),
            kafka_cluster_resources=(cluster1, cluster2),
        )
        mock_conn = MagicMock()

        with patch("plugins.confluent_cloud.gathering.gather_connectors", return_value=[]) as mock_gather:
            handler = ConnectorHandler(connection=mock_conn, config=None, ecosystem="confluent_cloud")
            list(handler.gather_resources("org-123", mock_uow, ctx))

        clusters_arg = list(mock_gather.call_args[0][3])
        assert ("env-1", "lkc-001") in clusters_arg
        assert ("env-2", "lkc-002") in clusters_arg


# ---------------------------------------------------------------------------
# SchemaRegistryHandler.gather_resources — shared_ctx path
# ---------------------------------------------------------------------------


class TestSchemaRegistryHandlerGatherResourcesSharedContext:
    """TASK-008: SchemaRegistryHandler uses shared_ctx.env_ids — no redundant API call."""

    def test_calls_gather_schema_registries_with_env_ids_from_shared_ctx(self, mock_uow: MagicMock) -> None:
        """gather_schema_registries receives env_ids derived from shared_ctx.environment_resources."""
        from plugins.confluent_cloud.handlers.schema_registry import SchemaRegistryHandler
        from plugins.confluent_cloud.shared_context import CCloudSharedContext

        env = _make_env_resource("env-1")
        ctx = CCloudSharedContext(
            environment_resources=(env,),
            kafka_cluster_resources=(),
        )
        mock_conn = MagicMock()
        sr_resource = MagicMock()

        with patch(
            "plugins.confluent_cloud.gathering.gather_schema_registries",
            return_value=[sr_resource],
        ) as mock_gather:
            from plugins.confluent_cloud.handlers.schema_registry import SchemaRegistryHandler

            handler = SchemaRegistryHandler(connection=mock_conn, config=None, ecosystem="confluent_cloud")
            result = list(handler.gather_resources("org-123", mock_uow, ctx))

        mock_gather.assert_called_once()
        env_ids_arg = mock_gather.call_args[0][3]
        assert env_ids_arg == ["env-1"]
        assert result == [sr_resource]

    def test_does_not_call_gather_environments(self, mock_uow: MagicMock) -> None:
        """gather_environments must NOT be called — eliminates the redundant API round-trip."""
        from plugins.confluent_cloud.handlers.schema_registry import SchemaRegistryHandler
        from plugins.confluent_cloud.shared_context import CCloudSharedContext

        env = _make_env_resource("env-1")
        ctx = CCloudSharedContext(
            environment_resources=(env,),
            kafka_cluster_resources=(),
        )
        mock_conn = MagicMock()

        with (
            patch("plugins.confluent_cloud.gathering.gather_environments") as mock_envs,
            patch("plugins.confluent_cloud.gathering.gather_schema_registries", return_value=[]),
        ):
            from plugins.confluent_cloud.handlers.schema_registry import SchemaRegistryHandler

            handler = SchemaRegistryHandler(connection=mock_conn, config=None, ecosystem="confluent_cloud")
            list(handler.gather_resources("org-123", mock_uow, ctx))

        mock_envs.assert_not_called()

    def test_returns_empty_when_shared_ctx_wrong_type(self, mock_uow: MagicMock) -> None:
        """gather_resources yields nothing when shared_ctx is not CCloudSharedContext."""
        from plugins.confluent_cloud.handlers.schema_registry import SchemaRegistryHandler

        mock_conn = MagicMock()
        handler = SchemaRegistryHandler(connection=mock_conn, config=None, ecosystem="confluent_cloud")
        result = list(handler.gather_resources("org-123", mock_uow, shared_ctx=None))
        assert result == []


# ---------------------------------------------------------------------------
# KsqldbHandler.gather_resources — shared_ctx path
# ---------------------------------------------------------------------------


class TestKsqldbHandlerGatherResourcesSharedContext:
    """TASK-008: KsqldbHandler uses shared_ctx.env_ids instead of UoW query."""

    def test_calls_gather_ksqldb_clusters_with_env_ids_from_shared_ctx(self, mock_uow: MagicMock) -> None:
        """gather_ksqldb_clusters receives env_ids from shared_ctx."""
        from plugins.confluent_cloud.handlers.ksqldb import KsqldbHandler
        from plugins.confluent_cloud.shared_context import CCloudSharedContext

        env = _make_env_resource("env-1")
        ctx = CCloudSharedContext(
            environment_resources=(env,),
            kafka_cluster_resources=(),
        )
        mock_conn = MagicMock()
        ksqldb_resource = MagicMock()

        with patch(
            "plugins.confluent_cloud.gathering.gather_ksqldb_clusters",
            return_value=[ksqldb_resource],
        ) as mock_gather:
            handler = KsqldbHandler(connection=mock_conn, config=None, ecosystem="confluent_cloud")
            result = list(handler.gather_resources("org-123", mock_uow, ctx))

        mock_gather.assert_called_once()
        env_ids_arg = mock_gather.call_args[0][3]
        assert env_ids_arg == ["env-1"]
        assert result == [ksqldb_resource]

    def test_no_uow_find_by_period_called(self, mock_uow: MagicMock) -> None:
        """UoW.resources.find_by_period must NOT be called — env_ids come from shared_ctx."""
        from plugins.confluent_cloud.handlers.ksqldb import KsqldbHandler
        from plugins.confluent_cloud.shared_context import CCloudSharedContext

        env = _make_env_resource("env-1")
        ctx = CCloudSharedContext(
            environment_resources=(env,),
            kafka_cluster_resources=(),
        )
        mock_conn = MagicMock()

        with patch("plugins.confluent_cloud.gathering.gather_ksqldb_clusters", return_value=[]):
            handler = KsqldbHandler(connection=mock_conn, config=None, ecosystem="confluent_cloud")
            list(handler.gather_resources("org-123", mock_uow, ctx))

        mock_uow.resources.find_by_period.assert_not_called()

    def test_returns_empty_when_shared_ctx_wrong_type(self, mock_uow: MagicMock) -> None:
        """gather_resources yields nothing when shared_ctx is not CCloudSharedContext."""
        from plugins.confluent_cloud.handlers.ksqldb import KsqldbHandler

        mock_conn = MagicMock()
        handler = KsqldbHandler(connection=mock_conn, config=None, ecosystem="confluent_cloud")
        result = list(handler.gather_resources("org-123", mock_uow, shared_ctx=None))
        assert result == []


# ---------------------------------------------------------------------------
# FlinkHandler.gather_resources — shared_ctx path
# ---------------------------------------------------------------------------


class TestFlinkHandlerGatherResourcesSharedContext:
    """TASK-008: FlinkHandler uses shared_ctx.env_ids instead of UoW query."""

    def test_calls_gather_flink_compute_pools_with_env_ids_from_shared_ctx(self, mock_uow: MagicMock) -> None:
        """gather_flink_compute_pools receives env_ids from shared_ctx."""
        from plugins.confluent_cloud.handlers.flink import FlinkHandler
        from plugins.confluent_cloud.shared_context import CCloudSharedContext

        env = _make_env_resource("env-1")
        ctx = CCloudSharedContext(
            environment_resources=(env,),
            kafka_cluster_resources=(),
        )
        mock_conn = MagicMock()

        with (
            patch(
                "plugins.confluent_cloud.gathering.gather_flink_compute_pools",
                return_value=[],
            ) as mock_pools,
            patch(
                "plugins.confluent_cloud.gathering.gather_flink_statements",
                return_value=[],
            ),
        ):
            handler = FlinkHandler(connection=mock_conn, config=None, ecosystem="confluent_cloud")
            list(handler.gather_resources("org-123", mock_uow, ctx))

        mock_pools.assert_called_once()
        env_ids_arg = mock_pools.call_args[0][3]
        assert env_ids_arg == ["env-1"]

    def test_no_uow_find_by_period_called(self, mock_uow: MagicMock) -> None:
        """UoW.resources.find_by_period must NOT be called — env_ids come from shared_ctx."""
        from plugins.confluent_cloud.handlers.flink import FlinkHandler
        from plugins.confluent_cloud.shared_context import CCloudSharedContext

        env = _make_env_resource("env-1")
        ctx = CCloudSharedContext(
            environment_resources=(env,),
            kafka_cluster_resources=(),
        )
        mock_conn = MagicMock()

        with (
            patch("plugins.confluent_cloud.gathering.gather_flink_compute_pools", return_value=[]),
            patch("plugins.confluent_cloud.gathering.gather_flink_statements", return_value=[]),
        ):
            handler = FlinkHandler(connection=mock_conn, config=None, ecosystem="confluent_cloud")
            list(handler.gather_resources("org-123", mock_uow, ctx))

        mock_uow.resources.find_by_period.assert_not_called()

    def test_returns_empty_when_shared_ctx_wrong_type(self, mock_uow: MagicMock) -> None:
        """gather_resources yields nothing when shared_ctx is not CCloudSharedContext."""
        from plugins.confluent_cloud.handlers.flink import FlinkHandler

        mock_conn = MagicMock()
        handler = FlinkHandler(connection=mock_conn, config=None, ecosystem="confluent_cloud")
        result = list(handler.gather_resources("org-123", mock_uow, shared_ctx=None))
        assert result == []

    def test_allocatable_pools_passed_to_gather_flink_statements(self, mock_uow: MagicMock) -> None:
        """gather_flink_statements receives non-empty allocatable_pools when pool region is configured."""
        from unittest.mock import MagicMock, patch

        from plugins.confluent_cloud.config import CCloudFlinkRegionConfig, CCloudPluginConfig
        from plugins.confluent_cloud.handlers.flink import FlinkHandler
        from plugins.confluent_cloud.shared_context import CCloudSharedContext

        env = _make_env_resource("env-1")
        ctx = CCloudSharedContext(
            environment_resources=(env,),
            kafka_cluster_resources=(),
        )
        pool = Resource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="pool-1",
            resource_type="flink_compute_pool",
            status=ResourceStatus.ACTIVE,
            metadata={"region": "us-east-1"},
        )
        mock_conn = MagicMock()
        mock_config = MagicMock(spec=CCloudPluginConfig)
        mock_config.flink = [CCloudFlinkRegionConfig(region_id="us-east-1", key="k", secret="s")]

        with (
            patch(
                "plugins.confluent_cloud.gathering.gather_flink_compute_pools",
                return_value=[pool],
            ),
            patch(
                "plugins.confluent_cloud.gathering.gather_flink_statements",
                return_value=[],
            ) as mock_stmts,
        ):
            handler = FlinkHandler(connection=mock_conn, config=mock_config, ecosystem="confluent_cloud")
            list(handler.gather_resources("org-123", mock_uow, ctx))

        mock_stmts.assert_called_once()
        allocatable_pools_arg = mock_stmts.call_args[0][2]
        assert len(allocatable_pools_arg) == 1
        assert allocatable_pools_arg[0][0].resource_id == "pool-1"
