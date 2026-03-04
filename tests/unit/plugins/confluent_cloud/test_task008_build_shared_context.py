"""TASK-008: ConfluentCloudPlugin.build_shared_context() tests.

Verifies that build_shared_context() is a new method that gathers environments
and Kafka clusters once, returning a CCloudSharedContext. Also tests ordering
independence of handlers.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch


class TestConfluentCloudPluginBuildSharedContext:
    """build_shared_context() gathers envs + clusters and returns CCloudSharedContext."""

    def test_returns_ccloud_shared_context_type(self) -> None:
        """build_shared_context returns a CCloudSharedContext instance."""
        from plugins.confluent_cloud import ConfluentCloudPlugin
        from plugins.confluent_cloud.shared_context import CCloudSharedContext

        plugin = ConfluentCloudPlugin()
        plugin.initialize({"ccloud_api": {"key": "k", "secret": "s"}})

        mock_env = MagicMock()
        mock_env.resource_id = "env-1"
        mock_cluster = MagicMock()
        mock_cluster.resource_id = "lkc-1"

        with (
            patch(
                "plugins.confluent_cloud.gathering.gather_environments",
                return_value=[mock_env],
            ),
            patch(
                "plugins.confluent_cloud.gathering.gather_kafka_clusters",
                return_value=[mock_cluster],
            ),
        ):
            result = plugin.build_shared_context("org-123")

        assert isinstance(result, CCloudSharedContext)

    def test_environment_resources_are_tuple(self) -> None:
        """build_shared_context returns context where environment_resources is a tuple."""
        from plugins.confluent_cloud import ConfluentCloudPlugin
        from plugins.confluent_cloud.shared_context import CCloudSharedContext

        plugin = ConfluentCloudPlugin()
        plugin.initialize({"ccloud_api": {"key": "k", "secret": "s"}})

        mock_env = MagicMock()
        mock_env.resource_id = "env-1"

        with (
            patch(
                "plugins.confluent_cloud.gathering.gather_environments",
                return_value=[mock_env],
            ),
            patch(
                "plugins.confluent_cloud.gathering.gather_kafka_clusters",
                return_value=[],
            ),
        ):
            result = plugin.build_shared_context("org-123")

        assert isinstance(result, CCloudSharedContext)
        assert isinstance(result.environment_resources, tuple)
        assert len(result.environment_resources) == 1

    def test_kafka_cluster_resources_are_tuple(self) -> None:
        """build_shared_context returns context where kafka_cluster_resources is a tuple."""
        from plugins.confluent_cloud import ConfluentCloudPlugin
        from plugins.confluent_cloud.shared_context import CCloudSharedContext

        plugin = ConfluentCloudPlugin()
        plugin.initialize({"ccloud_api": {"key": "k", "secret": "s"}})

        mock_env = MagicMock()
        mock_env.resource_id = "env-1"
        mock_cluster = MagicMock()
        mock_cluster.resource_id = "lkc-1"

        with (
            patch(
                "plugins.confluent_cloud.gathering.gather_environments",
                return_value=[mock_env],
            ),
            patch(
                "plugins.confluent_cloud.gathering.gather_kafka_clusters",
                return_value=[mock_cluster],
            ),
        ):
            result = plugin.build_shared_context("org-123")

        assert isinstance(result, CCloudSharedContext)
        assert isinstance(result.kafka_cluster_resources, tuple)
        assert len(result.kafka_cluster_resources) == 1

    def test_calls_gather_environments_with_tenant_id(self) -> None:
        """build_shared_context calls gather_environments with the given tenant_id."""
        from plugins.confluent_cloud import ConfluentCloudPlugin

        plugin = ConfluentCloudPlugin()
        plugin.initialize({"ccloud_api": {"key": "k", "secret": "s"}})

        with (
            patch(
                "plugins.confluent_cloud.gathering.gather_environments",
                return_value=[],
            ) as mock_envs,
            patch(
                "plugins.confluent_cloud.gathering.gather_kafka_clusters",
                return_value=[],
            ),
        ):
            plugin.build_shared_context("org-999")

        mock_envs.assert_called_once()
        assert "org-999" in mock_envs.call_args[0]

    def test_calls_gather_kafka_clusters_with_env_ids(self) -> None:
        """build_shared_context passes env_ids to gather_kafka_clusters."""
        from plugins.confluent_cloud import ConfluentCloudPlugin

        plugin = ConfluentCloudPlugin()
        plugin.initialize({"ccloud_api": {"key": "k", "secret": "s"}})

        mock_env = MagicMock()
        mock_env.resource_id = "env-abc"

        with (
            patch(
                "plugins.confluent_cloud.gathering.gather_environments",
                return_value=[mock_env],
            ),
            patch(
                "plugins.confluent_cloud.gathering.gather_kafka_clusters",
                return_value=[],
            ) as mock_clusters,
        ):
            plugin.build_shared_context("org-123")

        mock_clusters.assert_called_once()
        env_ids_arg = mock_clusters.call_args[0][3]
        assert "env-abc" in env_ids_arg

    def test_returns_none_when_connection_is_none(self) -> None:
        """build_shared_context returns None when no connection is available."""
        from plugins.confluent_cloud import ConfluentCloudPlugin

        plugin = ConfluentCloudPlugin()
        plugin.initialize({"ccloud_api": {"key": "k", "secret": "s"}})
        plugin._connection = None

        result = plugin.build_shared_context("org-123")
        assert result is None

    def test_context_contains_correct_resources(self) -> None:
        """CCloudSharedContext fields match the resources returned by gathering functions."""
        from plugins.confluent_cloud import ConfluentCloudPlugin
        from plugins.confluent_cloud.shared_context import CCloudSharedContext

        plugin = ConfluentCloudPlugin()
        plugin.initialize({"ccloud_api": {"key": "k", "secret": "s"}})

        env1 = MagicMock()
        env1.resource_id = "env-1"
        env2 = MagicMock()
        env2.resource_id = "env-2"
        cluster = MagicMock()
        cluster.resource_id = "lkc-1"

        with (
            patch(
                "plugins.confluent_cloud.gathering.gather_environments",
                return_value=[env1, env2],
            ),
            patch(
                "plugins.confluent_cloud.gathering.gather_kafka_clusters",
                return_value=[cluster],
            ),
        ):
            result = plugin.build_shared_context("org-123")

        assert isinstance(result, CCloudSharedContext)
        assert len(result.environment_resources) == 2
        assert len(result.kafka_cluster_resources) == 1
        env_ids = [r.resource_id for r in result.environment_resources]
        assert "env-1" in env_ids
        assert "env-2" in env_ids
        assert result.kafka_cluster_resources[0].resource_id == "lkc-1"


class TestConfluentCloudPluginHandlerOrderingIndependence:
    """TASK-008 integration: Handler ordering no longer affects correctness."""

    def test_connectors_gathered_regardless_of_handler_dict_order(self) -> None:
        """With shared_ctx, reversing handler order still produces correct connector gather."""
        from plugins.confluent_cloud import ConfluentCloudPlugin
        from plugins.confluent_cloud.shared_context import CCloudSharedContext

        plugin = ConfluentCloudPlugin()
        plugin.initialize({"ccloud_api": {"key": "k", "secret": "s"}})

        mock_env = MagicMock()
        mock_env.resource_id = "env-1"
        mock_cluster = MagicMock()
        mock_cluster.resource_id = "lkc-1"
        mock_cluster.parent_id = "env-1"

        # Simulate: connector handler runs before kafka handler.
        # With shared_ctx, it should still receive cluster info.
        ctx = CCloudSharedContext(
            environment_resources=(mock_env,),
            kafka_cluster_resources=(mock_cluster,),
        )

        from plugins.confluent_cloud.handlers.connectors import ConnectorHandler

        mock_conn = MagicMock()
        handler = ConnectorHandler(connection=mock_conn, config=None, ecosystem="confluent_cloud")

        connector_resource = MagicMock()
        mock_uow = MagicMock()

        with patch(
            "plugins.confluent_cloud.gathering.gather_connectors",
            return_value=[connector_resource],
        ) as mock_gather:
            result = list(handler.gather_resources("org-123", mock_uow, ctx))

        # Even without KafkaHandler having run first, connectors are gathered correctly
        mock_gather.assert_called_once()
        clusters_arg = list(mock_gather.call_args[0][3])
        assert ("env-1", "lkc-1") in clusters_arg
        assert result == [connector_resource]
