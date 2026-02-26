"""Tests for Kafka Admin API-based resource discovery."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


class TestCreateAdminClient:
    def test_import_error_when_kafka_python_not_installed(self):
        from plugins.self_managed_kafka.config import ResourceSourceConfig
        from plugins.self_managed_kafka.gathering.admin_api import create_admin_client

        config = ResourceSourceConfig.model_validate({"source": "admin_api", "bootstrap_servers": "kafka:9092"})

        with (
            patch("builtins.__import__", side_effect=ImportError("No module named 'kafka'")),
            pytest.raises(ImportError, match="kafka-python is required"),
        ):
            create_admin_client(config)

    def test_creates_client_with_minimal_config(self):
        from plugins.self_managed_kafka.config import ResourceSourceConfig
        from plugins.self_managed_kafka.gathering.admin_api import create_admin_client

        config = ResourceSourceConfig.model_validate({"source": "admin_api", "bootstrap_servers": "kafka:9092"})

        mock_client_class = MagicMock()
        mock_client_instance = MagicMock()
        mock_client_class.return_value = mock_client_instance

        with patch.dict("sys.modules", {"kafka": MagicMock(KafkaAdminClient=mock_client_class)}):
            create_admin_client(config)

        mock_client_class.assert_called_once()
        call_kwargs = mock_client_class.call_args.kwargs
        assert call_kwargs["bootstrap_servers"] == "kafka:9092"

    def test_creates_client_with_sasl_config(self):
        from plugins.self_managed_kafka.config import ResourceSourceConfig
        from plugins.self_managed_kafka.gathering.admin_api import create_admin_client

        config = ResourceSourceConfig.model_validate(
            {
                "source": "admin_api",
                "bootstrap_servers": "kafka:9092",
                "security_protocol": "SASL_SSL",
                "sasl_mechanism": "SCRAM-SHA-256",
                "sasl_username": "admin",
                "sasl_password": "secret",
            }
        )

        mock_client_class = MagicMock()
        mock_client_class.return_value = MagicMock()

        with patch.dict("sys.modules", {"kafka": MagicMock(KafkaAdminClient=mock_client_class)}):
            create_admin_client(config)

        call_kwargs = mock_client_class.call_args.kwargs
        assert call_kwargs["sasl_mechanism"] == "SCRAM-SHA-256"
        assert call_kwargs["sasl_plain_username"] == "admin"
        assert call_kwargs["sasl_plain_password"] == "secret"


class TestGatherBrokersFromAdmin:
    def test_discovers_brokers_from_cluster_metadata(self):
        from plugins.self_managed_kafka.gathering.admin_api import gather_brokers_from_admin

        mock_client = MagicMock()
        mock_client.describe_cluster.return_value = {
            "brokers": [
                {"node_id": 0, "host": "kafka-1", "port": 9092},
                {"node_id": 1, "host": "kafka-2", "port": 9092},
            ]
        }

        brokers = list(gather_brokers_from_admin(mock_client, "self_managed_kafka", "t1", "cluster-001"))
        assert len(brokers) == 2
        ids = {b.resource_id for b in brokers}
        assert "cluster-001:broker:0" in ids
        assert "cluster-001:broker:1" in ids

    def test_broker_parent_id_is_cluster(self):
        from plugins.self_managed_kafka.gathering.admin_api import gather_brokers_from_admin

        mock_client = MagicMock()
        mock_client.describe_cluster.return_value = {"brokers": [{"node_id": 0, "host": "kafka-1", "port": 9092}]}

        brokers = list(gather_brokers_from_admin(mock_client, "self_managed_kafka", "t1", "cluster-001"))
        assert brokers[0].parent_id == "cluster-001"
        assert brokers[0].resource_type == "broker"

    def test_connection_error_raises_runtime_error(self):
        from plugins.self_managed_kafka.gathering.admin_api import gather_brokers_from_admin

        mock_client = MagicMock()
        mock_client.describe_cluster.side_effect = ConnectionError("Connection refused")

        with pytest.raises(RuntimeError, match="Failed to gather brokers"):
            list(gather_brokers_from_admin(mock_client, "self_managed_kafka", "t1", "cluster-001"))


class TestGatherTopicsFromAdmin:
    def test_discovers_topics_from_list(self):
        from plugins.self_managed_kafka.gathering.admin_api import gather_topics_from_admin

        mock_client = MagicMock()
        mock_client.list_topics.return_value = ["orders", "payments", "inventory"]

        topics = list(gather_topics_from_admin(mock_client, "self_managed_kafka", "t1", "cluster-001"))
        assert len(topics) == 3
        ids = {t.resource_id for t in topics}
        assert "cluster-001:topic:orders" in ids

    def test_topic_resource_type(self):
        from plugins.self_managed_kafka.gathering.admin_api import gather_topics_from_admin

        mock_client = MagicMock()
        mock_client.list_topics.return_value = ["orders"]

        topics = list(gather_topics_from_admin(mock_client, "self_managed_kafka", "t1", "cluster-001"))
        assert topics[0].resource_type == "topic"
        assert topics[0].parent_id == "cluster-001"

    def test_auth_failure_raises_runtime_error(self):
        from plugins.self_managed_kafka.gathering.admin_api import gather_topics_from_admin

        mock_client = MagicMock()
        mock_client.list_topics.side_effect = PermissionError("Authorization failed")

        with pytest.raises(RuntimeError, match="Failed to gather topics"):
            list(gather_topics_from_admin(mock_client, "self_managed_kafka", "t1", "cluster-001"))

    def test_empty_topic_list(self):
        from plugins.self_managed_kafka.gathering.admin_api import gather_topics_from_admin

        mock_client = MagicMock()
        mock_client.list_topics.return_value = []

        topics = list(gather_topics_from_admin(mock_client, "self_managed_kafka", "t1", "cluster-001"))
        assert topics == []
