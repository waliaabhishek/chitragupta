"""Kafka Admin API-based resource discovery for self-managed Kafka.

kafka-python is an optional dependency. It is only imported when
resource_source.source="admin_api" is configured.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from core.models import Resource

if TYPE_CHECKING:
    from plugins.self_managed_kafka.config import ResourceSourceConfig

logger = logging.getLogger(__name__)


def create_admin_client(config: ResourceSourceConfig) -> Any:
    """Create Kafka AdminClient from config.

    Returns a KafkaAdminClient instance. kafka-python must be installed.

    Raises:
        ImportError: If kafka-python is not installed.
        ValueError: If config is invalid for admin_api source.
    """
    logger.debug("Creating Kafka admin client bootstrap_servers=%s", config.bootstrap_servers)
    try:
        # kafka-python is an optional dependency with no type stubs.
        # --ignore-missing-imports suppresses mypy errors for this import globally.
        from kafka import KafkaAdminClient
    except ImportError as exc:
        logger.exception("kafka-python not installed — admin_api source unavailable")
        raise ImportError(
            "kafka-python is required for admin_api resource discovery. Install it with: uv add kafka-python"
        ) from exc

    client_config: dict[str, Any] = {
        "bootstrap_servers": config.bootstrap_servers,
        "security_protocol": config.security_protocol,
    }

    if config.sasl_mechanism:
        client_config["sasl_mechanism"] = config.sasl_mechanism
        if config.sasl_username:
            client_config["sasl_plain_username"] = config.sasl_username
        if config.sasl_password:
            client_config["sasl_plain_password"] = config.sasl_password.get_secret_value()

    client = KafkaAdminClient(**client_config)
    logger.info("Kafka admin client connected bootstrap_servers=%s", config.bootstrap_servers)
    return client


def gather_brokers_from_admin(
    admin_client: Any,
    ecosystem: str,
    tenant_id: str,
    cluster_id: str,
) -> Iterable[Resource]:
    """Query Kafka Admin API for broker metadata → Resource objects.

    Uses describe_cluster() to get broker information.
    """
    try:
        cluster_metadata = admin_client.describe_cluster()
        brokers = cluster_metadata.get("brokers", [])
        for broker in brokers:
            broker_id = str(broker.get("node_id", broker.get("id", "unknown")))
            host = broker.get("host", "")
            port = broker.get("port", "")
            display_name = f"{host}:{port}" if host and port else broker_id
            yield Resource(
                ecosystem=ecosystem,
                tenant_id=tenant_id,
                resource_id=f"{cluster_id}:broker:{broker_id}",
                resource_type="broker",
                display_name=display_name,
                parent_id=cluster_id,
                created_at=None,
                deleted_at=None,
                last_seen_at=datetime.now(UTC),
                metadata={"cluster_id": cluster_id, "broker_id": broker_id},
            )
    except (OSError, RuntimeError, KeyError, TypeError) as exc:
        # Broad catch: kafka-python raises various errors (NoBrokersAvailable, AuthFailed, etc.)
        # that are not exported from a single base class; wrap for consistent interface.
        raise RuntimeError(f"Failed to gather brokers from Kafka Admin API: {exc}") from exc


def gather_topics_from_admin(
    admin_client: Any,
    ecosystem: str,
    tenant_id: str,
    cluster_id: str,
) -> Iterable[Resource]:
    """Query Kafka Admin API for topic list → Resource objects.

    Uses list_topics() to discover all topics.
    """
    try:
        topics = admin_client.list_topics()
        for topic_name in topics:
            yield Resource(
                ecosystem=ecosystem,
                tenant_id=tenant_id,
                resource_id=f"{cluster_id}:topic:{topic_name}",
                resource_type="topic",
                display_name=topic_name,
                parent_id=cluster_id,
                created_at=None,
                deleted_at=None,
                last_seen_at=datetime.now(UTC),
                metadata={"cluster_id": cluster_id},
            )
    except (OSError, RuntimeError, PermissionError, TypeError) as exc:
        # Broad catch: kafka-python raises various errors (NoBrokersAvailable, AuthFailed, etc.)
        # that are not exported from a single base class; wrap for consistent interface.
        raise RuntimeError(f"Failed to gather topics from Kafka Admin API: {exc}") from exc
