"""Kafka Admin API-based resource discovery for self-managed Kafka.

kafka-python is an optional dependency. It is only imported when
resource_source.source="admin_api" is configured.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from core.logging_context import safe_exception_context, safe_log_context
from core.models import CoreResource, Resource

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
    logger.info(
        "provider_client_started provider=self_managed_kafka_admin%s",
        safe_log_context(stage="admin_client", operation="create_admin_client", outcome="started"),
    )
    try:
        # kafka-python is an optional dependency with no type stubs.
        # --ignore-missing-imports suppresses mypy errors for this import globally.
        from kafka import KafkaAdminClient
    except ImportError as exc:
        logger.error(
            "provider_client_failed provider=self_managed_kafka_admin%s",
            safe_log_context(
                stage="admin_client",
                operation="create_admin_client",
                outcome="failed",
                retryable=False,
                **safe_exception_context(exc),
            ),
        )
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

    try:
        client = KafkaAdminClient(**client_config)
    except Exception as exc:
        logger.error(
            "provider_client_failed provider=self_managed_kafka_admin%s",
            safe_log_context(
                stage="admin_client",
                operation="create_admin_client",
                outcome="failed",
                retryable=True,
                **safe_exception_context(exc),
            ),
        )
        raise
    logger.info(
        "provider_client_completed provider=self_managed_kafka_admin%s",
        safe_log_context(stage="admin_client", operation="create_admin_client", outcome="completed"),
    )
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
    logger.info(
        "provider_gather_started provider=self_managed_kafka_admin cluster_id=%s resource_type=broker%s",
        cluster_id,
        safe_log_context(
            tenant_id=tenant_id,
            ecosystem=ecosystem,
            stage="admin_gather",
            operation="gather_brokers",
            outcome="started",
        ),
    )
    try:
        cluster_metadata = admin_client.describe_cluster()
        brokers = cluster_metadata.get("brokers", [])
        for broker in brokers:
            broker_id = str(broker.get("node_id", broker.get("id", "unknown")))
            host = broker.get("host", "")
            port = broker.get("port", "")
            display_name = f"{host}:{port}" if host and port else broker_id
            yield CoreResource(
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
        logger.info(
            "provider_gather_completed provider=self_managed_kafka_admin cluster_id=%s resource_type=broker count=%d%s",
            cluster_id,
            len(brokers),
            safe_log_context(
                tenant_id=tenant_id,
                ecosystem=ecosystem,
                stage="admin_gather",
                operation="gather_brokers",
                outcome="completed",
            ),
        )
    except (OSError, RuntimeError, KeyError, TypeError) as exc:
        # Broad catch: kafka-python raises various errors (NoBrokersAvailable, AuthFailed, etc.)
        # that are not exported from a single base class; wrap for consistent interface.
        logger.error(
            "provider_gather_failed provider=self_managed_kafka_admin cluster_id=%s resource_type=broker%s",
            cluster_id,
            safe_log_context(
                tenant_id=tenant_id,
                ecosystem=ecosystem,
                stage="admin_gather",
                operation="gather_brokers",
                outcome="failed",
                retryable=True,
                **safe_exception_context(exc),
            ),
        )
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
    logger.info(
        "provider_gather_started provider=self_managed_kafka_admin cluster_id=%s resource_type=topic%s",
        cluster_id,
        safe_log_context(
            tenant_id=tenant_id,
            ecosystem=ecosystem,
            stage="admin_gather",
            operation="gather_topics",
            outcome="started",
        ),
    )
    try:
        topics = admin_client.list_topics()
        for topic_name in topics:
            yield CoreResource(
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
        logger.info(
            "provider_gather_completed provider=self_managed_kafka_admin cluster_id=%s resource_type=topic count=%d%s",
            cluster_id,
            len(topics),
            safe_log_context(
                tenant_id=tenant_id,
                ecosystem=ecosystem,
                stage="admin_gather",
                operation="gather_topics",
                outcome="completed",
            ),
        )
    except (OSError, RuntimeError, PermissionError, TypeError) as exc:
        # Broad catch: kafka-python raises various errors (NoBrokersAvailable, AuthFailed, etc.)
        # that are not exported from a single base class; wrap for consistent interface.
        logger.error(
            "provider_gather_failed provider=self_managed_kafka_admin cluster_id=%s resource_type=topic%s",
            cluster_id,
            safe_log_context(
                tenant_id=tenant_id,
                ecosystem=ecosystem,
                stage="admin_gather",
                operation="gather_topics",
                outcome="failed",
                retryable=True,
                **safe_exception_context(exc),
            ),
        )
        raise RuntimeError(f"Failed to gather topics from Kafka Admin API: {exc}") from exc
