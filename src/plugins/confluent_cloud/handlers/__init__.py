"""CCloud service handlers."""

from __future__ import annotations

from plugins.confluent_cloud.handlers.kafka import KafkaHandler
from plugins.confluent_cloud.handlers.schema_registry import SchemaRegistryHandler

__all__ = ["KafkaHandler", "SchemaRegistryHandler"]
