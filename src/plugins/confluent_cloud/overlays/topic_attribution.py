"""CCloud-specific MetricQuery definitions for topic attribution discovery."""

from __future__ import annotations

import logging

from core.models.metrics import MetricQuery

logger = logging.getLogger(__name__)

_DISC_BYTES_IN = MetricQuery(
    key="disc_bytes_in",
    query_expression="sum by (kafka_id, topic) (confluent_kafka_server_received_bytes{})",
    label_keys=("kafka_id", "topic"),
    resource_label="kafka_id",
    query_mode="range",
    metadata={"value_type": "delta_gauge"},
)

_DISC_BYTES_OUT = MetricQuery(
    key="disc_bytes_out",
    query_expression="sum by (kafka_id, topic) (confluent_kafka_server_sent_bytes{})",
    label_keys=("kafka_id", "topic"),
    resource_label="kafka_id",
    query_mode="range",
    metadata={"value_type": "delta_gauge"},
)

_DISC_RETAINED = MetricQuery(
    key="disc_retained",
    query_expression="sum by (kafka_id, topic) (confluent_kafka_server_retained_bytes{})",
    label_keys=("kafka_id", "topic"),
    resource_label="kafka_id",
    query_mode="range",
    metadata={"value_type": "gauge"},
)

_DISCOVERY_QUERIES = [_DISC_BYTES_IN, _DISC_BYTES_OUT, _DISC_RETAINED]
