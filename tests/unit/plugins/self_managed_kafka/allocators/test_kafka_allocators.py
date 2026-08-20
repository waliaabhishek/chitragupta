"""Regression tests for self-managed Kafka allocator exports."""

from __future__ import annotations

import pytest


@pytest.mark.parametrize(
    "name",
    [
        "self_kafka_compute_allocator",
        "self_kafka_storage_allocator",
        "self_kafka_network_ingress_allocator",
        "self_kafka_network_egress_allocator",
    ],
)
def test_removed_principal_usage_allocator_exports_are_not_available(name: str) -> None:
    with pytest.raises(ModuleNotFoundError):
        __import__("plugins.self_managed_kafka.allocators.kafka_allocators", fromlist=[name])
