"""Shared fixtures for handler tests."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest


@pytest.fixture
def mock_uow() -> MagicMock:
    """Mock UnitOfWork with identities repository."""
    uow = MagicMock()
    uow.identities = MagicMock()
    uow.identities.find_by_period.return_value = []
    return uow
