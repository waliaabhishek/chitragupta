"""Shared fixtures for handler tests."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest


@pytest.fixture
def mock_uow() -> MagicMock:
    """Mock UnitOfWork with identities and resources repositories."""
    uow = MagicMock()
    uow.identities = MagicMock()
    uow.identities.find_by_period.return_value = ([], 0)
    uow.resources = MagicMock()
    uow.resources.find_by_period.return_value = ([], 0)
    return uow
