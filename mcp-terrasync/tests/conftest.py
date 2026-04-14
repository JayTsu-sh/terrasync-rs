"""Shared test fixtures."""

from __future__ import annotations

import pytest

from mcp_terrasync.client import TerrasyncClient
from mcp_terrasync.config import Settings


@pytest.fixture
def settings() -> Settings:
    return Settings(terrasync_api_url="http://localhost:9999/api/v1")


@pytest.fixture
def client(settings: Settings) -> TerrasyncClient:
    return TerrasyncClient(settings)
