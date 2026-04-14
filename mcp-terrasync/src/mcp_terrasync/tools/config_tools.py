"""System configuration tools."""

from __future__ import annotations

import json
from typing import Any

from mcp.server.fastmcp import FastMCP

from ..client import APIError, TerrasyncClient


def register(mcp: FastMCP, client: TerrasyncClient) -> None:

    @mcp.tool()
    async def config_manage(
        action: str,
        updates: list[dict[str, Any]] | None = None,
    ) -> str:
        """Get or update system configuration.

        Args:
            action: One of "get", "update".
            updates: Array of {key, value} pairs (required for "update").
                Example: [{"key": "database.enabled", "value": true}]
        """
        try:
            if action == "get":
                result = await client.get("/config")
                return json.dumps(result, ensure_ascii=False, indent=2)
            elif action == "update":
                if not updates:
                    return "Error: 'updates' is required for update action."
                result = await client.put("/config", json_body={"updates": updates})
                return json.dumps(result, ensure_ascii=False, indent=2)
            else:
                return f"Error: Unknown action '{action}'. Use 'get' or 'update'."
        except APIError as e:
            return f"Error: {e}"

    @mcp.tool()
    async def config_test_clickhouse(
        dsn: str,
        username: str,
        password: str,
        database: str,
    ) -> str:
        """Test ClickHouse database connectivity.

        Args:
            dsn: ClickHouse DSN (e.g. "http://localhost:8123").
            username: Database username.
            password: Database password.
            database: Database name.
        """
        try:
            result = await client.post(
                "/config/test-clickhouse",
                json_body={
                    "dsn": dsn,
                    "username": username,
                    "password": password,
                    "database": database,
                },
            )
            return json.dumps(result, ensure_ascii=False, indent=2)
        except APIError as e:
            return f"Error: {e}"
