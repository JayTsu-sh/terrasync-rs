"""Execution history tools."""

from __future__ import annotations

import json

from mcp.server.fastmcp import FastMCP

from ..client import APIError, TerrasyncClient


def register(mcp: FastMCP, client: TerrasyncClient) -> None:

    @mcp.tool()
    async def execution_query(
        action: str,
        task_id: str | None = None,
        execution_id: str | None = None,
        level: str | None = None,
        offset: int = 0,
        limit: int = 100,
    ) -> str:
        """Query execution records and logs for a task.

        Args:
            action: One of "list", "detail", "logs".
            task_id: Task ID (required for "list").
            execution_id: Execution ID (required for "detail" and "logs").
            level: Log level filter for "logs": "info", "warn", "error".
            offset: Log offset for pagination (default 0).
            limit: Log limit for pagination (default 100).
        """
        try:
            if action == "list":
                if not task_id:
                    return "Error: 'task_id' is required for list action."
                result = await client.get(f"/tasks/{task_id}/executions")
                return json.dumps(result, ensure_ascii=False, indent=2)
            elif action == "detail":
                if not execution_id:
                    return "Error: 'execution_id' is required for detail action."
                result = await client.get(f"/executions/{execution_id}")
                return json.dumps(result, ensure_ascii=False, indent=2)
            elif action == "logs":
                if not execution_id:
                    return "Error: 'execution_id' is required for logs action."
                params: dict[str, str | int] = {"offset": offset, "limit": limit}
                if level:
                    params["level"] = level
                result = await client.get(f"/executions/{execution_id}/logs", params=params)
                return json.dumps(result, ensure_ascii=False, indent=2)
            else:
                return f"Error: Unknown action '{action}'. Use 'list', 'detail', or 'logs'."
        except APIError as e:
            return f"Error: {e}"
