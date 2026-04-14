"""Task analytics tools."""

from __future__ import annotations

import json

from mcp.server.fastmcp import FastMCP

from ..client import APIError, TerrasyncClient


def register(mcp: FastMCP, client: TerrasyncClient) -> None:

    @mcp.tool()
    async def task_analytics(task_id: str) -> str:
        """Get file type, size, and time distribution analytics for a task.

        Returns analytics data including file_type_distribution, size_distribution,
        and time_distribution. Only available after a scan task completes.

        Args:
            task_id: The task ID to get analytics for.
        """
        try:
            result = await client.get(f"/tasks/{task_id}/analytics")
            return json.dumps(result, ensure_ascii=False, indent=2)
        except APIError as e:
            return f"Error: {e}"
