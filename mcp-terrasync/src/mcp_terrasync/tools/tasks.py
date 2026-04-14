"""Task management tools."""

from __future__ import annotations

import json
from typing import Any

from mcp.server.fastmcp import FastMCP

from ..client import APIError, TerrasyncClient


def register(mcp: FastMCP, client: TerrasyncClient) -> None:

    @mcp.tool()
    async def task_list(
        status: str | None = None,
        task_type: str | None = None,
    ) -> str:
        """List migration tasks with optional filters.

        Args:
            status: Filter by status: "pending", "running", "completed", "failed", "cancelled".
            task_type: Filter by type: "scan", "sync", "integrity_check".
        """
        try:
            params: dict[str, str] = {}
            if status:
                params["status"] = status
            if task_type:
                params["task_type"] = task_type
            result = await client.get("/tasks", params=params)
            return json.dumps(result, ensure_ascii=False, indent=2)
        except APIError as e:
            return f"Error: {e}"

    @mcp.tool()
    async def task_manage(
        action: str,
        id: str | None = None,
        name: str | None = None,
        task_type: str | None = None,
        source_endpoint_id: str | None = None,
        source_path_id: str | None = None,
        dest_endpoint_id: str | None = None,
        dest_path_id: str | None = None,
        config: dict[str, Any] | None = None,
    ) -> str:
        """Create or delete a migration task.

        Args:
            action: One of "create", "delete".
            id: Task ID (required for delete).
            name: Task name (required for create).
            task_type: One of "scan", "sync", "integrity_check" (required for create).
            source_endpoint_id: Source endpoint ID (required for create).
            source_path_id: Source path ID (required for create).
            dest_endpoint_id: Destination endpoint ID (required for sync/integrity_check).
            dest_path_id: Destination path ID (required for sync/integrity_check).
            config: Task configuration object with optional fields:
                match_expr, exclude_expr, depth, enable_integrity_check, enable_acl,
                qos (e.g. "1GiB/s"), peak_qos_rate, block_size, iops.
        """
        try:
            if action == "create":
                body: dict[str, Any] = {}
                if name:
                    body["name"] = name
                if task_type:
                    body["task_type"] = task_type
                if source_endpoint_id:
                    body["source_endpoint_id"] = source_endpoint_id
                if source_path_id:
                    body["source_path_id"] = source_path_id
                if dest_endpoint_id:
                    body["dest_endpoint_id"] = dest_endpoint_id
                if dest_path_id:
                    body["dest_path_id"] = dest_path_id
                if config:
                    body["config"] = config
                result = await client.post("/tasks", json_body=body)
                return json.dumps(result, ensure_ascii=False, indent=2)
            elif action == "delete":
                if not id:
                    return "Error: 'id' is required for delete action."
                await client.delete(f"/tasks/{id}")
                return f"Task {id} deleted successfully."
            else:
                return f"Error: Unknown action '{action}'. Use 'create' or 'delete'."
        except APIError as e:
            return f"Error: {e}"

    @mcp.tool()
    async def task_control(action: str, id: str) -> str:
        """Start, cancel a task, or get its real-time progress / detail.

        This tool returns immediately for start/cancel actions.
        Use action="progress" to poll the status of a running task.

        Args:
            action: One of "start", "cancel", "progress", "detail".
            id: The task ID.
        """
        try:
            if action == "start":
                result = await client.post(f"/tasks/{id}/start")
                return json.dumps(result, ensure_ascii=False, indent=2)
            elif action == "cancel":
                await client.post(f"/tasks/{id}/cancel")
                return f"Task {id} cancel request sent."
            elif action == "progress":
                result = await client.get(f"/tasks/{id}/progress")
                return json.dumps(result, ensure_ascii=False, indent=2)
            elif action == "detail":
                result = await client.get(f"/tasks/{id}")
                return json.dumps(result, ensure_ascii=False, indent=2)
            else:
                return f"Error: Unknown action '{action}'. Use 'start', 'cancel', 'progress', or 'detail'."
        except APIError as e:
            return f"Error: {e}"
