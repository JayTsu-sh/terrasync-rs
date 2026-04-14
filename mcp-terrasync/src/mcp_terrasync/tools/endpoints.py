"""Endpoint management tools."""

from __future__ import annotations

import json
from typing import Any

from mcp.server.fastmcp import FastMCP

from ..client import APIError, TerrasyncClient


def register(mcp: FastMCP, client: TerrasyncClient) -> None:

    @mcp.tool()
    async def endpoint_list() -> str:
        """List all configured storage endpoints.

        Returns a JSON array of endpoints, each with id, name, endpoint_type,
        endpoint_type_display, config, created_at, updated_at.
        """
        try:
            result = await client.get("/endpoints")
            return json.dumps(result, ensure_ascii=False, indent=2)
        except APIError as e:
            return f"Error: {e}"

    @mcp.tool()
    async def endpoint_manage(
        action: str,
        id: str | None = None,
        name: str | None = None,
        endpoint_type: str | None = None,
        config: dict[str, Any] | None = None,
    ) -> str:
        """Create, update, or delete a storage endpoint.

        Args:
            action: One of "create", "update", "delete".
            id: Endpoint ID (required for update/delete).
            name: Endpoint name (required for create, optional for update).
            endpoint_type: One of "local", "nfs_v3", "s3" (required for create).
            config: Endpoint configuration object (required for create, optional for update).
                Local: {"type": "local", "path": "/data"}
                NFS: {"type": "nfs", "server": "host", "port": 2049, "export_path": "/export", "prefix": null, "uid": 1000, "gid": 1000}
                S3: {"type": "s3", "access_key": "AK", "secret_key": "SK", "bucket": "name", "host": "s3.example.com", "port": 9000, "prefix": null, "protocol": "http"}
        """
        try:
            if action == "create":
                body: dict[str, Any] = {}
                if name:
                    body["name"] = name
                if endpoint_type:
                    body["endpoint_type"] = endpoint_type
                if config:
                    body["config"] = config
                result = await client.post("/endpoints", json_body=body)
                return json.dumps(result, ensure_ascii=False, indent=2)
            elif action == "update":
                if not id:
                    return "Error: 'id' is required for update action."
                body = {}
                if name:
                    body["name"] = name
                if config:
                    body["config"] = config
                result = await client.put(f"/endpoints/{id}", json_body=body)
                return json.dumps(result, ensure_ascii=False, indent=2)
            elif action == "delete":
                if not id:
                    return "Error: 'id' is required for delete action."
                await client.delete(f"/endpoints/{id}")
                return f"Endpoint {id} deleted successfully."
            else:
                return f"Error: Unknown action '{action}'. Use 'create', 'update', or 'delete'."
        except APIError as e:
            return f"Error: {e}"

    @mcp.tool()
    async def endpoint_test(id: str) -> str:
        """Test connectivity to a storage endpoint.

        Args:
            id: The endpoint ID to test.
        """
        try:
            result = await client.post(f"/endpoints/{id}/test")
            return json.dumps(result, ensure_ascii=False, indent=2)
        except APIError as e:
            return f"Error: {e}"
