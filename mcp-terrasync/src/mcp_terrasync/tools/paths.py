"""Path management tools."""

from __future__ import annotations

import json

from mcp.server.fastmcp import FastMCP

from ..client import APIError, TerrasyncClient


def register(mcp: FastMCP, client: TerrasyncClient) -> None:

    @mcp.tool()
    async def path_manage(
        action: str,
        endpoint_id: str | None = None,
        path_id: str | None = None,
        sub_path: str | None = None,
        description: str | None = None,
    ) -> str:
        """List, create, or delete scan paths under an endpoint.

        Args:
            action: One of "list", "create", "delete".
            endpoint_id: Endpoint ID (required for list/create).
            path_id: Path ID (required for delete).
            sub_path: Sub-path relative to endpoint root (required for create).
            description: Optional description for the path (for create).
        """
        try:
            if action == "list":
                if not endpoint_id:
                    return "Error: 'endpoint_id' is required for list action."
                result = await client.get(f"/endpoints/{endpoint_id}/paths")
                return json.dumps(result, ensure_ascii=False, indent=2)
            elif action == "create":
                if not endpoint_id:
                    return "Error: 'endpoint_id' is required for create action."
                if not sub_path:
                    return "Error: 'sub_path' is required for create action."
                body = {"sub_path": sub_path}
                if description:
                    body["description"] = description
                result = await client.post(f"/endpoints/{endpoint_id}/paths", json_body=body)
                return json.dumps(result, ensure_ascii=False, indent=2)
            elif action == "delete":
                if not path_id:
                    return "Error: 'path_id' is required for delete action."
                await client.delete(f"/paths/{path_id}")
                return f"Path {path_id} deleted successfully."
            else:
                return f"Error: Unknown action '{action}'. Use 'list', 'create', or 'delete'."
        except APIError as e:
            return f"Error: {e}"
