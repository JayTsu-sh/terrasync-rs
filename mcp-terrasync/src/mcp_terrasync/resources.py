"""MCP Resource handlers — read-only, URI-addressable data."""

from __future__ import annotations

import json

from mcp.server.fastmcp import FastMCP

from .client import APIError, TerrasyncClient


def register(mcp: FastMCP, client: TerrasyncClient) -> None:

    @mcp.resource("terrasync://endpoints")
    async def list_endpoints() -> str:
        """All configured storage endpoints."""
        try:
            result = await client.get("/endpoints")
            return json.dumps(result, ensure_ascii=False, indent=2)
        except APIError as e:
            return f"Error: {e}"

    @mcp.resource("terrasync://endpoints/{id}")
    async def get_endpoint(id: str) -> str:
        """Detailed info for a single storage endpoint."""
        try:
            result = await client.get(f"/endpoints/{id}")
            return json.dumps(result, ensure_ascii=False, indent=2)
        except APIError as e:
            return f"Error: {e}"

    @mcp.resource("terrasync://tasks")
    async def list_tasks() -> str:
        """All migration tasks with current status."""
        try:
            result = await client.get("/tasks")
            return json.dumps(result, ensure_ascii=False, indent=2)
        except APIError as e:
            return f"Error: {e}"

    @mcp.resource("terrasync://tasks/{id}")
    async def get_task(id: str) -> str:
        """Full detail for a single migration task."""
        try:
            result = await client.get(f"/tasks/{id}")
            return json.dumps(result, ensure_ascii=False, indent=2)
        except APIError as e:
            return f"Error: {e}"

    @mcp.resource("terrasync://tasks/{id}/progress")
    async def get_task_progress(id: str) -> str:
        """Real-time progress snapshot for a running task."""
        try:
            result = await client.get(f"/tasks/{id}/progress")
            return json.dumps(result, ensure_ascii=False, indent=2)
        except APIError as e:
            return f"Error: {e}"

    @mcp.resource("terrasync://tasks/{id}/analytics")
    async def get_task_analytics(id: str) -> str:
        """File distribution analytics for a completed scan task."""
        try:
            result = await client.get(f"/tasks/{id}/analytics")
            return json.dumps(result, ensure_ascii=False, indent=2)
        except APIError as e:
            return f"Error: {e}"

    @mcp.resource("terrasync://config")
    async def get_config() -> str:
        """Current system configuration."""
        try:
            result = await client.get("/config")
            return json.dumps(result, ensure_ascii=False, indent=2)
        except APIError as e:
            return f"Error: {e}"
