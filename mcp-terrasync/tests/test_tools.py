"""Tool registration and basic invocation tests using httpx mock."""

from __future__ import annotations

import json

import httpx
import pytest
import respx

from mcp_terrasync.client import TerrasyncClient
from mcp_terrasync.config import Settings
from mcp_terrasync.server import create_server


@pytest.fixture
def settings() -> Settings:
    return Settings(terrasync_api_url="http://testserver/api/v1")


@pytest.fixture
def client(settings: Settings) -> TerrasyncClient:
    return TerrasyncClient(settings)


class TestEndpointTools:
    @respx.mock
    @pytest.mark.asyncio
    async def test_endpoint_list(self, client: TerrasyncClient) -> None:
        from mcp.server.fastmcp import FastMCP
        from mcp_terrasync.tools import endpoints

        mcp = FastMCP("test")
        endpoints.register(mcp, client)

        mock_data = [{"id": "1", "name": "local-test", "endpoint_type": "local"}]
        respx.get("http://testserver/api/v1/endpoints").mock(
            return_value=httpx.Response(200, json=mock_data)
        )

        # Find the tool function and call it
        tool_fn = None
        for name, fn in mcp._tool_manager._tools.items():
            if name == "endpoint_list":
                tool_fn = fn
                break

        assert tool_fn is not None
        result = await tool_fn.fn()
        parsed = json.loads(result)
        assert len(parsed) == 1
        assert parsed[0]["name"] == "local-test"

    @respx.mock
    @pytest.mark.asyncio
    async def test_endpoint_manage_create(self, client: TerrasyncClient) -> None:
        from mcp.server.fastmcp import FastMCP
        from mcp_terrasync.tools import endpoints

        mcp = FastMCP("test")
        endpoints.register(mcp, client)

        mock_response = {"id": "2", "name": "nfs-prod", "endpoint_type": "nfs_v3"}
        respx.post("http://testserver/api/v1/endpoints").mock(
            return_value=httpx.Response(200, json=mock_response)
        )

        tool_fn = mcp._tool_manager._tools.get("endpoint_manage")
        assert tool_fn is not None
        result = await tool_fn.fn(
            action="create",
            name="nfs-prod",
            endpoint_type="nfs_v3",
            config={"type": "nfs", "server": "fs01", "port": 2049, "export_path": "/data"},
        )
        parsed = json.loads(result)
        assert parsed["id"] == "2"

    @respx.mock
    @pytest.mark.asyncio
    async def test_endpoint_manage_delete(self, client: TerrasyncClient) -> None:
        from mcp.server.fastmcp import FastMCP
        from mcp_terrasync.tools import endpoints

        mcp = FastMCP("test")
        endpoints.register(mcp, client)

        respx.delete("http://testserver/api/v1/endpoints/abc").mock(
            return_value=httpx.Response(204)
        )

        tool_fn = mcp._tool_manager._tools.get("endpoint_manage")
        assert tool_fn is not None
        result = await tool_fn.fn(action="delete", id="abc")
        assert "deleted" in result


class TestTaskTools:
    @respx.mock
    @pytest.mark.asyncio
    async def test_task_list(self, client: TerrasyncClient) -> None:
        from mcp.server.fastmcp import FastMCP
        from mcp_terrasync.tools import tasks

        mcp = FastMCP("test")
        tasks.register(mcp, client)

        mock_data = [{"id": "t1", "name": "scan-test", "status": "pending", "task_type": "scan"}]
        respx.get("http://testserver/api/v1/tasks").mock(
            return_value=httpx.Response(200, json=mock_data)
        )

        tool_fn = mcp._tool_manager._tools.get("task_list")
        assert tool_fn is not None
        result = await tool_fn.fn()
        parsed = json.loads(result)
        assert parsed[0]["status"] == "pending"

    @respx.mock
    @pytest.mark.asyncio
    async def test_task_control_start(self, client: TerrasyncClient) -> None:
        from mcp.server.fastmcp import FastMCP
        from mcp_terrasync.tools import tasks

        mcp = FastMCP("test")
        tasks.register(mcp, client)

        respx.post("http://testserver/api/v1/tasks/t1/start").mock(
            return_value=httpx.Response(200, json={"execution_id": "ex1"})
        )

        tool_fn = mcp._tool_manager._tools.get("task_control")
        assert tool_fn is not None
        result = await tool_fn.fn(action="start", id="t1")
        parsed = json.loads(result)
        assert parsed["execution_id"] == "ex1"


class TestClientErrors:
    @respx.mock
    @pytest.mark.asyncio
    async def test_api_404(self, client: TerrasyncClient) -> None:
        from mcp.server.fastmcp import FastMCP
        from mcp_terrasync.tools import endpoints

        mcp = FastMCP("test")
        endpoints.register(mcp, client)

        respx.get("http://testserver/api/v1/endpoints").mock(
            return_value=httpx.Response(404, json={"error": "Not found"})
        )

        tool_fn = mcp._tool_manager._tools.get("endpoint_list")
        assert tool_fn is not None
        result = await tool_fn.fn()
        assert "Error" in result
        assert "404" in result

    @respx.mock
    @pytest.mark.asyncio
    async def test_connection_error(self, client: TerrasyncClient) -> None:
        from mcp.server.fastmcp import FastMCP
        from mcp_terrasync.tools import endpoints

        mcp = FastMCP("test")
        endpoints.register(mcp, client)

        respx.get("http://testserver/api/v1/endpoints").mock(side_effect=httpx.ConnectError("refused"))

        tool_fn = mcp._tool_manager._tools.get("endpoint_list")
        assert tool_fn is not None
        result = await tool_fn.fn()
        assert "Error" in result
        assert "Cannot connect" in result
