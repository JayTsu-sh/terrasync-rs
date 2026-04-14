"""MCP server entry point for rust-terrasync."""

from __future__ import annotations

from mcp.server.fastmcp import FastMCP

from .client import TerrasyncClient
from .config import parse_settings
from .tools import analytics, config_tools, endpoints, executions, paths, storage, tasks
from . import prompts, resources


def create_server() -> FastMCP:
    """Create and configure the MCP server with all tools, resources, and prompts."""
    settings = parse_settings()
    client = TerrasyncClient(settings)

    mcp = FastMCP(
        "terrasync",
        instructions=(
            "This MCP server provides access to rust-terrasync, a high-performance "
            "data migration tool supporting Local, NFS v3, and S3 storage. "
            "You can manage storage endpoints, create and monitor scan/sync tasks, "
            "query execution history and logs, and analyze scan results. "
            "Long-running operations (scan/sync) return immediately — use "
            "task_control(action='progress') to poll status."
        ),
    )

    # Register all tool groups
    for module in [endpoints, storage, paths, tasks, executions, analytics, config_tools]:
        module.register(mcp, client)

    # Register resources and prompts
    resources.register(mcp, client)
    prompts.register(mcp)

    return mcp


def main() -> None:
    """CLI entry point — runs the MCP server with stdio transport."""
    mcp = create_server()
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
