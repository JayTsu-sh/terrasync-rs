"""MCP Prompt templates — guided workflows for common operations."""

from __future__ import annotations

from mcp.server.fastmcp import FastMCP


def register(mcp: FastMCP) -> None:

    @mcp.prompt()
    def setup_scan(storage_type: str = "local") -> str:
        """Guided workflow to create an endpoint, path, and scan task, then start it.

        Args:
            storage_type: One of "local", "nfs_v3", "s3".
        """
        return f"""Please help me set up a scan task step by step:

1. First, create a storage endpoint of type "{storage_type}" using the endpoint_manage tool (action="create").
   Ask me for the connection details.

2. Then, create a scan path under that endpoint using the path_manage tool (action="create").
   Ask me which directory/prefix to scan.

3. Create a scan task using the task_manage tool (action="create") with task_type="scan".
   Ask me about optional configuration: match/exclude filters, scan depth.

4. Start the task using task_control (action="start").

5. Monitor progress using task_control (action="progress") until it completes.

6. Show me the analytics using task_analytics.

Please proceed step by step, asking for my input at each stage."""

    @mcp.prompt()
    def setup_sync() -> str:
        """Guided workflow to configure source and destination endpoints and start a sync task."""
        return """Please help me set up a data sync task step by step:

1. First, list existing endpoints using endpoint_list.
   If I need new endpoints, create them using endpoint_manage.

2. Set up source and destination paths using path_manage.

3. Create a sync task using task_manage (action="create") with task_type="sync".
   Ask me about:
   - QoS bandwidth limit (e.g. "1GiB/s")
   - Whether to enable integrity check
   - Match/exclude filters
   - Block size

4. Start the task and monitor progress.

Please proceed step by step."""

    @mcp.prompt()
    def diagnose_task(task_id: str) -> str:
        """Investigate a failed or stalled task.

        Args:
            task_id: The task ID to diagnose.
        """
        return f"""Please diagnose task "{task_id}":

1. Get task detail using task_control (action="detail", id="{task_id}").
2. Check current progress using task_control (action="progress", id="{task_id}").
3. List execution history using execution_query (action="list", task_id="{task_id}").
4. For the latest execution, get its logs using execution_query (action="logs").
   Focus on "error" and "warn" level logs.
5. Summarize: what went wrong, and suggest remediation steps."""

    @mcp.prompt()
    def storage_overview() -> str:
        """Summarize all endpoints, paths, and task statuses for situational awareness."""
        return """Please provide a complete overview of the current terrasync environment:

1. List all endpoints using endpoint_list.
2. For each endpoint, list its paths using path_manage (action="list").
3. List all tasks using task_list.
4. Summarize in a table: endpoints with their types, paths, and any running/failed tasks.
5. Highlight any issues that need attention (failed tasks, unreachable endpoints)."""

    @mcp.prompt()
    def analyze_scan_results(task_id: str) -> str:
        """After a scan completes, retrieve and interpret the analytics data.

        Args:
            task_id: The completed scan task ID.
        """
        return f"""Please analyze the scan results for task "{task_id}":

1. Get task detail to confirm it completed successfully.
2. Retrieve analytics using task_analytics (task_id="{task_id}").
3. Summarize the findings:
   - Total file count and size
   - File type distribution (top 10 extensions by count and size)
   - Size distribution (how many small vs large files)
   - Time distribution (when were files last modified)
4. Highlight any noteworthy patterns or anomalies."""
