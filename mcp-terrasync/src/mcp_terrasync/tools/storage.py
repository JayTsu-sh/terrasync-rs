"""Storage browsing tools."""

from __future__ import annotations

import json

from mcp.server.fastmcp import FastMCP

from ..client import APIError, TerrasyncClient


def register(mcp: FastMCP, client: TerrasyncClient) -> None:

    @mcp.tool()
    async def storage_browse_dirs(path: str, storage_url: str | None = None) -> str:
        """Browse directories on local or remote storage (path auto-completion).

        Args:
            path: Directory path to browse.
            storage_url: Optional remote storage URL (e.g. "nfs://server:2049/export").
                If omitted, browses local filesystem.
        """
        try:
            body = {"path": path}
            if storage_url:
                body["storage_url"] = storage_url
            result = await client.post("/fs/list-dirs", json_body=body)
            return json.dumps(result, ensure_ascii=False, indent=2)
        except APIError as e:
            return f"Error: {e}"

    @mcp.tool()
    async def storage_list_nfs_exports(server: str, port: int = 2049) -> str:
        """List NFS exports from a server.

        Args:
            server: NFS server hostname or IP.
            port: NFS port (default 2049).
        """
        try:
            result = await client.post("/nfs/exports", json_body={"server": server, "port": port})
            return json.dumps(result, ensure_ascii=False, indent=2)
        except APIError as e:
            return f"Error: {e}"

    @mcp.tool()
    async def storage_list_s3_buckets(
        server: str,
        access_key: str,
        secret_key: str,
        use_https: bool = False,
    ) -> str:
        """List S3 buckets from a server.

        Args:
            server: S3 server hostname.
            access_key: S3 access key.
            secret_key: S3 secret key.
            use_https: Whether to use HTTPS (default false).
        """
        try:
            result = await client.post(
                "/s3/buckets",
                json_body={
                    "server": server,
                    "access_key": access_key,
                    "secret_key": secret_key,
                    "use_https": use_https,
                },
            )
            return json.dumps(result, ensure_ascii=False, indent=2)
        except APIError as e:
            return f"Error: {e}"
