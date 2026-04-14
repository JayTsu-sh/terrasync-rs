"""Async HTTP client wrapper for rust-terrasync Web API."""

from __future__ import annotations

import json
from typing import Any

import httpx

from .config import Settings


class TerrasyncClient:
    """Wraps httpx.AsyncClient with unified error handling for the terrasync API."""

    def __init__(self, settings: Settings) -> None:
        self._base_url = settings.terrasync_api_url
        self._timeout = settings.terrasync_api_timeout
        self._long_timeout = settings.terrasync_long_timeout
        self._client: httpx.AsyncClient | None = None

    async def _ensure_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(
                base_url=self._base_url,
                timeout=self._timeout,
                headers={"Content-Type": "application/json"},
            )
        return self._client

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        return await self._request("GET", path, params=params)

    async def post(self, path: str, json_body: dict[str, Any] | None = None) -> dict[str, Any]:
        return await self._request("POST", path, json_body=json_body)

    async def put(self, path: str, json_body: dict[str, Any] | None = None) -> dict[str, Any]:
        return await self._request("PUT", path, json_body=json_body)

    async def delete(self, path: str) -> dict[str, Any]:
        return await self._request("DELETE", path)

    async def _request(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        client = await self._ensure_client()
        try:
            resp = await client.request(method, path, params=params, json=json_body)
        except httpx.ConnectError:
            raise APIError(f"Cannot connect to terrasync API at {self._base_url}. Is the service running?")
        except httpx.TimeoutException:
            raise APIError(f"Request timed out after {self._timeout}s. The operation may still be running server-side.")

        if resp.status_code >= 400:
            try:
                body = resp.json()
                detail = body.get("error", body.get("message", resp.text))
            except (json.JSONDecodeError, ValueError):
                detail = resp.text
            if resp.status_code >= 500:
                raise APIError(f"Internal server error ({resp.status_code}): {detail}")
            raise APIError(f"API error ({resp.status_code}): {detail}")

        if resp.status_code == 204 or not resp.content:
            return {}
        try:
            return resp.json()
        except (json.JSONDecodeError, ValueError):
            return {"text": resp.text}


class APIError(Exception):
    """Raised when the terrasync API returns an error or is unreachable."""
