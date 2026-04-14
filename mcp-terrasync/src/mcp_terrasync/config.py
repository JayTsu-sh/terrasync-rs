"""Configuration management for mcp-terrasync."""

from __future__ import annotations

import argparse
import sys

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """MCP server settings, loaded from environment variables."""

    terrasync_api_url: str = "http://localhost:8080/api/v1"
    terrasync_api_timeout: float = 30.0
    terrasync_long_timeout: float = 300.0


def parse_settings() -> Settings:
    """Parse settings from CLI args + environment variables.

    CLI --api-url takes precedence over env TERRASYNC_API_URL.
    """
    parser = argparse.ArgumentParser(description="MCP server for rust-terrasync")
    parser.add_argument("--api-url", type=str, default=None, help="rust-terrasync Web API base URL")
    args, _ = parser.parse_known_args(sys.argv[1:])

    overrides = {}
    if args.api_url:
        overrides["terrasync_api_url"] = args.api_url

    return Settings(**overrides)
