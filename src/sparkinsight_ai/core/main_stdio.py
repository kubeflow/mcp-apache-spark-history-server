"""STDIO entry point for SparkInsight AI MCP server.

This module provides a dedicated entry point for STDIO-based MCP connections,
used by clients like Claude Desktop, Amazon Q CLI, and Gemini CLI.
"""

import asyncio
import os
import sys
from pathlib import Path

from sparkinsight_ai.config.config import Config


def main():
    """
    Main entry point for STDIO MCP server.

    This is specifically designed for MCP clients that use STDIO transport
    like Claude Desktop, Amazon Q CLI, and Gemini CLI.
    """
    try:
        # Import FastMCP with error handling
        from mcp.server.fastmcp import FastMCP
        from sparkinsight_ai.core.app import app_lifespan
    except ImportError as e:
        print(f"Error: MCP dependencies not available: {e}", file=sys.stderr)
        print("Install with: pip install sparkinsight-ai[mcp]", file=sys.stderr)
        sys.exit(1)

    # Load configuration
    config_path = Path("config.yaml")
    if not config_path.exists():
        # Try alternative locations
        for alt_path in [
            Path.home() / ".sparkinsight" / "config.yaml",
            Path("/etc/sparkinsight/config.yaml"),
        ]:
            if alt_path.exists():
                config_path = alt_path
                break

    try:
        config = Config.from_file(str(config_path))
    except FileNotFoundError:
        print(f"Error: Configuration file not found: {config_path}", file=sys.stderr)
        print("Run 'sparkinsight-ai config init' to create a configuration file", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error loading configuration: {e}", file=sys.stderr)
        sys.exit(1)

    # Override transport to STDIO for this entry point
    config.mcp.transports = ["stdio"]

    # Override with environment variables if set
    if os.getenv("SHS_MCP_DEBUG"):
        config.mcp.debug = os.getenv("SHS_MCP_DEBUG").lower() in ("true", "1", "yes")

    # Create FastMCP instance
    mcp = FastMCP("SparkInsight AI", lifespan=app_lifespan)

    # Import tools to register them
    from sparkinsight_ai.tools import tools  # noqa: F401

    # Run in STDIO mode
    asyncio.run(mcp.run(transport="stdio"))


if __name__ == "__main__":
    main()