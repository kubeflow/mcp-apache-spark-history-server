import json
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from mcp.server.fastmcp import FastMCP

from sparkinsight_ai.api.emr_persistent_ui_client import EMRPersistentUIClient
from sparkinsight_ai.api.spark_client import SparkRestClient
from sparkinsight_ai.config.config import Config


@dataclass
class AppContext:
    clients: dict[str, SparkRestClient]
    default_client: Optional[SparkRestClient] = None


class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder that handles datetime objects."""

    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


@asynccontextmanager
async def app_lifespan(server: FastMCP) -> AsyncIterator[AppContext]:
    config = Config.from_file("config.yaml")

    clients: dict[str, SparkRestClient] = {}
    default_client = None

    for name, server_config in config.servers.items():
        # Check if this is an EMR server configuration
        if server_config.emr_cluster_arn:
            # Create EMR client
            emr_client = EMRPersistentUIClient(server_config)

            # Initialize EMR client (create persistent UI, get presigned URL, setup session)
            base_url, session = emr_client.initialize()

            # Create a modified server config with the base URL
            emr_server_config = server_config.model_copy()
            emr_server_config.url = base_url

            # Create SparkRestClient with the session
            spark_client = SparkRestClient(emr_server_config)
            spark_client.session = session  # Use the authenticated session

            clients[name] = spark_client
        else:
            # Regular Spark REST client
            clients[name] = SparkRestClient(server_config)

        if server_config.default:
            default_client = clients[name]

    yield AppContext(clients=clients, default_client=default_client)


def run(config: Config):
    """Run the MCP server with the specified configuration."""
    import asyncio

    # Get transport from environment or config
    transport = os.getenv("SHS_MCP_TRANSPORT", config.mcp.transports[0])

    if transport == "stdio":
        # For STDIO transport, run directly without server settings
        mcp.settings.debug = bool(config.mcp.debug)
        asyncio.run(mcp.run(transport="stdio"))
    else:
        # For HTTP-based transports (streamable-http, sse), set server settings
        mcp.settings.host = config.mcp.address
        mcp.settings.port = int(config.mcp.port)
        mcp.settings.debug = bool(config.mcp.debug)

        # Map config transport names to FastMCP transport names
        transport_map = {
            "streamable-http": "streamable-http",
            "sse": "sse",
            "http": "streamable-http"  # Alias
        }

        actual_transport = transport_map.get(transport, transport)
        asyncio.run(mcp.run(transport=actual_transport))


mcp = FastMCP("Spark Events", lifespan=app_lifespan)

# Import tools to register them with MCP
from sparkinsight_ai.tools import tools  # noqa: E402,F401
