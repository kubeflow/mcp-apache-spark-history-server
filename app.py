from typing import Optional
from contextlib import asynccontextmanager
from collections.abc import AsyncIterator
from dataclasses import dataclass
import json
from datetime import datetime

from spark_client import SparkRestClient
from config import Config
from mcp.server.fastmcp import FastMCP


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
        clients[name] = SparkRestClient(server_config)
        if server_config.default:
            default_client = clients[name]

    yield AppContext(clients=clients, default_client=default_client)


import os

mcp = FastMCP("Spark Events", lifespan=app_lifespan)
mcp.settings.port = int(os.getenv("MCP_PORT", "18888"))
mcp.settings.debug = os.getenv("MCP_DEBUG", "false").lower() == "true"

# Import tools to register them with MCP
import tools  # noqa: E402,F401
