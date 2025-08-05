import json
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from mcp.server.fastmcp import FastMCP

from spark_history_mcp.api.client_factory import create_spark_client_from_config
from spark_history_mcp.api.emr_client import EMRClient
from spark_history_mcp.api.spark_client import SparkRestClient
from spark_history_mcp.config.config import Config


@dataclass
class StaticClients:
    clients: dict[str, SparkRestClient]
    default_client: Optional[SparkRestClient] = None


@dataclass
class AppContext:
    dynamic_emr_clusters_mode: bool = False
    emr_client: Optional[EMRClient] = None
    static_clients: Optional[StaticClients] = None


class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder that handles datetime objects."""

    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


@asynccontextmanager
async def app_lifespan(server: FastMCP) -> AsyncIterator[AppContext]:
    config = Config.from_file("config.yaml")

    if config.dynamic_emr_clusters_mode:
        yield AppContext(dynamic_emr_clusters_mode=True, emr_client=EMRClient())
        return

    clients: dict[str, SparkRestClient] = {}
    default_client = None

    for name, server_config in config.servers.items():
        clients[name] = create_spark_client_from_config(server_config)

        if server_config.default:
            default_client = clients[name]

    yield AppContext(
        static_clients=StaticClients(clients=clients, default_client=default_client)
    )


def run(config: Config):
    mcp.settings.host = config.mcp.address
    mcp.settings.port = int(config.mcp.port)
    mcp.settings.debug = bool(config.mcp.debug)
    mcp.run(transport=os.getenv("SHS_MCP_TRANSPORT", config.mcp.transports[0]))


mcp = FastMCP("Spark Events", lifespan=app_lifespan)

# Import tools to register them with MCP
from spark_history_mcp.tools import tools  # noqa: E402,F401
