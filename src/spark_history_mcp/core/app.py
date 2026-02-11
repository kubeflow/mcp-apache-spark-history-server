import json
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from mcp.server.fastmcp import FastMCP

# For handling different mcp version
try:
    # mcp version higher than 1.23.0 we are able to import TransportSecuritySettings
    from mcp.server.transport_security import TransportSecuritySettings
except ImportError:
    TransportSecuritySettings = None

from spark_history_mcp.api.emr_persistent_ui_client import EMRPersistentUIClient
from spark_history_mcp.api.emr_serverless_hybrid_client import EMRServerlessHybridClient
from spark_history_mcp.api.spark_client import SparkRestClient
from spark_history_mcp.config.config import Config

from ..utils.utils import ApplicationDiscovery


@dataclass
class AppContext:
    clients: dict[str, SparkRestClient]
    default_client: Optional[SparkRestClient] = None
    app_discovery: Optional[ApplicationDiscovery] = None


class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder that handles datetime objects."""

    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


@asynccontextmanager
async def app_lifespan(server: FastMCP) -> AsyncIterator[AppContext]:
    # Config() automatically loads from SHS_MCP_CONFIG env var (set in main.py)
    config = Config()

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
        # Check if this is an EMR Serverless server configuration
        elif server_config.emr_serverless_app_id:
            # Create EMR Serverless hybrid client
            emr_serverless_client = EMRServerlessHybridClient(server_config)

            # Get the base URL (dummy for hybrid client)
            base_url = emr_serverless_client.get_spark_history_server_url()
            
            # Create a modified server config with the base URL
            emr_serverless_server_config = server_config.model_copy()
            emr_serverless_server_config.url = base_url

            # Create SparkRestClient with custom request method
            spark_client = SparkRestClient(emr_serverless_server_config)
            
            # Override the _make_request method to use EMR Serverless hybrid client
            def hybrid_make_request(path: str, params: Optional[dict] = None):
                """Custom request method that uses EMR Serverless hybrid client."""
                import logging
                logger = logging.getLogger(__name__)
                
                logger.debug(f"EMR Serverless hybrid request: {path}")
                if params:
                    logger.debug(f"Request params: {params}")
                
                # Use hybrid client for requests
                return emr_serverless_client.make_request(path, params=params)
            
            spark_client._make_request = hybrid_make_request
            clients[name] = spark_client
        else:
            # Regular Spark REST client
            clients[name] = SparkRestClient(server_config)

        if server_config.default:
            default_client = clients[name]

    app_discovery = ApplicationDiscovery(clients)
    yield AppContext(
        clients=clients, default_client=default_client, app_discovery=app_discovery
    )


def run(config: Config):
    mcp.settings.host = config.mcp.address
    mcp.settings.port = int(config.mcp.port)
    mcp.settings.debug = bool(config.mcp.debug)

    # Configure transport security settings for DNS rebinding protection
    # See: https://github.com/modelcontextprotocol/python-sdk/issues/1798
    if config.mcp.transport_security:
        ts_config = config.mcp.transport_security
        mcp.settings.transport_security = TransportSecuritySettings(
            enable_dns_rebinding_protection=ts_config.enable_dns_rebinding_protection,
            allowed_hosts=ts_config.allowed_hosts,
            allowed_origins=ts_config.allowed_origins,
        )

    mcp.run(transport=os.getenv("SHS_MCP_TRANSPORT", config.mcp.transports[0]))


mcp = FastMCP("Spark Events", lifespan=app_lifespan)

# Import tools to register them with MCP
from spark_history_mcp.tools import tools  # noqa: E402,F401
