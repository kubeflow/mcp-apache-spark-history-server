import functools
import inspect
import logging
import os
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from functools import partial
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as pkg_version
from typing import Any, Optional

from mcp import MCPError
from mcp.server.mcpserver import MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from mcp.server.transport_security import TransportSecuritySettings
from starlette.requests import Request
from starlette.responses import JSONResponse

from spark_history_mcp.api.emr_persistent_ui_client import EMRPersistentUIClient
from spark_history_mcp.api.spark_client import SparkRestClient
from spark_history_mcp.config.config import Config, load_config

from ..utils.aws_credentials import isolate_credential_process_stdin
from ..utils.utils import ApplicationDiscovery, error_text

logger = logging.getLogger(__name__)


def _emr_cookie_reauth(emr_client: EMRPersistentUIClient) -> str:
    """Re-establish the EMR session and return a fresh Cookie header value."""
    emr_client.setup_http_session()
    return emr_client.cookie_header()


@dataclass
class AppContext:
    clients: dict[str, SparkRestClient]
    default_client: Optional[SparkRestClient] = None
    app_discovery: Optional[ApplicationDiscovery] = None


@asynccontextmanager
async def app_lifespan(server: MCPServer) -> AsyncIterator[AppContext]:
    config = load_config()

    clients: dict[str, SparkRestClient] = {}
    default_client = None

    for name, server_config in config.servers.items():
        # Check if this is an EMR server configuration
        if server_config.emr_cluster_arn:
            # Create EMR client
            emr_client = EMRPersistentUIClient(server_config)

            # Initialize EMR client (create persistent UI, get presigned URL, setup session)
            base_url, _session = emr_client.initialize()

            # Create a modified server config with the base URL
            emr_server_config = server_config.model_copy()
            emr_server_config.url = base_url

            # Route EMR through the generated client using the session cookies as
            # a Cookie header, with a re-auth callback to refresh on 401/403.
            spark_client = SparkRestClient(emr_server_config)
            spark_client.configure_cookies(
                emr_client.cookie_header(),
                reauth=partial(_emr_cookie_reauth, emr_client),
            )

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
    # Before any boto3 credential lookup: startup check, EMR, and AWS tools.
    isolate_credential_process_stdin()

    # Auto-detect AWS credentials and register troubleshooting tools if available
    try:
        import boto3

        session = boto3.Session()
        creds = session.get_credentials()
        if creds is not None and session.region_name:
            from spark_history_mcp.tools.aws_troubleshooting import (
                register_troubleshooting_tools,
            )

            register_troubleshooting_tools(session.region_name)
    except Exception as e:  # noqa: BLE001
        logging.getLogger(__name__).debug(
            "AWS troubleshooting tools not registered: %s", e
        )

    mcp.settings.debug = bool(config.mcp.debug)

    mcp_cfg = config.mcp
    transport = os.getenv("SHS_MCP_TRANSPORT") or mcp_cfg.transport
    if (
        not transport
        and "transports" in mcp_cfg.model_fields_set
        and mcp_cfg.transports
    ):
        transport = mcp_cfg.transports[0]
    transport = transport or "streamable-http"

    if transport == "stdio":
        mcp.run(transport="stdio")
        return

    mcp.run(
        transport=transport,
        host=config.mcp.address,
        port=int(config.mcp.port),
        transport_security=_transport_security(config),
    )


# SDK 1.x applied this allowlist for every bind address. SDK 2.x applies it
# only to a localhost host, so pass it explicitly to keep the 1.x behavior.
_DEFAULT_TRANSPORT_SECURITY = TransportSecuritySettings(
    enable_dns_rebinding_protection=True,
    allowed_hosts=["127.0.0.1:*", "localhost:*", "[::1]:*"],
    allowed_origins=["http://127.0.0.1:*", "http://localhost:*", "http://[::1]:*"],
)


def _transport_security(config: Config) -> TransportSecuritySettings:
    # See: https://github.com/modelcontextprotocol/python-sdk/issues/1798
    ts_config = config.mcp.transport_security
    if not ts_config:
        return _DEFAULT_TRANSPORT_SECURITY
    return TransportSecuritySettings(
        enable_dns_rebinding_protection=ts_config.enable_dns_rebinding_protection,
        allowed_hosts=ts_config.allowed_hosts,
        allowed_origins=ts_config.allowed_origins,
    )


def _server_version() -> str:
    # The distribution name differs from the package name (spark_history_mcp).
    try:
        return pkg_version("mcp-apache-spark-history-server")
    except PackageNotFoundError:
        return ""


mcp: MCPServer[Any] = MCPServer(
    "Spark Events", version=_server_version(), lifespan=app_lifespan
)


@mcp.custom_route("/health", methods=["GET"])
async def health(_request: Request) -> JSONResponse:
    return JSONResponse({"status": "ok"})


@mcp.custom_route("/ready", methods=["GET"])
async def ready(_request: Request) -> JSONResponse:
    return JSONResponse({"status": "ok"})


def _with_error_text(fn: Callable[..., Any]) -> Callable[..., Any]:
    """Keep the SDK 1.x error text, which SDK 2.x hides unless it is a ToolError."""
    if inspect.iscoroutinefunction(fn):

        @functools.wraps(fn)
        async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
            try:
                return await fn(*args, **kwargs)
            except (ToolError, MCPError):
                raise
            except Exception as exc:
                logger.debug("Tool %s failed", fn.__name__, exc_info=True)
                raise ToolError(error_text(exc)) from exc

        return async_wrapper

    @functools.wraps(fn)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        try:
            return fn(*args, **kwargs)
        except (ToolError, MCPError):
            raise
        except Exception as exc:
            logger.debug("Tool %s failed", fn.__name__, exc_info=True)
            raise ToolError(error_text(exc)) from exc

    return wrapper


def tool(**kwargs: Any) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Like ``@mcp.tool(**kwargs)`` with SDK 1.x error text; returns ``fn`` as is."""

    def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
        mcp.add_tool(_with_error_text(fn), **kwargs)
        return fn

    return decorator


# Import tools and prompts to register them with MCP
from spark_history_mcp.prompts import prompts  # noqa: E402,F401
from spark_history_mcp.tools import tools  # noqa: E402,F401
