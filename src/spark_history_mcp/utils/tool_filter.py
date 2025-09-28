"""Tool filtering utilities for conditional MCP tool registration."""

import logging
import os
from typing import Callable, Optional, TypeVar

from spark_history_mcp.config.config import Config

F = TypeVar("F", bound=Callable)
# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def is_tool_enabled(tool_name: str, config_path: str = "config.yaml") -> bool:
    """
    Check if a tool is enabled based on configuration and environment variables.

    Args:
        tool_name: Name of the tool to check
        config_path: Path to configuration file (default: "config.yaml")

    Returns:
        bool: True if tool is enabled, False if disabled
    """
    # Check environment variable first (highest priority)
    env_var = f"SHS_DISABLE_{tool_name.upper()}"
    if os.getenv(env_var, "").lower() in ("true", "1", "yes"):
        return False

    # Check global environment variable for disabled tools
    disabled_tools_env = os.getenv("SHS_GLOBAL_DISABLED_TOOLS", "")
    if disabled_tools_env:
        disabled_tools = [tool.strip() for tool in disabled_tools_env.split(",")]
        if tool_name in disabled_tools:
            return False

    # Check configuration file
    try:
        config = Config.from_file(config_path)

        # Check if any server has this tool disabled
        for server_config in config.servers.values():
            if tool_name in server_config.disabled_tools:
                return False

    except Exception as e:
        logger.error(f"Error loading configuration and loading disabled tools: {e}")
    return True


def conditional_tool(
    mcp_instance, tool_name: Optional[str] = None, config_path: str = "config.yaml"
):
    """
    Decorator that conditionally registers an MCP tool based on configuration.

    Args:
        mcp_instance: The FastMCP instance to register tools with
        tool_name: Name of the tool (defaults to function name)
        config_path: Path to configuration file

    Returns:
        Decorator function
    """

    def decorator(func: F) -> F:
        actual_tool_name = tool_name or func.__name__

        if is_tool_enabled(actual_tool_name, config_path):
            # Tool is enabled, register it with MCP
            return mcp_instance.tool()(func)
        else:
            # Tool is disabled, return unregistered function
            return func

    return decorator
