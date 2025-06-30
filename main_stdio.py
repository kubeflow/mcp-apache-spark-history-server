"""Official MCP SDK entry point for STDIO transport (Claude Desktop, Amazon Q CLI)."""

import asyncio
import json
import logging
import sys
from typing import Any

from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import Tool

import tools
from config import Config
from spark_client import SparkRestClient

# Configure logging to stderr for debugging
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger(__name__)

# Global variables for clients
clients: dict[str, SparkRestClient] = {}
default_client: SparkRestClient | None = None

server = Server("spark-history-server-mcp")


# Context class to mimic FastMCP context for compatibility with tools.py
class MockContext:
    def __init__(self, clients_dict, default_client_instance):
        self.request_context = MockRequestContext(clients_dict, default_client_instance)


class MockRequestContext:
    def __init__(self, clients_dict, default_client_instance):
        self.lifespan_context = MockLifespanContext(
            clients_dict, default_client_instance
        )


class MockLifespanContext:
    def __init__(self, clients_dict, default_client_instance):
        self.clients = clients_dict
        self.default_client = default_client_instance


# Monkey patch to provide context to tools.py functions
def get_mock_context():
    return MockContext(clients, default_client)


# Replace the mcp.get_context function used in tools.py
tools.mcp.get_context = get_mock_context


@server.list_tools()
async def list_tools() -> list[Tool]:
    """List available tools."""
    return [
        Tool(
            name="get_application",
            description="Get detailed information about a specific Spark application",
            inputSchema={
                "type": "object",
                "properties": {
                    "spark_id": {
                        "type": "string",
                        "description": "Spark application ID",
                    },
                    "server": {"type": "string", "description": "Optional server name"},
                },
                "required": ["spark_id"],
            },
        ),
        Tool(
            name="get_jobs",
            description="Get a list of all jobs for a Spark application",
            inputSchema={
                "type": "object",
                "properties": {
                    "spark_id": {
                        "type": "string",
                        "description": "Spark application ID",
                    },
                    "server": {"type": "string", "description": "Optional server name"},
                    "status": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Optional job status filter",
                    },
                },
                "required": ["spark_id"],
            },
        ),
        Tool(
            name="compare_job_performance",
            description="Compare performance metrics between two Spark jobs",
            inputSchema={
                "type": "object",
                "properties": {
                    "spark_id1": {
                        "type": "string",
                        "description": "First Spark application ID",
                    },
                    "spark_id2": {
                        "type": "string",
                        "description": "Second Spark application ID",
                    },
                    "server": {"type": "string", "description": "Optional server name"},
                },
                "required": ["spark_id1", "spark_id2"],
            },
        ),
        Tool(
            name="compare_sql_execution_plans",
            description="Compare SQL execution plans between two Spark jobs",
            inputSchema={
                "type": "object",
                "properties": {
                    "spark_id1": {
                        "type": "string",
                        "description": "First Spark application ID",
                    },
                    "spark_id2": {
                        "type": "string",
                        "description": "Second Spark application ID",
                    },
                    "execution_id1": {
                        "type": "integer",
                        "description": "Optional execution ID for first app",
                    },
                    "execution_id2": {
                        "type": "integer",
                        "description": "Optional execution ID for second app",
                    },
                    "server": {"type": "string", "description": "Optional server name"},
                },
                "required": ["spark_id1", "spark_id2"],
            },
        ),
        Tool(
            name="get_job_bottlenecks",
            description="Identify performance bottlenecks in a Spark job",
            inputSchema={
                "type": "object",
                "properties": {
                    "spark_id": {
                        "type": "string",
                        "description": "Spark application ID",
                    },
                    "server": {"type": "string", "description": "Optional server name"},
                    "top_n": {
                        "type": "integer",
                        "description": "Number of bottlenecks to return",
                        "default": 5,
                    },
                },
                "required": ["spark_id"],
            },
        ),
        Tool(
            name="get_slowest_jobs",
            description="Get the N slowest jobs for a Spark application",
            inputSchema={
                "type": "object",
                "properties": {
                    "spark_id": {
                        "type": "string",
                        "description": "Spark application ID",
                    },
                    "server": {"type": "string", "description": "Optional server name"},
                    "include_running": {
                        "type": "boolean",
                        "description": "Include running jobs",
                        "default": False,
                    },
                    "n": {
                        "type": "integer",
                        "description": "Number of jobs to return",
                        "default": 5,
                    },
                },
                "required": ["spark_id"],
            },
        ),
    ]


@server.call_tool()
async def call_tool(name: str, arguments: dict[str, Any]) -> list[dict]:
    """Handle tool calls."""
    try:
        # Convert result to JSON string for consistent output
        def format_result(result):
            try:
                from app import DateTimeEncoder

                return json.dumps(result, cls=DateTimeEncoder, indent=2, default=str)
            except Exception:
                return str(result)

        # Map tool names to functions from tools.py
        tool_functions = {
            "get_application": tools.get_application,
            "get_jobs": tools.get_jobs,
            "compare_job_performance": tools.compare_job_performance,
            "compare_sql_execution_plans": tools.compare_sql_execution_plans,
            "get_job_bottlenecks": tools.get_job_bottlenecks,
            "get_slowest_jobs": tools.get_slowest_jobs,
        }

        if name in tool_functions:
            func = tool_functions[name]
            result = func(**arguments)
            return [{"type": "text", "text": format_result(result)}]
        else:
            return [{"type": "text", "text": f"Unknown tool: {name}"}]

    except Exception as e:
        logger.error(f"Tool call error for {name}: {e}")
        return [{"type": "text", "text": f"Error calling {name}: {str(e)}"}]


async def main():
    """Main entry point."""
    global clients, default_client

    try:
        logger.info("Loading configuration...")
        config = Config.from_file("config.yaml")

        logger.info("Initializing clients...")
        for name, server_config in config.servers.items():
            clients[name] = SparkRestClient(server_config)
            if server_config.default:
                default_client = clients[name]

        logger.info("Starting MCP server with stdio transport...")
        async with stdio_server() as (read_stream, write_stream):
            await server.run(
                read_stream, write_stream, server.create_initialization_options()
            )

    except Exception as e:
        logger.error(f"Failed to start MCP server: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
