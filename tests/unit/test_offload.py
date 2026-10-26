"""Event-loop offload for sync MCP tools and SHS client fail-fast behavior."""

import asyncio
import time
import unittest
from unittest.mock import MagicMock

from urllib3.exceptions import ReadTimeoutError

from spark_history_mcp.api.spark_client import (
    SparkHistoryTimeoutError,
    SparkRestClient,
)
from spark_history_mcp.config.config import ServerConfig
from spark_history_mcp.core.offload import offload_sync_tools


class TestOffloadSyncTools(unittest.IsolatedAsyncioTestCase):
    async def test_offloaded_tool_does_not_block_event_loop(self):
        """While a sync tool sleeps in a worker thread, the loop can still tick."""
        from mcp.server.mcpserver import MCPServer

        server = MCPServer("offload-test")

        @server.tool()
        def sleepy() -> str:
            time.sleep(0.2)
            return "done"

        self.assertFalse(server._tool_manager.get_tool("sleepy").is_async)
        self.assertEqual(offload_sync_tools(server), 1)
        tool = server._tool_manager.get_tool("sleepy")
        self.assertTrue(tool.is_async)

        started = time.perf_counter()
        # Call the wrapped fn directly (Tool.run also needs a Context).
        result_task = asyncio.create_task(tool.fn())
        await asyncio.sleep(0.05)
        interleaved_at = time.perf_counter() - started
        self.assertLess(
            interleaved_at,
            0.15,
            "event loop should stay responsive while the sync tool runs",
        )
        self.assertEqual(await result_task, "done")

    def test_already_async_tools_are_left_alone(self):
        from mcp.server.mcpserver import MCPServer

        server = MCPServer("async-test")

        @server.tool()
        async def already_async() -> str:
            return "ok"

        self.assertEqual(offload_sync_tools(server), 0)
        self.assertTrue(server._tool_manager.get_tool("already_async").is_async)


class TestSparkClientTimeoutAndRetries(unittest.TestCase):
    def test_retries_disabled_on_generated_client(self):
        client = SparkRestClient(ServerConfig(url="http://spark-history-server:18080"))
        self.assertIs(client._api.api_client.configuration.retries, False)

    def test_invoke_maps_read_timeout_to_friendly_error(self):
        client = SparkRestClient(ServerConfig(url="http://spark-history-server:18080"))
        pool = MagicMock()
        with self.assertRaises(SparkHistoryTimeoutError) as ctx:
            client._invoke(
                MagicMock(
                    side_effect=ReadTimeoutError(
                        pool,
                        "http://spark-history-server:18080/api/v1/applications/app-1",
                        "read timed out",
                    )
                )
            )

        message = str(ctx.exception)
        self.assertIn("did not respond within", message)
        self.assertIn("try again shortly", message)


if __name__ == "__main__":
    unittest.main()
