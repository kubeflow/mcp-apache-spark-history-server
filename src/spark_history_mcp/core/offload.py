"""Keep the MCP event loop responsive while sync tool handlers talk to SHS.

FastMCP invokes sync ``@mcp.tool`` functions directly on the asyncio event
loop (see ``FuncMetadata.call_fn_with_arg_validation``). Those handlers call
the urllib3-based Spark History Server client, so a slow SHS read blocks TCP
accepts on the MCP port and Kubernetes liveness probes fail.

Wrapping every sync tool with ``asyncio.to_thread`` moves SHS I/O off the
loop without rewriting each tool as ``async def``.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any, Callable

if TYPE_CHECKING:
    from mcp.server.mcpserver import MCPServer

logger = logging.getLogger(__name__)


def _offload(fn: Callable[..., Any]) -> Callable[..., Any]:
    """Return an async wrapper that runs ``fn`` in the default thread pool."""

    async def _wrapper(*args: Any, **kwargs: Any) -> Any:
        return await asyncio.to_thread(fn, *args, **kwargs)

    # Preserve identity for debugging; schema still comes from Tool.fn_metadata.
    _wrapper.__name__ = getattr(fn, "__name__", "_offloaded_tool")
    _wrapper.__doc__ = getattr(fn, "__doc__", None)
    return _wrapper


def offload_sync_tools(mcp: "MCPServer") -> int:
    """Rewrite registered sync tools so they run in a worker thread.

    Returns the number of tools that were wrapped.
    """
    wrapped = 0
    for tool in mcp._tool_manager.list_tools():
        if tool.is_async:
            continue
        tool.fn = _offload(tool.fn)
        tool.is_async = True
        wrapped += 1
    logger.info("Offloaded %d sync MCP tool(s) to a thread pool", wrapped)
    return wrapped
