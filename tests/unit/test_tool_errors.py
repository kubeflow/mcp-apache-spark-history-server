"""Tests for the error text of tool results (SDK 1.x behavior on SDK 2.x)."""

import pytest
from mcp import Client, MCPError
from mcp.server.mcpserver.exceptions import ToolError

from spark_history_mcp.api_client.exceptions import NotFoundException
from spark_history_mcp.core.app import _with_error_text, mcp


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["legacy", "2026-07-28"])
async def test_tool_error_keeps_exception_text(mode):
    """Both eras get the SDK 1.x text "Error executing tool <name>: <text>"."""
    async with Client(mcp, mode=mode, cache=None) as client:
        result = await client.call_tool(
            "list_jobs", {"app_id": "app-1", "server": "does-not-exist"}
        )
    assert result.is_error
    assert result.content[0].text.startswith(
        "Error executing tool list_jobs: No Spark server named 'does-not-exist' "
        "is configured"
    )


def _api_error():
    return NotFoundException(
        status=404,
        reason="Not Found",
        body='{"message": "no such app"}',
        data=None,
    )


def test_api_error_text_has_no_http_headers():
    """HTTP response headers can hold cookies, so they are not sent."""
    exc = _api_error()
    exc.headers = {"Set-Cookie": "session=secret-value"}

    def failing():
        raise exc

    with pytest.raises(ToolError) as info:
        _with_error_text(failing)()
    text = str(info.value)
    assert (
        text
        == '(404)\nReason: Not Found\nHTTP response body: {"message": "no such app"}\n'
    )
    assert "secret-value" not in text
    assert info.value.__cause__ is exc


@pytest.mark.asyncio
async def test_async_tool_error_keeps_exception_text():
    async def failing():
        raise ValueError("bad input")

    with pytest.raises(ToolError, match="^bad input$"):
        await _with_error_text(failing)()


@pytest.mark.parametrize(
    "exc", [ToolError("already a tool error"), MCPError(code=-32602, message="m")]
)
def test_tool_error_and_mcp_error_pass_through(exc):
    """ToolError keeps its text, and MCPError stays a JSON-RPC error."""

    def failing():
        raise exc

    with pytest.raises(type(exc)) as info:
        _with_error_text(failing)()
    assert info.value is exc


def test_wrapper_keeps_signature_and_return_value():
    def tool_fn(app_id: str, limit: int = 3) -> str:
        """Doc."""
        return f"{app_id}:{limit}"

    wrapped = _with_error_text(tool_fn)
    assert wrapped("a", limit=5) == "a:5"
    assert wrapped.__wrapped__ is tool_fn
    assert wrapped.__doc__ == "Doc."


def test_parallel_execute_errors_have_no_http_headers():
    """compare_job_performance returns these strings in a normal (not error) result."""
    from spark_history_mcp.utils.utils import parallel_execute

    exc = _api_error()
    exc.headers = {"Set-Cookie": "session=secret-value"}

    def failing():
        raise exc

    result = parallel_execute([("app1", failing)])
    assert result["results"] == {}
    assert result["errors"] == [
        "app1 failed: HTTP 404 - (404)\nReason: Not Found\n"
        'HTTP response body: {"message": "no such app"}\n'
    ]
    assert "secret-value" not in result["errors"][0]
