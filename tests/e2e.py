import json
from contextlib import AsyncExitStack, asynccontextmanager
from types import TracebackType

import pytest
from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client
from mcp.types import TextContent

from spark_history_mcp.models.spark_types import ApplicationInfo, JobData

mcp_endpoint = "http://localhost:18888/mcp/"
test_app_id = "spark-cc4d115f011443d787f03a71a476a745"
test_app_id2 = "spark-bcec39f6201b42b9925124595baad262"


class McpClient:
    def __init__(self):
        self._client_session = None
        self._exit_stack = None

    @asynccontextmanager
    async def initialize(self):
        self._exit_stack = AsyncExitStack()
        async with AsyncExitStack() as stack:
            read, write, _ = await stack.enter_async_context(
                streamablehttp_client(mcp_endpoint)
            )
            mcp_client = await stack.enter_async_context(ClientSession(read, write))
            await mcp_client.initialize()
            self._client_session = mcp_client
            yield mcp_client

    @classmethod
    @asynccontextmanager
    async def get_mcp_client(cls):
        client = cls()
        async with client.initialize() as session:
            yield session

    async def call_tool(self, name, arguments):
        return await self._client_session.call_tool(name=name, arguments=arguments)

    async def list_tools(self):
        return await self._client_session.list_tools()

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        await self._exit_stack.aclose()

    async def __aenter__(self):
        self._exit_stack = AsyncExitStack()
        read, write, _ = await self._exit_stack.enter_async_context(
            streamablehttp_client(mcp_endpoint)
        )
        self._client_session = await self._exit_stack.enter_async_context(
            ClientSession(read, write)
        )
        await self._client_session.initialize()
        return self


@pytest.mark.asyncio
async def test_tools_not_empty():
    async with McpClient() as client:
        tool_result = await client.list_tools()
        assert tool_result, "Tools list should not be empty"
        assert len(tool_result.tools) > 0, "Tools list should contain at least one tool"


@pytest.mark.asyncio
async def test_get_application():
    async with McpClient() as client:
        app_result = await client.call_tool("get_application", {"app_id": test_app_id})
        assert not app_result.isError
        assert isinstance(app_result.content[0], TextContent), (
            "get_application should return a TextContent object"
        )

        app_data = json.loads(app_result.content[0].text)
        app_info = ApplicationInfo.model_validate(app_data)

        # Validate specific fields
        assert app_info.id == test_app_id
        assert app_info.name == "NewYorkTaxiData_2025_06_27_03_56_52"


@pytest.mark.asyncio
async def test_list_jobs_no_filter():
    async with McpClient() as client:
        # Test with status filter
        jobs_result = await client.call_tool("list_jobs", {"app_id": test_app_id})
        assert not jobs_result.isError
        assert len(jobs_result.content) == 6
        for content in jobs_result.content:
            assert isinstance(content, TextContent), (
                "list_jobs should return a TextContent object"
            )
            stage = JobData.model_validate_json(content.text)
            assert stage.status == "SUCCEEDED", "All jobs should have SUCCEEDED status"


@pytest.mark.asyncio
async def test_list_jobs_with_status_filter():
    async with McpClient() as client:
        # Test with status filter
        jobs_result = await client.call_tool(
            "list_jobs", {"app_id": test_app_id, "status": ["SUCCEEDED"]}
        )
        assert not jobs_result.isError
        assert len(jobs_result.content) > 0
        for content in jobs_result.content:
            assert isinstance(content, TextContent), (
                "list_jobs should return a TextContent object"
            )
            stage = JobData.model_validate_json(content.text)
            assert stage.status == "SUCCEEDED", "All jobs should have SUCCEEDED status"


@pytest.mark.asyncio
async def test_get_application_from_second_server():
    async with McpClient() as client:
        # This should automatically discover the app on server 2
        app_result = await client.call_tool("get_application", {"app_id": test_app_id2})
        assert not app_result.isError
        assert isinstance(app_result.content[0], TextContent), (
            "get_application should return a TextContent object"
        )

        app_data = json.loads(app_result.content[0].text)
        app_info = ApplicationInfo.model_validate(app_data)
        assert app_info.id == test_app_id2
