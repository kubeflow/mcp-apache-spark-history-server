from contextlib import AsyncExitStack, asynccontextmanager
from types import TracebackType

import pytest
from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client
from mcp.types import TextContent

from spark_history_mcp.api_client.models.application import Application
from spark_history_mcp.api_client.models.environment import Environment
from spark_history_mcp.api_client.models.executor import Executor
from spark_history_mcp.api_client.models.job import Job
from spark_history_mcp.api_client.models.stage_data import StageData
from spark_history_mcp.models.mcp_types import (
    SqlExecutionComparison,
    SqlExecutionDetail,
    SqlExecutionSummary,
)

mcp_endpoint = "http://localhost:18888/mcp/"

# Apps from the shared e2e corpus (see skills/cli/e2e/fixtures.yaml).
app1_id = "local-1776286786993"  # shs-e2e-app1
app2_id = "local-1776286804625"  # shs-e2e-app2
# A YARN app with two attempts (attempt 2 completed, attempt 1 incomplete).
yarn_app_id = "application_1713000000000_0001"  # shs-e2e-yarn

# A SQL execution present in both apps: the join + window + aggregation query.
sql_exec_id = 6


class McpClient:
    def __init__(self):
        self._client_session = None
        self._exit_stack = None

    @asynccontextmanager
    async def initialize(self):
        self._exit_stack = AsyncExitStack()
        async with AsyncExitStack() as stack:
            read, write, _ = await stack.enter_async_context(
                streamable_http_client(mcp_endpoint)
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
            streamable_http_client(mcp_endpoint)
        )
        self._client_session = await self._exit_stack.enter_async_context(
            ClientSession(read, write)
        )
        await self._client_session.initialize()
        return self


def _parse_list(result, model):
    """Parse a list-returning tool result into a list of model instances."""
    return [model.model_validate_json(c.text) for c in result.content]


def _parse_one(result, model):
    """Parse a single-object tool result into a model instance."""
    assert isinstance(result.content[0], TextContent)
    return model.model_validate_json(result.content[0].text)


@pytest.mark.asyncio
async def test_tools_not_empty():
    async with McpClient() as client:
        tool_result = await client.list_tools()
        assert tool_result, "Tools list should not be empty"
        assert len(tool_result.tools) > 0, "Tools list should contain at least one tool"
        names = {t.name for t in tool_result.tools}
        # New curated SQL tools must be registered.
        assert {
            "list_sql_executions",
            "get_sql_execution",
            "compare_sql_executions",
        }.issubset(names)


@pytest.mark.asyncio
async def test_list_applications():
    async with McpClient() as client:
        result = await client.call_tool("list_applications", {})
        assert not result.isError
        apps = [Application.model_validate_json(c.text) for c in result.content]
        by_id = {a.id: a.name for a in apps}
        assert by_id.get(app1_id) == "shs-e2e-app1"
        assert by_id.get(app2_id) == "shs-e2e-app2"


@pytest.mark.asyncio
async def test_list_applications_by_id():
    async with McpClient() as client:
        result = await client.call_tool("list_applications", {"app_id": app1_id})
        assert not result.isError
        apps = [Application.model_validate_json(c.text) for c in result.content]
        assert len(apps) == 1
        assert apps[0].id == app1_id
        assert apps[0].name == "shs-e2e-app1"


@pytest.mark.asyncio
async def test_list_applications_by_id_via_secondary_server():
    async with McpClient() as client:
        # Exercise explicit multi-server routing.
        result = await client.call_tool(
            "list_applications", {"app_id": app2_id, "server": "secondary"}
        )
        assert not result.isError
        apps = [Application.model_validate_json(c.text) for c in result.content]
        assert len(apps) == 1
        assert apps[0].id == app2_id
        assert apps[0].name == "shs-e2e-app2"


@pytest.mark.asyncio
async def test_list_applications_includes_attempts():
    """The unfiltered listing embeds each application's attempts."""
    async with McpClient() as client:
        result = await client.call_tool("list_applications", {})
        assert not result.isError
        apps = [Application.model_validate_json(c.text) for c in result.content]
        by_id = {a.id: a for a in apps}

        # Single-attempt local apps still carry an attempts list.
        app1 = by_id[app1_id]
        assert app1.attempts is not None
        assert len(app1.attempts) == 1
        assert app1.attempts[0].completed is True

        # The YARN app has two attempts: attempt 2 completed, attempt 1 not.
        yarn = by_id[yarn_app_id]
        assert yarn.attempts is not None
        assert len(yarn.attempts) == 2
        attempts_by_id = {a.attempt_id: a for a in yarn.attempts}
        assert set(attempts_by_id) == {"1", "2"}
        assert attempts_by_id["2"].completed is True
        assert attempts_by_id["2"].duration is not None
        assert attempts_by_id["1"].completed is False


@pytest.mark.asyncio
async def test_list_applications_by_id_includes_attempts():
    """Fetching a single app by id returns the same embedded attempts."""
    async with McpClient() as client:
        result = await client.call_tool("list_applications", {"app_id": yarn_app_id})
        assert not result.isError
        apps = [Application.model_validate_json(c.text) for c in result.content]
        assert len(apps) == 1
        attempts = apps[0].attempts
        assert attempts is not None
        assert len(attempts) == 2
        # Newest attempt is listed first and is the completed one.
        assert attempts[0].attempt_id == "2"
        assert attempts[0].completed is True
        assert attempts[0].start_time is not None
        assert attempts[0].end_time is not None
        assert attempts[1].attempt_id == "1"
        assert attempts[1].completed is False


@pytest.mark.asyncio
async def test_list_jobs_no_filter():
    async with McpClient() as client:
        jobs_result = await client.call_tool("list_jobs", {"app_id": app1_id})
        assert not jobs_result.isError
        jobs = [Job.model_validate_json(c.text) for c in jobs_result.content]
        assert len(jobs) == 23
        failed = [j for j in jobs if j.status == "FAILED"]
        assert len(failed) == 1
        assert failed[0].job_id == 4
        # Default ordering: failed jobs first (job 4 is the only failed job).
        assert jobs[0].job_id == 4
        assert jobs[0].status == "FAILED"


@pytest.mark.asyncio
async def test_list_jobs_with_status_filter():
    async with McpClient() as client:
        jobs_result = await client.call_tool(
            "list_jobs", {"app_id": app1_id, "status": ["FAILED"]}
        )
        assert not jobs_result.isError
        jobs = [Job.model_validate_json(c.text) for c in jobs_result.content]
        assert len(jobs) == 1
        assert jobs[0].status == "FAILED"
        assert jobs[0].job_id == 4


@pytest.mark.asyncio
async def test_list_jobs_with_job_id_filter():
    async with McpClient() as client:
        jobs_result = await client.call_tool(
            "list_jobs", {"app_id": app1_id, "job_id": 4}
        )
        assert not jobs_result.isError
        jobs = [Job.model_validate_json(c.text) for c in jobs_result.content]
        assert len(jobs) == 1
        assert jobs[0].job_id == 4
        assert jobs[0].status == "FAILED"


@pytest.mark.asyncio
async def test_list_jobs_sort_by_id():
    async with McpClient() as client:
        jobs_result = await client.call_tool(
            "list_jobs", {"app_id": app1_id, "sort_by": "id", "length": 5}
        )
        assert not jobs_result.isError
        jobs = [Job.model_validate_json(c.text) for c in jobs_result.content]
        # Descending job ID, limited to 5.
        assert [j.job_id for j in jobs] == [22, 21, 20, 19, 18]


@pytest.mark.asyncio
async def test_list_jobs_sort_by_duration():
    async with McpClient() as client:
        jobs_result = await client.call_tool(
            "list_jobs", {"app_id": app1_id, "sort_by": "duration"}
        )
        assert not jobs_result.isError
        jobs = [Job.model_validate_json(c.text) for c in jobs_result.content]
        # Job 20 is the longest-running job (~2.085s); pure duration sort ignores
        # status, so it comes before the failed job.
        assert jobs[0].job_id == 20


@pytest.mark.asyncio
async def test_list_jobs_sort_by_failed_tasks():
    async with McpClient() as client:
        jobs_result = await client.call_tool(
            "list_jobs", {"app_id": app1_id, "sort_by": "failed-tasks"}
        )
        assert not jobs_result.isError
        jobs = [Job.model_validate_json(c.text) for c in jobs_result.content]
        # Job 4 has the most failed tasks (7), then job 3 (4); the rest have 0.
        assert [j.job_id for j in jobs[:2]] == [4, 3]


# ---------------------------------------------------------------------------
# Stages
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_list_stages_default_order():
    async with McpClient() as client:
        result = await client.call_tool("list_stages", {"app_id": app1_id, "length": 5})
        assert not result.isError
        stages = [StageData.model_validate_json(c.text) for c in result.content]
        # Default: failed first, then by duration descending.
        assert [s.stage_id for s in stages] == [5, 43, 4, 0, 14]
        assert stages[0].status == "FAILED"


@pytest.mark.asyncio
async def test_list_stages_sort_by_duration():
    async with McpClient() as client:
        result = await client.call_tool(
            "list_stages", {"app_id": app1_id, "sort_by": "duration"}
        )
        assert not result.isError
        stages = [StageData.model_validate_json(c.text) for c in result.content]
        # Stage 43 is the longest-running stage (~2.083s).
        assert stages[0].stage_id == 43


@pytest.mark.asyncio
async def test_list_stages_sort_by_failed_tasks():
    async with McpClient() as client:
        result = await client.call_tool(
            "list_stages", {"app_id": app1_id, "sort_by": "failed-tasks"}
        )
        assert not result.isError
        stages = [StageData.model_validate_json(c.text) for c in result.content]
        # Stage 5 has the most failed tasks (7), then stage 4 (4); the rest have 0.
        assert [s.stage_id for s in stages[:2]] == [5, 4]


@pytest.mark.asyncio
async def test_list_stages_sort_by_id():
    async with McpClient() as client:
        result = await client.call_tool(
            "list_stages", {"app_id": app1_id, "sort_by": "id", "length": 5}
        )
        assert not result.isError
        stages = [StageData.model_validate_json(c.text) for c in result.content]
        # Descending stage ID, limited to 5 (app1 has 47 stages: 0..46).
        assert [s.stage_id for s in stages] == [46, 45, 44, 43, 42]


# ---------------------------------------------------------------------------
# Executors
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_list_executors_default():
    async with McpClient() as client:
        result = await client.call_tool("list_executors", {"app_id": app1_id})
        assert not result.isError
        execs = [Executor.model_validate_json(c.text) for c in result.content]
        # The driver is always present in this corpus (local mode).
        assert any(e.id == "driver" for e in execs)


@pytest.mark.asyncio
async def test_list_executors_executor_id_filter():
    async with McpClient() as client:
        result = await client.call_tool(
            "list_executors", {"app_id": app1_id, "executor_id": "driver"}
        )
        assert not result.isError
        execs = [Executor.model_validate_json(c.text) for c in result.content]
        assert len(execs) == 1
        assert execs[0].id == "driver"


# ---------------------------------------------------------------------------
# SQL tools
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_list_sql_executions_default_order():
    async with McpClient() as client:
        result = await client.call_tool(
            "list_sql_executions", {"app_id": app1_id, "limit": 0}
        )
        assert not result.isError
        execs = _parse_list(result, SqlExecutionSummary)
        # Default ordering: failed first, then by duration descending.
        assert [e.id for e in execs] == [2, 8, 6, 0, 1, 3, 7, 9, 4, 5]
        assert execs[0].status == "FAILED"
        assert execs[0].id == 2


@pytest.mark.asyncio
async def test_list_sql_executions_status_filter():
    async with McpClient() as client:
        result = await client.call_tool(
            "list_sql_executions", {"app_id": app1_id, "status": "FAILED"}
        )
        assert not result.isError
        execs = _parse_list(result, SqlExecutionSummary)
        assert [e.id for e in execs] == [2]


@pytest.mark.asyncio
async def test_list_sql_executions_description_filter():
    async with McpClient() as client:
        result = await client.call_tool(
            "list_sql_executions",
            {"app_id": app1_id, "description": "createOrReplaceTempView"},
        )
        assert not result.isError
        execs = _parse_list(result, SqlExecutionSummary)
        assert {e.id for e in execs} == {4, 5}


@pytest.mark.asyncio
async def test_get_sql_execution_header_only():
    async with McpClient() as client:
        result = await client.call_tool(
            "get_sql_execution", {"app_id": app1_id, "execution_id": sql_exec_id}
        )
        assert not result.isError
        detail = _parse_one(result, SqlExecutionDetail)
        assert detail.execution.id == sql_exec_id
        assert detail.execution.status == "COMPLETED"
        assert detail.execution.duration == 1659
        assert set(detail.execution.success_job_ids) == {8, 9, 10, 11, 12, 13, 14}
        # Header-only: heavier sections must be absent by default.
        assert detail.plan_description is None
        assert detail.node_metrics is None
        assert detail.jobs is None
        assert detail.stage_metrics is None
        assert detail.stages is None


@pytest.mark.asyncio
async def test_get_sql_execution_with_summary():
    async with McpClient() as client:
        result = await client.call_tool(
            "get_sql_execution",
            {
                "app_id": app1_id,
                "execution_id": sql_exec_id,
                "include_aggregated_metrics": True,
            },
        )
        assert not result.isError
        detail = _parse_one(result, SqlExecutionDetail)
        assert detail.jobs is not None
        assert len(detail.jobs) == 7
        agg = detail.stage_metrics
        assert agg is not None
        assert agg.stage_count == 25
        assert agg.tasks == 47
        assert agg.duration == 1188
        assert agg.input_bytes == 5527592
        assert agg.shuffle_read_bytes == 7568036
        assert agg.shuffle_write_bytes == 7556736
        assert agg.disk_bytes_spilled == 4223661
        assert agg.jvm_gc_time == 66
        # Stages list is only included when explicitly requested.
        assert detail.stages is None


@pytest.mark.asyncio
async def test_get_sql_execution_with_plan():
    async with McpClient() as client:
        result = await client.call_tool(
            "get_sql_execution",
            {"app_id": app1_id, "execution_id": sql_exec_id, "include_plan": True},
        )
        assert not result.isError
        detail = _parse_one(result, SqlExecutionDetail)
        assert detail.plan_description
        # AQE initial plans are stripped unless explicitly requested.
        assert "Initial Plan" not in detail.plan_description
        assert detail.node_metrics is not None
        assert len(detail.node_metrics) > 0


@pytest.mark.asyncio
async def test_get_sql_execution_with_stages():
    async with McpClient() as client:
        result = await client.call_tool(
            "get_sql_execution",
            {"app_id": app1_id, "execution_id": sql_exec_id, "include_stages": True},
        )
        assert not result.isError
        detail = _parse_one(result, SqlExecutionDetail)
        assert detail.stages is not None
        assert len(detail.stages) == 25


@pytest.mark.asyncio
async def test_compare_sql_executions():
    async with McpClient() as client:
        result = await client.call_tool(
            "compare_sql_executions",
            {
                "app_id1": app1_id,
                "app_id2": app2_id,
                "execution_id1": sql_exec_id,
                "execution_id2": sql_exec_id,
            },
        )
        assert not result.isError
        cmp = _parse_one(result, SqlExecutionComparison)

        assert cmp.a.app == app1_id
        assert cmp.a.sql_id == sql_exec_id
        assert cmp.a.duration == 1659
        assert cmp.a.jobs == 7
        assert cmp.a.stages == 25
        assert cmp.a.tasks == 47
        assert cmp.a.stage_time == 1188
        assert cmp.a.input_bytes == 5527592
        assert cmp.a.shuffle_read_bytes == 7568036
        assert cmp.a.shuffle_write_bytes == 7556736
        assert cmp.a.disk_bytes_spilled == 4223661
        assert cmp.a.jvm_gc_time == 66

        assert cmp.b.app == app2_id
        assert cmp.b.sql_id == sql_exec_id
        assert cmp.b.duration == 999
        assert cmp.b.jobs == 6
        assert cmp.b.stages == 14
        assert cmp.b.tasks == 20
        assert cmp.b.stage_time == 522
        assert cmp.b.input_bytes == 5527592
        assert cmp.b.shuffle_read_bytes == 54714
        assert cmp.b.shuffle_write_bytes == 43049
        assert cmp.b.disk_bytes_spilled == 0
        assert cmp.b.jvm_gc_time == 20


@pytest.mark.asyncio
async def test_compare_sql_executions_with_plan_diff():
    async with McpClient() as client:
        result = await client.call_tool(
            "compare_sql_executions",
            {
                "app_id1": app1_id,
                "app_id2": app2_id,
                "execution_id1": sql_exec_id,
                "execution_id2": sql_exec_id,
                "include_plan_diff": True,
            },
        )
        assert not result.isError
        cmp = _parse_one(result, SqlExecutionComparison)

        # Metrics are still present.
        assert cmp.a.tasks == 47
        assert cmp.b.tasks == 20

        # Plan diff is attached.
        pc = cmp.plan_comparison
        assert pc is not None
        assert pc.app_a == app1_id
        assert pc.app_b == app2_id
        assert pc.exec_id_a == sql_exec_id
        assert pc.exec_id_b == sql_exec_id
        assert pc.node_count_a == 37
        assert pc.node_count_b == 29
        assert pc.edge_count_a == 26
        assert pc.edge_count_b == 21

        diffs = {d.node_type: (d.a, d.b) for d in pc.node_type_diffs}
        assert diffs == {
            "AQEShuffleRead": (5, 3),
            "BroadcastExchange": (0, 1),
            "BroadcastHashJoin": (0, 1),
            "Exchange": (5, 3),
            "Sort": (4, 2),
            "SortMergeJoin": (1, 0),
            "WholeStageCodegen (7)": (1, 0),
            "WholeStageCodegen (8)": (1, 0),
            "WholeStageCodegen (9)": (1, 0),
        }


# ---------------------------------------------------------------------------
# Environment tool
# ---------------------------------------------------------------------------
# Every individually selectable section of the Environment model.
ENV_SECTION_FIELDS = [
    "runtime",
    "spark_properties",
    "system_properties",
    "hadoop_properties",
    "metrics_properties",
    "classpath_entries",
]


@pytest.mark.asyncio
async def test_get_environment_full():
    async with McpClient() as client:
        result = await client.call_tool("get_environment", {"app_id": app1_id})
        assert not result.isError
        env = _parse_one(result, Environment)
        # The full environment exposes every section the corpus provides.
        for field in ENV_SECTION_FIELDS:
            assert getattr(env, field) is not None, field


@pytest.mark.asyncio
@pytest.mark.parametrize("section", ENV_SECTION_FIELDS)
async def test_get_environment_section(section):
    async with McpClient() as client:
        result = await client.call_tool(
            "get_environment", {"app_id": app1_id, "section": section}
        )
        assert not result.isError
        env = _parse_one(result, Environment)
        # The requested section is populated and every other section is cleared.
        assert getattr(env, section) is not None, section
        for other in ENV_SECTION_FIELDS:
            if other != section:
                assert getattr(env, other) is None, other
        assert env.resource_profiles is None


@pytest.mark.asyncio
async def test_get_environment_invalid_section():
    async with McpClient() as client:
        result = await client.call_tool(
            "get_environment", {"app_id": app1_id, "section": "invalid"}
        )
        assert result.isError
