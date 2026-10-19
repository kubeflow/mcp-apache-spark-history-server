import os
import socket
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import pytest
from mcp import Client, StdioServerParameters
from mcp.types import TextContent
from mcp.types.version import LATEST_HANDSHAKE_VERSION, LATEST_MODERN_VERSION

from spark_history_mcp.api_client.models.application import Application
from spark_history_mcp.api_client.models.environment import Environment
from spark_history_mcp.api_client.models.executor import Executor
from spark_history_mcp.api_client.models.job import Job
from spark_history_mcp.api_client.models.stage_data import StageData
from spark_history_mcp.models.mcp_types import (
    FailedTask,
    SqlExecutionComparison,
    SqlExecutionDetail,
    SqlExecutionSummary,
    StageComparison,
)

mcp_endpoint = "http://localhost:18888/mcp/"

# Apps from the shared e2e corpus (see skills/cli/e2e/fixtures.yaml).
app1_id = "local-1776286786993"  # shs-e2e-app1
app2_id = "local-1776286804625"  # shs-e2e-app2
# A YARN app with two attempts (attempt 2 completed, attempt 1 incomplete).
yarn_app_id = "application_1713000000000_0001"  # shs-e2e-yarn

# A SQL execution present in both apps: the join + window + aggregation query.
sql_exec_id = 6


repo_root = Path(__file__).resolve().parent.parent
e2e_config = repo_root / "tests" / "config-e2e.yaml"

# Pin the modern version instead of "auto": a server that rejects it then fails
# the test instead of falling back to legacy.
LEGACY_MODE = "legacy"
MODERN_MODE = LATEST_MODERN_VERSION


@dataclass(frozen=True)
class Connection:
    mode: str
    transport: Literal["http", "stdio"]


CONNECTIONS = [
    Connection(LEGACY_MODE, "http"),
    Connection(MODERN_MODE, "http"),
    Connection(LEGACY_MODE, "stdio"),
    Connection(MODERN_MODE, "stdio"),
]
HTTP_CONNECTIONS = [c for c in CONNECTIONS if c.transport == "http"]

# The connection for the test that runs now. The `connection` fixture sets it,
# so each test body keeps `async with McpClient() as client:` unchanged.
_current_connection = CONNECTIONS[0]


def _connection_id(c: Connection) -> str:
    return f"{'modern' if c.mode == MODERN_MODE else 'legacy'}-{c.transport}"


def pytest_generate_tests(metafunc):
    """Run on HTTP in both eras; on stdio only for stdio_smoke or E2E_FULL_STDIO=1."""
    if "connection" not in metafunc.fixturenames:
        return
    full_stdio = os.environ.get("E2E_FULL_STDIO") == "1"
    smoke = metafunc.definition.get_closest_marker("stdio_smoke") is not None
    connections = CONNECTIONS if (full_stdio or smoke) else HTTP_CONNECTIONS
    metafunc.parametrize("connection", connections, ids=_connection_id, indirect=True)


@pytest.fixture(autouse=True)
def connection(request):
    global _current_connection
    _current_connection = request.param
    yield request.param
    _current_connection = CONNECTIONS[0]


def _stdio_server_params(
    env_overrides: dict[str, str | None] | None = None,
) -> StdioServerParameters:
    """Build the stdio server command; an override of None removes the variable."""
    # Drop SHS_* vars from the parent, so only the values below apply.
    env = {k: v for k, v in os.environ.items() if not k.startswith("SHS_")}
    env["SHS_MCP_CONFIG"] = str(e2e_config)
    env["SHS_MCP__TRANSPORT"] = "stdio"
    # Off EC2, the metadata probe for AWS credentials slows each server start.
    env["AWS_EC2_METADATA_DISABLED"] = "true"
    for key, value in (env_overrides or {}).items():
        if value is None:
            env.pop(key, None)
        else:
            env[key] = value
    return StdioServerParameters(
        command=sys.executable,
        args=["-m", "spark_history_mcp.core.main"],
        env=env,
        cwd=str(repo_root),
    )


class McpClient:
    """Connect to the e2e MCP server with the current era and transport."""

    def __init__(self, connection: Connection | None = None, endpoint=mcp_endpoint):
        conn = connection or _current_connection
        target = endpoint if conn.transport == "http" else _stdio_server_params()
        # Each call must reach the server, not the client response cache.
        self._client = Client(target, mode=conn.mode, cache=None)

    @property
    def protocol_version(self) -> str:
        return self._client.protocol_version

    @property
    def server_info(self):
        return self._client.server_info

    async def call_tool(self, name, arguments):
        return await self._client.call_tool(name, arguments)

    async def list_tools(self):
        return await self._client.list_tools()

    async def get_prompt(self, name, arguments):
        return await self._client.get_prompt(name, arguments)

    async def __aenter__(self):
        await self._client.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self._client.__aexit__(exc_type, exc_val, exc_tb)


def _parse_list(result, model):
    """Parse a list-returning tool result into a list of model instances."""
    return [model.model_validate_json(c.text) for c in result.content]


def _parse_one(result, model):
    """Parse a single-object tool result into a model instance."""
    assert isinstance(result.content[0], TextContent)
    return model.model_validate_json(result.content[0].text)


@pytest.mark.stdio_smoke
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


@pytest.mark.stdio_smoke
@pytest.mark.asyncio
async def test_protocol_version_negotiated(connection):
    """Each era connects at the expected protocol version."""
    async with McpClient() as client:
        # A pinned modern mode sends nothing at connect time. This request
        # makes sure that the server accepts the version.
        await client.list_tools()
        if connection.mode == LEGACY_MODE:
            assert client.protocol_version == LATEST_HANDSHAKE_VERSION
            assert client.server_info is not None
            assert client.server_info.name == "Spark Events"
        else:
            assert client.protocol_version == MODERN_MODE


_CREDENTIAL_HELPER = """
import json, os, sys
fd0, null = os.fstat(0), os.stat(os.devnull)
same = (fd0.st_dev, fd0.st_ino) == (null.st_dev, null.st_ino)
with open(sys.argv[1], "w") as f:
    f.write("devnull" if same else "inherited")
print(json.dumps({"Version": 1, "AccessKeyId": "AKIDEXAMPLE",
                  "SecretAccessKey": "secret", "SessionToken": "token"}))
"""


@pytest.mark.stdio_smoke
@pytest.mark.asyncio
async def test_credential_process_does_not_inherit_stdio(connection, tmp_path):
    """A credential_process helper must not get the MCP stdin pipe."""
    if connection.transport != "stdio":
        pytest.skip("stdio only")
    helper = tmp_path / "credential_helper.py"
    helper.write_text(_CREDENTIAL_HELPER)
    stdin_kind = tmp_path / "stdin_kind"
    aws_config = tmp_path / "aws_config"
    aws_config.write_text(
        "[default]\n"
        "region = us-west-2\n"
        f"credential_process = {sys.executable} {helper} {stdin_kind}\n"
    )
    # Remove other credential sources, so only credential_process applies.
    env: dict[str, str | None] = {k: None for k in os.environ if k.startswith("AWS_")}
    env.update(
        AWS_CONFIG_FILE=str(aws_config),
        AWS_SHARED_CREDENTIALS_FILE=str(tmp_path / "no_credentials"),
        AWS_EC2_METADATA_DISABLED="true",
    )
    client = Client(_stdio_server_params(env), mode=connection.mode, cache=None)
    async with client:
        names = {t.name for t in (await client.list_tools()).tools}

    assert "aws_analyze_spark_workload" in names
    assert stdin_kind.read_text() == "devnull"


@pytest.mark.stdio_smoke
@pytest.mark.asyncio
async def test_auto_mode_discovers_modern_server(connection):
    """The SDK default (mode="auto") discovers the newest modern version."""
    if connection.mode != MODERN_MODE:
        pytest.skip("runs once for each transport, on the modern rows")
    async with McpClient(Connection("auto", connection.transport)) as client:
        assert client.protocol_version == MODERN_MODE
        assert client.server_info is not None
        assert client.server_info.name == "Spark Events"


def _prompt_text(result):
    """Concatenate the text of every message in a GetPromptResult."""
    parts = []
    for message in result.messages:
        content = message.content
        if isinstance(content, TextContent):
            parts.append(content.text)
    return "\n".join(parts)


@pytest.mark.asyncio
async def test_investigate_failure_prompt_without_server():
    """The prompt renders with the app id and explains cross-server discovery
    when no server is specified, without injecting a concrete server name."""
    async with McpClient() as client:
        result = await client.get_prompt("investigate_failure", {"app_id": app1_id})
        text = _prompt_text(result)

        assert app1_id in text
        # Names the investigation tools the agent should call.
        for tool_name in ("list_jobs", "list_stages", "list_stage_task_failures"):
            assert tool_name in text
        # Explains automatic discovery; no concrete server name is threaded in.
        assert "search every configured" in text
        assert 'server="local"' not in text
        assert 'server="secondary"' not in text


@pytest.mark.stdio_smoke
@pytest.mark.asyncio
async def test_investigate_failure_prompt_with_server():
    """When a server is specified, it is threaded into the rendered tool calls."""
    async with McpClient() as client:
        result = await client.get_prompt(
            "investigate_failure", {"app_id": app1_id, "server": "secondary"}
        )
        text = _prompt_text(result)

        assert app1_id in text
        assert 'server="secondary"' in text


@pytest.mark.asyncio
async def test_compare_applications_prompt_without_server():
    """The compare prompt renders both app ids and the comparison tools, and
    explains cross-environment discovery without injecting a server name."""
    async with McpClient() as client:
        result = await client.get_prompt(
            "compare_applications", {"app_a": app1_id, "app_b": app2_id}
        )
        text = _prompt_text(result)

        assert app1_id in text
        assert app2_id in text
        for tool_name in (
            "compare_job_environments",
            "compare_job_performance",
            "compare_sql_executions",
        ):
            assert tool_name in text
        assert "may live on different" in text
        assert 'server="local"' not in text
        assert 'server="secondary"' not in text


@pytest.mark.asyncio
async def test_compare_applications_prompt_with_server_and_context():
    """A specified server is threaded into the calls and context is embedded."""
    async with McpClient() as client:
        result = await client.get_prompt(
            "compare_applications",
            {
                "app_a": app1_id,
                "app_b": app2_id,
                "server": "secondary",
                "context": "A is the baseline run",
            },
        )
        text = _prompt_text(result)

        assert 'server="secondary"' in text
        assert "A is the baseline run" in text


@pytest.mark.stdio_smoke
@pytest.mark.asyncio
async def test_list_applications():
    async with McpClient() as client:
        result = await client.call_tool("list_applications", {})
        assert not result.is_error
        apps = [Application.model_validate_json(c.text) for c in result.content]
        by_id = {a.id: a.name for a in apps}
        assert by_id.get(app1_id) == "shs-e2e-app1"
        assert by_id.get(app2_id) == "shs-e2e-app2"


@pytest.mark.asyncio
async def test_list_applications_by_id():
    async with McpClient() as client:
        result = await client.call_tool("list_applications", {"app_id": app1_id})
        assert not result.is_error
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
        assert not result.is_error
        apps = [Application.model_validate_json(c.text) for c in result.content]
        assert len(apps) == 1
        assert apps[0].id == app2_id
        assert apps[0].name == "shs-e2e-app2"


@pytest.mark.stdio_smoke
@pytest.mark.asyncio
async def test_unknown_server_errors():
    async with McpClient() as client:
        # An explicitly named server that does not exist must error, not
        # silently fall back to the default server.
        result = await client.call_tool(
            "list_applications", {"app_id": app2_id, "server": "does-not-exist"}
        )
        assert result.is_error
        # The error text tells the agent how to correct the call.
        assert result.content[0].text == (
            "Error executing tool list_applications: No Spark server named "
            "'does-not-exist' is configured (available servers: local, secondary)."
        )


@pytest.mark.asyncio
async def test_list_applications_includes_attempts():
    """The unfiltered listing embeds each application's attempts."""
    async with McpClient() as client:
        result = await client.call_tool("list_applications", {})
        assert not result.is_error
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
        assert not result.is_error
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
        assert not jobs_result.is_error
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
        assert not jobs_result.is_error
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
        assert not jobs_result.is_error
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
        assert not jobs_result.is_error
        jobs = [Job.model_validate_json(c.text) for c in jobs_result.content]
        # Descending job ID, limited to 5.
        assert [j.job_id for j in jobs] == [22, 21, 20, 19, 18]


@pytest.mark.asyncio
async def test_list_jobs_sort_by_duration():
    async with McpClient() as client:
        jobs_result = await client.call_tool(
            "list_jobs", {"app_id": app1_id, "sort_by": "duration"}
        )
        assert not jobs_result.is_error
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
        assert not jobs_result.is_error
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
        assert not result.is_error
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
        assert not result.is_error
        stages = [StageData.model_validate_json(c.text) for c in result.content]
        # Stage 43 is the longest-running stage (~2.083s).
        assert stages[0].stage_id == 43


@pytest.mark.asyncio
async def test_list_stages_sort_by_failed_tasks():
    async with McpClient() as client:
        result = await client.call_tool(
            "list_stages", {"app_id": app1_id, "sort_by": "failed-tasks"}
        )
        assert not result.is_error
        stages = [StageData.model_validate_json(c.text) for c in result.content]
        # Stage 5 has the most failed tasks (7), then stage 4 (4); the rest have 0.
        assert [s.stage_id for s in stages[:2]] == [5, 4]


@pytest.mark.asyncio
async def test_list_stages_sort_by_id():
    async with McpClient() as client:
        result = await client.call_tool(
            "list_stages", {"app_id": app1_id, "sort_by": "id", "length": 5}
        )
        assert not result.is_error
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
        assert not result.is_error
        execs = [Executor.model_validate_json(c.text) for c in result.content]
        # The driver is always present in this corpus (local mode).
        assert any(e.id == "driver" for e in execs)


@pytest.mark.asyncio
async def test_list_executors_executor_id_filter():
    async with McpClient() as client:
        result = await client.call_tool(
            "list_executors", {"app_id": app1_id, "executor_id": "driver"}
        )
        assert not result.is_error
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
        assert not result.is_error
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
        assert not result.is_error
        execs = _parse_list(result, SqlExecutionSummary)
        assert [e.id for e in execs] == [2]


@pytest.mark.asyncio
async def test_list_sql_executions_description_filter():
    async with McpClient() as client:
        result = await client.call_tool(
            "list_sql_executions",
            {"app_id": app1_id, "description": "createOrReplaceTempView"},
        )
        assert not result.is_error
        execs = _parse_list(result, SqlExecutionSummary)
        assert {e.id for e in execs} == {4, 5}


@pytest.mark.asyncio
async def test_get_sql_execution_header_only():
    async with McpClient() as client:
        result = await client.call_tool(
            "get_sql_execution", {"app_id": app1_id, "execution_id": sql_exec_id}
        )
        assert not result.is_error
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


@pytest.mark.stdio_smoke
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
        assert not result.is_error
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
        assert not result.is_error
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
        assert not result.is_error
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
        assert not result.is_error
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
        assert not result.is_error
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
        assert not result.is_error
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
        assert not result.is_error
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
        assert result.is_error
        assert result.content[0].text.startswith(
            "Error executing tool get_environment: invalid section 'invalid'"
        )


# ---------------------------------------------------------------------------
# compare_stages tool
# ---------------------------------------------------------------------------
# Stage 43 of app1 and stage 33 of app2 are the same query stage in each run
# (see skills/cli/e2e/compare_stages.json for the golden values).
app1_stage_id = 43
app2_stage_id = 33


@pytest.mark.asyncio
async def test_compare_stages_across_apps():
    async with McpClient() as client:
        result = await client.call_tool(
            "compare_stages",
            {
                "app_id1": app1_id,
                "stage_id1": app1_stage_id,
                "app_id2": app2_id,
                "stage_id2": app2_stage_id,
            },
        )
        assert not result.is_error
        cmp = _parse_one(result, StageComparison)

        # Side A: app1 / stage 43.
        a = cmp.a
        assert a.app == app1_id
        assert a.stage_id == app1_stage_id
        assert a.attempt_id == 0
        assert a.status == "COMPLETE"
        assert a.duration == 2083
        assert a.tasks == 3
        assert a.failed_tasks == 0
        assert a.input_bytes == 0
        assert a.output_bytes == 57978751
        assert a.shuffle_read_bytes == 60573008
        assert a.shuffle_write_bytes == 0
        assert a.disk_bytes_spilled == 70308061
        assert a.memory_bytes_spilled == 83884800
        assert a.jvm_gc_time == 96

        # Side B: app2 / stage 33.
        b = cmp.b
        assert b.app == app2_id
        assert b.stage_id == app2_stage_id
        assert b.duration == 1407
        assert b.tasks == 3
        assert b.output_bytes == 57979805
        assert b.shuffle_read_bytes == 60597318
        assert b.disk_bytes_spilled == 0
        assert b.memory_bytes_spilled == 0
        assert b.jvm_gc_time == 84


@pytest.mark.asyncio
async def test_compare_stages_task_quantiles():
    async with McpClient() as client:
        result = await client.call_tool(
            "compare_stages",
            {
                "app_id1": app1_id,
                "stage_id1": app1_stage_id,
                "app_id2": app2_id,
                "stage_id2": app2_stage_id,
            },
        )
        assert not result.is_error
        cmp = _parse_one(result, StageComparison)

        qa = cmp.a.task_quantiles
        assert qa is not None
        assert qa.quantiles == [0.25, 0.5, 0.75, 1.0]
        # Small integer-valued metrics are exact.
        assert qa.duration == [1008, 1055, 1063, 1063]
        assert qa.gc_time == [18, 36, 42, 42]
        assert qa.scheduler_delay == [4, 6, 9, 9]
        assert qa.peak_execution_memory == [6291392, 6291392, 12582784, 12582784]
        assert qa.input_bytes == [0, 0, 0, 0]
        assert qa.shuffle_write_bytes == [0, 0, 0, 0]
        # Large byte-valued metrics are 32-bit floats in the API; the Python
        # client widens them to 64-bit, so they differ by a few bytes from the
        # Go CLI golden values. Tolerate that representation gap.
        assert qa.output_bytes == pytest.approx(
            [14679969, 14719199, 28579584, 28579584], abs=2
        )
        assert qa.shuffle_read_bytes == pytest.approx(
            [15329098, 15377783, 29866128, 29866128], abs=2
        )
        assert qa.disk_bytes_spilled == pytest.approx(
            [17384420, 17588552, 35335090, 35335090], abs=2
        )
        assert qa.memory_bytes_spilled == pytest.approx(
            [20971200, 20971200, 41942400, 41942400], abs=2
        )

        # Side B has no spill, so its spill quantiles are all zero.
        qb = cmp.b.task_quantiles
        assert qb is not None
        assert qb.disk_bytes_spilled == [0, 0, 0, 0]
        assert qb.duration == [273, 1118, 1264, 1264]


@pytest.mark.asyncio
async def test_compare_stages_same_stage():
    async with McpClient() as client:
        result = await client.call_tool(
            "compare_stages",
            {
                "app_id1": app1_id,
                "stage_id1": app1_stage_id,
                "app_id2": app1_id,
                "stage_id2": app1_stage_id,
            },
        )
        assert not result.is_error
        cmp = _parse_one(result, StageComparison)
        # Comparing a stage with itself yields identical sides.
        assert cmp.a.duration == cmp.b.duration
        assert cmp.a.output_bytes == cmp.b.output_bytes
        assert cmp.a.task_quantiles.duration == cmp.b.task_quantiles.duration


@pytest.mark.asyncio
async def test_compare_stages_not_found():
    async with McpClient() as client:
        result = await client.call_tool(
            "compare_stages",
            {
                "app_id1": app1_id,
                "stage_id1": 999999,
                "app_id2": app2_id,
                "stage_id2": app2_stage_id,
            },
        )
        assert result.is_error


# ---------------------------------------------------------------------------
# get_executor_thread_dump tool
# ---------------------------------------------------------------------------
# The e2e corpus is replayed from event logs, so every application is completed.
# The History Server does not persist thread dumps, so the endpoint returns 404
# and the tool surfaces an error. This confirms the tool is wired up end to end.


@pytest.mark.asyncio
async def test_get_executor_thread_dump_completed_app_errors():
    async with McpClient() as client:
        result = await client.call_tool(
            "get_executor_thread_dump",
            {"app_id": app1_id, "executor_id": "driver"},
        )
        assert result.is_error
        text = result.content[0].text
        assert text.startswith("Error executing tool get_executor_thread_dump: (")
        assert "Reason:" in text
        assert "HTTP response headers" not in text


# ---------------------------------------------------------------------------
# list_stage_task_failures tool
# ---------------------------------------------------------------------------
# Stage 5 of app1 is a FAILED stage with 7 failed tasks, each raising a Python
# UDF exception (see skills/cli/e2e/fixtures.yaml: stage5_errors_app1).
failed_stage_id = 5


@pytest.mark.asyncio
async def test_list_stage_task_failures():
    async with McpClient() as client:
        result = await client.call_tool(
            "list_stage_task_failures",
            {"app_id": app1_id, "stage_id": failed_stage_id},
        )
        assert not result.is_error
        tasks = _parse_list(result, FailedTask)
        assert len(tasks) == 7
        for t in tasks:
            assert t.status == "FAILED"
            assert t.executor_id == "driver"
            assert t.error_message is not None
            assert t.error_message.startswith(
                "org.apache.spark.api.python.PythonException"
            )
        # Task IDs match the failed tasks recorded in the corpus.
        assert sorted(t.task_id for t in tasks) == [13, 14, 15, 16, 17, 18, 19]


@pytest.mark.asyncio
async def test_list_stage_task_failures_none_for_successful_stage():
    async with McpClient() as client:
        # Stage 43 completed successfully, so it has no failed tasks.
        result = await client.call_tool(
            "list_stage_task_failures",
            {"app_id": app1_id, "stage_id": app1_stage_id},
        )
        assert not result.is_error
        tasks = _parse_list(result, FailedTask)
        assert tasks == []


@pytest.mark.asyncio
async def test_list_stage_task_failures_not_found():
    async with McpClient() as client:
        result = await client.call_tool(
            "list_stage_task_failures",
            {"app_id": app1_id, "stage_id": 999999},
        )
        assert result.is_error
        # The Spark History Server answers HTTP 404 for an unknown stage.
        text = result.content[0].text
        assert text.startswith(
            "Error executing tool list_stage_task_failures: (404)\n"
        ), text
        assert "HTTP response headers" not in text, text


# Spark History Server backing the e2e corpus (started by `task cli:start-shs`).
shs_url = "http://localhost:18080"


def _wait_for_port(host, port, timeout=30.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(1.0)
            if sock.connect_ex((host, port)) == 0:
                return True
        time.sleep(0.5)
    return False


@pytest.mark.asyncio
async def test_legacy_single_underscore_env_vars_still_work(tmp_path, connection):
    """A server configured entirely with deprecated single-underscore SHS_* env
    vars still starts, serves, and warns about the deprecation.

    SHS_MCP_PORT proves the value was applied (the server listens on that port)
    and SHS_SERVERS_LOCAL_URL proves the configured Spark History Server is
    reached (the e2e corpus comes back).
    """
    if connection.transport != "http":
        pytest.skip("this test starts its own HTTP server")
    # Drop every SHS_* var the harness set, then configure exclusively through
    # the legacy single-underscore form. This dict is the child's env only; the
    # pytest process environment is never modified.

    # A distinct port so the legacy-config server never clashes with the harness.
    legacy_mcp_port = 18899
    legacy_mcp_endpoint = f"http://localhost:{legacy_mcp_port}/mcp/"

    env = {k: v for k, v in os.environ.items() if not k.startswith("SHS_")}
    env["SHS_MCP_TRANSPORT"] = "streamable-http"
    env["SHS_MCP_PORT"] = str(legacy_mcp_port)
    env["SHS_MCP_ADDRESS"] = "localhost"
    env["SHS_SERVERS_LOCAL_URL"] = shs_url
    env["SHS_SERVERS_LOCAL_DEFAULT"] = "true"

    log_path = tmp_path / "legacy-mcp.log"
    with open(log_path, "w") as log_file:
        proc = subprocess.Popen(  # noqa: S603  # fully controlled command
            [sys.executable, "-m", "spark_history_mcp.core.main"],
            env=env,
            cwd=tmp_path,  # avoid discovering any ./config.yaml in the repo
            stdout=log_file,
            stderr=subprocess.STDOUT,
        )
        try:
            assert _wait_for_port("localhost", legacy_mcp_port), (
                "server did not start on the port from SHS_MCP_PORT; log:\n"
                + log_path.read_text()
            )
            # Listening on legacy_mcp_port proves SHS_MCP_PORT was honored.
            async with McpClient(connection, endpoint=legacy_mcp_endpoint) as client:
                result = await client.call_tool("list_applications", {})
                assert not result.is_error
                apps = [Application.model_validate_json(c.text) for c in result.content]
                by_id = {a.id: a.name for a in apps}
                # SHS_SERVERS_LOCAL_URL routed to the e2e corpus.
                assert by_id.get(app1_id) == "shs-e2e-app1"
        finally:
            proc.terminate()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=10)

    log = log_path.read_text()
    # The deprecation warning fired and named the legacy variables in use.
    assert "deprecated" in log.lower()
    assert "SHS_MCP_PORT" in log
    assert "SHS_SERVERS_LOCAL_URL" in log
