import os
from datetime import datetime
import heapq
import logging
from typing import Any, Dict, List, Optional

from yoshi_client.domains.data_eng_infra.shared.libs.py.yoshi_client import Job

from spark_history_mcp.core.app import mcp
from spark_history_mcp.models.mcp_types import (
    JobSummary,
    SqlQuerySummary,
)
from spark_history_mcp.models.spark_types import (
    ApplicationInfo,
    ExecutionData,
    JobData,
    JobExecutionStatus,
    SQLExecutionStatus,
    StageData,
    StageStatus,
    TaskMetricDistributions,
)
from ..common.yoshi import Yoshi

from ..utils.utils import parallel_execute

logger = logging.getLogger(__name__)

DATACENTER = os.environ.get("DD_DATACENTER", "us1.staging.dog")


def get_client_or_default(
    ctx, server_name: Optional[str] = None, app_id: Optional[str] = None
):
    """
    Get a client by server name, app discovery, or default client.

    Args:
        ctx: The MCP context
        server_name: Optional server name
        app_id: Optional app ID for discovery

    Returns:
        SparkRestClient: The requested client

    Raises:
        ValueError: If no client is found
    """
    app_discovery = ctx.request_context.lifespan_context.app_discovery
    default_client = ctx.request_context.lifespan_context.default_client

    # If app_id provided, use discovery
    if app_id and not server_name:
        client, _ = app_discovery.get_client_for_app(app_id, server_name)
        return client

    clients = ctx.request_context.lifespan_context.clients

    if server_name:
        client = clients.get(server_name)
        if client:
            return client

    if default_client:
        return default_client

    raise ValueError(
        "No Spark client found. Please specify a valid server name or set a default server."
    )


@mcp.tool()
def list_applications(
    server: Optional[str] = None,
    status: Optional[list[str]] = None,
    min_date: Optional[str] = None,
    max_date: Optional[str] = None,
    min_end_date: Optional[str] = None,
    max_end_date: Optional[str] = None,
    limit: Optional[int] = None,
) -> list[ApplicationInfo]:
    """
    Get a list of applications from the Spark History Server.

    Args:
        server: Optional server name to use (uses default if not specified)
        status: Optional list only applications in the chosen state: [completed|running]
        min_date: Optional earliest start date/time to list
        max_date: Optional latest start date/time to list
        min_end_date: Optional earliest end date/time to list
        max_end_date: Optional latest end date/time to list
        limit: Optional maximum number of applications to return (limits the number of applications listed)
    Date format:
        - Accepted: YYYY-MM-DD["T"HH:mm:ss.SSS"GMT"]
        - Time is optional. If present, it must include milliseconds and the literal GMT.
        - Examples: 2015-02-10, 2015-02-03T16:42:40.000GMT
        - Timezone: All values are interpreted as GMT.
    Returns:
        List of ApplicationInfo objects for all applications
    """
    ctx = mcp.get_context()

    if server:
        # Return from specific server
        client = get_client_or_default(ctx, server)
        return client.list_applications(
            status=status,
            min_date=min_date,
            max_date=max_date,
            min_end_date=min_end_date,
            max_end_date=max_end_date,
            limit=limit,
        )
    else:
        # Return from all servers
        all_apps = []
        clients = ctx.request_context.lifespan_context.clients

        for server_name, client in clients.items():
            try:
                apps = client.list_applications(
                    status=status,
                    min_date=min_date,
                    max_date=max_date,
                    min_end_date=min_end_date,
                    max_end_date=max_end_date,
                    limit=limit,
                )
                all_apps.extend(apps)
            except Exception as e:
                logger.warning(
                    f"Failed to get applications from server '{server_name}': {e}"
                )
                continue  # Skip unreachable servers

        return all_apps


@mcp.tool()
def get_application(app_id: str, server: Optional[str] = None) -> ApplicationInfo:
    """
    Get detailed information about a specific Spark application.

    Retrieves comprehensive information about a Spark application including its
    status, resource usage, duration, and attempt details.

    Args:
        app_id: The Spark application ID (Must start by spark-)
        server: Optional server name to use (uses default if not specified)

    Returns:
        ApplicationInfo object containing application details
    """
    ctx = mcp.get_context()
    client = get_client_or_default(ctx, server, app_id)

    return client.get_application(app_id)


@mcp.tool()
def list_jobs(
    app_id: str, server: Optional[str] = None, status: Optional[list[str]] = None
) -> list:
    """
    Get a list of all jobs for a Spark application.

    Args:
        app_id: The Spark application ID
        server: Optional server name to use (uses default if not specified)
        status: Optional list of job status values to filter by

    Returns:
        List of JobData objects for the application
    """
    ctx = mcp.get_context()
    client = get_client_or_default(ctx, server, app_id)

    # Convert string status values to JobExecutionStatus enum if provided
    job_statuses = None
    if status:
        job_statuses = [JobExecutionStatus.from_string(s) for s in status]

    return client.list_jobs(app_id=app_id, status=job_statuses)


@mcp.tool()
def list_slowest_jobs(
    app_id: str,
    server: Optional[str] = None,
    include_running: bool = False,
    n: int = 5,
) -> List[JobData]:
    """
    Get the N slowest jobs for a Spark application.

    Retrieves all jobs for the application and returns the ones with the longest duration.

    Args:
        app_id: The Spark application ID
        server: Optional server name to use (uses default if not specified)
        include_running: Whether to include running jobs in the search
        n: Number of slowest jobs to return (default: 5)

    Returns:
        List of JobData objects for the slowest jobs, or empty list if no jobs found
    """
    ctx = mcp.get_context()
    client = get_client_or_default(ctx, server, app_id)

    # Get all jobs
    jobs = client.list_jobs(app_id=app_id)

    if not jobs:
        return []

    # Filter out running jobs if not included
    if not include_running:
        jobs = [job for job in jobs if job.status != JobExecutionStatus.RUNNING.value]

    if not jobs:
        return []

    def get_job_duration(job):
        if job.completion_time and job.submission_time:
            return (job.completion_time - job.submission_time).total_seconds()
        return 0

    return heapq.nlargest(n, jobs, key=get_job_duration)


@mcp.tool()
def list_stages(
    app_id: str,
    server: Optional[str] = None,
    status: Optional[list[str]] = None,
    with_summaries: bool = False,
) -> list:
    """
    Get a list of all stages for a Spark application.

    Retrieves information about stages in a Spark application with options to filter
    by status and include additional details and summary metrics.

    Args:
        app_id: The Spark application ID
        server: Optional server name to use (uses default if not specified)
        status: Optional list of stage status values to filter by
        with_summaries: Whether to include summary metrics in the response

    Returns:
        List of StageData objects for the application
    """
    ctx = mcp.get_context()
    client = get_client_or_default(ctx, server, app_id)

    # Convert string status values to StageStatus enum if provided
    stage_statuses = None
    if status:
        stage_statuses = [StageStatus.from_string(s) for s in status]

    return client.list_stages(
        app_id=app_id,
        status=stage_statuses,
        with_summaries=with_summaries,
    )


@mcp.tool()
def list_slowest_stages(
    app_id: str,
    server: Optional[str] = None,
    include_running: bool = False,
    n: int = 5,
) -> List[StageData]:
    """
    Get the N slowest stages for a Spark application.

    Retrieves all stages for the application and returns the ones with the longest duration.

    Args:
        app_id: The Spark application ID
        server: Optional server name to use (uses default if not specified)
        include_running: Whether to include running stages in the search
        n: Number of slowest stages to return (default: 5)

    Returns:
        List of StageData objects for the slowest stages, or empty list if no stages found
    """
    ctx = mcp.get_context()
    client = get_client_or_default(ctx, server, app_id)

    stages = client.list_stages(app_id=app_id)

    # Filter out running stages if not included. This avoids using the `details` param which can significantly slow down the execution time
    if not include_running:
        stages = [stage for stage in stages if stage.status != "RUNNING"]

    if not stages:
        return []

    def get_stage_duration(stage: StageData):
        if stage.completion_time and stage.first_task_launched_time:
            return (
                stage.completion_time - stage.first_task_launched_time
            ).total_seconds()
        return 0

    return heapq.nlargest(n, stages, key=get_stage_duration)


@mcp.tool()
def get_stage(
    app_id: str,
    stage_id: int,
    attempt_id: Optional[int] = None,
    server: Optional[str] = None,
    with_summaries: bool = False,
) -> StageData:
    """
    Get information about a specific stage.

    Args:
        app_id: The Spark application ID
        stage_id: The stage ID
        attempt_id: Optional stage attempt ID (if not provided, returns the latest attempt)
        server: Optional server name to use (uses default if not specified)
        with_summaries: Whether to include summary metrics

    Returns:
        StageData object containing stage information
    """
    ctx = mcp.get_context()
    client = get_client_or_default(ctx, server, app_id)

    if attempt_id is not None:
        # Get specific attempt
        stage_data = client.get_stage_attempt(
            app_id=app_id,
            stage_id=stage_id,
            attempt_id=attempt_id,
            details=False,
            with_summaries=with_summaries,
        )
    else:
        # Get all attempts and use the latest one
        stages = client.list_stage_attempts(
            app_id=app_id,
            stage_id=stage_id,
            details=False,
            with_summaries=with_summaries,
        )

        if not stages:
            raise ValueError(f"No stage found with ID {stage_id}")

        # If multiple attempts exist, get the one with the highest attempt_id
        if isinstance(stages, list):
            stage_data = max(stages, key=lambda s: s.attempt_id)
        else:
            stage_data = stages

    # If summaries were requested but metrics distributions are missing, fetch them separately
    if with_summaries and (
        not hasattr(stage_data, "task_metrics_distributions")
        or stage_data.task_metrics_distributions is None
    ):
        task_summary = client.get_stage_task_summary(
            app_id=app_id,
            stage_id=stage_id,
            attempt_id=stage_data.attempt_id,
        )
        stage_data.task_metrics_distributions = task_summary

    return stage_data


@mcp.tool()
def get_environment(app_id: str, server: Optional[str] = None):
    """
    Get the comprehensive Spark runtime configuration for a Spark application.

    Details including JVM information, Spark properties, system properties,
    classpath entries, and environment variables.

    Args:
        app_id: The Spark application ID
        server: Optional server name to use (uses default if not specified)

    Returns:
        ApplicationEnvironmentInfo object containing environment details
    """
    ctx = mcp.get_context()
    client = get_client_or_default(ctx, server, app_id)

    return client.get_environment(app_id=app_id)


@mcp.tool()
def list_executors(
    app_id: str, server: Optional[str] = None, include_inactive: bool = False
):
    """
    Get executor information for a Spark application.

    Retrieves a list of executors (active by default) for the specified Spark application
    with their resource allocation, task statistics, and performance metrics.

    Args:
        app_id: The Spark application ID
        server: Optional server name to use (uses default if not specified)
        include_inactive: Whether to include inactive executors (default: False)

    Returns:
        List of ExecutorSummary objects containing executor information
    """
    ctx = mcp.get_context()
    client = get_client_or_default(ctx, server, app_id)

    if include_inactive:
        return client.list_all_executors(app_id=app_id)
    else:
        return client.list_executors(app_id=app_id)


@mcp.tool()
def get_executor(app_id: str, executor_id: str, server: Optional[str] = None):
    """
    Get information about a specific executor.

    Retrieves detailed information about a single executor including resource allocation,
    task statistics, memory usage, and performance metrics.

    Args:
        app_id: The Spark application ID
        executor_id: The executor ID
        server: Optional server name to use (uses default if not specified)

    Returns:
        ExecutorSummary object containing executor details or None if not found
    """
    ctx = mcp.get_context()
    client = get_client_or_default(ctx, server, app_id)

    # Get all executors and find the one with matching ID
    executors = client.list_all_executors(app_id=app_id)

    for executor in executors:
        if executor.id == executor_id:
            return executor

    return None


@mcp.tool()
def get_executor_summary(app_id: str, server: Optional[str] = None):
    """
    Aggregates metrics across all executors for a Spark application.

    Retrieves all executors (active and inactive) and calculates summary statistics
    including memory usage, disk usage, task counts, and performance metrics.

    Args:
        app_id: The Spark application ID
        server: Optional server name to use (uses default if not specified)

    Returns:
        Dictionary containing aggregated executor metrics
    """
    ctx = mcp.get_context()
    client = get_client_or_default(ctx, server, app_id)

    executors = client.list_all_executors(app_id=app_id)
    return _calculate_executor_metrics(executors)


@mcp.tool()
def compare_job_environments(
    app_id1: str, app_id2: str, server: Optional[str] = None
) -> Dict[str, Any]:
    """
    Compare Spark environment configurations between two jobs.

    Identifies differences in Spark properties, JVM settings, system properties,
    and other configuration parameters between two Spark applications.

    Args:
        app_id1: First Spark application ID
        app_id2: Second Spark application ID
        server: Optional server name to use (uses default if not specified)

    Returns:
        Dictionary containing configuration differences and similarities
    """
    ctx = mcp.get_context()
    client1 = get_client_or_default(ctx, server, app_id1)
    client2 = get_client_or_default(ctx, server, app_id2)

    env1 = client1.get_environment(app_id=app_id1)
    env2 = client2.get_environment(app_id=app_id2)

    def props_to_dict(props):
        return {k: v for k, v in props} if props else {}

    spark_props1 = props_to_dict(env1.spark_properties)
    spark_props2 = props_to_dict(env2.spark_properties)

    system_props1 = props_to_dict(env1.system_properties)
    system_props2 = props_to_dict(env2.system_properties)

    comparison = {
        "applications": {"app1": app_id1, "app2": app_id2},
        "runtime_comparison": {
            "app1": {
                "java_version": env1.runtime.java_version,
                "java_home": env1.runtime.java_home,
                "scala_version": env1.runtime.scala_version,
            },
            "app2": {
                "java_version": env2.runtime.java_version,
                "java_home": env2.runtime.java_home,
                "scala_version": env2.runtime.scala_version,
            },
        },
        "spark_properties": {
            "common": {
                k: {"app1": v, "app2": spark_props2.get(k)}
                for k, v in spark_props1.items()
                if k in spark_props2 and v == spark_props2[k]
            },
            "different": {
                k: {"app1": v, "app2": spark_props2.get(k, "NOT_SET")}
                for k, v in spark_props1.items()
                if k in spark_props2 and v != spark_props2[k]
            },
            "only_in_app1": {
                k: v for k, v in spark_props1.items() if k not in spark_props2
            },
            "only_in_app2": {
                k: v for k, v in spark_props2.items() if k not in spark_props1
            },
        },
        "system_properties": {
            "key_differences": {
                k: {
                    "app1": system_props1.get(k, "NOT_SET"),
                    "app2": system_props2.get(k, "NOT_SET"),
                }
                for k in [
                    "java.version",
                    "java.runtime.version",
                    "os.name",
                    "os.version",
                    "user.timezone",
                    "file.encoding",
                ]
                if system_props1.get(k) != system_props2.get(k)
            }
        },
    }

    return comparison


def _calculate_executor_metrics(executors):
    """Calculate executor summary metrics from executor list."""
    return {
        "total_executors": len(executors),
        "active_executors": sum(1 for e in executors if e.is_active),
        "memory_used": sum(
            e.memory_metrics.used_on_heap_storage_memory
            + e.memory_metrics.used_off_heap_storage_memory
            for e in executors
        ),
        "disk_used": sum(e.disk_used for e in executors),
        "completed_tasks": sum(e.completed_tasks for e in executors),
        "failed_tasks": sum(e.failed_tasks for e in executors),
        "total_duration": sum(e.total_duration for e in executors),
        "total_gc_time": sum(e.total_gc_time for e in executors),
        "total_input_bytes": sum(e.total_input_bytes for e in executors),
        "total_shuffle_read": sum(e.total_shuffle_read for e in executors),
        "total_shuffle_write": sum(e.total_shuffle_write for e in executors),
    }


def _calc_executor_summary_from_client(client, app_id: str):
    """Helper function to calculate executor summary without MCP context."""
    executors = client.list_all_executors(app_id=app_id)
    return _calculate_executor_metrics(executors)


@mcp.tool()
def compare_job_performance(
    app_id1: str, app_id2: str, server: Optional[str] = None
) -> Dict[str, Any]:
    """
    Compare performance metrics between two Spark jobs.

    Analyzes execution times, resource usage, task distribution, and other
    performance indicators to identify differences between jobs.

    Args:
        app_id1: First Spark application ID
        app_id2: Second Spark application ID
        server: Optional server name to use (uses default if not specified)

    Returns:
        Dictionary containing detailed performance comparison
    """
    ctx = mcp.get_context()
    client1 = get_client_or_default(ctx, server, app_id1)
    client2 = get_client_or_default(ctx, server, app_id2)

    # Define API calls for parallel execution
    api_calls = [
        ("app1", lambda: client1.get_application(app_id1)),
        ("app2", lambda: client2.get_application(app_id2)),
        ("exec_summary1", lambda: _calc_executor_summary_from_client(client1, app_id1)),
        ("exec_summary2", lambda: _calc_executor_summary_from_client(client2, app_id2)),
        ("jobs1", lambda: client1.list_jobs(app_id=app_id1)),
        ("jobs2", lambda: client2.list_jobs(app_id=app_id2)),
    ]

    # Execute all API calls in parallel
    execution_result = parallel_execute(
        api_calls,
        max_workers=6,
        timeout=300,  # Apply generous timeout for large scale Spark applications
    )

    # If parallel execution fails, try sequential as fallback
    if execution_result["errors"] and len(execution_result["results"]) == 0:
        try:
            # Sequential fallback - get basic info first
            app1 = client1.get_application(app_id1)
            app2 = client2.get_application(app_id2)

            # Use the actual errors from parallel execution
            error_summary = "; ".join(execution_result["errors"])
            return {
                "error": f"Parallel execution failed: {error_summary}. Falling back to basic app info only.",
                "partial_data": {
                    "app1": {"id": app_id1, "name": app1.name},
                    "app2": {"id": app_id2, "name": app2.name},
                },
            }
        except Exception as e:
            # If even basic app info fails, provide the original errors plus this failure
            all_errors = execution_result["errors"] + [
                f"Sequential fallback failed: {str(e)}"
            ]
            return {"error": f"Complete failure: {'; '.join(all_errors)}"}

    if execution_result["errors"]:
        return {"error": f"API failures: {'; '.join(execution_result['errors'])}"}

    results = execution_result["results"]

    # Extract results
    app1 = results["app1"]
    app2 = results["app2"]
    exec_summary1 = results["exec_summary1"]
    exec_summary2 = results["exec_summary2"]
    jobs1 = results["jobs1"]
    jobs2 = results["jobs2"]

    # Calculate job duration statistics
    def calc_job_stats(jobs):
        if not jobs:
            return {"count": 0, "total_duration": 0, "avg_duration": 0}

        completed_jobs = [j for j in jobs if j.completion_time and j.submission_time]
        if not completed_jobs:
            return {"count": len(jobs), "total_duration": 0, "avg_duration": 0}

        durations = [
            (j.completion_time - j.submission_time).total_seconds()
            for j in completed_jobs
        ]

        return {
            "count": len(jobs),
            "completed_count": len(completed_jobs),
            "total_duration": sum(durations),
            "avg_duration": sum(durations) / len(durations),
            "min_duration": min(durations),
            "max_duration": max(durations),
        }

    job_stats1 = calc_job_stats(jobs1)
    job_stats2 = calc_job_stats(jobs2)

    comparison = {
        "applications": {
            "app1": {"id": app_id1, "name": app1.name},
            "app2": {"id": app_id2, "name": app2.name},
        },
        "resource_allocation": {
            "app1": {
                "cores_granted": app1.cores_granted,
                "max_cores": app1.max_cores,
                "cores_per_executor": app1.cores_per_executor,
                "memory_per_executor_mb": app1.memory_per_executor_mb,
            },
            "app2": {
                "cores_granted": app2.cores_granted,
                "max_cores": app2.max_cores,
                "cores_per_executor": app2.cores_per_executor,
                "memory_per_executor_mb": app2.memory_per_executor_mb,
            },
        },
        "executor_metrics": {
            "app1": exec_summary1,
            "app2": exec_summary2,
            "comparison": {
                "executor_count_ratio": exec_summary2["total_executors"]
                / max(exec_summary1["total_executors"], 1),
                "memory_usage_ratio": exec_summary2["memory_used"]
                / max(exec_summary1["memory_used"], 1),
                "task_completion_ratio": exec_summary2["completed_tasks"]
                / max(exec_summary1["completed_tasks"], 1),
                "gc_time_ratio": exec_summary2["total_gc_time"]
                / max(exec_summary1["total_gc_time"], 1),
            },
        },
        "job_performance": {
            "app1": job_stats1,
            "app2": job_stats2,
            "comparison": {
                "job_count_ratio": job_stats2["count"] / max(job_stats1["count"], 1),
                "avg_duration_ratio": job_stats2["avg_duration"]
                / max(job_stats1["avg_duration"], 1)
                if job_stats1["avg_duration"] > 0
                else 0,
                "total_duration_ratio": job_stats2["total_duration"]
                / max(job_stats1["total_duration"], 1)
                if job_stats1["total_duration"] > 0
                else 0,
            },
        },
    }

    return comparison


@mcp.tool()
def compare_sql_execution_plans(
    app_id1: str,
    app_id2: str,
    execution_id1: Optional[int] = None,
    execution_id2: Optional[int] = None,
    server: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Compare SQL execution plans between two Spark jobs.

    Analyzes the logical and physical plans, identifies differences in operations,
    and compares execution metrics between SQL queries.

    Args:
        app_id1: First Spark application ID
        app_id2: Second Spark application ID
        execution_id1: Optional specific execution ID for first app (uses longest if not specified)
        execution_id2: Optional specific execution ID for second app (uses longest if not specified)
        server: Optional server name to use (uses default if not specified)

    Returns:
        Dictionary containing SQL execution plan comparison
    """
    ctx = mcp.get_context()
    client1 = get_client_or_default(ctx, server, app_id1)
    client2 = get_client_or_default(ctx, server, app_id2)

    # Get SQL executions for both applications
    sql_execs1 = client1.get_sql_list(
        app_id=app_id1, details=True, plan_description=True
    )
    sql_execs2 = client2.get_sql_list(
        app_id=app_id2, details=True, plan_description=True
    )

    # If specific execution IDs not provided, use the longest running ones
    if execution_id1 is None and sql_execs1:
        execution_id1 = max(sql_execs1, key=lambda x: x.duration or 0).id
    if execution_id2 is None and sql_execs2:
        execution_id2 = max(sql_execs2, key=lambda x: x.duration or 0).id

    if execution_id1 is None or execution_id2 is None:
        return {
            "error": "No SQL executions found in one or both applications",
            "app1_sql_count": len(sql_execs1),
            "app2_sql_count": len(sql_execs2),
        }

    # Get specific execution details
    exec1 = client1.get_sql_execution(
        app_id1, execution_id1, details=True, plan_description=True
    )
    exec2 = client2.get_sql_execution(
        app_id2, execution_id2, details=True, plan_description=True
    )

    # Analyze nodes and operations
    def analyze_nodes(execution):
        node_types = {}
        for node in execution.nodes:
            node_type = node.node_name
            if node_type not in node_types:
                node_types[node_type] = 0
            node_types[node_type] += 1
        return node_types

    nodes1 = analyze_nodes(exec1)
    nodes2 = analyze_nodes(exec2)

    all_node_types = set(nodes1.keys()) | set(nodes2.keys())

    comparison = {
        "applications": {"app1": app_id1, "app2": app_id2},
        "executions": {
            "app1": {
                "execution_id": execution_id1,
                "duration": exec1.duration,
                "status": exec1.status,
                "node_count": len(exec1.nodes),
                "edge_count": len(exec1.edges),
            },
            "app2": {
                "execution_id": execution_id2,
                "duration": exec2.duration,
                "status": exec2.status,
                "node_count": len(exec2.nodes),
                "edge_count": len(exec2.edges),
            },
        },
        "plan_structure": {
            "node_type_comparison": {
                node_type: {
                    "app1_count": nodes1.get(node_type, 0),
                    "app2_count": nodes2.get(node_type, 0),
                }
                for node_type in sorted(all_node_types)
            },
            "complexity_metrics": {
                "node_count_ratio": len(exec2.nodes) / max(len(exec1.nodes), 1),
                "edge_count_ratio": len(exec2.edges) / max(len(exec1.edges), 1),
                "duration_ratio": (exec2.duration or 0) / max(exec1.duration or 1, 1),
            },
        },
        "job_associations": {
            "app1": {
                "running_jobs": exec1.running_job_ids,
                "success_jobs": exec1.success_job_ids,
                "failed_jobs": exec1.failed_job_ids,
            },
            "app2": {
                "running_jobs": exec2.running_job_ids,
                "success_jobs": exec2.success_job_ids,
                "failed_jobs": exec2.failed_job_ids,
            },
        },
    }

    return comparison


@mcp.tool()
def get_stage_task_summary(
    app_id: str,
    stage_id: int,
    attempt_id: int = 0,
    server: Optional[str] = None,
    quantiles: str = "0.05,0.25,0.5,0.75,0.95",
) -> TaskMetricDistributions:
    """
    Get a summary of task metrics for a specific stage.

    Retrieves statistical distributions of task metrics for a stage, including
    execution times, memory usage, I/O metrics, and shuffle metrics.

    Args:
        app_id: The Spark application ID
        stage_id: The stage ID
        attempt_id: The stage attempt ID (default: 0)
        server: Optional server name to use (uses default if not specified)
        quantiles: Comma-separated list of quantiles to use for summary metrics

    Returns:
        TaskMetricDistributions object containing metric distributions
    """
    ctx = mcp.get_context()
    client = get_client_or_default(ctx, server, app_id)

    return client.get_stage_task_summary(
        app_id=app_id, stage_id=stage_id, attempt_id=attempt_id, quantiles=quantiles
    )


def truncate_plan_description(plan_desc: str, max_length: int) -> str:
    """
    Truncate plan description while preserving structure.

    Args:
        plan_desc: The plan description to truncate
        max_length: Maximum length in characters

    Returns:
        Truncated plan description with indicator if truncated
    """
    if not plan_desc or len(plan_desc) <= max_length:
        return plan_desc

    # Try to truncate at a logical boundary (end of a line)
    truncated = plan_desc[:max_length]
    last_newline = truncated.rfind("\n")

    # If we can preserve most content by truncating at newline, do so
    if last_newline > max_length * 0.8:
        truncated = truncated[:last_newline]

    return truncated + "\n... [truncated]"


@mcp.tool()
def list_slowest_sql_queries(
    app_id: str,
    server: Optional[str] = None,
    attempt_id: Optional[str] = None,
    top_n: int = 1,
    page_size: int = 100,
    include_running: bool = False,
    include_plan_description: Optional[bool] = None,
    plan_description_max_length: int = 2000,
) -> List[SqlQuerySummary]:
    """
    Get the N slowest SQL queries for a Spark application.

    Args:
        app_id: The Spark application ID
        server: Optional server name to use (uses default if not specified)
        attempt_id: Optional attempt ID
        top_n: Number of slowest queries to return (default: 1)
        page_size: Number of executions to fetch per page (default: 100)
        include_running: Whether to include running queries (default: False)
        include_plan_description: Whether to include execution plans (uses server config if not specified)
        plan_description_max_length: Max characters for plan description (default: 1500)

    Returns:
        List of SqlQuerySummary objects for the slowest queries
    """
    ctx = mcp.get_context()
    client = get_client_or_default(ctx, server, app_id)

    # Config takes priority: if config is set (True/False), use it; otherwise default to True
    if client.config.include_plan_description is not None:
        include_plan_description = client.config.include_plan_description
    else:
        include_plan_description = True

    all_executions: List[ExecutionData] = []
    offset = 0

    # Fetch all pages of SQL executions
    while True:
        executions: List[ExecutionData] = client.get_sql_list(
            app_id=app_id,
            attempt_id=attempt_id,
            details=True,
            plan_description=True,
            offset=offset,
            length=page_size,
        )

        if not executions:
            break

        all_executions.extend(executions)
        offset += page_size

        # If we got fewer executions than the page size, we've reached the end
        if len(executions) < page_size:
            break

    # Filter out running queries if not included
    if not include_running:
        all_executions = [
            e for e in all_executions if e.status != SQLExecutionStatus.RUNNING.value
        ]

    # Get the top N slowest executions
    slowest_executions = heapq.nlargest(top_n, all_executions, key=lambda e: e.duration)

    # Create simplified results without additional API calls. Raw object is too verbose.
    simplified_results = []
    for execution in slowest_executions:
        job_summary = JobSummary(
            success_job_ids=execution.success_job_ids,
            failed_job_ids=execution.failed_job_ids,
            running_job_ids=execution.running_job_ids,
        )

        # Handle plan description based on include_plan_description flag
        plan_description = ""
        if include_plan_description and execution.plan_description:
            plan_description = truncate_plan_description(
                execution.plan_description, plan_description_max_length
            )

        query_summary = SqlQuerySummary(
            id=execution.id,
            duration=execution.duration,
            description=execution.description,
            status=execution.status,
            submission_time=execution.submission_time.isoformat()
            if execution.submission_time
            else None,
            plan_description=plan_description,
            job_summary=job_summary,
        )

        simplified_results.append(query_summary)

    return simplified_results


@mcp.tool()
def get_job_bottlenecks(
    app_id: str, server: Optional[str] = None, top_n: int = 5
) -> Dict[str, Any]:
    """
    Identify performance bottlenecks in a Spark job.

    Analyzes stages, tasks, and executors to find the most time-consuming
    operations and resource-intensive components.

    Args:
        app_id: The Spark application ID
        server: Optional server name to use (uses default if not specified)
        top_n: Number of top bottlenecks to return

    Returns:
        Dictionary containing identified bottlenecks and recommendations
    """
    ctx = mcp.get_context()
    client = get_client_or_default(ctx, server, app_id)

    # Get slowest stages
    slowest_stages = list_slowest_stages(app_id, server, False, top_n)

    # Get slowest jobs
    slowest_jobs = list_slowest_jobs(app_id, server, False, top_n)

    # Get executor summary
    exec_summary = get_executor_summary(app_id, server)

    all_stages = client.list_stages(app_id=app_id)

    # Identify stages with high spill
    high_spill_stages = []
    for stage in all_stages:
        if (
            stage.memory_bytes_spilled
            and stage.memory_bytes_spilled > 100 * 1024 * 1024
        ):  # > 100MB
            high_spill_stages.append(
                {
                    "stage_id": stage.stage_id,
                    "attempt_id": stage.attempt_id,
                    "name": stage.name,
                    "memory_spilled_mb": stage.memory_bytes_spilled / (1024 * 1024),
                    "disk_spilled_mb": stage.disk_bytes_spilled / (1024 * 1024)
                    if stage.disk_bytes_spilled
                    else 0,
                }
            )

    high_spill_stages = heapq.nlargest(
        len(high_spill_stages), high_spill_stages, key=lambda x: x["memory_spilled_mb"]
    )

    # Identify GC pressure
    gc_pressure = (
        exec_summary["total_gc_time"] / max(exec_summary["total_duration"], 1)
        if exec_summary["total_duration"] > 0
        else 0
    )

    bottlenecks = {
        "application_id": app_id,
        "performance_bottlenecks": {
            "slowest_stages": [
                {
                    "stage_id": stage.stage_id,
                    "attempt_id": stage.attempt_id,
                    "name": stage.name,
                    "duration_seconds": (
                        stage.completion_time - stage.submission_time
                    ).total_seconds()
                    if stage.completion_time and stage.submission_time
                    else 0,
                    "task_count": stage.num_tasks,
                    "failed_tasks": stage.num_failed_tasks,
                }
                for stage in slowest_stages[:top_n]
            ],
            "slowest_jobs": [
                {
                    "job_id": job.job_id,
                    "name": job.name,
                    "duration_seconds": (
                        job.completion_time - job.submission_time
                    ).total_seconds()
                    if job.completion_time and job.submission_time
                    else 0,
                    "failed_tasks": job.num_failed_tasks,
                    "status": job.status,
                }
                for job in slowest_jobs[:top_n]
            ],
        },
        "resource_bottlenecks": {
            "memory_spill_stages": high_spill_stages[:top_n],
            "gc_pressure_ratio": gc_pressure,
            "executor_utilization": {
                "total_executors": exec_summary["total_executors"],
                "active_executors": exec_summary["active_executors"],
                "utilization_ratio": exec_summary["active_executors"]
                / max(exec_summary["total_executors"], 1),
            },
        },
        "recommendations": [],
    }

    # Generate recommendations
    if gc_pressure > 0.1:  # More than 10% time in GC
        bottlenecks["recommendations"].append(
            {
                "type": "memory",
                "priority": "high",
                "issue": f"High GC pressure ({gc_pressure:.1%})",
                "suggestion": "Consider increasing executor memory or reducing memory usage",
            }
        )

    if high_spill_stages:
        bottlenecks["recommendations"].append(
            {
                "type": "memory",
                "priority": "high",
                "issue": f"Memory spilling detected in {len(high_spill_stages)} stages",
                "suggestion": "Increase executor memory or optimize data partitioning",
            }
        )

    if exec_summary["failed_tasks"] > 0:
        bottlenecks["recommendations"].append(
            {
                "type": "reliability",
                "priority": "medium",
                "issue": f"{exec_summary['failed_tasks']} failed tasks",
                "suggestion": "Investigate task failures and consider increasing task retry settings",
            }
        )

    return bottlenecks


@mcp.tool()
def get_resource_usage_timeline(
    app_id: str, server: Optional[str] = None
) -> Dict[str, Any]:
    """
    Get resource usage timeline for a Spark application.

    Provides a chronological view of resource allocation and usage patterns
    including executor additions/removals and stage execution overlap.

    Args:
        app_id: The Spark application ID
        server: Optional server name to use (uses default if not specified)

    Returns:
        Dictionary containing timeline of resource usage
    """
    ctx = mcp.get_context()
    client = get_client_or_default(ctx, server, app_id)

    # Get application info
    app = client.get_application(app_id)

    # Get all executors
    executors = client.list_all_executors(app_id=app_id)

    # Get stages
    stages = client.list_stages(app_id=app_id)

    # Create timeline events
    timeline_events = []

    # Add executor events
    for executor in executors:
        if executor.add_time:
            timeline_events.append(
                {
                    "timestamp": executor.add_time,
                    "type": "executor_add",
                    "executor_id": executor.id,
                    "cores": executor.total_cores,
                    "memory_mb": executor.max_memory / (1024 * 1024)
                    if executor.max_memory
                    else 0,
                }
            )

        if executor.remove_time:
            timeline_events.append(
                {
                    "timestamp": executor.remove_time,
                    "type": "executor_remove",
                    "executor_id": executor.id,
                    "reason": executor.remove_reason,
                }
            )

    # Add stage events
    for stage in stages:
        if stage.submission_time:
            timeline_events.append(
                {
                    "timestamp": stage.submission_time,
                    "type": "stage_start",
                    "stage_id": stage.stage_id,
                    "attempt_id": stage.attempt_id,
                    "name": stage.name,
                    "task_count": stage.num_tasks,
                }
            )

        if stage.completion_time:
            timeline_events.append(
                {
                    "timestamp": stage.completion_time,
                    "type": "stage_end",
                    "stage_id": stage.stage_id,
                    "attempt_id": stage.attempt_id,
                    "status": stage.status,
                    "duration_seconds": (
                        stage.completion_time - stage.submission_time
                    ).total_seconds()
                    if stage.submission_time
                    else 0,
                }
            )

    # Sort events by timestamp
    timeline_events.sort(key=lambda x: x["timestamp"])

    # Calculate resource utilization over time
    active_executors = 0
    total_cores = 0
    total_memory = 0

    resource_timeline = []

    for event in timeline_events:
        if event["type"] == "executor_add":
            active_executors += 1
            total_cores += event["cores"]
            total_memory += event["memory_mb"]
        elif event["type"] == "executor_remove":
            active_executors -= 1
            # Note: We don't have cores/memory info in remove events

        resource_timeline.append(
            {
                "timestamp": event["timestamp"],
                "active_executors": active_executors,
                "total_cores": total_cores,
                "total_memory_mb": total_memory,
                "event": event,
            }
        )

    return {
        "application_id": app_id,
        "application_name": app.name,
        "summary": {
            "total_events": len(timeline_events),
            "executor_additions": len(
                [e for e in timeline_events if e["type"] == "executor_add"]
            ),
            "executor_removals": len(
                [e for e in timeline_events if e["type"] == "executor_remove"]
            ),
            "stage_executions": len(
                [e for e in timeline_events if e["type"] == "stage_start"]
            ),
            "peak_executors": max(
                [r["active_executors"] for r in resource_timeline] + [0]
            ),
            "peak_cores": max([r["total_cores"] for r in resource_timeline] + [0]),
        },
    }


@mcp.tool()
def get_job_definition(job_id: str) -> Job:
    """
    Get job definition about a mortar/yoshi job.
    The mortar/yoshi job contains the information of the spark app id

    Args:
        job_id: Job identifier

    Returns:
        Job: Job definition
    """
    return Yoshi(DATACENTER).get_job_definition(job_id)


# @mcp.tool()
def get_job_logs(
    job_id: str,
    status: Optional[str] = None,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
):
    pass


# @mcp.tool()
def get_operator_logs(
    job_id: str,
    status: Optional[str] = None,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
):
    # can be merged with get_job_logs???
    pass


# @mcp.tool()
def get_workflow_logs(
    workflow_id: str,
    status: Optional[str] = None,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
):
    pass


# @mcp.tool()
def get_admission_logs(
    job_id: str,
    status: Optional[str] = None,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
):
    pass


# @mcp.tool()
def get_oom_metrics_job(job_id: str):
    # oom_kill.oom_process.count
    pass


# @mcp.tool()
def get_cpu_metrics_job(job_id: str):
    pass


# @mcp.tool()
def get_memory_metrics_job(job_id: str):
    pass
