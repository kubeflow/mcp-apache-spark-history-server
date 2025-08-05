from collections import defaultdict
from typing import Any, Callable, Dict, List, Optional, TypeVar

from spark_history_mcp.api.client_factory import create_spark_emr_client
from spark_history_mcp.api.emr_client import EMRClient
from spark_history_mcp.api.spark_client import SparkRestClient
from spark_history_mcp.core.app import mcp
from spark_history_mcp.models.server_spec import ServerSpec
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

emr_cluster_id_to_arn_cache: Dict[str, str] = {}
arn_to_spark_emr_client_cache: Dict[str, SparkRestClient] = {}
session_emr_cluster_name_to_arn_cache: Dict[int, Dict[str, str]] = defaultdict(dict)


K = TypeVar("K")  # Key type
V = TypeVar("V")  # Value type


def _get_cacheable(key: K, cache: Dict[K, V], get_value_func: Callable[[K], V]) -> V:
    if key in cache:
        return cache[key]

    value = get_value_func(key)
    cache[key] = value
    return value


def _get_emr_client(ctx) -> EMRClient:
    emr_client: EMRClient = ctx.request_context.lifespan_context.emr_client
    if not emr_client:
        raise ValueError("EMR client is not initialized in dynamic mode")
    return emr_client


def get_client(ctx, server_spec: ServerSpec) -> SparkRestClient:
    """
    Get a client by ServerSpec.

    Args:
        ctx: The MCP context
        server_spec: ServerSpec

    Returns:
        SparkRestClient: The requested client or default client

    Raises:
        ValueError: If no client is found or configuration is invalid
    """

    if server_spec.static_server_spec:
        if ctx.request_context.lifespan_context.dynamic_emr_clusters_mode:
            raise ValueError(
                "MCP is running in dynamic EMR mode, but static server spec was provided."
            )

        spec = server_spec.static_server_spec

        if spec.default_client:
            default_client = (
                ctx.request_context.lifespan_context.static_clients.default_client
            )
            if not default_client:
                raise ValueError("No default client configured")
            return default_client

        if spec.server_name:
            clients = ctx.request_context.lifespan_context.static_clients.clients
            client = clients.get(spec.server_name)
            if not client:
                raise ValueError(f"No server configured with name: {spec.server_name}")
            return client

    if server_spec.dynamic_emr_server_spec:
        if not ctx.request_context.lifespan_context.dynamic_emr_clusters_mode:
            raise ValueError(
                "MCP is not running in dynamic EMR mode, but dynamic server spec was provided."
            )

        spec = server_spec.dynamic_emr_server_spec

        if spec.emr_cluster_arn:
            arn = spec.emr_cluster_arn

        elif spec.emr_cluster_id:
            arn = _get_cacheable(
                spec.emr_cluster_id,
                emr_cluster_id_to_arn_cache,
                lambda cluster_id: _get_emr_client(ctx).get_cluster_arn_by_id(
                    cluster_id
                ),
            )

        elif spec.emr_cluster_name:
            arn = _get_cacheable(
                spec.emr_cluster_name,
                session_emr_cluster_name_to_arn_cache[id(ctx.session)],
                lambda cluster_name: _get_emr_client(
                    ctx
                ).get_active_cluster_arn_by_name(cluster_name),
            )

        else:
            raise ValueError("Invalid server_spec")

        return _get_cacheable(
            arn,
            arn_to_spark_emr_client_cache,
            lambda _arn: create_spark_emr_client(_arn),
        )

    raise ValueError("Invalid server_spec")


@mcp.tool()
def get_application(app_id: str, server_spec: ServerSpec) -> ApplicationInfo:
    """
    Get detailed information about a specific Spark application.

    Retrieves comprehensive information about a Spark application including its
    status, resource usage, duration, and attempt details.

    Args:
        app_id: The Spark application ID
        server_spec: ServerSpec for specifying the server

    Returns:
        ApplicationInfo object containing application details
    """
    ctx = mcp.get_context()
    client = get_client(ctx, server_spec)

    return client.get_application(app_id)


@mcp.tool()
def list_jobs(
    app_id: str, server_spec: ServerSpec, status: Optional[list[str]] = None
) -> list:
    """
    Get a list of all jobs for a Spark application.

    Args:
        app_id: The Spark application ID
        server_spec: ServerSpec for specifying the server
        status: Optional list of job status values to filter by

    Returns:
        List of JobData objects for the application
    """
    ctx = mcp.get_context()
    client = get_client(ctx, server_spec)

    # Convert string status values to JobExecutionStatus enum if provided
    job_statuses = None
    if status:
        job_statuses = [JobExecutionStatus.from_string(s) for s in status]

    return client.list_jobs(app_id=app_id, status=job_statuses)


@mcp.tool()
def list_slowest_jobs(
    app_id: str,
    server_spec: ServerSpec,
    include_running: bool = False,
    n: int = 5,
) -> List[JobData]:
    """
    Get the N slowest jobs for a Spark application.

    Retrieves all jobs for the application and returns the ones with the longest duration.

    Args:
        app_id: The Spark application ID
        server_spec: ServerSpec for specifying the server
        include_running: Whether to include running jobs in the search
        n: Number of slowest jobs to return (default: 5)

    Returns:
        List of JobData objects for the slowest jobs, or empty list if no jobs found
    """
    ctx = mcp.get_context()
    client = get_client(ctx, server_spec)

    # Get all jobs
    jobs = client.list_jobs(app_id=app_id)

    if not jobs:
        return []

    # Filter out running jobs if not included
    if not include_running:
        jobs = [job for job in jobs if job.status != JobExecutionStatus.RUNNING.value]

    if not jobs:
        return []

    # Sort jobs by duration (descending)
    def get_job_duration(job):
        if job.completion_time and job.submission_time:
            return (job.completion_time - job.submission_time).total_seconds()
        return 0

    sorted_jobs = sorted(jobs, key=get_job_duration, reverse=True)

    return sorted_jobs[:n]


@mcp.tool()
def list_stages(
    app_id: str,
    server_spec: ServerSpec,
    status: Optional[list[str]] = None,
    with_summaries: bool = False,
) -> list:
    """
    Get a list of all stages for a Spark application.

    Retrieves information about stages in a Spark application with options to filter
    by status and include additional details and summary metrics.

    Args:
        app_id: The Spark application ID
        server_spec: ServerSpec for specifying the server
        status: Optional list of stage status values to filter by
        with_summaries: Whether to include summary metrics in the response

    Returns:
        List of StageData objects for the application
    """
    ctx = mcp.get_context()
    client = get_client(ctx, server_spec)

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
    server_spec: ServerSpec,
    include_running: bool = False,
    n: int = 5,
) -> List[StageData]:
    """
    Get the N slowest stages for a Spark application.

    Retrieves all stages for the application and returns the ones with the longest duration.

    Args:
        app_id: The Spark application ID
        server_spec: ServerSpec for specifying the server
        include_running: Whether to include running stages in the search
        n: Number of slowest stages to return (default: 5)

    Returns:
        List of StageData objects for the slowest stages, or empty list if no stages found
    """
    ctx = mcp.get_context()
    client = get_client(ctx, server_spec)

    # Get all stages with details
    stages = client.list_stages(app_id=app_id, details=True)

    if not stages:
        return []

    # Filter out running stages if not included
    if not include_running:
        stages = [stage for stage in stages if stage.status != "RUNNING"]

    if not stages:
        return []

    # Sort stages by duration (descending) and take top N
    # Calculate duration from completion_time and submission_time
    def get_stage_duration(stage):
        if stage.completion_time and stage.submission_time:
            return (stage.completion_time - stage.submission_time).total_seconds()
        return 0

    sorted_stages = sorted(stages, key=get_stage_duration, reverse=True)

    return sorted_stages[:n]


@mcp.tool()
def get_stage(
    app_id: str,
    stage_id: int,
    server_spec: ServerSpec,
    attempt_id: Optional[int] = None,
    with_summaries: bool = False,
) -> StageData:
    """
    Get information about a specific stage.

    Args:
        app_id: The Spark application ID
        stage_id: The stage ID
        server_spec: ServerSpec for specifying the server
        attempt_id: Optional stage attempt ID (if not provided, returns the latest attempt)
        with_summaries: Whether to include summary metrics

    Returns:
        StageData object containing stage information
    """
    ctx = mcp.get_context()
    client = get_client(ctx, server_spec)

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
def get_environment(app_id: str, server_spec: ServerSpec):
    """
    Get the comprehensive Spark runtime configuration for a Spark application.

    Details including JVM information, Spark properties, system properties,
    classpath entries, and environment variables.

    Args:
        app_id: The Spark application ID
        server_spec: ServerSpec for specifying the server

    Returns:
        ApplicationEnvironmentInfo object containing environment details
    """
    ctx = mcp.get_context()
    client = get_client(ctx, server_spec)

    return client.get_environment(app_id=app_id)


@mcp.tool()
def list_executors(
    app_id: str, server_spec: ServerSpec, include_inactive: bool = False
):
    """
    Get executor information for a Spark application.

    Retrieves a list of executors (active by default) for the specified Spark application
    with their resource allocation, task statistics, and performance metrics.

    Args:
        app_id: The Spark application ID
        server_spec: ServerSpec for specifying the server
        include_inactive: Whether to include inactive executors (default: False)

    Returns:
        List of ExecutorSummary objects containing executor information
    """
    ctx = mcp.get_context()
    client = get_client(ctx, server_spec)

    if include_inactive:
        return client.list_all_executors(app_id=app_id)
    else:
        return client.list_executors(app_id=app_id)


@mcp.tool()
def get_executor(app_id: str, executor_id: str, server_spec: ServerSpec):
    """
    Get information about a specific executor.

    Retrieves detailed information about a single executor including resource allocation,
    task statistics, memory usage, and performance metrics.

    Args:
        app_id: The Spark application ID
        executor_id: The executor ID
        server_spec: ServerSpec for specifying the server

    Returns:
        ExecutorSummary object containing executor details or None if not found
    """
    ctx = mcp.get_context()
    client = get_client(ctx, server_spec)

    # Get all executors and find the one with matching ID
    executors = client.list_all_executors(app_id=app_id)

    for executor in executors:
        if executor.id == executor_id:
            return executor

    return None


@mcp.tool()
def get_executor_summary(app_id: str, server_spec: ServerSpec):
    """
    Aggregates metrics across all executors for a Spark application.

    Retrieves all executors (active and inactive) and calculates summary statistics
    including memory usage, disk usage, task counts, and performance metrics.

    Args:
        app_id: The Spark application ID
        server_spec: ServerSpec for specifying the server

    Returns:
        Dictionary containing aggregated executor metrics
    """
    ctx = mcp.get_context()
    client = get_client(ctx, server_spec)

    executors = client.list_all_executors(app_id=app_id)

    summary = {
        "total_executors": len(executors),
        "active_executors": sum(1 for e in executors if e.is_active),
        "memory_used": 0,
        "disk_used": 0,
        "completed_tasks": 0,
        "failed_tasks": 0,
        "total_duration": 0,
        "total_gc_time": 0,
        "total_input_bytes": 0,
        "total_shuffle_read": 0,
        "total_shuffle_write": 0,
    }

    # Aggregate metrics from all executors
    for executor in executors:
        summary["memory_used"] += (
            executor.memory_metrics.used_on_heap_storage_memory
            + executor.memory_metrics.used_off_heap_storage_memory
        )
        summary["disk_used"] += executor.disk_used
        summary["completed_tasks"] += executor.completed_tasks
        summary["failed_tasks"] += executor.failed_tasks
        summary["total_duration"] += executor.total_duration
        summary["total_gc_time"] += executor.total_gc_time
        summary["total_input_bytes"] += executor.total_input_bytes
        summary["total_shuffle_read"] += executor.total_shuffle_read
        summary["total_shuffle_write"] += executor.total_shuffle_write

    return summary


@mcp.tool()
def compare_job_environments(
    app_id1: str, app_id2: str, server_spec: ServerSpec
) -> Dict[str, Any]:
    """
    Compare Spark environment configurations between two jobs.

    Identifies differences in Spark properties, JVM settings, system properties,
    and other configuration parameters between two Spark applications.

    Args:
        app_id1: First Spark application ID
        app_id2: Second Spark application ID
        server_spec: ServerSpec for specifying the server

    Returns:
        Dictionary containing configuration differences and similarities
    """
    ctx = mcp.get_context()
    client = get_client(ctx, server_spec)

    env1 = client.get_environment(app_id=app_id1)
    env2 = client.get_environment(app_id=app_id2)

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


@mcp.tool()
def compare_job_performance(
    app_id1: str, app_id2: str, server_spec: ServerSpec
) -> Dict[str, Any]:
    """
    Compare performance metrics between two Spark jobs.

    Analyzes execution times, resource usage, task distribution, and other
    performance indicators to identify differences between jobs.

    Args:
        app_id1: First Spark application ID
        app_id2: Second Spark application ID
        server_spec: ServerSpec for specifying the server

    Returns:
        Dictionary containing detailed performance comparison
    """
    ctx = mcp.get_context()
    client = get_client(ctx, server_spec)

    # Get application info
    app1 = client.get_application(app_id1)
    app2 = client.get_application(app_id2)

    # Get executor summaries
    exec_summary1 = get_executor_summary(app_id1, server_spec)
    exec_summary2 = get_executor_summary(app_id2, server_spec)

    # Get job data
    jobs1 = client.list_jobs(app_id=app_id1)
    jobs2 = client.list_jobs(app_id=app_id2)

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
    server_spec: ServerSpec,
    execution_id1: Optional[int] = None,
    execution_id2: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Compare SQL execution plans between two Spark jobs.

    Analyzes the logical and physical plans, identifies differences in operations,
    and compares execution metrics between SQL queries.

    Args:
        app_id1: First Spark application ID
        app_id2: Second Spark application ID
        server_spec: ServerSpec for specifying the server
        execution_id1: Optional specific execution ID for first app (uses longest if not specified)
        execution_id2: Optional specific execution ID for second app (uses longest if not specified)

    Returns:
        Dictionary containing SQL execution plan comparison
    """
    ctx = mcp.get_context()
    client = get_client(ctx, server_spec)

    # Get SQL executions for both applications
    sql_execs1 = client.get_sql_list(
        app_id=app_id1, details=True, plan_description=True
    )
    sql_execs2 = client.get_sql_list(
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
    exec1 = client.get_sql_execution(
        app_id1, execution_id1, details=True, plan_description=True
    )
    exec2 = client.get_sql_execution(
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
    server_spec: ServerSpec,
    attempt_id: int = 0,
    quantiles: str = "0.05,0.25,0.5,0.75,0.95",
) -> TaskMetricDistributions:
    """
    Get a summary of task metrics for a specific stage.

    Retrieves statistical distributions of task metrics for a stage, including
    execution times, memory usage, I/O metrics, and shuffle metrics.

    Args:
        app_id: The Spark application ID
        stage_id: The stage ID
        server_spec: ServerSpec for specifying the server
        attempt_id: The stage attempt ID (default: 0)
        quantiles: Comma-separated list of quantiles to use for summary metrics

    Returns:
        TaskMetricDistributions object containing metric distributions
    """
    ctx = mcp.get_context()
    client = get_client(ctx, server_spec)

    return client.get_stage_task_summary(
        app_id=app_id, stage_id=stage_id, attempt_id=attempt_id, quantiles=quantiles
    )


@mcp.tool()
def list_slowest_sql_queries(
    app_id: str,
    server_spec: ServerSpec,
    attempt_id: Optional[str] = None,
    top_n: int = 1,
    page_size: int = 100,
    include_running: bool = False,
) -> List[ExecutionData]:
    """
    Get a summary of the top N slowest SQL queries for an application.

    Args:
        app_id: The Spark application ID
        server_spec: ServerSpec for specifying the server
        attempt_id: Optional attempt ID
        top_n: Number of slowest queries to return
        page_size: Number of executions to fetch per page
        include_running: Whether to include running queries in the results

    Returns:
        List of ExecutionData objects for the slowest queries
        The total time metric (shown with time unit "m" for minutes) represents cumulative CPU time spent across all parallel tasks performing the scan operation
        This should be interpreted alongside the min/median/max metrics, which show the distribution of individual task durations.
    """
    ctx = mcp.get_context()
    client = get_client(ctx, server_spec)

    all_executions: List[ExecutionData] = []
    offset = 0

    # Fetch all pages of SQL executions
    while True:
        executions: List[ExecutionData] = client.get_sql_list(
            app_id=app_id,
            attempt_id=attempt_id,
            details=True,
            plan_description=False,
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

    # Sort by duration (descending) and take top N
    sorted_executions = sorted(all_executions, key=lambda e: e.duration, reverse=True)
    return sorted_executions[:top_n]


@mcp.tool()
def get_job_bottlenecks(
    app_id: str, server_spec: ServerSpec, top_n: int = 5
) -> Dict[str, Any]:
    """
    Identify performance bottlenecks in a Spark job.

    Analyzes stages, tasks, and executors to find the most time-consuming
    operations and resource-intensive components.

    Args:
        app_id: The Spark application ID
        server_spec: ServerSpec for specifying the server
        top_n: Number of top bottlenecks to return

    Returns:
        Dictionary containing identified bottlenecks and recommendations
    """
    ctx = mcp.get_context()
    client = get_client(ctx, server_spec)

    # Get slowest stages
    slowest_stages = list_slowest_stages(app_id, server_spec, False, top_n)

    # Get slowest jobs
    slowest_jobs = list_slowest_jobs(app_id, server_spec, False, top_n)

    # Get executor summary
    exec_summary = get_executor_summary(app_id, server_spec)

    # Get all stages for detailed analysis
    all_stages = client.list_stages(app_id=app_id, details=True)

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

    # Sort by memory spilled
    high_spill_stages.sort(key=lambda x: x["memory_spilled_mb"], reverse=True)

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
def get_resource_usage_timeline(app_id: str, server_spec: ServerSpec) -> Dict[str, Any]:
    """
    Get resource usage timeline for a Spark application.

    Provides a chronological view of resource allocation and usage patterns
    including executor additions/removals and stage execution overlap.

    Args:
        app_id: The Spark application ID
        server_spec: ServerSpec for specifying the server

    Returns:
        Dictionary containing timeline of resource usage
    """
    ctx = mcp.get_context()
    client = get_client(ctx, server_spec)

    # Get application info
    app = client.get_application(app_id)

    # Get all executors
    executors = client.list_all_executors(app_id=app_id)

    # Get stages
    stages = client.list_stages(app_id=app_id, details=True)

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
        "timeline": resource_timeline,
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
