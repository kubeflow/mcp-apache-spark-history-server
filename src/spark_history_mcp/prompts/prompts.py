from typing import Optional

from spark_history_mcp.core.app import mcp


@mcp.prompt(
    title="Investigate Spark application failure",
)
def investigate_failure(app_id: str, server: Optional[str] = None) -> str:
    """Guided root-cause investigation for a failed Spark application.

    Walks from the failed application down to the individual task exceptions,
    using the Spark History Server tools exposed by this server.

    Args:
        app_id: The Spark application ID to investigate.
        server: Optional configured server name to query. When omitted, the
            tools discover which configured server hosts the application.
    """
    server_arg = f', server="{server}"' if server else ""

    if server:
        server_note = f"Target the `{server}` server on every call below."
    else:
        server_note = (
            "Omitting `server` makes each tool search every configured server "
            "for the application and use the one that has it; add "
            '`server="<name>"` only to disambiguate an id that exists on more '
            "than one server."
        )

    return f"""Investigate why Spark application `{app_id}` failed. Work through the steps in order; stop early only if the root cause becomes unambiguous.

{server_note}

1. Confirm the application: `list_applications(app_id="{app_id}"{server_arg})` — note status, attempts, and duration. If there are multiple attempts, the later tools default to the latest; pass `app_attempt_id` to those that accept it (e.g. `list_jobs`, `get_stage`, `get_environment`) to target a specific one.
2. Find failed jobs: `list_jobs(app_id="{app_id}", status=["FAILED"]{server_arg})`. If none report FAILED, list without a status filter and look for non-zero `num_failed_tasks` / `num_failed_stages`.
3. Find failed stages: `list_stages(app_id="{app_id}", status=["FAILED"]{server_arg})`, then `get_stage(app_id="{app_id}", stage_id=<id>, with_summaries=true{server_arg})` for detail.
4. Read the task exceptions (key step): `list_stage_task_failures(app_id="{app_id}", stage_id=<id>{server_arg})` for the per-task stack traces.
5. Check the environment when a version or config cause is suspected: `get_environment(app_id="{app_id}", section="runtime"{server_arg})` and `section="classpath_entries"` for Java/Scala/Spark and library versions; `section="spark_properties"` for relevant config (memory, serializer, shuffle).
6. Optional: if the job's source is in your workspace, map the failing class/method/line and any SQL call site to the code. Use it only to explain errors already in the data; do not assume it exists or speculate.
7. Summarize the root cause with specific evidence (stage, task, exception, version/config) and recommend a fix. For a resource issue (OOM, GC pressure), confirm first via `get_executor_summary`, `list_executors`, or `get_job_bottlenecks`."""


@mcp.prompt(
    title="Compare two Spark applications",
)
def compare_applications(
    app_a: str,
    app_b: str,
    server: Optional[str] = None,
    context: Optional[str] = None,
) -> str:
    """Guided, evidence-driven comparison of two Spark applications.

    Layered analysis (configuration, application-level performance, SQL
    executions or jobs, then stages) describing how two runs differ. Descriptive
    only: it reports what the data shows and does not diagnose causes or
    recommend changes.

    Args:
        app_a: The first Spark application ID (referred to as "A").
        app_b: The second Spark application ID (referred to as "B").
        server: Optional configured server name to query. When omitted, each
            application is discovered independently across all configured
            servers (so A and B may live on different servers).
        context: Optional free-text description of what the two runs represent
            (e.g. "A is yesterday's run, B is today's after raising shuffle
            partitions").
    """
    server_arg = f', server="{server}"' if server else ""

    if server:
        server_note = f"Target the `{server}` server on every call below."
    else:
        server_note = (
            "Omitting `server` locates each application independently across "
            "all configured servers, so A and B may live on different servers "
            '(e.g. prod vs staging); add `server="<name>"` only to force one '
            "server or to disambiguate an id present on more than one."
        )

    context_note = (
        f"\nContext: {context} — use it to focus, but back every observation "
        "with tool data.\n"
        if context
        else ""
    )

    return f"""Compare Spark applications A (`{app_a}`) and B (`{app_b}`) and report how they differ. Analysis only: describe what the data shows; do not diagnose why a difference occurred and do not recommend changes.

{server_note}
{context_note}
1. Configuration: `compare_job_environments(app_id1="{app_a}", app_id2="{app_b}"{server_arg})`. Focus on execution-affecting properties (shuffle partitions, executor/driver memory and cores, AQE, plugins/extensions, shuffle manager); ignore infrastructure noise (application ids, host/pod names, timestamps, ports).
2. Application-level metrics: `compare_job_performance(app_id1="{app_a}", app_id2="{app_b}"{server_arg})` — note deltas in executors, jobs, stages, tasks, input, shuffle read/write, disk spill, and GC time.
3. Unit of work — first call `list_sql_executions` on each app:
   - SQL/DataFrame apps (executions returned): match by description; for the largest gaps call `compare_sql_executions(app_id1="{app_a}", app_id2="{app_b}", execution_id1=<id_a>, execution_id2=<id_b>, include_plan_diff=true{server_arg})` for aggregated metrics and the plan-structure diff.
   - RDD / low-level apps (none returned): compare jobs instead via `list_jobs` on each app, matched by name and order; the plan diff is unavailable.
4. Stages: for the most divergent stages, call `get_stage(app_id="{app_a}", stage_id=<id>, with_summaries=true{server_arg})` (and the match for B) and compare task-metric quantiles for skew, spill, and GC (p25/p50/p75/max).
5. Summarize the differences side by side across configuration, application metrics, SQL/jobs, and stages, backing each with a specific number from the output above."""
