package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

const primeText = `shs — Spark History Server CLI (for AI agents)

COMMANDS
  shs apps                        List applications
  shs apps -a APP_ID --attempts   List attempts for a YARN application
  shs jobs -a APP_ID              List jobs for an application
  shs jobs -a APP_ID JOB_ID       Get job detail
  shs stages -a APP_ID            List stages for an application
  shs stages -a APP_ID STAGE      Get stage detail with full metrics and task quantiles
  shs stages -a APP_ID STAGE --errors  Show failed tasks with errors
  shs executors -a APP_ID         List active executors
  shs executors -a APP_ID EXEC    Get executor detail
  shs executors -a APP_ID --summary   Peak memory, lifecycle, remove reasons
  shs executors -a APP_ID --timeline  Executor add/remove timeline with resource totals
  shs sql -a APP_ID               List SQL executions
  shs sql -a APP_ID EXEC_ID       Get SQL execution header (status, duration, job IDs)
  shs sql -a APP_ID EXEC_ID --plan          Include query plan and node metrics
  shs sql -a APP_ID EXEC_ID --summary       Include job summaries + aggregate stage metrics
  shs sql -a APP_ID EXEC_ID --initial-plan  Include initial AQE plans (implies --plan)
  shs env -a APP_ID               Show environment/config
  shs env -a APP_ID --section S   Show specific section (runtime|spark|system|hadoop|metrics|classpath)
  shs compare sql --app-a APP1 --app-b APP2 EXEC1 EXEC2        Compare SQL execution metrics
  shs compare sql --app-a APP1 --app-b APP2 --env               Compare Spark configurations
  shs compare sql --app-a APP1 --app-b APP2 --plans EXEC1 EXEC2 Compare SQL plan structure
  shs compare apps --app-a APP1 --app-b APP2                     Compare app-level performance
  shs compare stages --app-a APP1 --app-b APP2 STAGE1 STAGE2    Compare stage metrics + task quantiles
    --server-a NAME  Server for app A (overrides --server)
    --server-b NAME  Server for app B (overrides --server)
  shs servers                     List configured servers
  shs version                     CLI + server Spark version

GLOBAL FLAGS
  -a, --app-id STRING     Application ID (required for most commands)
      --attempt STRING    Application attempt ID (for YARN apps with multiple attempts)
  -s, --server STRING     Server name from config file
  -o, --output FORMAT     txt (default) | json | yaml
  -c, --config PATH       Config file (default: config.yaml)
      --timeout DURATION  HTTP timeout (default: 10s)

LIST FLAGS (apps, jobs, stages, sql)
  --limit N            Max results, default 20. Use --limit 0 for all.
  --status VALUE       Filter by status (values vary per command).
  --sort FIELD         Sort field (values vary per command).
  --description TEXT   (sql only) Filter by description substring (case-insensitive).

COMMAND DETAILS
  apps       --status running|completed  --sort name|id|date|duration  --desc
             --all-servers (query all configured servers)  --attempts (list attempts, requires -a)
  jobs       --status running|succeeded|failed|unknown  --sort failed-tasks|duration|id  --group GROUP
  stages     --status active|complete|pending|failed  --sort failed-tasks|duration|id  --errors
  executors  --all (include dead)  --summary (peak memory/OOM view)  --timeline (resource usage over time)
             --sort failed-tasks|duration|gc|id
  sql        --status completed|running|failed  --sort duration|id  --description TEXT  --plan  --summary  --initial-plan
  env        --section runtime|spark|system|hadoop|metrics|classpath
  compare    sql (default: metrics diff)  sql --env (config diff)  sql --plans (plan structure diff)
             apps (executors, jobs, stages, aggregate I/O)  stages (stage metrics + task quantiles)

CONFIG FILE
  Default path: config.yaml (override with -c or SHS_CLI__CONFIG env var).

  servers:
    local:
      default: true
      url: http://localhost:18080
    prod:
      url: https://prod:18080
      verify_ssl: true
      auth:
        username: user
        password: pass
      # or token-based:  auth: { token: "Bearer ..." }

  Fields per server:
    url             Base URL of the Spark History Server (required)
    default         true to use this server when --server is omitted
    verify_ssl      TLS verification (default: true)
    auth.username   Basic auth username
    auth.password   Basic auth password
    auth.token      Bearer token (alternative to username/password)

  Environment variable overrides (double underscore = nesting):
    SHS_CLI__SERVERS__LOCAL__URL=http://localhost2:18080

DEFAULT SORT
  jobs:       failed first, then by duration descending
  stages:     failed → complete → active → pending → skipped, then duration desc
  executors:  active first, then by task time descending
  sql:        failed first, then by duration descending

OUTPUT
  -o txt    Human-readable tables (default).
  -o json   Full API response objects. Best for programmatic use.
  -o yaml   Same data as json, YAML formatted.

COMMON WORKFLOWS

  Find the application ID:
    shs apps
    shs apps --status completed --limit 5

  Investigate a specific attempt (YARN apps):
    shs apps -a APP_ID --attempts                     # list attempts with status
    shs jobs -a APP_ID --attempt 1                    # attempt 1
    shs jobs -a APP_ID --attempt 2                    # attempt 2
    shs jobs -a APP_ID                                # latest attempt (default)

  Investigate failures:
    shs jobs -a APP_ID --status failed
    shs stages -a APP_ID --status failed
    shs stages -a APP_ID STAGE_ID --errors  # failed tasks + error messages
    shs stages -a APP_ID STAGE_ID --errors -o json  # full task details

  Find slowest stages:
    shs stages -a APP_ID --sort duration --limit 10

  Check for data skew (look at task quantiles in stage detail):
    shs stages -a APP_ID STAGE_ID

  Find executor bottlenecks:
    shs executors -a APP_ID --sort gc
    shs executors -a APP_ID --summary   # peak memory, OOM status, dead first
    shs executors -a APP_ID --timeline  # resource usage: executor count, cores, memory over time
    shs executors -a APP_ID EXECUTOR_ID

  Find SQL executions by description:
    shs sql -a APP_ID --description "benchmark q5"
    shs sql -a APP_ID --description "benchmark q5" --status completed

  Investigate slow SQL queries:
    shs sql -a APP_ID --sort duration --limit 10
    shs sql -a APP_ID EXEC_ID          # header: status, duration, job IDs
    shs sql -a APP_ID EXEC_ID --plan     # physical plan + node-level metrics
    shs sql -a APP_ID EXEC_ID --summary   # jobs + aggregate shuffle/input/spill/GC metrics

  Compare same query across two runs:
    shs compare sql --app-a APP1 --app-b APP2 EXEC1 EXEC2          # metrics: duration, stages, shuffle, spill
    shs compare sql --app-a APP1 --app-b APP2 --env                 # config diff: spark properties
    shs compare sql --app-a APP1 --app-b APP2 --plans EXEC1 EXEC2  # plan diff: node types, edges
    shs compare sql --app-a APP1 --server-a prod --app-b APP2 --server-b staging EXEC1 EXEC2  # cross-server

  Compare app-level performance:
    shs compare apps --app-a APP1 --app-b APP2                      # executors, jobs, stages, I/O, spill, GC

  Compare two stages (e.g. same stage across runs):
    shs compare stages --app-a APP1 --app-b APP2 STAGE1 STAGE2     # metrics + task quantile distributions
    shs compare stages --server-a prod --server-b staging --app-a APP1 --app-b APP2 STAGE1 STAGE2  # cross-server

  Get Spark config for an app:
    shs env -a APP_ID --section spark

DATA MODEL
  Application  A Spark app with one or more attempts. YARN apps may have multiple
               attempts if the ApplicationMaster restarts. Use --attempt to select one;
               omit it to get the latest attempt. Use --attempts to list all attempts.
  Job          A Spark action (collect, save, etc). Contains stages.
  Stage        A unit of parallel work. Has tasks and may have retry attempts.
               Detail view shows: task counts, input/output bytes,
               shuffle read/write, spill, GC time, scheduling pool,
               and task quantile distributions (p25/p50/p75/max).
  Job Group    Optional grouping set by the application. Filter with --group.
  Executor     A JVM process running tasks. Has cores, memory, shuffle/IO stats.
               Detail view shows: memory usage, disk, task breakdown, RDD blocks.
  SQL          A SQL/DataFrame execution. Links to jobs via job IDs.
               Detail view shows the execution header by default.
               Use --plan for the physical plan and node-level metrics,
               --summary for inlined job summaries and aggregate stage metrics.

TIPS
  - Stage IDs appear in job output for cross-referencing.
  - Stage detail shows the latest attempt by default.
  - Duration is computed from submissionTime/completionTime.
  - Use -o json when you need to extract specific fields.
  - SQL detail shows only the header by default; use --plan for the physical plan.
  - --initial-plan includes AQE initial plans (implies --plan).
  - SQL job IDs cross-reference with shs jobs output; use --summary to inline them.
  - Executor TASK_TIME is cumulative task execution time, not wall-clock uptime.
  - compare sql --env shows only differing Spark properties (skips identical ones).
  - compare sql --plans shows node type differences in the SQL execution DAG.
  - All timestamps are UTC.
`

func newPrimeCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "prime",
		Short: "Print CLI usage reference for AI agents",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			_, _ = fmt.Fprint(cmd.OutOrStdout(), primeText)
			return nil
		},
	}
}
