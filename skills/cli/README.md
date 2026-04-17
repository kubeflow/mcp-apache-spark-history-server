# Spark History Server CLI

CLI for [Apache Spark History Server](https://spark.apache.org/docs/latest/monitoring.html#viewing-after-the-fact). Built for human terminals, AI agents, and CI/CD pipelines.

## Installation

```bash
task build
```

The binary is built to `./bin/shs`.

## Quick Start

Create a `config.yaml`:

```yaml
servers:
  default:
    default: true
    url: "http://localhost:18080"
```

Start Spark History Server:

```bash
task start-shs
```

List applications:

```
$ shs apps
ID                   NAME          DURATION  ATTEMPTS
local-1776286804625  shs-e2e-app2  13.159s   1
local-1776286786993  shs-e2e-app1  14.47s    1
```

See full usage with:

```bash
shs prime
```

<details><summary>Full Usage</summary>

```bash
shs — Spark History Server CLI (for AI agents)

COMMANDS
  shs apps                        List applications
  shs jobs -a APP_ID              List jobs for an application
  shs jobs -a APP_ID JOB_ID       Get job detail
  shs stages -a APP_ID            List stages for an application
  shs stages -a APP_ID STAGE      Get stage detail with full metrics
  shs stages -a APP_ID STAGE --errors  Show failed tasks with errors
  shs executors -a APP_ID         List active executors
  shs executors -a APP_ID EXEC    Get executor detail
  shs sql -a APP_ID               List SQL executions
  shs sql -a APP_ID EXEC_ID       Get SQL execution header (status, duration, job IDs)
  shs sql -a APP_ID EXEC_ID --plan          Include query plan and node metrics
  shs sql -a APP_ID EXEC_ID --summary       Include job summaries + aggregate stage metrics
  shs sql -a APP_ID EXEC_ID --initial-plan  Include initial AQE plans (implies --plan)
  shs env -a APP_ID               Show environment/config
  shs compare --app-a APP1 --app-b APP2 EXEC1 EXEC2  Compare SQL executions across apps
    --server-a NAME  Server for app A (overrides --server)
    --server-b NAME  Server for app B (overrides --server)
  shs version                     CLI + server Spark version

GLOBAL FLAGS
  -a, --app-id STRING     Application ID (required for most commands)
  -s, --server STRING     Server name from config file
  -o, --output FORMAT     txt (default) | json | yaml
  -c, --config PATH       Config file (default: config.yaml)
      --timeout DURATION  HTTP timeout (default: 10s)

LIST FLAGS (apps, jobs, stages, sql)
  --limit N       Max results, default 20. Use --limit 0 for all.
  --status VALUE  Filter by status (values vary per command).
  --sort FIELD    Sort field (values vary per command).

COMMAND DETAILS
  apps       --status running|completed  --sort name|id|date|duration  --desc
  jobs       --status running|succeeded|failed|unknown  --sort failed-tasks|duration|id  --group GROUP
  stages     --status active|complete|pending|failed  --sort failed-tasks|duration|id  --errors
  executors  --all (include dead)  --summary (peak memory/OOM view)  --sort failed-tasks|duration|gc|id
  sql        --status completed|running|failed  --sort duration|id  --plan  --summary  --initial-plan

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

  Investigate failures:
    shs jobs -a APP_ID --status failed
    shs stages -a APP_ID --status failed
    shs stages -a APP_ID STAGE_ID --errors  # failed tasks + error messages
    shs stages -a APP_ID STAGE_ID --errors -o json  # full task details

  Find slowest stages:
    shs stages -a APP_ID --sort duration --limit 10

  Check for data skew (look at shuffle and spill metrics):
    shs stages -a APP_ID STAGE_ID -o json

  Find executor bottlenecks:
    shs executors -a APP_ID --sort gc
    shs executors -a APP_ID --summary   # peak memory, OOM status, dead first
    shs executors -a APP_ID EXECUTOR_ID

  Investigate slow SQL queries:
    shs sql -a APP_ID --sort duration --limit 10
    shs sql -a APP_ID EXEC_ID          # header: status, duration, job IDs
    shs sql -a APP_ID EXEC_ID --plan     # physical plan + node-level metrics
    shs sql -a APP_ID EXEC_ID --summary   # jobs + aggregate shuffle/input/spill/GC metrics

  Compare same query across two runs:
    shs compare --app-a APP1 --app-b APP2 EXEC1 EXEC2  # duration, jobs, stages, shuffle, spill diff
    shs compare --app-a APP1 --server-a prod --app-b APP2 --server-b staging EXEC1 EXEC2  # cross-server

  Get Spark config for an app:
    shs env -a APP_ID --section spark

DATA MODEL
  Application  A Spark app with one or more attempts.
  Job          A Spark action (collect, save, etc). Contains stages.
  Stage        A unit of parallel work. Has tasks and may have retry attempts.
               Detail view shows: task counts, input/output bytes,
               shuffle read/write, spill, GC time, scheduling pool.
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
  - All timestamps are UTC.
```

</details>

## Configuration

The config file defines one or more Spark History Server connections:

```yaml
servers:
  default:
    default: true
    url: "http://localhost:18080"
  production:
    url: "https://shs.prod.example.com"
```

Use `--server` or `-s` to target a specific server:

```bash
shs apps -s production
```

### Environment Variables

Config values can be set or overridden with environment variables using the `SHS_CLI__` prefix with double underscores as nesting delimiters:

```bash
export SHS_CLI__SERVERS__DEFAULT__URL="http://localhost:18080"
export SHS_CLI__SERVERS__DEFAULT__DEFAULT=true
```

This maps to `servers.default.url` and `servers.default.default` in the config file. Double underscores are needed to distinguish nesting from field names that contain underscores (e.g. `verify_ssl`).

Env vars merge on top of the config file, so you can keep a base config and override per environment.

| Variable | Maps to |
|---|---|
| `SHS_CLI__CONFIG` | `-c` / `--config` flag |
| `SHS_CLI__SERVERS__PROD__URL` | `servers.prod.url` |
| `SHS_CLI__SERVERS__PROD__VERIFY_SSL` | `servers.prod.verify_ssl` |
| `SHS_CLI__SERVERS__PROD__AUTH__TOKEN` | `servers.prod.auth.token` |
| `SHS_APP_ID` | `-a` / `--app-id` flag |

## Commands

| Command | Description |
|---|---|
| `shs apps` | List applications |
| `shs jobs -a APP` | List jobs |
| `shs jobs -a APP JOB_ID` | Job detail |
| `shs stages -a APP` | List stages |
| `shs stages -a APP STAGE` | Stage detail with full metrics |
| `shs stages -a APP STAGE --errors` | Failed tasks with error messages |
| `shs executors -a APP` | List executors |
| `shs executors -a APP --summary` | Peak memory and OOM status |
| `shs sql -a APP` | List SQL executions |
| `shs sql -a APP EXEC_ID` | SQL execution header |
| `shs sql -a APP EXEC_ID --plan` | Query plan and node metrics |
| `shs sql -a APP EXEC_ID --summary` | Job summaries and aggregate stage metrics |
| `shs compare --app-a A --app-b B E1 E2` | Compare SQL executions across apps |
| `shs env -a APP` | Environment and Spark config |
| `shs version` | CLI and server Spark version |

All list commands support `--limit`, `--status`, `--sort`. Use `--help` on any command for details.

## Output Formats

```bash
shs jobs -a APP                  # human-readable table (default)
shs jobs -a APP -o json          # full API response as JSON
shs jobs -a APP -o yaml          # full API response as YAML
```

## Common Workflows

Investigate failures:

```bash
shs jobs -a APP --status failed
shs stages -a APP --status failed
shs stages -a APP STAGE --errors
```

Find slow queries:

```bash
shs sql -a APP --sort duration --limit 10
shs sql -a APP EXEC_ID --plan
shs sql -a APP EXEC_ID --summary
```

Compare the same query across two runs:

```bash
shs compare --app-a APP1 --app-b APP2 6 6
```

Compare across servers:

```bash
shs compare --server-a prod --server-b staging --app-a APP1 --app-b APP2 6 6
```

List apps across all configured servers:

```bash
shs apps --all-servers
```

## AI Agent Usage

`shs prime` prints a complete CLI reference designed for LLM context windows — commands, flags, workflows, data model, and tips:

```bash
shs prime
```

Feed this to an AI agent as a system prompt or tool description. The agent can then use `shs` commands to investigate Spark applications autonomously.

## Development

```bash
task build          # Build the binary
task test           # Run unit tests
task test-e2e       # Run e2e tests (starts/stops SHS automatically)
task lint           # Run golangci-lint
task generate       # Regenerate client from OpenAPI spec
```
