<div align="center">


# 🎉 ✨ NEW ✨ 🎉

## `shs` — Spark History Server CLI is here!

![NEW](https://img.shields.io/badge/🆕%20Just%20Launched-SHS%20CLI-brightgreen?style=for-the-badge)
![Go](https://img.shields.io/badge/Go-Standalone%20Binary-00ADD8?style=for-the-badge&logo=go)
![No Dependencies](https://img.shields.io/badge/No%20Deps-Zero%20Setup-orange?style=for-the-badge)

**A standalone Go binary for querying Spark History Server directly from your terminal.**
No MCP server. No Python. No daemon. Just `shs`.

[⬇️ Install now](#installation) · [⚡ Quick Start](#quick-start) · [📖 Full Docs](../../README.md)

---

</div>

# Spark History Server CLI (`shs`)


`shs` is a CLI for [Apache Spark History Server](https://spark.apache.org/docs/latest/monitoring.html#viewing-after-the-fact) — a standalone Go binary that lets platform engineers, SREs, data engineers, and coding agents query Spark applications directly from the terminal. Inspect jobs, investigate failures, analyze stage performance, compare runs, and script against the Spark REST API. No MCP server, no Python runtime, no running daemon — just a single binary.

**Use `shs` when** you know the command you want to run: look up a failed job, find the slowest stage, diff two app configs, or pipe JSON into `jq`. **Use the MCP server when** you want an AI agent to do multi-step investigation via natural language.

## How the CLI Differs from the MCP Server

| | `shs` CLI | MCP Server (`spark-mcp`) |
|---|---|---|
| **Primary user** | Humans, shell scripts, CI/CD, coding agents | AI agents and MCP-compatible clients |
| **Mental model** | "I know the command I want to run" | "Agent, investigate this Spark app" |
| **Interface** | Terminal command | MCP tool server |
| **Protocol** | No MCP protocol — direct command execution | Model Context Protocol (MCP) |
| **Transport** | CLI process calling Spark REST API over HTTP | stdio, streamable-http, or SSE |
| **Output** | Human-readable table, JSON, YAML | Structured tool responses for agent consumption |
| **Discovery** | `shs --help`, `shs prime`, docs, examples | Agent calls `tools/list` at runtime |
| **Runtime** | One process per command | Long-running server or stdio server session |
| **State** | Stateless | Can maintain config, session, and cache (e.g. ApplicationDiscovery TTL cache) |
| **Best for** | Fast lookup, automation, CI checks, debugging | Multi-step investigation, natural-language analysis, autonomous workflows |
| **Failure behavior** | Exit codes, stdout/stderr | Tool call result/errors returned to agent |
| **Operational footprint** | Lightweight single binary, no dependencies | Server runtime, dependencies, lifecycle, security controls |
| **AWS EMR** | Config field only | Full Persistent UI + presigned URL session setup |
| **Multi-server** | `shs apps --all-servers` (apps listing only) | Auto-discovery by app ID across all servers |
| **Config prefix** | `SHS_CLI__` env vars | `SHS_` env vars |
| **Binary** | `shs` (compiled Go, no runtime deps) | `spark-mcp` (Python 3.12+, uv, FastMCP) |

## Installation

### Download binary (recommended)

Download the latest release for your platform from the [GitHub Releases page](https://github.com/kubeflow/mcp-apache-spark-history-server/releases). CLI releases are tagged `cli/v*`.

```bash
# Replace VERSION with the latest cli/v* release (e.g. v1.0.0)
VERSION=v1.0.0

# macOS (Apple Silicon)
curl -sSL "https://github.com/kubeflow/mcp-apache-spark-history-server/releases/download/cli%2F${VERSION}/shs-${VERSION}-darwin-arm64.tar.gz" | tar xz
sudo mv shs /usr/local/bin/

# macOS (Intel)
curl -sSL "https://github.com/kubeflow/mcp-apache-spark-history-server/releases/download/cli%2F${VERSION}/shs-${VERSION}-darwin-amd64.tar.gz" | tar xz
sudo mv shs /usr/local/bin/

# Linux (amd64)
curl -sSL "https://github.com/kubeflow/mcp-apache-spark-history-server/releases/download/cli%2F${VERSION}/shs-${VERSION}-linux-amd64.tar.gz" | tar xz
sudo mv shs /usr/local/bin/

# Linux (arm64)
curl -sSL "https://github.com/kubeflow/mcp-apache-spark-history-server/releases/download/cli%2F${VERSION}/shs-${VERSION}-linux-arm64.tar.gz" | tar xz
sudo mv shs /usr/local/bin/
```

### Build from source

From the `skills/cli/` directory:

```bash
task build
```

The binary is built to `./bin/shs`. Add it to your PATH so you can run `shs` directly:

```bash
export PATH="$(pwd)/bin:$PATH"
```

Add that export to your `~/.zshrc` or `~/.bashrc` to make it permanent.

## Quick Start

A `config.yaml` is already provided in `skills/cli/`:

```yaml
servers:
  local:
    default: true
    url: "http://localhost:18080"
```

Start Spark History Server with sample data (from `skills/cli/`):

```bash
task start-shs
```

This starts a local SHS via Docker using the pre-generated event logs in `e2e/spark-events/`.

List applications:

```
$ shs apps
ID                              NAME          DURATION  ATTEMPTS
application_1713000000000_0001  shs-e2e-yarn  13.07s    2
local-1776286804625             shs-e2e-app2  13.159s   1
local-1776286786993             shs-e2e-app1  14.47s    1
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
  shs env -a APP_ID --section S   Show specific section (runtime|spark|system|hadoop|metrics|classpath)
  shs compare sql --app-a APP1 --app-b APP2 EXEC1 EXEC2        Compare SQL execution metrics
  shs compare sql --app-a APP1 --app-b APP2 --env               Compare Spark configurations
  shs compare sql --app-a APP1 --app-b APP2 --plans EXEC1 EXEC2 Compare SQL plan structure
  shs compare apps --app-a APP1 --app-b APP2                     Compare app-level performance
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
  --limit N       Max results, default 20. Use --limit 0 for all.
  --status VALUE  Filter by status (values vary per command).
  --sort FIELD    Sort field (values vary per command).

COMMAND DETAILS
  apps       --status running|completed  --sort name|id|date|duration  --desc
             --all-servers (query all configured servers)  --attempts (list attempts, requires -a)
  jobs       --status running|succeeded|failed|unknown  --sort failed-tasks|duration|id  --group GROUP
  stages     --status active|complete|pending|failed  --sort failed-tasks|duration|id  --errors
  executors  --all (include dead)  --summary (peak memory/OOM view)  --timeline (resource usage over time)
             --sort failed-tasks|duration|gc|id
  sql        --status completed|running|failed  --sort duration|id  --plan  --summary  --initial-plan
  env        --section runtime|spark|system|hadoop|metrics|classpath
  compare    sql (metrics diff)  sql --env (config diff)  sql --plans (plan structure diff)
             apps (executors, jobs, stages, aggregate I/O)

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

  Check for data skew (look at task quantiles in stage detail):
    shs stages -a APP_ID STAGE_ID

  Find executor bottlenecks:
    shs executors -a APP_ID --sort gc
    shs executors -a APP_ID --summary   # peak memory, OOM status, dead first
    shs executors -a APP_ID --timeline  # resource usage over time
    shs executors -a APP_ID EXECUTOR_ID

  Investigate slow SQL queries:
    shs sql -a APP_ID --sort duration --limit 10
    shs sql -a APP_ID EXEC_ID            # header: status, duration, job IDs
    shs sql -a APP_ID EXEC_ID --plan     # physical plan + node-level metrics
    shs sql -a APP_ID EXEC_ID --summary  # jobs + aggregate shuffle/input/spill/GC metrics

  Compare same query across two runs:
    shs compare sql --app-a APP1 --app-b APP2 EXEC1 EXEC2         # metrics diff
    shs compare sql --app-a APP1 --app-b APP2 --env               # config diff
    shs compare sql --app-a APP1 --app-b APP2 --plans EXEC1 EXEC2 # plan diff
    shs compare sql --app-a APP1 --server-a prod --app-b APP2 --server-b staging EXEC1 EXEC2  # cross-server

  Compare app-level performance:
    shs compare apps --app-a APP1 --app-b APP2

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
| `shs compare sql --app-a A --app-b B E1 E2` | Compare SQL execution metrics across apps |
| `shs compare apps --app-a A --app-b B` | Compare app-level performance metrics |
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

### Investigate failures

```bash
# Find failed jobs
shs jobs -a local-1776286786993 --status failed

# Find failed stages
shs stages -a local-1776286786993 --status failed

# Get error messages from failed tasks in stage 5
shs stages -a local-1776286786993 5 --errors

# Full task detail as JSON
shs stages -a local-1776286786993 5 --errors -o json
```

### Investigate slow applications

```bash
# Find the slowest stages
shs stages -a local-1776286786993 --sort duration --limit 10

# Drill into a stage — task counts, I/O, shuffle, spill, and task quantiles (p25/p50/p75/max)
# A large gap between p50 and max executorRunTime indicates data skew
shs stages -a local-1776286786993 43

# Check for memory pressure and GC overhead
shs executors -a local-1776286786993 --summary
shs executors -a local-1776286786993 --sort gc
shs executors -a local-1776286786993 --timeline
```

### Investigate slow SQL queries

```bash
shs sql -a local-1776286786993 --sort duration --limit 10
shs sql -a local-1776286786993 8            # header: status, duration, job IDs
shs sql -a local-1776286786993 8 --plan     # physical plan + per-node metrics
shs sql -a local-1776286786993 8 --summary  # jobs + aggregate shuffle/spill/GC metrics
```

### Compare two runs of the same job

```bash
# Overall app metrics: executors, jobs, stages, I/O, spill, GC
shs compare apps --app-a local-1776286804625 --app-b local-1776286786993

# SQL execution metrics diff
shs compare sql --app-a local-1776286804625 --app-b local-1776286786993 8 8

# Config diff — only shows properties that differ between the two apps
shs compare sql --app-a local-1776286804625 --app-b local-1776286786993 --env

# Plan structure diff — detects join strategy changes (e.g. SortMergeJoin → BroadcastHashJoin)
shs compare sql --app-a local-1776286804625 --app-b local-1776286786993 --plans 8 8

# Cross-server comparison
shs compare sql --app-a APP1 --server-a production --app-b APP2 --server-b staging 8 8
```

### YARN applications (multi-attempt)

```bash
# List all attempts with status and duration
shs apps -a application_1713000000000_0001 --attempts

# Query jobs from a specific attempt
shs jobs -a application_1713000000000_0001 --attempt 1   # failed attempt
shs jobs -a application_1713000000000_0001 --attempt 2   # successful attempt
shs jobs -a application_1713000000000_0001               # latest attempt (default)
```

### List apps across all servers

```bash
shs apps --all-servers
shs apps --all-servers --sort duration --desc
```

### JSON output for scripting

```bash
shs apps -o json | jq '.[].id'
shs stages -a APP STAGE_ID -o json | jq '.taskMetricDistributions.executorRunTime.max'
```

## AI Agent Usage

`shs prime` prints a complete CLI reference designed for LLM context windows — commands, flags, workflows, data model, and tips:

```bash
shs prime
```

Feed this to an AI agent as a system prompt or tool description. The agent can then use `shs` commands to investigate Spark applications autonomously. See [`examples/compare/`](./examples/compare/) for a worked example of Claude Code analyzing a TPC-DS benchmark across 103 queries with parallel subagents.

## Development

```bash
task build          # Build the binary to ./bin/shs (run from skills/cli/)
task test           # Run unit tests
task test-e2e       # Run e2e tests (starts/stops SHS automatically via Docker)
task lint           # Run golangci-lint
task generate       # Regenerate client from OpenAPI spec (openapi/spark-history-server.yaml)
task start-shs      # Start Spark History Server with e2e sample data
task stop-shs       # Stop the Spark History Server
```
