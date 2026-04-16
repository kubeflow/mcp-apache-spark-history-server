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
