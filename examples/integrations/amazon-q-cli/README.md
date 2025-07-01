# Amazon Q CLI Integration

Connect Amazon Q CLI to Spark History Server for command-line Spark analysis.

## Prerequisites

1. **Clone and setup repository**:
```bash
git clone https://github.com/DeepDiagnostix-AI/spark-history-server-mcp.git
cd spark-history-server-mcp

# Install Task (if not already installed)
brew install go-task  # macOS
# or see https://taskfile.dev/installation/ for other platforms

# Setup dependencies
task install
```

2. **Start Spark History Server with sample data**:
```bash
task start-spark-bg
# Starts server at http://localhost:18080 with 3 sample applications
```

3. **Verify setup**:
```bash
curl http://localhost:18080/api/v1/applications
# Should return 3 applications
```

## Setup

1. **Add MCP server**:
```bash
q mcp add \
  --name spark-history-server-mcp \
  --command uv \
  --args "run,main.py" \
  --env SHS_MCP_TRANSPORT=stdio \
  --scope workspace
```

<details>
<summary><strong>⚠️ Important</strong></summary>

- The command above adds q configuration in this repository root. i.e. `.amazonq/mcp.json`
- If you want to use it outside of this repository, you need to run:
  ```bash
  q mcp add \
  --name spark-history-server-mcp \
  --command /Users/username/.local/bin/uv \
  --args "run,--project,/Users/username/spark-history-server-mcp,python,main_stdio.py" \
  --scope workspace
  ```
- Replace `/Users/username/.local/bin/uv` with output of `which uv`
- Replace `/Users/username/spark-history-server-mcp` with your actual repository path
</details>

2. **Test connection**: `q chat --trust-all-tools`

## Usage

Start interactive session:
```bash
q chat --trust-all-tools
```

![amazon-q-cli](amazon-q-cli.png)

Example query:
```
Compare performance between spark-cc4d115f011443d787f03a71a476a745 and spark-110be3a8424d4a2789cb88134418217b
```

## Batch Analysis
```bash
echo "What are the bottlenecks in spark-cc4d115f011443d787f03a71a476a745?" | q chat --trust-all-tools
```

## Management
- List servers: `q mcp list`
- Remove: `q mcp remove --name spark-history-server-mcp`

## Remote Spark History Server

To connect to a remote Spark History Server, edit `config.yaml` in the repository:

```yaml
servers:
  production:
    default: true
    url: "https://spark-history-prod.company.com:18080"
    auth:
      username: "user"
      password: "pass"
```

**Note**: Amazon Q CLI requires local MCP server execution. For remote MCP servers, consider:
- SSH tunnel: `ssh -L 18080:remote-server:18080 user@server`
- Deploy MCP server locally pointing to remote Spark History Server

## Troubleshooting
- **Path errors**: Use full paths (`which uv`)
- **Tool issues**: Always use `--trust-all-tools`
- **Connection fails**: Check Spark History Server is running and accessible
