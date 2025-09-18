# Amazon Q CLI Integration

Connect Amazon Q CLI to Spark History Server for command-line Spark analysis.

## Prerequisites

1. **Install uv** (if not already installed):

```bash
# macOS/Linux
curl -LsSf https://astral.sh/uv/install.sh | sh
# or see https://docs.astral.sh/uv/getting-started/installation/
```

2. **Start Spark History Server** (optional - for testing with sample data):

```bash
# Clone repository for sample data
git clone https://github.com/kubeflow/mcp-apache-spark-history-server.git
cd mcp-apache-spark-history-server

# Install Task and start sample server
brew install go-task  # macOS, see https://taskfile.dev/installation/ for others
task start-spark-bg   # Starts server at http://localhost:18080 with 3 sample applications

# Verify setup
curl http://localhost:18080/api/v1/applications
# Should return 3 applications
```

## Setup

1. **Add MCP server** (using default configuration):

```bash
q mcp add \
  --name spark-history-server \
  --command uvx \
  --args "--from,mcp-apache-spark-history-server,spark-mcp" \
  --env SHS_MCP_TRANSPORT=stdio \
  --scope global
```

2. **Add MCP server with custom configuration**:

```bash
# Using command line config argument
q mcp add \
  --name spark-history-server \
  --command uvx \
  --args "--from,mcp-apache-spark-history-server,spark-mcp,--config,/path/to/config.yaml" \
  --env SHS_MCP_TRANSPORT=stdio \
  --scope global

# Using environment variable
q mcp add \
  --name spark-history-server \
  --command uvx \
  --args "--from,mcp-apache-spark-history-server,spark-mcp" \
  --env SHS_MCP_CONFIG=/path/to/config.yaml \
  --env SHS_MCP_TRANSPORT=stdio \
  --scope global
```

Results should look something like this:


```bash
cat ~/.aws/amazonq/mcp.json
```
```json
{
  "mcpServers": {
    "spark-history-server": {
      "command": "uvx",
      "args": [
        "--from",
        "mcp-apache-spark-history-server",
        "spark-mcp"
      ],
      "timeout": 120000,
      "disabled": false
    }
  }
}
```

## Usage

Start interactive session:
```bash
q chat
```

![amazon-q-cli](amazon-q-cli.png)

Example query:
```
Compare performance between spark-cc4d115f011443d787f03a71a476a745 and spark-110be3a8424d4a2789cb88134418217b
```

## Batch Analysis
```bash
echo "What are the bottlenecks in spark-cc4d115f011443d787f03a71a476a745?"
```

## Management
- List servers: `q mcp list`
- Remove: `q mcp remove --name mcp-apache-spark-history-server`

## Configuration

The MCP server supports flexible configuration file paths:

### Configuration Priority
1. **Command line argument** (highest priority): `--config /path/to/config.yaml`
2. **Environment variable**: `SHS_MCP_CONFIG=/path/to/config.yaml`
3. **Default**: Uses `config.yaml` in current directory

### Configuration File Format
Create a `config.yaml` file for your Spark History Server:

```yaml
servers:
  production:
    default: true
    url: "https://spark-history-prod.company.com:18080"
    auth:  # optional
      username: "user"
      password: "pass"
  staging:
    url: "https://spark-history-staging.company.com:18080"
```

### Remote Spark History Server Examples

**Using command line config:**
```bash
q mcp add \
  --name spark-history-server \
  --command uvx \
  --args "--from,mcp-apache-spark-history-server,spark-mcp,--config,/path/to/prod-config.yaml" \
  --scope global
```

**Using environment variable:**
```bash
q mcp add \
  --name spark-history-server \
  --command uvx \
  --args "--from,mcp-apache-spark-history-server,spark-mcp" \
  --env SHS_MCP_CONFIG=/path/to/staging-config.yaml \
  --scope global
```

**Note**: Amazon Q CLI requires local MCP server execution. For remote MCP servers, consider:
- SSH tunnel: `ssh -L 18080:remote-server:18080 user@server`
- Deploy MCP server locally pointing to remote Spark History Server

## Troubleshooting
- **Path errors**: Use full paths (`which uv`)
- **Connection fails**: Check Spark History Server is running and accessible
