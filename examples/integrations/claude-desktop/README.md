# Claude Desktop Integration

Connect Claude Desktop to Spark History Server for AI-powered job analysis.

## Prerequisites

1. **Clone and setup repository**:
```bash
git clone https://github.com/DeepDiagnostix-AI/mcp-apache-spark-history-server.git
cd mcp-apache-spark-history-server

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

1. **Configure Claude Desktop** (`~/Library/Application Support/Claude/claude_desktop_config.json`):

```json
{
    "mcpServers": {
        "sparkinsight-ai": {
            "command": "uv",
            "args": [
                "run",
                "--project",
                "/path/to/sparkinsight-ai",
                "-m",
                "sparkinsight_ai.core.main_stdio"
            ],
            "cwd": "/path/to/sparkinsight-ai",
            "env": {
                "SHS_MCP_DEBUG": "true",
                "SHS_SERVERS_LOCAL_URL": "http://localhost:18080"
            }
        }
    }
}
```

**⚠️ Important**: Replace `/path/to/sparkinsight-ai` with your actual repository path.

### Alternative: Using Published Package

If you have SparkInsight AI installed via pip, you can use the dedicated STDIO command:

```json
{
    "mcpServers": {
        "sparkinsight-ai": {
            "command": "sparkinsight-ai-stdio"
        }
    }
}
```

2. **Restart Claude Desktop**

## Test Connection

Ask Claude: "Are you connected to the Spark History Server? What tools are available?"

## Example Usage

```
Compare performance between these Spark applications:
- spark-cc4d115f011443d787f03a71a476a745
- spark-110be3a8424d4a2789cb88134418217b

Analyze execution times, bottlenecks, and provide optimization recommendations.
```

![claude-desktop](claude-desktop.png)

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

**Note**: Claude Desktop requires local MCP server execution. For remote MCP servers, consider:
- SSH tunnel: `ssh -L 18080:remote-server:18080 user@server`
- Deploy MCP server locally pointing to remote Spark History Server

## Troubleshooting

- **Connection fails**: Check paths in config file
- **No tools**: Restart Claude Desktop
- **No apps found**: Ensure Spark History Server is running and accessible
