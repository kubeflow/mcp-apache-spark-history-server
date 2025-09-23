# Gemini CLI Integration

Connect Gemini CLI to SparkInsight AI for intelligent Spark job analysis through natural language queries.

## Prerequisites

1. **Install Gemini CLI**: Follow the [official installation guide](https://gemini-cli.xyz/docs/en/installation)
2. **Running Spark History Server**: Either use our test setup or have your own server accessible

## Configuration Options

Choose one of the three configuration options below based on your use case:

### Option 1: Published Package (Recommended)

**Use case**: You want to use SparkInsight AI without local source code

**Configuration**: Add to your `~/.gemini/settings.json`:

```json
{
  "mcpServers": {
    "sparkinsight-ai": {
      "command": "sparkinsight-ai-stdio",
      "args": [],
      "env": {
        "SHS_MCP_DEBUG": "true",
        "SHS_SERVERS_LOCAL_URL": "http://localhost:18080"
      },
      "timeout": 120000,
      "disabled": false
    }
  }
}
```

**Setup steps**:
```bash
# Install SparkInsight AI
pip install sparkinsight-ai

# Verify the STDIO command is available
sparkinsight-ai-stdio --help

# Start your Spark History Server or use our test setup
git clone https://github.com/DeepDiagnostix-AI/sparkinsight-ai.git
cd sparkinsight-ai
brew install go-task  # macOS
task start-spark-bg   # Starts test server with sample data
```

### Option 2: Source Code Development

**Use case**: You're developing SparkInsight AI or need custom modifications

**Configuration**: Add to your `~/.gemini/settings.json`:

```json
{
  "mcpServers": {
    "sparkinsight-ai-dev": {
      "command": "uv",
      "args": ["run", "-m", "sparkinsight_ai.core.main_stdio"],
      "env": {
        "SHS_MCP_DEBUG": "true",
        "SHS_SERVERS_LOCAL_URL": "http://localhost:18080",
        "SHS_SERVERS_PRODUCTION_URL": "https://spark-history.company.com:18080",
        "SHS_SERVERS_PRODUCTION_AUTH_USERNAME": "${SPARK_USERNAME}",
        "SHS_SERVERS_PRODUCTION_AUTH_PASSWORD": "${SPARK_PASSWORD}"
      },
      "cwd": "/path/to/sparkinsight-ai",
      "timeout": 120000,
      "disabled": false
    }
  }
}
```

**Setup steps**:
```bash
# Clone and setup repository
git clone https://github.com/DeepDiagnostix-AI/sparkinsight-ai.git
cd sparkinsight-ai

# Install dependencies
brew install go-task  # macOS
task install

# Start test environment
task start-spark-bg   # Start Spark History Server with sample data

# Update the "cwd" path in your Gemini settings to your actual repository path
```

### Option 3: Remote HTTP Server

**Use case**: SparkInsight AI MCP server is already running elsewhere

**Configuration**: Add to your `~/.gemini/settings.json`:

```json
{
  "mcpServers": {
    "sparkinsight-ai-remote": {
      "httpUrl": "http://localhost:18888",
      "env": {},
      "timeout": 120000,
      "disabled": false
    }
  }
}
```

**Setup steps**:
```bash
# Start the MCP server separately
uvx --from sparkinsight-ai sparkinsight-ai server start
# OR from source code:
# cd /path/to/sparkinsight-ai && uv run -m sparkinsight_ai.core.main server start
```

## Environment Variables

SparkInsight AI supports the following environment variables for configuration:

### Core MCP Settings
- `SHS_MCP_PORT`: Port for MCP server (default: 18888)
- `SHS_MCP_DEBUG`: Enable debug mode (default: false)
- `SHS_MCP_ADDRESS`: Address for MCP server (default: localhost)
- `SHS_MCP_TRANSPORT`: MCP transport mode (use "stdio" for Gemini CLI)

### Spark History Server Configuration
- `SHS_SERVERS_LOCAL_URL`: URL for local Spark History Server
- `SHS_SERVERS_PRODUCTION_URL`: URL for production server
- `SHS_SERVERS_PRODUCTION_AUTH_USERNAME`: Username for production server
- `SHS_SERVERS_PRODUCTION_AUTH_PASSWORD`: Password for production server
- `SHS_SERVERS_PRODUCTION_AUTH_TOKEN`: Token for production server
- `SHS_SERVERS_PRODUCTION_VERIFY_SSL`: Whether to verify SSL (true/false)
- `SHS_SERVERS_PRODUCTION_TIMEOUT`: HTTP request timeout in seconds (default: 30)
- `SHS_SERVERS_PRODUCTION_EMR_CLUSTER_ARN`: EMR cluster ARN for AWS integrations

## Test Connection

1. **Restart Gemini CLI** after updating settings
2. **Test the connection**:
   ```bash
   gemini ask "Are you connected to SparkInsight AI? What tools are available?"
   ```

## Example Usage

### Basic Queries
```bash
# List available Spark applications
gemini ask "List all Spark applications in the history server"

# Analyze a specific application
gemini ask "Analyze the performance of Spark application spark-110be3a8424d4a2789cb88134418217b"

# Compare applications
gemini ask "Compare the performance between applications spark-cc4d115f011443d787f03a71a476a745 and spark-110be3a8424d4a2789cb88134418217b"
```

### Advanced Analysis
```bash
# Identify bottlenecks
gemini ask "What are the main performance bottlenecks in application spark-bcec39f6201b42b9925124595baad260?"

# Check for data skew
gemini ask "Analyze data skew in shuffle operations for the latest Spark application"

# Get optimization recommendations
gemini ask "Provide auto-scaling recommendations for application spark-110be3a8424d4a2789cb88134418217b"
```

## Multi-Server Configuration

To connect to multiple Spark History Servers, configure them in your environment variables:

```json
{
  "env": {
    "SHS_SERVERS_LOCAL_URL": "http://localhost:18080",
    "SHS_SERVERS_STAGING_URL": "https://staging-spark.company.com:18080",
    "SHS_SERVERS_PRODUCTION_URL": "https://prod-spark.company.com:18080",
    "SHS_SERVERS_PRODUCTION_AUTH_USERNAME": "${SPARK_USERNAME}",
    "SHS_SERVERS_PRODUCTION_AUTH_PASSWORD": "${SPARK_PASSWORD}"
  }
}
```

Then specify which server to use in your queries:
```bash
gemini ask "List applications from the production server"
gemini ask "Compare application app-123 from staging with app-456 from production"
```

## Security Considerations

- **Environment Variables**: Use `${VARIABLE_NAME}` syntax to reference environment variables for sensitive data
- **Credentials**: Never hardcode passwords or tokens in the configuration file
- **SSL Verification**: Always enable SSL verification (`SHS_SERVERS_*_VERIFY_SSL=true`) for production servers
- **Timeouts**: Adjust timeout values based on your network conditions and server response times

## Troubleshooting

### Connection Issues
- **"Command not found"**: Ensure `uvx` or `uv` is installed and in PATH
- **"Server not found"**: Check if SparkInsight AI package is available: `uvx --from sparkinsight-ai sparkinsight-ai --help`
- **"Permission denied"**: Check file permissions and ensure the working directory is accessible

### Configuration Issues
- **"No tools available"**: Restart Gemini CLI after configuration changes
- **"Invalid JSON"**: Validate your `settings.json` syntax
- **"Connection timeout"**: Increase the timeout value or check network connectivity

### Spark History Server Issues
- **"No applications found"**: Ensure Spark History Server is running and accessible
- **"Authentication failed"**: Verify credentials and authentication settings
- **"SSL errors"**: Check SSL certificate validity or disable SSL verification for development

### Debug Mode
Enable debug mode to see detailed logs:
```json
{
  "env": {
    "SHS_MCP_DEBUG": "true"
  }
}
```

## Remote Spark History Server

For production environments, update the server URLs in your environment variables:

```bash
# Set environment variables
export SPARK_USERNAME="your-username"
export SPARK_PASSWORD="your-password"

# Or use a .env file in your project directory
echo "SPARK_USERNAME=your-username" >> .env
echo "SPARK_PASSWORD=your-password" >> .env
```

## Sample Data

The repository includes real Spark event logs for testing:
- `spark-bcec39f6201b42b9925124595baad260` - ✅ Successful ETL job
- `spark-110be3a8424d4a2789cb88134418217b` - 🔄 Data processing job
- `spark-cc4d115f011443d787f03a71a476a745` - 📈 Multi-stage analytics job

Start the test environment to use these samples:
```bash
task start-spark-bg   # Starts server with sample data at http://localhost:18080
```

## Additional Resources

- **[SparkInsight AI Documentation](../../..)** - Main project documentation
- **[Gemini CLI Documentation](https://gemini-cli.xyz/)** - Official Gemini CLI docs
- **[MCP Protocol](https://modelcontextprotocol.io/)** - Model Context Protocol specification