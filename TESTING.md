# Testing Guide: Spark History Server MCP

## 🧪 Quick Test with MCP Inspector (5 minutes)

**Use this for**: Local development, testing tools, understanding capabilities

### Prerequisites
- Docker must be running (for Spark History Server)
- Node.js installed (for MCP Inspector)
- Python 3.12+ with uv package manager
- Run commands from project root directory

### Setup Repository
```bash
git clone https://github.com/DeepDiagnostix-AI/spark-history-server-mcp.git
cd spark-history-server-mcp

# Install Task (if not already installed)
brew install go-task  # macOS
# or see https://taskfile.dev/installation/ for other platforms

# Setup dependencies
task install
```

### Start Testing

```bash
# One-command setup (recommended)
task start-spark-bg && task start-mcp-bg && task start-inspector-bg

# Opens http://localhost:6274 automatically in your browser
# When done: task stop-all
```

**Alternative** (if you prefer manual control):
```bash
# Terminal 1: Start Local Spark History Server
task start-spark

# Terminal 2: Start MCP server
task start-mcp

# Terminal 3: Start MCP Inspector
task start-inspector
```

#### Expected Output from Terminal 1:
```
📊 Available Test Applications:
===============================
📋 spark-110be3a8424d4a2789cb88134418217b (512K)
📋 spark-bcec39f6201b42b9925124595baad260 (104K)
📋 spark-cc4d115f011443d787f03a71a476a745 (704K)

📍 Will be available at: http://localhost:18080
```

### Test Applications Available
Your 3 real Spark applications (all successful):
- `spark-bcec39f6201b42b9925124595baad260` - ETL job (104K events)
- `spark-110be3a8424d4a2789cb88134418217b` - Data processing job (512K events)
- `spark-cc4d115f011443d787f03a71a476a745` - Multi-stage analytics job (704K events)

**Note**: Testing uses HTTP transport with `main.py` providing access to all 18 tools.

## 🌐 Using MCP Inspector

Once the MCP Inspector opens in your browser (http://localhost:6274), you can:

1. **View Available Tools** - See all MCP tools in the left sidebar
2. **Test Tools Interactively** - Click any tool to see its parameters
3. **Execute Tools** - Fill in parameters and run tools
4. **View Results** - See structured responses from your Spark History Server

### Example Tool Tests:

#### Get Application Details
- **Tool**: `get_application`
- **Parameter**: `app_id` = `spark-cc4d115f011443d787f03a71a476a745`
- **Expected**: Application info including name, duration, status

#### List All Applications
- **Tool**: `list_applications`
- **Parameters**: (none required)
- **Expected**: Array of 3 applications

#### Compare Job Performance
- **Tool**: `compare_job_performance`
- **Parameters**:
  - `app_id1` = `spark-bcec39f6201b42b9925124595baad260`
  - `app_id2` = `spark-110be3a8424d4a2789cb88134418217b`
- **Expected**: Performance comparison metrics


## ✅ Success Criteria

- [ ] All 3 applications visible in list_applications
- [ ] Job comparison tools return detailed analysis
- [ ] Performance comparison shows meaningful differences
- [ ] Bottleneck analysis provides recommendations
- [ ] No errors in any tool execution
- [ ] Upstream stage optimizations work (no overwhelming data)

## 🛠️ Troubleshooting

### Common Issues

#### Script Issues
```bash
# If you get "Docker not running" error:
./start_local_spark_history.sh --dry-run  # Check prerequisites

# If you get "No containers to stop" warning:
# This is normal - just means no previous containers are running

# To get help with script options:
./start_local_spark_history.sh --help
```

#### MCP Server Issues
```bash
# If MCP server fails to start:
# 1. Ensure Spark History Server is running (Terminal 1)
# 2. Check if port 18080 is accessible: curl http://localhost:18080
# 3. Verify config.yaml exists and has correct server URL
```

#### Inspector Connection Issues
```bash
# If MCP Inspector can't connect:
# 1. Ensure MCP server is running (Terminal 2)
# 2. Try restarting the MCP server
# 3. Check for any error messages in Terminal 2
```

## 🚀 Ready for Production

Once all tests pass, the enhanced MCP server with job comparison capabilities is ready for production use!

### Quick Validation Commands
```bash
# Validate everything is working:
curl http://localhost:18080/api/v1/applications  # Should return 3 applications
```
