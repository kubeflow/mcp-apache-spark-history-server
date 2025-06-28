# Testing Guide: Spark History Server MCP

## 🧪 Quick Test with MCP Inspector (5 minutes)

### Prerequisites
- Docker must be running (for Spark History Server)
- Node.js installed (for MCP Inspector)
- Run commands from project root directory

### Setup (2 terminals)

```bash
# Terminal 1: Start Spark History Server with sample data
./start_local_spark_history.sh

# Terminal 2: Start MCP server with Inspector
npx @modelcontextprotocol/inspector uv run main.py
# This will open http://localhost:6274 in your browser
```

### Alternative: Start MCP Server Separately
```bash
# Terminal 1: Start Spark History Server
./start_local_spark_history.sh

# Terminal 2: Start MCP Server
uv run main.py

# Terminal 3: Start MCP Inspector (connects to existing MCP server)
DANGEROUSLY_OMIT_AUTH=true npx @modelcontextprotocol/inspector
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
- `spark-bcec39f6201b42b9925124595baad260`
- `spark-110be3a8424d4a2789cb88134418217b`
- `spark-cc4d115f011443d787f03a71a476a745`

## 🌐 Using MCP Inspector

Once the MCP Inspector opens in your browser (http://localhost:6274), you can:

1. **View Available Tools** - See all MCP tools in the left sidebar
2. **Test Tools Interactively** - Click any tool to see its parameters
3. **Execute Tools** - Fill in parameters and run tools
4. **View Results** - See structured responses from your Spark History Server

### Example Tool Tests:

#### Get Application Details
- **Tool**: `get_application`
- **Parameter**: `spark_id` = `spark-cc4d115f011443d787f03a71a476a745`
- **Expected**: Application info including name, duration, status

#### List All Applications
- **Tool**: `list_applications`
- **Parameters**: (none required)
- **Expected**: Array of 3 applications

#### Compare Job Performance
- **Tool**: `compare_job_performance`
- **Parameters**:
  - `spark_id1` = `spark-bcec39f6201b42b9925124595baad260`
  - `spark_id2` = `spark-110be3a8424d4a2789cb88134418217b`
- **Expected**: Performance comparison metrics

## 🔬 Detailed Test Cases

### 1. **Basic Connectivity**
```json
Tool: list_applications
Expected: 3 applications returned
```

### 2. **Job Environment Comparison**
```json
Tool: compare_job_environments
Parameters: {
  "spark_id1": "spark-bcec39f6201b42b9925124595baad260",
  "spark_id2": "spark-110be3a8424d4a2789cb88134418217b"
}
Expected: Configuration differences including:
- Runtime comparison (Java/Scala versions)
- Spark property differences
- System property differences
```

### 3. **Performance Comparison**
```json
Tool: compare_job_performance
Parameters: {
  "spark_id1": "spark-bcec39f6201b42b9925124595baad260",
  "spark_id2": "spark-cc4d115f011443d787f03a71a476a745"
}
Expected: Performance metrics including:
- Resource allocation comparison
- Executor metrics comparison
- Job performance ratios
```

### 4. **Bottleneck Analysis**
```json
Tool: get_job_bottlenecks
Parameters: {
  "spark_id": "spark-cc4d115f011443d787f03a71a476a745"
}
Expected: Performance analysis with:
- Slowest stages identification
- Resource bottlenecks
- Optimization recommendations
```

### 5. **Resource Timeline**
```json
Tool: get_resource_usage_timeline
Parameters: {
  "spark_id": "spark-bcec39f6201b42b9925124595baad260"
}
Expected: Timeline showing:
- Executor addition/removal events
- Stage execution timeline
- Resource utilization over time
```

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
