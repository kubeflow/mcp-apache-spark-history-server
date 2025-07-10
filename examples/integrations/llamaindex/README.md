# LlamaIndex Integration Example

> **🚧 Blueprint Notice**: This is a foundational blueprint to showcase the usage of Spark History Server MCP with LlamaIndex. It is designed for demonstration and learning purposes only. For production-ready agents, you will need to extend this code with additional error handling, security measures, authentication, monitoring, logging, and custom business logic specific to your use case.

This example demonstrates how to integrate Spark History Server MCP with LlamaIndex for intelligent Spark performance analysis using local models.

## Setup

### 1. Install Dependencies

```bash
# Install required packages for this example
pip install llama-index llama-index-llms-ollama llama-index-tools-mcp requests
```

### 2. Install and Setup Ollama (Local LLM)

```bash
# Install Ollama
brew install ollama

# Start Ollama service
brew services start ollama

# Pull a small, fast model (0.6B model for speed)
ollama pull qwen3:0.6b
```

### 3. Start Services

```bash
# Start Spark History Server with sample data
task start-spark-bg

# Start MCP Server
task start-mcp-bg
```

## Usage

### Run the Example

**Interactive Mode (Recommended):**
```bash
python -i llamaindex_example.py
```

### Expected Output

```
🚀 LlamaIndex + Spark History Server MCP
✅ MCP server is running
✅ Found 3 applications
✅ Ollama available
🔗 MCP server is responding...
⚠️  MCP server connection failed: ...
📝 Using direct API fallback instead
✅ LlamaIndex FunctionAgent ready

🎯 Interactive session ready!
Examples:
  >>> agent.chat('Analyze performance for application spark-cc4d115f011443d787f03a71a476a745')
  >>> agent.chat('Get jobs info spark-110be3a8424d4a2789cb88134418217b')
  >>> agent.chat('Get application info spark-bcec39f6201b42b9925124595baad260')

Available sample app IDs: ['spark-cc4d115f011443d787f03a71a476a745', ...]
```

### Interactive Usage

```python
# Example agent queries (require specific Spark application IDs)

>>> agent.chat("Analyze performance for application spark-cc4d115f011443d787f03a71a476a745")
📊 Data source: ❌ Fallback (Direct API)
The performance analysis shows:
- Application: NewYorkTaxiData_2025_06_27_03_56_52
- Duration: 508.6 seconds
- Total Jobs: 6
- Failed Jobs: 0
- Issue: Long running application (>5 min)
- Recommendation: Check for stage bottlenecks

>>> agent.chat("Get jobs info spark-110be3a8424d4a2789cb88134418217b")
📊 Data source: ❌ Fallback (Direct API)
Job execution status:
- Total jobs executed: 6
- Succeeded jobs: 6
- Failed jobs: 0

# View available application IDs
>>> sample_apps
['spark-cc4d115f011443d787f03a71a476a745', 'spark-bcec39f6201b42b9925124595baad260', 'spark-110be3a8424d4a2789cb88134418217b']
```

## Key Features

- **MCP + Fallback**: Attempts MCP server first, falls back to direct API
- **Reliable**: Fixed event loop issues for unlimited consecutive queries
- **Modern LlamaIndex**: Uses current `FunctionAgent` workflow API
- **Subprocess Isolation**: Each call runs in fresh environment for maximum reliability

## Troubleshooting

### Services Not Running
```bash
# Check Spark History Server
curl http://localhost:18080/api/v1/applications

# Check MCP Server
curl http://localhost:18888/

# Check Ollama
curl http://localhost:11434/api/tags

# Restart if needed
task stop-all
task start-spark-bg
task start-mcp-bg
```

### Using Different Models

```python
# Smallest, fastest model (recommended for demos)
llm = Ollama(model="qwen3:0.6b")

# Larger, higher quality model
llm = Ollama(model="qwen3:4b")
```
