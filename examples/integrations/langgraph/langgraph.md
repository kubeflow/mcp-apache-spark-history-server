# LangGraph Integration with Spark History Server MCP

> **🚧 Blueprint Notice**: This is a foundational blueprint to showcase the usage of Spark History Server MCP with LangGraph. It is designed for demonstration and learning purposes only. For production-ready agents, you will need to extend this code with additional error handling, security measures, authentication, monitoring, logging, and custom business logic specific to your use case.

This guide shows how to integrate Spark History Server MCP with LangGraph for workflow-based Spark performance analysis using local models.

## Setup

### 1. Install Dependencies

```bash
# Install required packages for this example
pip install langgraph langchain-ollama requests
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
python -i examples/integrations/langgraph/langgraph_example.py
```

### Expected Output

```
🚀 LangGraph + Spark History Server MCP
✅ MCP server is running
✅ Found 3 applications
✅ Ollama available
✅ LangGraph workflow ready

🎯 Interactive session ready!
Examples:
  >>> workflow.analyze('spark-cc4d115f011443d787f03a71a476a745')
  >>> workflow.analyze('spark-110be3a8424d4a2789cb88134418217b')

Available sample app IDs: ['spark-cc4d115f011443d787f03a71a476a745', ...]
```

## Interactive Usage

```python
# Example workflow analysis (require specific Spark application IDs)

>>> workflow.analyze('spark-cc4d115f011443d787f03a71a476a745')
📊 Step 1: Collecting basic info...
📊 Step 2: Collecting jobs info...
📊 Step 3: Analyzing performance...
📊 Step 4: Generating recommendations...

Analysis Results:
- Application: NewYorkTaxiData_2025_06_27_03_56_52
- Status: Completed
- Duration: 508.6 seconds
- Jobs: 6 total, 0 failed
- Performance Assessment: Long running application requires optimization
- Recommendations: Check stage bottlenecks, optimize resource allocation

# View available application IDs
>>> sample_apps
['spark-cc4d115f011443d787f03a71a476a745', 'spark-bcec39f6201b42b9925124595baad260', 'spark-110be3a8424d4a2789cb88134418217b']
```

## Key Features

- **Workflow-based Analysis**: Multi-step structured analysis using LangGraph
- **MCP + Fallback**: Attempts MCP server first, falls back to direct API
- **Modern LangGraph**: Uses current StateGraph and workflow patterns
- **Interactive Sessions**: Global workflow object for REPL usage

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
llm = ChatOllama(model="qwen3:0.6b")

# Larger, higher quality model
llm = ChatOllama(model="qwen3:4b")
```