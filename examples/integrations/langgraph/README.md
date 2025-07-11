# LangGraph Integration Example

> **🚧 Blueprint Notice**: This is a foundational blueprint to showcase the usage of Spark History Server MCP with LangGraph. It is designed for demonstration and learning purposes only. For production-ready agents, you will need to extend this code with additional error handling, security measures, authentication, monitoring, logging, and custom business logic specific to your use case.

This example demonstrates how to integrate Spark History Server MCP with LangGraph for workflow-based Spark performance analysis using local models.

## Setup

### 1. Start Services (from project root)

```bash
# Navigate to project root first (if not already there)
cd spark-history-server-mcp

# Start Spark History Server with sample data
task start-spark-bg

# Start MCP Server
task start-mcp-bg
```

### 2. Navigate to LangGraph Example

```bash
# Navigate to the LangGraph integration folder
cd examples/integrations/langgraph
```

### 3. Install Dependencies

```bash
# Install required packages for this example
uv venv
source .venv/bin/activate
uv pip install langgraph langchain-ollama requests
```

### 4. Install and Setup Ollama (Local LLM)

```bash
# Install Ollama
brew install ollama

# Start Ollama service
brew services start ollama

# Pull a small, fast model (0.6B model for speed)
ollama pull qwen3:0.6b
```

## Usage

### Run the Example

**Interactive Mode (Recommended):**
```bash
python -i langgraph_example.py
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

### Interactive Usage

```python
# Example workflow analysis (require specific Spark application IDs)

>>> workflow.analyze('spark-cc4d115f011443d787f03a71a476a745')
📊 Step 1: Collecting basic info for spark-cc4d115f011443d787f03a71a476a745...
✅ App: NewYorkTaxiData_2025_06_27_03_56_52
📊 Step 2: Collecting jobs info for spark-cc4d115f011443d787f03a71a476a745...
✅ Jobs: 6 total, 0 failed
📊 Step 3: Analyzing performance for spark-cc4d115f011443d787f03a71a476a745...
✅ Performance analysis completed
📊 Step 4: Generating recommendations for spark-cc4d115f011443d787f03a71a476a745...
✅ Generated 3 recommendations

📋 Analysis Results:
========================================
Application: NewYorkTaxiData_2025_06_27_03_56_52
Status: Completed
Duration: 508.6 seconds
Jobs: 6 total, 0 failed

Performance Analysis:
The Spark application shows a 508-second duration with 6 total jobs, indicating potential
issues like inefficient data processing or memory constraints that could slow execution.

Recommendations:
  1. Optimize data loading and processing steps to reduce overhead
  2. Check resource allocation for better performance
  3. Review stage bottlenecks and parallelization strategies

✅ Analysis completed!

# View available application IDs
>>> sample_apps
['spark-cc4d115f011443d787f03a71a476a745', 'spark-bcec39f6201b42b9925124595baad260', 'spark-110be3a8424d4a2789cb88134418217b']

# For programmatic use, get the result data
>>> result = workflow.analyze('spark-cc4d115f011443d787f03a71a476a745', return_data=True)
>>> print(result['performance_analysis'])
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

# Larger, higher quality model (better instruction following)
llm = ChatOllama(model="qwen3:4b")
```

### Note on Thinking Blocks

This example uses `/no_think` parameters in prompts to prevent Qwen3 from outputting thinking blocks (`<think>...</think>`). This is the proper approach recommended by Qwen3 documentation.

The 0.6b model has limited instruction-following capacity, so it may still output empty thinking blocks despite `/no_think`. The code includes a cleanup function as a fallback to remove these empty blocks.

For better results:
- Use larger models (1.7b, 4b) for more reliable `/no_think` behavior
- The `/no_think` approach is much cleaner than post-processing
- For production use, larger models provide complete thinking block prevention
