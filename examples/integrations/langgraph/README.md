# LangGraph Integration Example

> **🚧 Blueprint Notice**: This is a foundational blueprint to showcase the usage of Spark History Server MCP with LangGraph. It is designed for demonstration and learning purposes only. For production-ready agents, you will need to extend this code with additional error handling, security measures, authentication, monitoring, logging, and custom business logic specific to your use case.

This example demonstrates how to integrate Spark History Server MCP with LangGraph for workflow-based Spark performance analysis.

## Important Note

This is a **standalone example** that does not modify the core MCP server dependencies.

## Quick Start

1. **Install example dependencies:**
   ```bash
   # Install required packages for this example
   pip install langgraph langchain-ollama requests
   ```

2. **Start services:**
   ```bash
   task start-spark-bg
   task start-mcp-bg
   ```

3. **Setup Ollama:**
   ```bash
   brew install ollama
   brew services start ollama
   ollama pull qwen3:0.6b
   ```

4. **Run the example:**
   ```bash
   python -i langgraph_example.py
   ```

5. **Test the workflow:**
   ```python
   >>> workflow.analyze('spark-cc4d115f011443d787f03a71a476a745')
   >>> workflow.analyze('spark-110be3a8424d4a2789cb88134418217b')
   ```

## Files

- `langgraph_example.py` - Main integration example
- `langgraph.md` - Detailed documentation

For complete documentation, see [langgraph.md](langgraph.md).