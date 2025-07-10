# LlamaIndex Integration Example

> **🚧 Blueprint Notice**: This is a foundational blueprint to showcase the usage of Spark History Server MCP with LlamaIndex. It is designed for demonstration and learning purposes only. For production-ready agents, you will need to extend this code with additional error handling, security measures, authentication, monitoring, logging, and custom business logic specific to your use case.

This example demonstrates how to integrate Spark History Server MCP with LlamaIndex for intelligent Spark performance analysis.

## Important Note

This is a **standalone example** that does not modify the core MCP server dependencies.

## Quick Start

1. **Install example dependencies:**
   ```bash
   # Install required packages for this example
   pip install llama-index llama-index-llms-ollama llama-index-tools-mcp requests
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
   python -i llamaindex_example.py
   ```

5. **Test the agent:**
   ```python
   >>> agent.chat('Get jobs info spark-110be3a8424d4a2789cb88134418217b')
   >>> agent.chat('Analyze performance for application spark-cc4d115f011443d787f03a71a476a745')
   ```

## Files

- `llamaindex_example.py` - Main integration example
- `llamaindex.md` - Detailed documentation

For complete documentation, see [llamaindex.md](llamaindex.md).
