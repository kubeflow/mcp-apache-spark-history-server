# AI Agent Integration Examples

This directory contains comprehensive guides for integrating the Spark History Server MCP with various AI agent frameworks and platforms.

## Available Integrations

### 🔧 **Production AI Framework Integrations**
- **[LlamaIndex](llamaindex.md)** - RAG systems and knowledge bases for Spark data
  - Vector indexing of Spark application data
  - Query engines for performance analysis
  - Real-time monitoring chat systems
  - Custom embeddings for technical content

- **[LangGraph](langgraph.md)** - Multi-agent workflows and state machines
  - Complex analysis workflows
  - Multi-agent monitoring systems
  - Optimization recommendation pipelines
  - State-based failure investigations

## Quick Start Guide

1. **Choose Your Platform**: Start with Claude Desktop for immediate interactive analysis
2. **Review Integration Guide**: Each guide includes complete setup instructions
3. **Test Locally**: Use the provided sample data and local Spark History Server
4. **Customize**: Adapt the examples to your specific use cases

## Common Integration Patterns

### **Interactive Analysis**
Perfect for ad-hoc investigation and exploration:
- Claude Desktop integration
- Jupyter notebook workflows
- Real-time query interfaces

### **Automated Monitoring**
Ideal for production monitoring and alerting:
- LangChain monitoring agents
- Custom alerting systems
- Integration with existing monitoring tools

### **Knowledge Systems**
Great for building organizational knowledge bases:
- LlamaIndex RAG systems
- Historical pattern analysis
- Performance regression detection

### **Complex Workflows**
For sophisticated analysis pipelines:
- LangGraph state machines
- Multi-step optimization workflows
- Batch failure investigations

## 🧪 Local Testing and Development

For local testing and development, use the **MCP Inspector** instead of complex AI agent setups:

- **[TESTING.md](../../TESTING.md)** - Complete guide for testing with MCP Inspector
- **Interactive Testing** - Use browser-based MCP Inspector for immediate tool testing
- **No Configuration Required** - Simple one-command setup for development

The MCP Inspector provides the fastest way to test your MCP server locally before deploying to production with AI agents.

## Best Practices

### **Development**
1. Start with MCP Inspector for local testing
2. Use the sample Spark applications for development
3. Implement error handling and retries
4. Log all interactions for debugging

### **Production**
1. Deploy using Kubernetes + Helm charts
2. Implement proper authentication
3. Add rate limiting and timeouts
4. Monitor agent performance and set up alerting

### **Performance**
1. Batch API calls when possible
2. Cache frequently accessed data
3. Use appropriate similarity thresholds
4. Optimize query patterns

## Sample Data

All integration examples work with the provided sample data:
- **spark-bcec39f6201b42b9925124595baad260** - Successful ETL job
- **spark-110be3a8424d4a2789cb88134418217b** - Data processing job
- **spark-cc4d115f011443d787f03a71a476a745** - Multi-stage analytics job

Use these applications to test your integrations before connecting to production data.

## Contributing

We welcome contributions to expand the integration examples:

1. **New Framework Integrations**: Add support for additional AI frameworks
2. **Production Examples**: Share real-world deployment patterns
3. **Specialized Agents**: Contribute domain-specific analysis agents
4. **Best Practices**: Document lessons learned from production deployments

See the main project [Contributing Guide](../../README.md#-contributing) for details.

## Support

- 🐛 **Issues**: [GitHub Issues](https://github.com/DeepDiagnostix-AI/spark-history-server-mcp/issues)
- 💡 **Discussions**: [GitHub Discussions](https://github.com/DeepDiagnostix-AI/spark-history-server-mcp/discussions)
- 📖 **Documentation**: [Project Wiki](https://github.com/DeepDiagnostix-AI/spark-history-server-mcp/wiki)