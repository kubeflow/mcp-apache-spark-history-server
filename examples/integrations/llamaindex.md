# LlamaIndex Integration

This guide shows how to integrate the Spark History Server MCP with LlamaIndex to build intelligent Spark job analysis and retrieval systems.

## Installation

```bash
pip install llama-index llama-index-tools-requests
# or
uv add llama-index llama-index-tools-requests
```

## Basic Integration

### 1. MCP Tool Integration with LlamaIndex

```python
from llama_index.core.agent import ReActAgent
from llama_index.core.tools import FunctionTool
from llama_index.llms.openai import OpenAI
import asyncio
import aiohttp

class SparkMCPTools:
    def __init__(self, mcp_server_url: str):
        self.mcp_url = mcp_server_url
    
    async def _call_mcp_tool(self, tool_name: str, parameters: dict = None):
        """Call MCP tool via HTTP API."""
        async with aiohttp.ClientSession() as session:
            payload = {
                "tool": tool_name,
                "parameters": parameters or {}
            }
            async with session.post(f"{self.mcp_url}/tools", json=payload) as response:
                return await response.json()
    
    def get_application_info(self, app_id: str) -> str:
        """Get detailed information about a Spark application."""
        async def _get_info():
            result = await self._call_mcp_tool("get_application_info", {"app_id": app_id})
            return str(result)
        
        return asyncio.run(_get_info())
    
    def list_applications(self) -> str:
        """List all Spark applications."""
        async def _list_apps():
            result = await self._call_mcp_tool("list_applications")
            return str(result)
        
        return asyncio.run(_list_apps())
    
    def compare_job_performance(self, spark_id1: str, spark_id2: str) -> str:
        """Compare performance between two Spark applications."""
        async def _compare():
            result = await self._call_mcp_tool("compare_job_performance", {
                "spark_id1": spark_id1,
                "spark_id2": spark_id2
            })
            return str(result)
        
        return asyncio.run(_compare())
    
    def get_job_bottlenecks(self, spark_id: str) -> str:
        """Analyze job bottlenecks and performance issues."""
        async def _analyze():
            result = await self._call_mcp_tool("get_job_bottlenecks", {"spark_id": spark_id})
            return str(result)
        
        return asyncio.run(_analyze())

# Create LlamaIndex tools from MCP tools
def create_spark_tools(mcp_server_url: str):
    spark_tools = SparkMCPTools(mcp_server_url)
    
    tools = [
        FunctionTool.from_defaults(
            fn=spark_tools.get_application_info,
            name="get_application_info",
            description="Get detailed information about a Spark application by app_id"
        ),
        FunctionTool.from_defaults(
            fn=spark_tools.list_applications,
            name="list_applications", 
            description="List all available Spark applications"
        ),
        FunctionTool.from_defaults(
            fn=spark_tools.compare_job_performance,
            name="compare_job_performance",
            description="Compare performance metrics between two Spark applications"
        ),
        FunctionTool.from_defaults(
            fn=spark_tools.get_job_bottlenecks,
            name="get_job_bottlenecks",
            description="Analyze performance bottlenecks for a Spark application"
        ),
    ]
    
    return tools

# Create the agent
llm = OpenAI(model="gpt-4", temperature=0)
tools = create_spark_tools("http://localhost:18888")

agent = ReActAgent.from_tools(
    tools,
    llm=llm,
    verbose=True,
    system_prompt="""
    You are a Spark performance analysis expert. You have access to tools that can:
    - List and retrieve Spark application details
    - Compare performance between different jobs
    - Analyze bottlenecks and performance issues
    
    Always provide actionable insights and specific recommendations.
    When analyzing performance issues, look for patterns in resource usage, 
    stage execution times, and configuration differences.
    """
)

# Use the agent
response = agent.chat("Analyze the performance of application spark-12345 and suggest optimizations")
print(response)
```

### 2. Building a Spark Knowledge Index

```python
from llama_index.core import VectorStoreIndex, Document, Settings
from llama_index.core.node_parser import SentenceSplitter
from llama_index.embeddings.openai import OpenAIEmbedding
from llama_index.core.storage.storage_context import StorageContext
from llama_index.vector_stores.faiss import FaissVectorStore
import faiss

class SparkKnowledgeIndex:
    def __init__(self, mcp_server_url: str):
        self.mcp_url = mcp_server_url
        self.index = None
        
        # Configure LlamaIndex settings
        Settings.embed_model = OpenAIEmbedding()
        Settings.node_parser = SentenceSplitter(chunk_size=512, chunk_overlap=50)
        
    async def build_index(self):
        """Build knowledge index from Spark application data."""
        documents = []
        spark_tools = SparkMCPTools(self.mcp_url)
        
        # Get all applications
        apps_result = await spark_tools._call_mcp_tool("list_applications")
        applications = apps_result.get('applications', [])
        
        for app in applications:
            app_id = app['id']
            
            # Get detailed application info
            app_info = await spark_tools._call_mcp_tool("get_application_info", {"app_id": app_id})
            
            # Get performance analysis
            try:
                bottlenecks = await spark_tools._call_mcp_tool("get_job_bottlenecks", {"spark_id": app_id})
                executor_summary = await spark_tools._call_mcp_tool("get_executor_summary", {"spark_id": app_id})
            except:
                bottlenecks = {}
                executor_summary = {}
            
            # Create comprehensive document
            doc_content = f"""
            Application ID: {app_id}
            Application Name: {app_info.get('name', 'Unknown')}
            Status: {app_info.get('attempts', [{}])[0].get('completed', 'Unknown')}
            Duration: {app_info.get('duration', 'Unknown')} ms
            
            Application Details:
            {app_info}
            
            Performance Analysis:
            {bottlenecks}
            
            Executor Summary:
            {executor_summary}
            """
            
            documents.append(Document(
                text=doc_content,
                metadata={
                    "app_id": app_id,
                    "app_name": app_info.get('name', 'Unknown'),
                    "status": app_info.get('attempts', [{}])[0].get('completed', 'Unknown'),
                    "type": "spark_application"
                }
            ))
        
        # Create FAISS vector store
        d = 1536  # OpenAI embedding dimension
        faiss_index = faiss.IndexFlatL2(d)
        vector_store = FaissVectorStore(faiss_index=faiss_index)
        storage_context = StorageContext.from_defaults(vector_store=vector_store)
        
        # Build the index
        self.index = VectorStoreIndex.from_documents(
            documents,
            storage_context=storage_context
        )
        
        return self.index
    
    def query(self, question: str, similarity_top_k: int = 5):
        """Query the knowledge index."""
        if not self.index:
            raise ValueError("Index not built. Call build_index() first.")
        
        query_engine = self.index.as_query_engine(
            similarity_top_k=similarity_top_k,
            response_mode="tree_summarize"
        )
        
        return query_engine.query(question)

# Usage
async def main():
    kb = SparkKnowledgeIndex("http://localhost:18888")
    await kb.build_index()
    
    # Query the knowledge base
    response = kb.query("What are the most common performance bottlenecks in failed Spark jobs?")
    print(response)
    
    response = kb.query("Show me applications that had memory-related issues")
    print(response)

asyncio.run(main())
```

### 3. Advanced RAG with Spark Metrics

```python
from llama_index.core.retrievers import VectorIndexRetriever
from llama_index.core.query_engine import RetrieverQueryEngine
from llama_index.core.postprocessor import SimilarityPostprocessor, KeywordNodePostprocessor

class SparkRAGSystem:
    def __init__(self, mcp_server_url: str):
        self.mcp_url = mcp_server_url
        self.index = None
        
    async def setup_advanced_rag(self):
        """Setup advanced RAG system with custom retrievers and post-processors."""
        # Build knowledge index
        kb = SparkKnowledgeIndex(self.mcp_url)
        self.index = await kb.build_index()
        
        # Custom retriever with higher recall
        retriever = VectorIndexRetriever(
            index=self.index,
            similarity_top_k=10,
        )
        
        # Post-processors for better relevance
        postprocessors = [
            KeywordNodePostprocessor(
                keywords=["error", "failure", "bottleneck", "performance", "memory", "cpu"],
                exclude_keywords=["success", "completed"]
            ),
            SimilarityPostprocessor(similarity_cutoff=0.7)
        ]
        
        # Create query engine
        self.query_engine = RetrieverQueryEngine(
            retriever=retriever,
            node_postprocessors=postprocessors,
            response_mode="tree_summarize"
        )
    
    async def analyze_with_context(self, app_id: str):
        """Analyze application with historical context."""
        if not self.query_engine:
            await self.setup_advanced_rag()
        
        # Get current app details
        spark_tools = SparkMCPTools(self.mcp_url)
        current_app = await spark_tools._call_mcp_tool("get_application_info", {"app_id": app_id})
        
        # Query for similar issues
        query = f"""
        Find similar Spark applications that had comparable issues to this one:
        
        Current Application: {app_id}
        Status: {current_app.get('attempts', [{}])[0].get('completed', 'Unknown')}
        Duration: {current_app.get('duration', 'Unknown')}
        
        Look for patterns in:
        - Similar execution times
        - Memory usage patterns
        - Common failure modes
        - Configuration similarities
        """
        
        similar_cases = self.query_engine.query(query)
        
        return {
            "current_application": current_app,
            "similar_cases": similar_cases,
            "recommendations": self._generate_recommendations(current_app, similar_cases)
        }
    
    def _generate_recommendations(self, current_app, similar_cases):
        """Generate recommendations based on historical patterns."""
        # This would use LLM to analyze patterns and generate recommendations
        llm = OpenAI(model="gpt-4")
        
        prompt = f"""
        Based on the current application and similar historical cases, provide recommendations:
        
        Current Application: {current_app}
        Similar Historical Cases: {similar_cases}
        
        Provide:
        1. Root cause analysis
        2. Specific configuration changes
        3. Resource optimization suggestions
        4. Monitoring recommendations
        """
        
        return llm.complete(prompt)

# Usage
async def analyze_application():
    rag_system = SparkRAGSystem("http://localhost:18888")
    analysis = await rag_system.analyze_with_context("spark-application-12345")
    
    print("Analysis Results:")
    print(f"Current App: {analysis['current_application']}")
    print(f"Similar Cases: {analysis['similar_cases']}")
    print(f"Recommendations: {analysis['recommendations']}")

asyncio.run(analyze_application())
```

### 4. Real-time Spark Monitoring with LlamaIndex

```python
from llama_index.core.chat_engine import SimpleChatEngine
from llama_index.core.memory import ChatMemoryBuffer
import asyncio
import schedule
import time

class SparkMonitoringChat:
    def __init__(self, mcp_server_url: str):
        self.mcp_url = mcp_server_url
        self.chat_engine = None
        self.memory = ChatMemoryBuffer.from_defaults(token_limit=3000)
        
    async def setup_monitoring_chat(self):
        """Setup chat engine for interactive monitoring."""
        # Build knowledge base
        kb = SparkKnowledgeIndex(self.mcp_url)
        index = await kb.build_index()
        
        # Create chat engine with memory
        self.chat_engine = SimpleChatEngine.from_defaults(
            llm=OpenAI(model="gpt-4", temperature=0),
            memory=self.memory,
            system_prompt="""
            You are a Spark monitoring assistant. You help users understand:
            - Current application status and performance
            - Historical patterns and trends
            - Performance optimization opportunities
            - Failure predictions and prevention
            
            Always provide actionable insights and be proactive about potential issues.
            """
        )
    
    async def monitor_and_alert(self):
        """Continuous monitoring with intelligent alerts."""
        spark_tools = SparkMCPTools(self.mcp_url)
        
        while True:
            try:
                # Get current applications
                apps = await spark_tools._call_mcp_tool("list_applications")
                
                for app in apps.get('applications', []):
                    app_id = app['id']
                    
                    # Analyze current state
                    analysis = await self._analyze_current_state(app_id)
                    
                    if analysis['needs_attention']:
                        alert_message = f"""
                        🚨 ALERT: Application {app_id} needs attention!
                        
                        Issue: {analysis['issue']}
                        Severity: {analysis['severity']}
                        Recommendation: {analysis['recommendation']}
                        """
                        
                        print(alert_message)
                        
                        # Add to chat memory for context
                        self.memory.put(f"ALERT: {alert_message}")
                
                await asyncio.sleep(300)  # Check every 5 minutes
                
            except Exception as e:
                print(f"Monitoring error: {e}")
                await asyncio.sleep(60)
    
    async def _analyze_current_state(self, app_id: str):
        """Analyze current application state."""
        spark_tools = SparkMCPTools(self.mcp_url)
        
        # Get app info and bottlenecks
        app_info = await spark_tools._call_mcp_tool("get_application_info", {"app_id": app_id})
        
        try:
            bottlenecks = await spark_tools._call_mcp_tool("get_job_bottlenecks", {"spark_id": app_id})
        except:
            bottlenecks = {}
        
        # Simple heuristics for demonstration
        needs_attention = False
        issue = None
        severity = "low"
        recommendation = ""
        
        # Check duration
        duration = app_info.get('duration', 0)
        if duration > 3600000:  # > 1 hour
            needs_attention = True
            issue = "Long running application"
            severity = "medium"
            recommendation = "Check for performance bottlenecks"
        
        # Check completion status
        attempts = app_info.get('attempts', [])
        if attempts and not attempts[0].get('completed', True):
            needs_attention = True
            issue = "Application failed"
            severity = "high"
            recommendation = "Investigate failure cause"
        
        return {
            'needs_attention': needs_attention,
            'issue': issue,
            'severity': severity,
            'recommendation': recommendation
        }
    
    async def interactive_chat(self):
        """Start interactive chat session."""
        if not self.chat_engine:
            await self.setup_monitoring_chat()
        
        print("Spark Monitoring Chat Started. Type 'quit' to exit.")
        
        while True:
            user_input = input("You: ")
            if user_input.lower() == 'quit':
                break
            
            response = self.chat_engine.chat(user_input)
            print(f"Assistant: {response}")

# Usage
async def start_monitoring_system():
    monitor = SparkMonitoringChat("http://localhost:18888")
    
    # Start background monitoring
    monitoring_task = asyncio.create_task(monitor.monitor_and_alert())
    
    # Start interactive chat
    await monitor.interactive_chat()
    
    # Cancel monitoring when chat ends
    monitoring_task.cancel()

# Run the monitoring system
asyncio.run(start_monitoring_system())
```

## Configuration Examples

### Custom Embeddings for Spark Metrics

```python
from llama_index.embeddings.huggingface import HuggingFaceEmbedding

# Use specialized embeddings for technical content
Settings.embed_model = HuggingFaceEmbedding(
    model_name="sentence-transformers/all-MiniLM-L6-v2"
)
```

### Custom Node Parsing for Spark Logs

```python
from llama_index.core.node_parser import SimpleNodeParser

class SparkLogNodeParser(SimpleNodeParser):
    def get_nodes_from_documents(self, documents):
        # Custom parsing logic for Spark application data
        nodes = []
        for doc in documents:
            # Extract key metrics and create focused nodes
            # Implementation would parse Spark-specific structures
            pass
        return nodes
```

## Best Practices

1. **Index Management**: Regularly rebuild indices with fresh Spark data
2. **Query Optimization**: Use specific keywords related to Spark performance
3. **Memory Management**: Implement proper cleanup for long-running monitoring
4. **Error Handling**: Robust handling of MCP connection issues
5. **Caching**: Cache frequently accessed application data

## Advanced Examples

See the `/examples/llamaindex/` directory for more examples including:
- Multi-modal analysis with Spark UI screenshots
- Integration with Spark streaming applications
- Custom evaluation metrics for RAG performance
- Automated performance regression detection