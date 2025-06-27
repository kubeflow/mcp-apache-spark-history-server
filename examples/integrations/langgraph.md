# LangGraph Integration

This guide demonstrates how to build sophisticated Spark analysis workflows using LangGraph's state management and multi-agent capabilities.

## Installation

```bash
pip install langgraph langchain-openai
# or  
uv add langgraph langchain-openai
```

## Basic Multi-Agent Spark Analysis

### 1. Spark Analysis State Machine

```python
from typing import Annotated, List, Dict, Any
from langgraph.graph import StateGraph, END
from langgraph.graph.message import add_messages
from langchain_core.messages import BaseMessage, HumanMessage, AIMessage
from langchain_openai import ChatOpenAI
import asyncio
from mcp import ClientSession

class SparkAnalysisState:
    """State for Spark analysis workflow."""
    messages: Annotated[List[BaseMessage], add_messages]
    app_id: str
    analysis_results: Dict[str, Any]
    recommendations: List[str]
    current_step: str
    
class SparkAnalysisWorkflow:
    def __init__(self, mcp_server_url: str):
        self.mcp_url = mcp_server_url
        self.llm = ChatOpenAI(model="gpt-4", temperature=0)
        self.graph = self._build_graph()
    
    def _build_graph(self):
        """Build the analysis workflow graph."""
        workflow = StateGraph(SparkAnalysisState)
        
        # Add nodes
        workflow.add_node("collect_basic_info", self.collect_basic_info)
        workflow.add_node("analyze_performance", self.analyze_performance)
        workflow.add_node("identify_bottlenecks", self.identify_bottlenecks)
        workflow.add_node("compare_with_historical", self.compare_with_historical)
        workflow.add_node("generate_recommendations", self.generate_recommendations)
        workflow.add_node("format_report", self.format_report)
        
        # Add edges
        workflow.add_edge("collect_basic_info", "analyze_performance")
        workflow.add_edge("analyze_performance", "identify_bottlenecks")
        workflow.add_edge("identify_bottlenecks", "compare_with_historical")
        workflow.add_edge("compare_with_historical", "generate_recommendations")
        workflow.add_edge("generate_recommendations", "format_report")
        workflow.add_edge("format_report", END)
        
        # Set entry point
        workflow.set_entry_point("collect_basic_info")
        
        return workflow.compile()
    
    async def collect_basic_info(self, state: SparkAnalysisState):
        """Collect basic application information."""
        async with ClientSession(self.mcp_url) as session:
            app_info = await session.call_tool("get_application_info", {"app_id": state["app_id"]})
            jobs = await session.call_tool("get_jobs", {"spark_id": state["app_id"]})
            
            state["analysis_results"]["basic_info"] = app_info
            state["analysis_results"]["jobs"] = jobs
            state["current_step"] = "basic_info_collected"
            
        state["messages"].append(AIMessage(content=f"Collected basic info for {state['app_id']}"))
        return state
    
    async def analyze_performance(self, state: SparkAnalysisState):
        """Analyze application performance metrics."""
        async with ClientSession(self.mcp_url) as session:
            try:
                executor_summary = await session.call_tool("get_executor_summary", {"spark_id": state["app_id"]})
                stages = await session.call_tool("get_stages", {"spark_id": state["app_id"]})
                
                state["analysis_results"]["performance"] = {
                    "executor_summary": executor_summary,
                    "stages": stages
                }
                
                # Analyze with LLM
                analysis_prompt = f"""
                Analyze the performance of this Spark application:
                
                Basic Info: {state['analysis_results']['basic_info']}
                Executor Summary: {executor_summary}
                Stages: {stages}
                
                Identify key performance metrics and potential issues.
                """
                
                analysis = self.llm.invoke([HumanMessage(content=analysis_prompt)])
                state["analysis_results"]["performance_analysis"] = analysis.content
                
            except Exception as e:
                state["analysis_results"]["performance_analysis"] = f"Performance analysis failed: {e}"
        
        state["current_step"] = "performance_analyzed"
        state["messages"].append(AIMessage(content="Completed performance analysis"))
        return state
    
    async def identify_bottlenecks(self, state: SparkAnalysisState):
        """Identify performance bottlenecks."""
        async with ClientSession(self.mcp_url) as session:
            try:
                bottlenecks = await session.call_tool("get_job_bottlenecks", {"spark_id": state["app_id"]})
                slowest_stages = await session.call_tool("get_slowest_stages", {"spark_id": state["app_id"]})
                
                state["analysis_results"]["bottlenecks"] = {
                    "job_bottlenecks": bottlenecks,
                    "slowest_stages": slowest_stages
                }
                
                # Analyze bottlenecks with LLM
                bottleneck_prompt = f"""
                Identify and prioritize bottlenecks in this Spark application:
                
                Job Bottlenecks: {bottlenecks}
                Slowest Stages: {slowest_stages}
                
                Rank bottlenecks by impact and provide specific areas for optimization.
                """
                
                analysis = self.llm.invoke([HumanMessage(content=bottleneck_prompt)])
                state["analysis_results"]["bottleneck_analysis"] = analysis.content
                
            except Exception as e:
                state["analysis_results"]["bottleneck_analysis"] = f"Bottleneck analysis failed: {e}"
        
        state["current_step"] = "bottlenecks_identified"
        state["messages"].append(AIMessage(content="Identified performance bottlenecks"))
        return state
    
    async def compare_with_historical(self, state: SparkAnalysisState):
        """Compare with historical applications."""
        async with ClientSession(self.mcp_url) as session:
            try:
                # Get list of applications to find similar ones
                apps = await session.call_tool("list_applications")
                
                # Find similar applications (simplified logic)
                similar_apps = []
                current_app = state["analysis_results"]["basic_info"]
                current_name = current_app.get("name", "")
                
                for app in apps.get("applications", []):
                    if (app["id"] != state["app_id"] and 
                        app.get("name", "").startswith(current_name.split("-")[0])):
                        similar_apps.append(app["id"])
                        if len(similar_apps) >= 3:  # Limit comparisons
                            break
                
                # Compare with similar applications
                comparisons = []
                for similar_app_id in similar_apps:
                    try:
                        comparison = await session.call_tool("compare_job_performance", {
                            "spark_id1": state["app_id"],
                            "spark_id2": similar_app_id
                        })
                        comparisons.append({
                            "compared_with": similar_app_id,
                            "comparison": comparison
                        })
                    except:
                        continue
                
                state["analysis_results"]["historical_comparison"] = comparisons
                
            except Exception as e:
                state["analysis_results"]["historical_comparison"] = f"Historical comparison failed: {e}"
        
        state["current_step"] = "historical_compared"
        state["messages"].append(AIMessage(content="Completed historical comparison"))
        return state
    
    async def generate_recommendations(self, state: SparkAnalysisState):
        """Generate optimization recommendations."""
        # Combine all analysis results
        all_analysis = state["analysis_results"]
        
        recommendation_prompt = f"""
        Based on comprehensive analysis of Spark application {state['app_id']}, generate specific optimization recommendations:
        
        Basic Info: {all_analysis.get('basic_info', {})}
        Performance Analysis: {all_analysis.get('performance_analysis', '')}
        Bottleneck Analysis: {all_analysis.get('bottleneck_analysis', '')}
        Historical Comparisons: {all_analysis.get('historical_comparison', [])}
        
        Provide:
        1. Top 3 optimization opportunities
        2. Specific configuration changes
        3. Resource allocation recommendations
        4. Expected performance improvements
        5. Implementation priority
        """
        
        recommendations = self.llm.invoke([HumanMessage(content=recommendation_prompt)])
        state["recommendations"] = recommendations.content.split("\n")
        state["current_step"] = "recommendations_generated"
        
        state["messages"].append(AIMessage(content="Generated optimization recommendations"))
        return state
    
    async def format_report(self, state: SparkAnalysisState):
        """Format final analysis report."""
        report_prompt = f"""
        Create a comprehensive Spark application analysis report:
        
        Application ID: {state['app_id']}
        Analysis Results: {state['analysis_results']}
        Recommendations: {state['recommendations']}
        
        Format as a professional report with:
        - Executive Summary
        - Key Findings
        - Performance Metrics
        - Recommendations with Priority
        - Next Steps
        """
        
        report = self.llm.invoke([HumanMessage(content=report_prompt)])
        state["analysis_results"]["final_report"] = report.content
        state["current_step"] = "report_completed"
        
        state["messages"].append(AIMessage(content="Analysis report completed"))
        return state
    
    async def analyze_application(self, app_id: str):
        """Run complete analysis workflow."""
        initial_state = {
            "messages": [HumanMessage(content=f"Starting analysis of {app_id}")],
            "app_id": app_id,
            "analysis_results": {},
            "recommendations": [],
            "current_step": "starting"
        }
        
        # Execute the workflow
        result = await self.graph.ainvoke(initial_state)
        return result

# Usage
async def analyze_spark_app():
    workflow = SparkAnalysisWorkflow("http://localhost:18888")
    result = await workflow.analyze_application("spark-application-12345")
    
    print("Analysis Complete!")
    print("Final Report:")
    print(result["analysis_results"]["final_report"])

asyncio.run(analyze_spark_app())
```

### 2. Multi-Agent Spark Monitoring System

```python
from langgraph.graph import StateGraph, END
from typing import Dict, List
import asyncio

class MonitoringState:
    """State for monitoring workflow."""
    applications: List[Dict]
    alerts: List[Dict]
    analysis_results: Dict[str, Any]
    current_time: str
    monitoring_enabled: bool

class SparkMonitoringSystem:
    def __init__(self, mcp_server_url: str):
        self.mcp_url = mcp_server_url
        self.llm = ChatOpenAI(model="gpt-4", temperature=0)
        self.monitoring_graph = self._build_monitoring_graph()
    
    def _build_monitoring_graph(self):
        """Build monitoring workflow graph."""
        workflow = StateGraph(MonitoringState)
        
        # Monitoring agents
        workflow.add_node("discovery_agent", self.discovery_agent)
        workflow.add_node("health_agent", self.health_agent)
        workflow.add_node("performance_agent", self.performance_agent)
        workflow.add_node("alert_agent", self.alert_agent)
        workflow.add_node("report_agent", self.report_agent)
        
        # Workflow
        workflow.add_edge("discovery_agent", "health_agent")
        workflow.add_edge("health_agent", "performance_agent")
        workflow.add_edge("performance_agent", "alert_agent")
        workflow.add_edge("alert_agent", "report_agent")
        workflow.add_edge("report_agent", END)
        
        workflow.set_entry_point("discovery_agent")
        return workflow.compile()
    
    async def discovery_agent(self, state: MonitoringState):
        """Discover current Spark applications."""
        async with ClientSession(self.mcp_url) as session:
            apps_result = await session.call_tool("list_applications")
            state["applications"] = apps_result.get("applications", [])
            
        print(f"🔍 Discovered {len(state['applications'])} applications")
        return state
    
    async def health_agent(self, state: MonitoringState):
        """Check health status of applications."""
        health_results = {}
        
        async with ClientSession(self.mcp_url) as session:
            for app in state["applications"]:
                app_id = app["id"]
                try:
                    app_info = await session.call_tool("get_application_info", {"app_id": app_id})
                    
                    # Simple health check
                    attempts = app_info.get("attempts", [])
                    is_healthy = True
                    health_issues = []
                    
                    if attempts:
                        attempt = attempts[0]
                        if not attempt.get("completed", True):
                            is_healthy = False
                            health_issues.append("Application failed")
                        
                        duration = app_info.get("duration", 0)
                        if duration > 7200000:  # 2 hours
                            is_healthy = False
                            health_issues.append("Long running application")
                    
                    health_results[app_id] = {
                        "healthy": is_healthy,
                        "issues": health_issues,
                        "app_info": app_info
                    }
                    
                except Exception as e:
                    health_results[app_id] = {
                        "healthy": False,
                        "issues": [f"Health check failed: {e}"],
                        "app_info": {}
                    }
        
        state["analysis_results"]["health"] = health_results
        unhealthy_count = sum(1 for h in health_results.values() if not h["healthy"])
        print(f"🏥 Health check complete: {unhealthy_count} unhealthy applications")
        return state
    
    async def performance_agent(self, state: MonitoringState):
        """Analyze performance of applications."""
        performance_results = {}
        
        async with ClientSession(self.mcp_url) as session:
            for app in state["applications"]:
                app_id = app["id"]
                try:
                    # Get performance bottlenecks
                    bottlenecks = await session.call_tool("get_job_bottlenecks", {"spark_id": app_id})
                    
                    # Analyze with LLM
                    perf_prompt = f"""
                    Quickly assess the performance of Spark application {app_id}:
                    
                    Bottlenecks: {bottlenecks}
                    
                    Provide:
                    - Performance score (1-10)
                    - Key issue (if any)
                    - Severity (low/medium/high)
                    """
                    
                    analysis = self.llm.invoke([HumanMessage(content=perf_prompt)])
                    performance_results[app_id] = {
                        "bottlenecks": bottlenecks,
                        "analysis": analysis.content
                    }
                    
                except Exception as e:
                    performance_results[app_id] = {
                        "error": str(e),
                        "analysis": "Performance analysis failed"
                    }
        
        state["analysis_results"]["performance"] = performance_results
        print(f"⚡ Performance analysis complete for {len(performance_results)} applications")
        return state
    
    async def alert_agent(self, state: MonitoringState):
        """Generate alerts based on health and performance."""
        alerts = []
        
        health_results = state["analysis_results"].get("health", {})
        performance_results = state["analysis_results"].get("performance", {})
        
        for app_id in state["applications"]:
            app_id_str = app_id["id"]
            
            # Health-based alerts
            health = health_results.get(app_id_str, {})
            if not health.get("healthy", True):
                alerts.append({
                    "type": "health",
                    "app_id": app_id_str,
                    "severity": "high",
                    "message": f"Health issues: {', '.join(health.get('issues', []))}",
                    "timestamp": state.get("current_time", "")
                })
            
            # Performance-based alerts
            perf = performance_results.get(app_id_str, {})
            if "high" in perf.get("analysis", "").lower():
                alerts.append({
                    "type": "performance",
                    "app_id": app_id_str,
                    "severity": "medium",
                    "message": "Performance degradation detected",
                    "timestamp": state.get("current_time", "")
                })
        
        state["alerts"] = alerts
        print(f"🚨 Generated {len(alerts)} alerts")
        return state
    
    async def report_agent(self, state: MonitoringState):
        """Generate monitoring report."""
        total_apps = len(state["applications"])
        total_alerts = len(state["alerts"])
        
        # Generate summary report
        report_prompt = f"""
        Create a monitoring summary report:
        
        Total Applications: {total_apps}
        Total Alerts: {total_alerts}
        Health Results: {state['analysis_results'].get('health', {})}
        Performance Results: {state['analysis_results'].get('performance', {})}
        Alerts: {state['alerts']}
        
        Provide:
        - Executive summary
        - Key issues requiring attention
        - Overall system health score
        - Recommended actions
        """
        
        report = self.llm.invoke([HumanMessage(content=report_prompt)])
        state["analysis_results"]["monitoring_report"] = report.content
        
        print("📊 Monitoring report generated")
        return state
    
    async def run_monitoring_cycle(self):
        """Run one complete monitoring cycle."""
        from datetime import datetime
        
        initial_state = {
            "applications": [],
            "alerts": [],
            "analysis_results": {},
            "current_time": datetime.now().isoformat(),
            "monitoring_enabled": True
        }
        
        result = await self.monitoring_graph.ainvoke(initial_state)
        return result
    
    async def continuous_monitoring(self, interval_minutes: int = 5):
        """Run continuous monitoring."""
        while True:
            try:
                print(f"\n{'='*50}")
                print(f"🔄 Starting monitoring cycle at {datetime.now()}")
                
                result = await self.run_monitoring_cycle()
                
                # Print summary
                print(f"📋 Monitoring Summary:")
                print(f"   Applications: {len(result['applications'])}")
                print(f"   Alerts: {len(result['alerts'])}")
                
                if result["alerts"]:
                    print("🚨 Active Alerts:")
                    for alert in result["alerts"]:
                        print(f"   - {alert['app_id']}: {alert['message']}")
                
                print(f"\n📊 Full Report:")
                print(result["analysis_results"]["monitoring_report"])
                
                # Wait for next cycle
                await asyncio.sleep(interval_minutes * 60)
                
            except Exception as e:
                print(f"❌ Monitoring error: {e}")
                await asyncio.sleep(60)  # Wait before retry

# Usage
async def start_monitoring():
    monitor = SparkMonitoringSystem("http://localhost:18888")
    await monitor.continuous_monitoring(interval_minutes=5)

# Run monitoring
asyncio.run(start_monitoring())
```

### 3. Spark Optimization Workflow

```python
class OptimizationState:
    """State for optimization workflow."""
    target_app_id: str
    baseline_metrics: Dict
    optimization_opportunities: List[Dict]
    proposed_changes: List[Dict]
    expected_improvements: Dict
    implementation_plan: List[str]

class SparkOptimizationWorkflow:
    def __init__(self, mcp_server_url: str):
        self.mcp_url = mcp_server_url
        self.llm = ChatOpenAI(model="gpt-4", temperature=0)
        self.graph = self._build_optimization_graph()
    
    def _build_optimization_graph(self):
        """Build optimization workflow."""
        workflow = StateGraph(OptimizationState)
        
        workflow.add_node("baseline_analysis", self.baseline_analysis)
        workflow.add_node("identify_opportunities", self.identify_opportunities)
        workflow.add_node("propose_changes", self.propose_changes)
        workflow.add_node("estimate_impact", self.estimate_impact)
        workflow.add_node("create_implementation_plan", self.create_implementation_plan)
        
        workflow.add_edge("baseline_analysis", "identify_opportunities")
        workflow.add_edge("identify_opportunities", "propose_changes")
        workflow.add_edge("propose_changes", "estimate_impact")
        workflow.add_edge("estimate_impact", "create_implementation_plan")
        workflow.add_edge("create_implementation_plan", END)
        
        workflow.set_entry_point("baseline_analysis")
        return workflow.compile()
    
    async def baseline_analysis(self, state: OptimizationState):
        """Establish baseline metrics."""
        async with ClientSession(self.mcp_url) as session:
            app_info = await session.call_tool("get_application_info", {"app_id": state["target_app_id"]})
            bottlenecks = await session.call_tool("get_job_bottlenecks", {"spark_id": state["target_app_id"]})
            executor_summary = await session.call_tool("get_executor_summary", {"spark_id": state["target_app_id"]})
            
            state["baseline_metrics"] = {
                "app_info": app_info,
                "bottlenecks": bottlenecks,
                "executor_summary": executor_summary,
                "duration": app_info.get("duration", 0),
                "resource_usage": executor_summary
            }
        
        print(f"📊 Baseline analysis complete for {state['target_app_id']}")
        return state
    
    async def identify_opportunities(self, state: OptimizationState):
        """Identify optimization opportunities."""
        opportunities_prompt = f"""
        Identify optimization opportunities for this Spark application:
        
        Baseline Metrics: {state['baseline_metrics']}
        
        Focus on:
        1. Configuration optimizations
        2. Resource allocation improvements
        3. Algorithm/approach changes
        4. Infrastructure optimizations
        
        Rank opportunities by potential impact.
        """
        
        analysis = self.llm.invoke([HumanMessage(content=opportunities_prompt)])
        
        # Parse opportunities (simplified)
        opportunities = [
            {"type": "configuration", "description": "Optimize executor memory", "impact": "high"},
            {"type": "resource", "description": "Adjust parallelism", "impact": "medium"},
            {"type": "algorithm", "description": "Enable adaptive query execution", "impact": "high"}
        ]
        
        state["optimization_opportunities"] = opportunities
        print(f"🎯 Identified {len(opportunities)} optimization opportunities")
        return state
    
    async def propose_changes(self, state: OptimizationState):
        """Propose specific configuration changes."""
        changes_prompt = f"""
        Based on these optimization opportunities, propose specific configuration changes:
        
        Opportunities: {state['optimization_opportunities']}
        Current Configuration: {state['baseline_metrics']['app_info']}
        
        Provide specific Spark configuration parameters and values.
        """
        
        analysis = self.llm.invoke([HumanMessage(content=changes_prompt)])
        
        # Example proposed changes
        proposed_changes = [
            {
                "parameter": "spark.executor.memory",
                "current_value": "2g",
                "proposed_value": "4g",
                "rationale": "Reduce memory spilling"
            },
            {
                "parameter": "spark.sql.adaptive.enabled",
                "current_value": "false",
                "proposed_value": "true",
                "rationale": "Enable adaptive query execution"
            }
        ]
        
        state["proposed_changes"] = proposed_changes
        print(f"⚙️ Proposed {len(proposed_changes)} configuration changes")
        return state
    
    async def estimate_impact(self, state: OptimizationState):
        """Estimate impact of proposed changes."""
        impact_prompt = f"""
        Estimate the performance impact of these proposed changes:
        
        Baseline Duration: {state['baseline_metrics']['duration']} ms
        Proposed Changes: {state['proposed_changes']}
        Current Bottlenecks: {state['baseline_metrics']['bottlenecks']}
        
        Estimate:
        1. Expected duration reduction (%)
        2. Resource efficiency improvements
        3. Risk factors
        4. Implementation complexity
        """
        
        analysis = self.llm.invoke([HumanMessage(content=impact_prompt)])
        
        state["expected_improvements"] = {
            "duration_reduction_percent": 25,
            "resource_efficiency_improvement": "15% better CPU utilization",
            "risk_level": "low",
            "implementation_complexity": "medium"
        }
        
        print("📈 Impact estimation complete")
        return state
    
    async def create_implementation_plan(self, state: OptimizationState):
        """Create step-by-step implementation plan."""
        plan_prompt = f"""
        Create a detailed implementation plan for these optimizations:
        
        Proposed Changes: {state['proposed_changes']}
        Expected Impact: {state['expected_improvements']}
        
        Include:
        1. Step-by-step implementation
        2. Testing strategy
        3. Rollback plan
        4. Monitoring approach
        """
        
        analysis = self.llm.invoke([HumanMessage(content=plan_prompt)])
        
        implementation_plan = [
            "1. Create test environment with proposed configurations",
            "2. Run validation tests with sample data",
            "3. Monitor performance metrics during test runs",
            "4. Compare results with baseline",
            "5. Implement in production with gradual rollout",
            "6. Monitor production performance",
            "7. Document learnings and update optimization playbook"
        ]
        
        state["implementation_plan"] = implementation_plan
        print("📋 Implementation plan created")
        return state
    
    async def optimize_application(self, app_id: str):
        """Run complete optimization workflow."""
        initial_state = {
            "target_app_id": app_id,
            "baseline_metrics": {},
            "optimization_opportunities": [],
            "proposed_changes": [],
            "expected_improvements": {},
            "implementation_plan": []
        }
        
        result = await self.graph.ainvoke(initial_state)
        return result

# Usage
async def optimize_spark_app():
    optimizer = SparkOptimizationWorkflow("http://localhost:18888")
    result = await optimizer.optimize_application("spark-application-12345")
    
    print("\n🎯 Optimization Plan Complete!")
    print(f"Proposed Changes: {len(result['proposed_changes'])}")
    print(f"Expected Improvements: {result['expected_improvements']}")
    print("\nImplementation Plan:")
    for step in result['implementation_plan']:
        print(f"  {step}")

asyncio.run(optimize_spark_app())
```

## Configuration and Best Practices

### Graph Visualization

```python
from langgraph.graph import StateGraph
import matplotlib.pyplot as plt

# Visualize workflow graphs
def visualize_workflow(graph):
    """Create visual representation of the workflow."""
    # Implementation would create workflow diagrams
    pass
```

### Error Handling and Resilience

```python
class RobustSparkWorkflow:
    def __init__(self, mcp_server_url: str):
        self.mcp_url = mcp_server_url
        self.max_retries = 3
        self.retry_delay = 5
    
    async def robust_mcp_call(self, tool_name: str, parameters: dict):
        """MCP call with retry logic and error handling."""
        for attempt in range(self.max_retries):
            try:
                async with ClientSession(self.mcp_url) as session:
                    return await session.call_tool(tool_name, parameters)
            except Exception as e:
                if attempt == self.max_retries - 1:
                    raise
                await asyncio.sleep(self.retry_delay)
        
        return None
```

## Advanced Examples

The `/examples/langgraph/` directory contains more sophisticated examples:
- Multi-tenant Spark monitoring across different environments
- Automated performance regression detection pipelines
- Intelligent resource scaling recommendations
- Integration with CI/CD for Spark application optimization