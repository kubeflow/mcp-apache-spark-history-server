#!/usr/bin/env python3
# ruff: noqa: T201,F841
"""
LangGraph integration with Spark History Server MCP.
Uses modern LangGraph workflow patterns and local Ollama models.
"""

import asyncio
import re
from typing import List, TypedDict

import requests
from langchain_ollama import ChatOllama
from langgraph.graph import END, StateGraph


def clean_empty_thinking_blocks(text: str) -> str:
    """Remove empty thinking blocks that 0.6b model may still output despite /no_think."""
    # Remove empty or whitespace-only <think>...</think> blocks
    cleaned = re.sub(r"<think>\s*</think>", "", text, flags=re.DOTALL)
    # Clean up any extra whitespace
    cleaned = re.sub(r"\n\s*\n\s*\n", "\n\n", cleaned)
    return cleaned.strip()


# Sample application IDs for testing
SAMPLE_APPS = [
    "spark-cc4d115f011443d787f03a71a476a745",  # NewYorkTaxiData
    "spark-bcec39f6201b42b9925124595baad260",  # PythonPi
    "spark-110be3a8424d4a2789cb88134418217b",  # NewYorkTaxiData_2
]


class AnalysisState(TypedDict):
    """State for Spark analysis workflow."""

    app_id: str
    basic_info: dict
    jobs_info: dict
    performance_analysis: str
    recommendations: List[str]
    current_step: str


async def get_mcp_tools():
    """Attempt to get tools from MCP server."""
    try:
        response = requests.get("http://localhost:18888/", timeout=5)
        print("🔗 MCP server is responding...")
        # For this example, we'll use direct API calls
        # MCP integration would go here
        return []
    except Exception as e:
        print(f"⚠️  MCP server not available: {e}")
        return []


def get_application_info(spark_id: str) -> dict:
    """Get basic information about a Spark application."""
    try:
        response = requests.get(
            f"http://localhost:18080/api/v1/applications/{spark_id}", timeout=10
        )
        if response.status_code == 200:
            data = response.json()
            duration_sec = data["attempts"][0]["duration"] / 1000
            status = (
                "Completed" if data["attempts"][0]["completed"] else "Running/Failed"
            )
            return {
                "name": data["name"],
                "status": status,
                "duration": duration_sec,
                "success": True,
            }
        return {"error": f"HTTP {response.status_code}", "success": False}
    except Exception as e:
        return {"error": str(e), "success": False}


def get_jobs_info(spark_id: str) -> dict:
    """Get job execution status for a Spark application."""
    try:
        response = requests.get(
            f"http://localhost:18080/api/v1/applications/{spark_id}/jobs", timeout=10
        )
        if response.status_code == 200:
            jobs = response.json()
            failed_jobs = [j for j in jobs if j["status"] == "FAILED"]
            succeeded_jobs = [j for j in jobs if j["status"] == "SUCCEEDED"]
            return {
                "total_jobs": len(jobs),
                "succeeded_jobs": len(succeeded_jobs),
                "failed_jobs": len(failed_jobs),
                "success": True,
            }
        return {"error": f"HTTP {response.status_code}", "success": False}
    except Exception as e:
        return {"error": str(e), "success": False}


class SparkAnalysisWorkflow:
    """LangGraph workflow for Spark performance analysis."""

    def __init__(self, model_name="qwen3:0.6b"):
        self.llm = ChatOllama(model=model_name, base_url="http://localhost:11434")
        self.graph = self._build_graph()

    def _build_graph(self):
        """Build the analysis workflow graph using modern LangGraph patterns."""
        workflow = StateGraph(AnalysisState)

        # Add workflow nodes
        workflow.add_node("collect_basic_info", self.collect_basic_info)
        workflow.add_node("collect_jobs_info", self.collect_jobs_info)
        workflow.add_node("analyze_performance", self.analyze_performance)
        workflow.add_node("generate_recommendations", self.generate_recommendations)

        # Define workflow edges
        workflow.add_edge("collect_basic_info", "collect_jobs_info")
        workflow.add_edge("collect_jobs_info", "analyze_performance")
        workflow.add_edge("analyze_performance", "generate_recommendations")
        workflow.add_edge("generate_recommendations", END)

        # Set entry point
        workflow.set_entry_point("collect_basic_info")

        return workflow.compile()

    def collect_basic_info(self, state: AnalysisState) -> AnalysisState:
        """Step 1: Collect basic application information."""
        print(f"📊 Step 1: Collecting basic info for {state['app_id']}...")

        basic_info = get_application_info(state["app_id"])
        state["basic_info"] = basic_info
        state["current_step"] = "basic_info_collected"

        if basic_info.get("success"):
            print(f"✅ App: {basic_info.get('name', 'Unknown')}")
        else:
            print(f"❌ Error: {basic_info.get('error', 'Unknown error')}")

        return state

    def collect_jobs_info(self, state: AnalysisState) -> AnalysisState:
        """Step 2: Collect job execution information."""
        print(f"📊 Step 2: Collecting jobs info for {state['app_id']}...")

        jobs_info = get_jobs_info(state["app_id"])
        state["jobs_info"] = jobs_info
        state["current_step"] = "jobs_info_collected"

        if jobs_info.get("success"):
            total = jobs_info.get("total_jobs", 0)
            failed = jobs_info.get("failed_jobs", 0)
            print(f"✅ Jobs: {total} total, {failed} failed")
        else:
            print(f"❌ Error: {jobs_info.get('error', 'Unknown error')}")

        return state

    def analyze_performance(self, state: AnalysisState) -> AnalysisState:
        """Step 3: Analyze application performance using LLM."""
        print(f"📊 Step 3: Analyzing performance for {state['app_id']}...")

        basic_info = state["basic_info"]
        jobs_info = state["jobs_info"]

        if not basic_info.get("success") or not jobs_info.get("success"):
            state["performance_analysis"] = (
                "Unable to analyze due to data collection errors"
            )
            state["current_step"] = "analysis_failed"
            print("❌ Analysis failed due to data collection errors")
            return state

        # Create analysis prompt with /no_think to avoid thinking blocks
        analysis_prompt = f"""
        Analyze this Spark application performance in 2-3 sentences:

        Application: {basic_info.get("name", "Unknown")}
        Status: {basic_info.get("status", "Unknown")}
        Duration: {basic_info.get("duration", 0):.1f} seconds
        Total jobs: {jobs_info.get("total_jobs", 0)}
        Failed jobs: {jobs_info.get("failed_jobs", 0)}

        Identify main performance issues and provide a brief assessment. /no_think
        """

        try:
            response = self.llm.invoke(analysis_prompt)
            analysis = (
                response.content if hasattr(response, "content") else str(response)
            )
            # Clean up any empty thinking blocks (0.6b model limitation)
            analysis = clean_empty_thinking_blocks(analysis)
            state["performance_analysis"] = analysis
            state["current_step"] = "performance_analyzed"
            print("✅ Performance analysis completed")
        except Exception as e:
            state["performance_analysis"] = f"Analysis failed: {e}"
            state["current_step"] = "analysis_failed"
            print(f"❌ Analysis failed: {e}")

        return state

    def generate_recommendations(self, state: AnalysisState) -> AnalysisState:
        """Step 4: Generate optimization recommendations."""
        print(f"📊 Step 4: Generating recommendations for {state['app_id']}...")

        basic_info = state["basic_info"]
        jobs_info = state["jobs_info"]
        performance_analysis = state["performance_analysis"]

        if "failed" in state["current_step"]:
            state["recommendations"] = [
                "Unable to generate recommendations due to analysis failure"
            ]
            state["current_step"] = "recommendations_failed"
            print("❌ Recommendations failed")
            return state

        # Create recommendations prompt with /no_think to avoid thinking blocks
        recommendations_prompt = f"""
        Based on this Spark analysis, provide 2-3 specific optimization recommendations:

        Application: {basic_info.get("name", "Unknown")}
        Duration: {basic_info.get("duration", 0):.1f} seconds
        Failed Jobs: {jobs_info.get("failed_jobs", 0)}

        Analysis: {performance_analysis}

        Provide practical recommendations like configuration changes or optimizations.
        Format as numbered list. /no_think
        """

        try:
            response = self.llm.invoke(recommendations_prompt)
            recommendations_text = (
                response.content if hasattr(response, "content") else str(response)
            )
            # Clean up any empty thinking blocks (0.6b model limitation)
            recommendations_text = clean_empty_thinking_blocks(recommendations_text)

            # Parse recommendations (simplified)
            lines = recommendations_text.split("\n")
            recommendations = [
                line.strip()
                for line in lines
                if line.strip()
                and any(line.strip().startswith(f"{i}.") for i in range(1, 10))
            ]

            if not recommendations:
                recommendations = [recommendations_text.strip()]

            state["recommendations"] = recommendations
            state["current_step"] = "analysis_complete"
            print(f"✅ Generated {len(recommendations)} recommendations")
        except Exception as e:
            state["recommendations"] = [f"Recommendation generation failed: {e}"]
            state["current_step"] = "recommendations_failed"
            print(f"❌ Recommendations failed: {e}")

        return state

    def analyze(self, app_id: str, return_data: bool = False) -> dict:
        """Run complete analysis workflow for an application.

        Args:
            app_id: Spark application ID to analyze
            return_data: If True, return the full result dict. If False (default),
                        return None to avoid JSON output in interactive mode.
        """
        print(f"\n🎯 Analyzing Spark application: {app_id}")
        print("=" * 60)

        initial_state = {
            "app_id": app_id,
            "basic_info": {},
            "jobs_info": {},
            "performance_analysis": "",
            "recommendations": [],
            "current_step": "starting",
        }

        try:
            # Execute the workflow
            result = self.graph.invoke(initial_state)

            # Display results
            print("\n📋 Analysis Results:")
            print("=" * 40)

            basic_info = result["basic_info"]
            jobs_info = result["jobs_info"]

            if basic_info.get("success"):
                print(f"Application: {basic_info.get('name', 'Unknown')}")
                print(f"Status: {basic_info.get('status', 'Unknown')}")
                print(f"Duration: {basic_info.get('duration', 0):.1f} seconds")

            if jobs_info.get("success"):
                total = jobs_info.get("total_jobs", 0)
                failed = jobs_info.get("failed_jobs", 0)
                print(f"Jobs: {total} total, {failed} failed")

            print("\nPerformance Analysis:")
            print(result["performance_analysis"])

            print("\nRecommendations:")
            for i, rec in enumerate(result["recommendations"], 1):
                print(f"  {i}. {rec}")

            print("\n✅ Analysis completed!\n")
            # Return data only if requested (for programmatic use)
            return result if return_data else None

        except Exception as e:
            error_msg = f"Workflow execution failed: {e}"
            print(f"❌ {error_msg}")
            return {"error": error_msg}


async def setup():
    """Setup the interactive environment."""
    print("🚀 LangGraph + Spark History Server MCP")

    # Test MCP Server
    await get_mcp_tools()

    # Test Spark History Server
    try:
        response = requests.get("http://localhost:18080/api/v1/applications", timeout=5)
        if response.status_code == 200:
            apps = response.json()
            print(f"✅ Found {len(apps)} applications")
        else:
            print(f"❌ Spark History Server error: HTTP {response.status_code}")
            return None
    except Exception as e:
        print(f"❌ Spark History Server not available: {e}")
        print("💡 Start with: task start-spark-bg")
        return None

    # Test Ollama
    try:
        response = requests.get("http://localhost:11434/api/tags", timeout=5)
        models = response.json().get("models", [])
        available_models = [m["name"] for m in models]
        if "qwen3:0.6b" not in available_models:
            print(
                f"❌ Ollama model qwen3:0.6b not found. Available: {available_models}"
            )
            return None
        print("✅ Ollama available")
    except Exception as e:
        print(f"❌ Ollama not available: {e}")
        return None

    # Create workflow
    try:
        workflow = SparkAnalysisWorkflow()
        print("✅ LangGraph workflow ready")
        return workflow
    except Exception as e:
        print(f"❌ Workflow creation failed: {e}")
        return None


# Setup when module is imported
workflow = asyncio.run(setup())
sample_apps = SAMPLE_APPS

if __name__ == "__main__":
    if workflow:
        print("\n🎯 Interactive session ready!")
        print("Examples:")
        print("  >>> workflow.analyze('spark-cc4d115f011443d787f03a71a476a745')")
        print("  >>> workflow.analyze('spark-110be3a8424d4a2789cb88134418217b')")
        print(f"\nAvailable sample app IDs: {sample_apps}")
        print(
            "\nRun with: python -i examples/integrations/langgraph/langgraph_example.py"
        )
    else:
        print("❌ Setup failed. Check services and try again.")
