#!/usr/bin/env python3
"""
LlamaIndex integration with Spark History Server MCP.
Uses proper MCP tools and current LlamaIndex workflow API.
"""

import asyncio
import os
import subprocess
import tempfile

import requests
from llama_index.core.agent.workflow import FunctionAgent
from llama_index.core.tools import FunctionTool
from llama_index.llms.ollama import Ollama

# Sample application IDs for testing
SAMPLE_APPS = [
    "spark-cc4d115f011443d787f03a71a476a745",  # NewYorkTaxiData
    "spark-bcec39f6201b42b9925124595baad260",  # PythonPi
    "spark-110be3a8424d4a2789cb88134418217b",  # NewYorkTaxiData_2
]


async def get_mcp_tools():
    """Get tools from the MCP server using the mcp package."""
    try:
        from llama_index.tools.mcp import aget_tools_from_mcp_url

        # Test basic connectivity first
        response = requests.get("http://localhost:18888/", timeout=5)
        print("🔗 MCP server is responding...")

        tools = await aget_tools_from_mcp_url("http://localhost:18888")
        print(f"✅ Retrieved {len(tools)} MCP tools from server:")
        for tool in tools:
            print(f"  - {tool.metadata.name}: {tool.metadata.description}")
        return tools
    except ImportError:
        print("⚠️  MCP tools package not available, using direct API fallback")
        return []
    except Exception as e:
        print(f"⚠️  MCP server connection failed: {type(e).__name__}: {str(e)[:100]}...")
        print("📝 Using direct API fallback instead")
        return []


def get_application_info_fallback(spark_id: str) -> str:
    """Fallback: Direct API call when MCP is unavailable."""
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
            return f"Application: {data['name']}, Status: {status}, Duration: {duration_sec:.1f}s"
        return f"Error: HTTP {response.status_code}"
    except Exception as e:
        return f"Error: {e}"


def get_jobs_info_fallback(spark_id: str) -> str:
    """Fallback: Direct API call when MCP is unavailable."""
    try:
        response = requests.get(
            f"http://localhost:18080/api/v1/applications/{spark_id}/jobs", timeout=10
        )
        if response.status_code == 200:
            jobs = response.json()
            failed_jobs = [j for j in jobs if j["status"] == "FAILED"]
            succeeded_jobs = [j for j in jobs if j["status"] == "SUCCEEDED"]
            return f"Jobs: {len(jobs)} total, {len(succeeded_jobs)} succeeded, {len(failed_jobs)} failed"
        return f"Error: HTTP {response.status_code}"
    except Exception as e:
        return f"Error: {e}"


def analyze_performance_fallback(spark_id: str) -> str:
    """Fallback: Direct API call when MCP is unavailable."""
    try:
        # Get application info
        app_response = requests.get(
            f"http://localhost:18080/api/v1/applications/{spark_id}", timeout=10
        )
        if app_response.status_code != 200:
            return f"Error: Could not fetch application {spark_id}"

        app_data = app_response.json()
        duration = app_data["attempts"][0]["duration"]

        # Get jobs info
        jobs_response = requests.get(
            f"http://localhost:18080/api/v1/applications/{spark_id}/jobs", timeout=10
        )

        analysis = [
            f"Performance Analysis for {spark_id}:",
            f"- Application: {app_data['name']}",
            f"- Duration: {duration / 1000:.1f} seconds",
        ]

        if jobs_response.status_code == 200:
            jobs = jobs_response.json()
            failed_jobs = [j for j in jobs if j["status"] == "FAILED"]

            analysis.extend(
                [f"- Total Jobs: {len(jobs)}", f"- Failed Jobs: {len(failed_jobs)}"]
            )

            if failed_jobs:
                analysis.append(
                    f"- Failed Job Names: {[j['name'][:50] for j in failed_jobs]}"
                )

            # Performance recommendations
            if duration > 300000:  # > 5 minutes
                analysis.append("- Issue: Long running application (>5 min)")
                analysis.append("- Recommendation: Check for stage bottlenecks")

            if len(failed_jobs) > 0:
                analysis.append("- Issue: Job failures detected")
                analysis.append("- Recommendation: Investigate failure causes")

        return "\n".join(analysis)

    except Exception as e:
        return f"Error analyzing performance: {e}"


async def create_agent():
    """Create LlamaIndex FunctionAgent with MCP tools or fallback."""
    # Try to get MCP tools first
    mcp_tools = await get_mcp_tools()

    tools = []
    data_source = "❌ Fallback (Direct API)"

    if mcp_tools:
        # Use MCP tools (preferred)
        tools = mcp_tools
        data_source = "✅ MCP Server"
        print("🎯 Using MCP tools from server")
    else:
        # Fallback to direct API tools
        tools = [
            FunctionTool.from_defaults(
                fn=get_application_info_fallback,
                name="get_application_info",
                description="Get basic information about a Spark application including name, status, and duration",
            ),
            FunctionTool.from_defaults(
                fn=get_jobs_info_fallback,
                name="get_jobs_info",
                description="Get job execution status showing total, succeeded, and failed job counts",
            ),
            FunctionTool.from_defaults(
                fn=analyze_performance_fallback,
                name="analyze_performance",
                description="Perform comprehensive performance analysis with optimization recommendations",
            ),
        ]
        print("🎯 Using fallback API tools")

    # Create LLM
    llm = Ollama(model="qwen3:0.6b", base_url="http://localhost:11434")

    # Create modern LlamaIndex FunctionAgent
    agent = FunctionAgent(
        tools=tools,
        llm=llm,
        system_prompt=f"""You are a Spark performance expert assistant.

Data Source: {data_source}
Available Spark application IDs: {SAMPLE_APPS}

When asked about applications without specific IDs, use: {SAMPLE_APPS[0]}

Use the available tools to provide accurate, data-driven responses about Spark applications.
Always explain which application you're analyzing.""",
    )

    return agent, data_source


class ReliableAgent:
    """Reliable wrapper for LlamaIndex FunctionAgent."""

    def __init__(self, agent, data_source):
        self.agent = agent
        self.data_source = data_source

    def chat(self, message: str) -> str:
        """Chat with the agent reliably by creating fresh context each time."""
        try:
            # Create a temporary script that runs the agent
            script_content = f'''
import asyncio
import sys
import os
sys.path.append("{os.getcwd()}")

from examples.integrations.llamaindex.llamaindex_example import create_agent

async def run_agent():
    try:
        agent, _ = await create_agent()
        result = await agent.run("""{message}""")
        print("RESULT_START")
        print(str(result))
        print("RESULT_END")
    except Exception as e:
        print("ERROR_START")
        print(str(e))
        print("ERROR_END")

if __name__ == "__main__":
    asyncio.run(run_agent())
'''

            with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
                f.write(script_content)
                temp_script = f.name

            try:
                # Run the script in a subprocess
                result = subprocess.run(
                    ["python", temp_script],
                    capture_output=True,
                    text=True,
                    timeout=30,
                    cwd=os.getcwd(),
                )

                output = result.stdout
                if "RESULT_START" in output and "RESULT_END" in output:
                    # Extract the result
                    start = output.find("RESULT_START") + len("RESULT_START")
                    end = output.find("RESULT_END")
                    response = output[start:end].strip()
                elif "ERROR_START" in output and "ERROR_END" in output:
                    # Extract the error
                    start = output.find("ERROR_START") + len("ERROR_START")
                    end = output.find("ERROR_END")
                    error_msg = output[start:end].strip()
                    raise Exception(error_msg)
                else:
                    raise Exception(f"Unexpected output: {output}")

            finally:
                # Clean up temp file
                try:
                    os.unlink(temp_script)
                except:
                    pass

            print(f"📊 Data source: {self.data_source}")
            print(response)
            return ""

        except Exception as e:
            error_msg = f"Error: {e}"
            print(error_msg)
            return ""


async def setup():
    """Setup the interactive environment with MCP integration."""
    print("🚀 LlamaIndex + Spark History Server MCP")

    # Test MCP Server
    try:
        response = requests.get("http://localhost:18888/", timeout=5)
        print("✅ MCP server is running")
    except Exception as e:
        print(f"⚠️  MCP server not available: {e}")
        print("💡 Start with: task start-mcp-bg")

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

    # Create agent with MCP or fallback
    try:
        workflow_agent, data_source = await create_agent()
        agent = ReliableAgent(workflow_agent, data_source)
        print("✅ LlamaIndex FunctionAgent ready")
        return agent
    except Exception as e:
        print(f"❌ Agent creation failed: {e}")
        return None


# Setup when module is imported
agent = asyncio.run(setup())
sample_apps = SAMPLE_APPS

if __name__ == "__main__":
    if agent:
        print("\n🎯 Interactive session ready!")
        print("Examples:")
        print(
            "  >>> agent.chat('Analyze performance for application spark-cc4d115f011443d787f03a71a476a745')"
        )
        print(
            "  >>> agent.chat('Get jobs info spark-110be3a8424d4a2789cb88134418217b')"
        )
        print(
            "  >>> agent.chat('Get application info spark-bcec39f6201b42b9925124595baad260')"
        )
        print(f"\nAvailable sample app IDs: {sample_apps}")
        print(
            "\nRun with: python -i examples/integrations/llamaindex/llamaindex_example.py"
        )
    else:
        print("❌ Setup failed. Check services and try again.")
