#!/usr/bin/env python3
"""
LangGraph + MCP + Ollama integration for Spark performance analysis.
Optimized interactive agent with enhanced terminal formatting.
"""

import asyncio
import platform
import re
import sys
from typing import Annotated, Any, Dict, List, Optional, Sequence, TypedDict


def console_print(*args, **kwargs) -> None:
    """Console output function to replace print for CLI tool."""
    print(*args, **kwargs)  # noqa: T201


if sys.version_info < (3, 9):
    console_print(
        "❌ Python 3.9+ required. Current version:", platform.python_version()
    )
    sys.exit(1)

try:
    import httpx
    from langchain_core.messages import BaseMessage, HumanMessage, SystemMessage
    from langchain_mcp_adapters.client import MultiServerMCPClient
    from langchain_ollama import ChatOllama
    from langgraph.graph import END, StateGraph
    from langgraph.graph.message import add_messages
    from langgraph.prebuilt import ToolNode
except ImportError as e:
    console_print(f"❌ Missing dependency: {e}")
    console_print("💡 Install with: uv pip install -r requirements.txt")
    sys.exit(1)


class Config:
    """Configuration constants."""

    # Service URLs
    MCP_SERVER_URL = "http://127.0.0.1:18888/"
    SPARK_HISTORY_URL = "http://localhost:18080/api/v1/applications"
    OLLAMA_URL = "http://localhost:11434"

    # Model settings
    DEFAULT_MODEL = "qwen3:1.7b"
    MODEL_OPTIONS = {
        "qwen3:0.6b": {"size": "522MB", "quality": "Basic", "speed": "Fast"},
        "qwen3:1.7b": {"size": "1.4GB", "quality": "Better", "speed": "Moderate"},
    }

    # Agent settings
    TIMEOUT = 30.0
    MAX_TOKENS = 2048
    TEMPERATURE = 0.1

    # Sample application IDs
    SAMPLE_APPS = [
        "spark-cc4d115f011443d787f03a71a476a745",
        "spark-bcec39f6201b42b9925124595baad260",
        "spark-110be3a8424d4a2789cb88134418217b",
    ]


class TerminalFormatter:
    """Handles terminal formatting with compiled regex patterns for performance."""

    def __init__(self):
        self._patterns = self._compile_patterns()

    def _compile_patterns(self) -> Dict[str, re.Pattern]:
        """Pre-compile regex patterns for better performance."""
        return {
            "thinking_blocks": re.compile(r"<think>.*?</think>", re.DOTALL),
            "header_newlines": re.compile(r"(?<!\n)(### \d+\..*)", re.MULTILINE),
            "extra_spacing": re.compile(r"\n{3,}"),
            "section_headers": re.compile(r"^### (\d+\..*)$", re.MULTILINE),
            "top_sections": re.compile(r"^Top \d+\s+(.*?):$", re.MULTILINE),
            "key_values": re.compile(r"^([A-Za-z][\w\s\-\/]*?):", re.MULTILINE),
            "durations": re.compile(r"(\d+\s*(seconds?|minutes?|hours?|ms))"),
            "percentages": re.compile(r"(\d+\s*(%|MB|GB|TB))"),
            "app_ids": re.compile(r"(spark-[a-f0-9]+)"),
            "job_stage_ids": re.compile(r"(Job\s+\d+|Stage\s+\w+)"),
            "bullets": re.compile(r"^\s*[-•]\s*", re.MULTILINE),
        }

    def format_for_terminal(self, text: str) -> str:
        """Apply terminal formatting with optimized regex patterns."""
        # Remove thinking blocks
        text = self._patterns["thinking_blocks"].sub("", text)

        # Fix header positioning and spacing
        text = self._patterns["header_newlines"].sub(r"\n\1", text)
        text = self._patterns["extra_spacing"].sub("\n\n", text)
        text = text.strip()

        # Apply color formatting
        text = self._patterns["section_headers"].sub(r"\n\n\033[94m▶ \1\033[0m", text)
        text = self._patterns["top_sections"].sub(r"\n\033[93m📌 \1\033[0m", text)
        text = self._patterns["key_values"].sub(r"\033[1m\1:\033[0m", text)
        text = self._patterns["durations"].sub(r"\033[93m\1\033[0m", text)
        text = self._patterns["percentages"].sub(r"\033[93m\1\033[0m", text)
        text = self._patterns["app_ids"].sub(r"\033[96m\1\033[0m", text)
        text = self._patterns["job_stage_ids"].sub(r"\033[92m\1\033[0m", text)
        text = self._patterns["bullets"].sub("", text)

        return f"\n\033[1m🤖 Spark Application Analysis:\033[0m\n{text}\n"


class ServiceChecker:
    """Handles service availability checks."""

    @staticmethod
    async def check_all_services() -> Dict[str, bool]:
        """Check if required services are running."""
        services = {"mcp_server": False, "spark_history": False, "ollama": False}

        checks = [
            ServiceChecker._check_service(
                "mcp_server", Config.MCP_SERVER_URL, [200, 404]
            ),
            ServiceChecker._check_service(
                "spark_history", Config.SPARK_HISTORY_URL, [200]
            ),
            ServiceChecker._check_service(
                "ollama", f"{Config.OLLAMA_URL}/api/tags", [200]
            ),
        ]

        results = await asyncio.gather(*checks, return_exceptions=True)

        for i, (service_name, _) in enumerate(
            [
                ("mcp_server", Config.MCP_SERVER_URL),
                ("spark_history", Config.SPARK_HISTORY_URL),
                ("ollama", f"{Config.OLLAMA_URL}/api/tags"),
            ]
        ):
            services[service_name] = (
                results[i] if not isinstance(results[i], Exception) else False
            )

        return services

    @staticmethod
    async def _check_service(name: str, url: str, valid_codes: List[int]) -> bool:
        """Check individual service availability."""
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                response = await client.get(url)
                return response.status_code in valid_codes
        except Exception:
            return False


class AgentState(TypedDict):
    """State for the Spark analysis agent."""

    messages: Annotated[Sequence[BaseMessage], add_messages]


class SparkAnalysisAgent:
    """Optimized interactive LangGraph agent with enhanced terminal formatting."""

    def __init__(self, model: str = Config.DEFAULT_MODEL, verbose: bool = False):
        self.model = model
        self.verbose = verbose
        self.formatter = TerminalFormatter()
        self._reset_state()

    def _reset_state(self) -> None:
        """Reset agent state."""
        self.mcp_client: Optional[MultiServerMCPClient] = None
        self.tools: List = []
        self.llm: Optional[ChatOllama] = None
        self.llm_with_tools = None
        self.graph = None
        self.initialized = False

    async def initialize(self) -> bool:
        """Initialize MCP client, tools, and LLM."""
        if self.initialized:
            return True

        try:
            await self._print_initialization_info()

            if not await self._check_and_report_services():
                return False

            await self._setup_mcp_client()
            await self._setup_llm()
            self._create_graph()

            self.initialized = True
            console_print("✅ Agent initialized successfully!")
            return True

        except Exception as e:
            console_print(f"❌ Initialization failed: {e}")
            return False

    async def _print_initialization_info(self) -> None:
        """Print initialization information."""
        console_print("🔄 Initializing Spark Analysis Agent...")
        console_print(f"🐍 Python {platform.python_version()} on {platform.system()}")
        console_print(f"🧠 Model: {self.model}")

    async def _check_and_report_services(self) -> bool:
        """Check services and report status."""
        if self.verbose:
            console_print("🔄 Checking required services...")

        services = await ServiceChecker.check_all_services()

        if self.verbose:
            for service, status in services.items():
                status_icon = "✅" if status else "❌"
                console_print(
                    f"  {status_icon} {service}: {'Running' if status else 'Not available'}"
                )

        if not all(services.values()):
            self._print_service_setup_instructions(services)
            return False

        return True

    def _print_service_setup_instructions(self, services: Dict[str, bool]) -> None:
        """Print setup instructions for failed services."""
        console_print("\n❌ Not all required services are running!")
        console_print("🔧 Setup instructions:")

        instructions = {
            "mcp_server": "Start MCP server: task start-mcp-bg",
            "spark_history": "Start Spark History server: task start-spark-bg",
            "ollama": "Install and start Ollama: brew install ollama && ollama pull qwen3:1.7b",
        }

        for service, running in services.items():
            if not running:
                console_print(f"  - {instructions[service]}")

    async def _setup_mcp_client(self) -> None:
        """Setup MCP client and load tools."""
        if self.verbose:
            console_print("🔄 Connecting to MCP server...")

        self.mcp_client = MultiServerMCPClient(
            {
                "spark": {
                    "url": f"{Config.MCP_SERVER_URL}mcp/",
                    "transport": "streamable_http",
                }
            }
        )

        if self.verbose:
            console_print("🔄 Loading MCP tools...")

        self.tools = await self.mcp_client.get_tools()
        self._print_tools_table()

    def _print_tools_table(self) -> None:
        """Print available tools in table format."""
        console_print(f"✅ Loaded {len(self.tools)} MCP tools:")
        console_print(f"\n{'Tool Name':<25} {'Description'}")
        console_print("─" * 75)

        for tool in self.tools:
            name = getattr(tool, "name", str(tool))
            desc = " ".join(
                getattr(tool, "description", "No description available").split()
            )
            desc = desc[:49] + "..." if len(desc) > 50 else desc
            console_print(f"{name:<25} {desc}")

        console_print("─" * 75)

    async def _setup_llm(self) -> None:
        """Setup Ollama LLM."""
        if self.verbose:
            console_print(f"🔄 Initializing Ollama with {self.model}...")

        self.llm = ChatOllama(
            model=self.model,
            base_url=Config.OLLAMA_URL,
            temperature=Config.TEMPERATURE,
            timeout=Config.TIMEOUT,
            num_predict=Config.MAX_TOKENS,
        )

        # Test connection
        await self.llm.ainvoke([HumanMessage(content="Hello")])
        if self.verbose:
            console_print("✅ Ollama connection successful")

        self.llm_with_tools = self.llm.bind_tools(self.tools)

    def _create_graph(self) -> None:
        """Create the LangGraph StateGraph."""
        workflow = StateGraph(AgentState)
        tool_node = ToolNode(self.tools)

        workflow.add_node("agent", self._call_model)
        workflow.add_node("tools", tool_node)
        workflow.set_entry_point("agent")
        workflow.add_conditional_edges(
            "agent", self._should_continue, {"continue": "tools", "end": END}
        )
        workflow.add_edge("tools", "agent")

        self.graph = workflow.compile()

    async def _call_model(self, state: AgentState) -> Dict[str, Any]:
        """Call the LLM with optimized system prompt."""
        messages = state["messages"]

        if not any("system" in str(msg).lower() for msg in messages):
            messages = [SystemMessage(content=self._get_system_prompt())] + messages

        response = await self.llm_with_tools.ainvoke(messages)
        return {"messages": [response]}

    def _get_system_prompt(self) -> str:
        """Get optimized system prompt."""
        return f"""You are a professional Spark performance analysis expert with MCP tools for Spark History Server data.

INSTRUCTIONS:
- Provide direct, professional analysis without <think> tags
- Use markdown headers (### 1., ### 2.) with proper spacing
- Format as key-value pairs where appropriate
- Use bullet points only for actual lists
- Be comprehensive but well-structured
- Focus on actionable insights

Available sample Spark application IDs:
{chr(10).join(f"- {app_id}" for app_id in Config.SAMPLE_APPS)}

Analysis approach:
1. Use MCP tools to fetch comprehensive application data
2. Identify performance metrics, bottlenecks, and resource utilization
3. Provide specific, actionable recommendations
4. Format with clear sections using markdown headers
5. Include specific metrics and measurements

Respond professionally with detailed technical analysis. /no_think"""

    def _should_continue(self, state: AgentState) -> str:
        """Determine if we should continue with tool calls."""
        last_message = state["messages"][-1]
        return (
            "continue"
            if hasattr(last_message, "tool_calls") and last_message.tool_calls
            else "end"
        )

    async def query(self, user_input: str) -> str:
        """Process user query with enhanced terminal formatting."""
        if not self.initialized:
            return "❌ Agent not initialized. Call initialize() first."

        try:
            message = HumanMessage(content=user_input)
            result = await self.graph.ainvoke({"messages": [message]})
            response_content = result["messages"][-1].content
            return self.formatter.format_for_terminal(response_content)
        except Exception as e:
            return f"❌ Error processing query: {e}"

    async def close(self) -> None:
        """Clean up resources."""
        if self.mcp_client:
            try:
                await self.mcp_client.close()
            except Exception as e:
                # Ignore cleanup errors during shutdown
                del e


class InteractiveMode:
    """Handles interactive mode operations."""

    def __init__(self):
        self.agent: Optional[SparkAnalysisAgent] = None

    async def run(self) -> None:
        """Run interactive mode."""
        try:
            model = self._get_model_choice()
            self.agent = SparkAnalysisAgent(model=model, verbose=True)

            if not await self.agent.initialize():
                console_print("❌ Failed to initialize agent")
                return

            await self._interaction_loop(model)
        finally:
            if self.agent:
                await self.agent.close()
            console_print("\n👋 \033[1mGoodbye!\033[0m")

    def _get_model_choice(self) -> str:
        """Get user's model choice."""
        console_print("🎯 \033[1mModel Options:\033[0m")
        for model, info in Config.MODEL_OPTIONS.items():
            style = "\033[93m" if "1.7b" in model else "\033[92m"
            rec = " \033[1m[RECOMMENDED]\033[0m" if "1.7b" in model else ""
            console_print(
                f"  - {style}{model}\033[0m ({info['quality'].lower()}, {info['speed'].lower()}, {info['size']}){rec}"
            )

        return (
            input(f"\n🤖 Choose model [{Config.DEFAULT_MODEL}]: ").strip()
            or Config.DEFAULT_MODEL
        )

    async def _interaction_loop(self, model: str) -> None:
        """Main interaction loop."""
        self._print_ready_message(model)

        while True:
            try:
                user_input = input("\n💬 \033[1mYour query:\033[0m ").strip()

                if user_input.lower() in ["exit", "quit", "bye"]:
                    break
                elif user_input.lower() == "help":
                    self._print_help()
                    continue
                elif not user_input:
                    continue

                console_print("\n🔄 Processing query...")
                response = await self.agent.query(user_input)
                console_print(response)
                console_print("\n" + "\033[90m" + "─" * 80 + "\033[0m")

            except KeyboardInterrupt:
                break
            except Exception as e:
                console_print(f"\n❌ Error: {e}")

    def _print_ready_message(self, model: str) -> None:
        """Print ready message with examples."""
        console_print(f"\n🎉 \033[1mSpark Analysis Agent Ready!\033[0m (Using {model})")
        console_print("\n📝 \033[1mExample commands:\033[0m")
        console_print(f"  - Get detailed analysis for {Config.SAMPLE_APPS[0]}")
        console_print(f"  - Analyze performance bottlenecks in {Config.SAMPLE_APPS[1]}")
        console_print("  - help | exit")
        console_print("\n📊 \033[1mAvailable sample app IDs:\033[0m")
        for app in Config.SAMPLE_APPS:
            console_print(f"  - \033[90m{app}\033[0m")

    def _print_help(self) -> None:
        """Print help information."""
        console_print("\n🔧 \033[1mCommands:\033[0m")
        console_print("  - Any natural language query about Spark applications")
        console_print("  - Use the sample app IDs provided above")
        console_print("  - \033[1mexit\033[0m: Quit the program")
        console_print("\n💡 \033[1mExample queries:\033[0m")
        console_print("  - 'What are the slowest stages in [app-id]?'")
        console_print("  - 'Analyze memory usage for [app-id]'")
        console_print("  - 'Compare performance between applications'")


async def main() -> None:
    """Main function."""
    console_print("🚀 \033[1mLangGraph + MCP + Ollama Integration\033[0m")
    console_print("=" * 55)

    interactive_mode = InteractiveMode()
    await interactive_mode.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        console_print("\n👋 Interrupted by user")
    except Exception as e:
        console_print(f"❌ Fatal error: {e}")
        sys.exit(1)
