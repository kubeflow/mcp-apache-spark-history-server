#!/usr/bin/env python3
"""
Strands + MCP + Spark History Server integration for intelligent Spark performance analysis.
Interactive agent with streamable HTTP transport.
"""

import asyncio
import contextlib
import io
import platform
import re
import sys
from typing import Any, Dict, Optional


def console_print(*args, **kwargs) -> None:
    """Console output function for CLI tool."""
    print(*args, **kwargs)  # noqa: T201


if sys.version_info < (3, 10):
    console_print(
        "❌ Python 3.10+ required. Current version:", platform.python_version()
    )
    sys.exit(1)

try:
    import httpx
    from mcp.client.streamable_http import streamablehttp_client
    from strands import Agent
    from strands.models.ollama import OllamaModel
    from strands.tools.mcp.mcp_client import MCPClient
except ImportError as e:
    console_print(f"❌ Missing dependency: {e}")
    console_print("💡 Install with: uv pip install -r requirements.txt")
    sys.exit(1)


class Config:
    """Configuration constants."""

    # Service URLs
    MCP_SERVER_URL = "http://127.0.0.1:18888/mcp"
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
            "thinking_blocks": re.compile(
                r"<think>.*?</think>", re.DOTALL | re.IGNORECASE
            ),
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

        return f"\n\033[1m🤖 Spark Analysis Result:\033[0m\n{text}\n"


class ServiceChecker:
    """Handles service availability checks."""

    @staticmethod
    async def check_all_services() -> Dict[str, bool]:
        """Check if required services are running."""
        services = {"mcp_server": False, "spark_history": False, "ollama": False}

        async with httpx.AsyncClient(timeout=5.0) as client:
            try:
                response = await client.get(Config.MCP_SERVER_URL.replace("/mcp", "/"))
                services["mcp_server"] = response.status_code in [200, 404]
            except Exception as e:
                # Ignore service check errors during startup
                del e

            try:
                response = await client.get(Config.SPARK_HISTORY_URL)
                services["spark_history"] = response.status_code == 200
            except Exception as e:
                # Ignore service check errors during startup
                del e

            try:
                response = await client.get(f"{Config.OLLAMA_URL}/api/tags")
                services["ollama"] = response.status_code == 200
            except Exception as e:
                # Ignore service check errors during startup
                del e

        return services


class SparkStrandsAgent:
    """Strands agent with MCP integration for Spark analysis."""

    def __init__(self, model: str = Config.DEFAULT_MODEL, verbose: bool = False):
        self.model = model
        self.verbose = verbose
        self.formatter = TerminalFormatter()
        self.mcp_client: Optional[MCPClient] = None
        self.agent: Optional[Agent] = None
        self.ollama_model: Optional[OllamaModel] = None
        self.tools = []
        self.initialized = False

    async def initialize(self) -> bool:
        """Initialize MCP client and Strands agent."""
        if self.initialized:
            return True

        try:
            await self._print_initialization_info()

            if not await self._check_and_report_services():
                return False

            await self._setup_mcp_client()
            await self._setup_ollama()
            self._create_agent()

            self.initialized = True
            console_print("✅ Strands Agent initialized successfully!")
            return True

        except Exception as e:
            console_print(f"❌ Initialization failed: {e}")
            return False

    async def _print_initialization_info(self) -> None:
        """Print initialization information."""
        console_print("🔄 Initializing Strands Spark Analysis Agent...")
        console_print(f"🐍 Python {platform.python_version()} on {platform.system()}")

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
            "ollama": "Start Ollama: brew services start ollama (macOS)",
        }

        for service, running in services.items():
            if not running:
                console_print(f"  - {instructions[service]}")

    async def _setup_mcp_client(self) -> None:
        """Setup MCP client and load tools."""
        if self.verbose:
            console_print("🔄 Connecting to MCP server via Streamable HTTP...")

        # Create MCP client with streamable HTTP transport
        self.mcp_client = MCPClient(
            lambda: streamablehttp_client(Config.MCP_SERVER_URL)
        )

        if self.verbose:
            console_print("🔄 Loading MCP tools...")

    async def _setup_ollama(self) -> None:
        """Setup Ollama model."""
        if self.verbose:
            console_print(f"🔄 Setting up Ollama model: {self.model}")

        try:
            # Check if model exists
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.get(f"{Config.OLLAMA_URL}/api/tags")
                if response.status_code == 200:
                    models = response.json().get("models", [])
                    model_names = [m["name"] for m in models]

                    if self.model not in model_names:
                        console_print(
                            f"⚠️  Model {self.model} not found. Available models: {model_names}"
                        )
                        console_print(
                            f"💡 Pull the model with: ollama pull {self.model}"
                        )
                        return

            # Create Ollama model instance
            self.ollama_model = OllamaModel(
                host=Config.OLLAMA_URL,
                model_id=self.model,
                temperature=Config.TEMPERATURE,
                max_tokens=Config.MAX_TOKENS,
            )

            if self.verbose:
                model_info = Config.MODEL_OPTIONS.get(self.model, {})
                console_print(f"✅ Ollama model configured: {self.model}")
                if model_info:
                    console_print(f"   Size: {model_info.get('size', 'Unknown')}")
                    console_print(f"   Quality: {model_info.get('quality', 'Unknown')}")

        except Exception as e:
            console_print(f"❌ Failed to setup Ollama model: {e}")
            raise

    def _create_agent(self) -> None:
        """Create Strands agent with MCP tools."""
        if not self.mcp_client:
            raise ValueError("MCP client not initialized")

        # Use the MCP client within context manager to get tools
        with self.mcp_client:
            self.tools = self.mcp_client.list_tools_sync()
            console_print(f"✅ Loaded {len(self.tools)} MCP tools:")

            # Print tools table
            self._print_tools_table()

            # Create Strands agent with system prompt, tools and Ollama model
            system_prompt = self._get_system_prompt()
            if self.ollama_model:
                self.agent = Agent(
                    model=self.ollama_model,
                    tools=self.tools,
                    system_prompt=system_prompt,
                )
            else:
                # Fallback to default model if Ollama setup failed
                self.agent = Agent(tools=self.tools, system_prompt=system_prompt)

    def _get_system_prompt(self) -> str:
        """Get comprehensive system prompt for Spark performance analysis."""
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

    def _print_tools_table(self) -> None:
        """Print available tools in table format."""
        console_print(f"\n{'Tool Name':<25} {'Description'}")
        console_print("─" * 75)

        for tool in self.tools:
            # Extract tool name and description from MCP tool structure
            name = "Unknown"
            desc = "No description available"

            # Check if this is a Strands MCP tool with mcp_tool attribute
            if hasattr(tool, "mcp_tool") and tool.mcp_tool:
                mcp_tool = tool.mcp_tool
                name = getattr(mcp_tool, "name", "Unknown")
                desc = getattr(mcp_tool, "description", "No description available")
            else:
                # Fallback to other attribute names
                name = (
                    getattr(tool, "name", None)
                    or getattr(tool, "tool_name", None)
                    or getattr(tool, "_name", None)
                    or str(type(tool).__name__)
                )

                desc = (
                    getattr(tool, "description", None)
                    or getattr(tool, "tool_description", None)
                    or getattr(tool, "_description", None)
                    or "No description available"
                )

            # Clean up description - remove extra whitespace and newlines
            if isinstance(desc, str):
                desc = " ".join(desc.split())
                # Truncate long descriptions to fit table
                if len(desc) > 50:
                    desc = desc[:49] + "..."

            console_print(f"{name:<25} {desc}")

        console_print("─" * 75)

    async def query(self, user_input: str) -> str:
        """Process user query using Strands agent."""
        if not self.initialized or not self.agent:
            return "❌ Agent not initialized. Call initialize() first."

        try:
            # Use the MCP client context for tool execution
            with self.mcp_client:
                # Capture stdout to prevent duplicate output from Strands agent
                captured_output = io.StringIO()
                with contextlib.redirect_stdout(captured_output):
                    response = self.agent(user_input)
                return self._format_response(response)

        except Exception as e:
            return f"❌ Error processing query: {e}"

    def _format_response(self, response: Any) -> str:
        """Format the agent response for terminal display with enhanced formatting."""
        # Strands agents return response objects, extract the content
        if hasattr(response, "content"):
            content = response.content
        elif isinstance(response, str):
            content = response
        else:
            content = str(response)

        # Apply terminal formatting
        return self.formatter.format_for_terminal(content)

    def close(self) -> None:
        """Clean up resources."""
        if self.mcp_client:
            try:
                # Strands MCP client cleanup is handled by context manager
                pass
            except Exception as e:
                # Ignore cleanup errors during shutdown
                del e


class InteractiveMode:
    """Handles interactive mode operations."""

    def __init__(self):
        self.agent: Optional[SparkStrandsAgent] = None

    async def run(self) -> None:
        """Run interactive mode."""
        try:
            model = self._get_model_choice()
            self.agent = SparkStrandsAgent(model=model, verbose=True)

            if not await self.agent.initialize():
                console_print("❌ Failed to initialize agent")
                return

            await self._interaction_loop(model)
        finally:
            if self.agent:
                self.agent.close()
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

        # Check if running in interactive mode
        if not sys.stdin.isatty():
            console_print(
                f"\n🤖 Non-interactive mode: Using default model [{Config.DEFAULT_MODEL}]"
            )
            return Config.DEFAULT_MODEL

        try:
            return (
                input(f"\n🤖 Choose model [{Config.DEFAULT_MODEL}]: ").strip()
                or Config.DEFAULT_MODEL
            )
        except EOFError:
            console_print(
                f"\n🤖 EOF detected: Using default model [{Config.DEFAULT_MODEL}]"
            )
            return Config.DEFAULT_MODEL

    async def _interaction_loop(self, model: str) -> None:
        """Main interaction loop."""
        self._print_ready_message(model)

        while True:
            try:
                # Check if running in interactive mode
                if not sys.stdin.isatty():
                    console_print("\n❌ Non-interactive environment detected. Exiting.")
                    break

                user_input = input("\n💬 \033[1mYour query:\033[0m ").strip()

                if user_input.lower() in ["exit", "quit", "bye"]:
                    break
                elif user_input.lower() == "help":
                    self._print_help()
                    continue
                elif not user_input:
                    continue

                console_print("\n🔄 Processing query with Strands Agent...")
                response = await self.agent.query(user_input)
                console_print(response)
                console_print("\n" + "\033[90m" + "─" * 80 + "\033[0m")

            except KeyboardInterrupt:
                break
            except EOFError:
                console_print("\n❌ EOF detected. Exiting interactive mode.")
                break
            except Exception as e:
                console_print(f"\n❌ Error: {e}")

    def _print_ready_message(self, model: str) -> None:
        """Print ready message with examples."""
        console_print(
            f"\n🎉 \033[1mStrands Spark Analysis Agent Ready!\033[0m (Using {model})"
        )
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
    console_print("🚀 \033[1mStrands + MCP + Spark History Server Integration\033[0m")
    console_print("=" * 60)

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
