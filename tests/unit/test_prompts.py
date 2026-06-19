import unittest

from spark_history_mcp.core.app import mcp
from spark_history_mcp.prompts.prompts import compare_applications, investigate_failure


class TestInvestigateFailurePrompt(unittest.TestCase):
    def test_renders_app_id_and_investigation_tools(self):
        """The rendered prompt embeds the app id and names the real tools."""
        result = investigate_failure(app_id="app-123")

        self.assertIn("app-123", result)
        # References the actual tools an agent should call, in order.
        for tool_name in (
            "list_applications",
            "list_jobs",
            "list_stages",
            "get_stage",
            "list_stage_task_failures",
        ):
            self.assertIn(tool_name, result)

    def test_omits_server_arg_when_not_provided(self):
        """Without a server, no concrete server name is injected and the
        automatic cross-server discovery behavior is explained."""
        result = investigate_failure(app_id="app-123")

        # No concrete server name should be threaded into the tool calls.
        self.assertNotIn('server="', result.replace('server="<name>"', ""))
        # The discovery behavior should be spelled out for the agent.
        self.assertIn("search every configured", result)

    def test_includes_server_arg_when_provided(self):
        """A provided server name is threaded into the rendered tool calls."""
        result = investigate_failure(app_id="app-123", server="production")

        self.assertIn('server="production"', result)

    def test_status_filter_uses_failed(self):
        """The job/stage steps filter on the FAILED status."""
        result = investigate_failure(app_id="app-123")

        self.assertIn('status=["FAILED"]', result)

    def test_prompt_is_registered_with_mcp(self):
        """The prompt is discoverable via the shared FastMCP instance."""
        # Importing the module registers the prompt; the manager lists it.
        registered = {p.name for p in mcp._prompt_manager.list_prompts()}

        self.assertIn("investigate_failure", registered)


class TestCompareApplicationsPrompt(unittest.TestCase):
    def test_renders_both_app_ids_and_comparison_tools(self):
        """The rendered prompt embeds both app ids and names the compare tools."""
        result = compare_applications(app_a="app-a", app_b="app-b")

        self.assertIn("app-a", result)
        self.assertIn("app-b", result)
        for tool_name in (
            "compare_job_environments",
            "compare_job_performance",
            "list_sql_executions",
            "compare_sql_executions",
            "get_stage",
        ):
            self.assertIn(tool_name, result)

    def test_is_analysis_only(self):
        """The prompt explicitly avoids root-cause diagnosis and fixes."""
        result = compare_applications(app_a="app-a", app_b="app-b")

        self.assertIn("do not diagnose", result)
        self.assertIn("do not recommend", result)

    def test_omits_server_arg_when_not_provided(self):
        """Without a server, no concrete name is injected and the cross-server
        (cross-environment) discovery behavior is explained."""
        result = compare_applications(app_a="app-a", app_b="app-b")

        self.assertNotIn('server="', result.replace('server="<name>"', ""))
        self.assertIn("may live on different", result)

    def test_includes_server_arg_when_provided(self):
        """A provided server name is threaded into the rendered tool calls."""
        result = compare_applications(app_a="app-a", app_b="app-b", server="prod")

        self.assertIn('server="prod"', result)

    def test_includes_context_when_provided(self):
        """Free-text context is embedded when supplied."""
        result = compare_applications(
            app_a="app-a",
            app_b="app-b",
            context="A is the baseline, B doubled shuffle partitions",
        )

        self.assertIn("A is the baseline, B doubled shuffle partitions", result)

    def test_omits_context_section_when_absent(self):
        """No context line leaks in when none is provided."""
        result = compare_applications(app_a="app-a", app_b="app-b")

        self.assertNotIn("Context for this comparison", result)

    def test_prompt_is_registered_with_mcp(self):
        """The prompt is discoverable via the shared FastMCP instance."""
        registered = {p.name for p in mcp._prompt_manager.list_prompts()}

        self.assertIn("compare_applications", registered)


if __name__ == "__main__":
    unittest.main()
