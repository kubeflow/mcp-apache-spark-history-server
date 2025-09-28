"""Tests for tool filtering functionality."""

import os
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from spark_history_mcp.utils.tool_filter import (
    conditional_tool,
    is_tool_enabled,
)


class TestToolFilter(unittest.TestCase):
    """Test tool filtering functionality."""

    def setUp(self):
        """Set up test fixtures."""
        # Clear environment variables that might affect tests
        env_vars_to_clear = [
            "SHS_GLOBAL_DISABLED_TOOLS",
            "SHS_DISABLE_TEST_TOOL",
            "SHS_SERVERS_LOCAL_DISABLED_TOOLS",
        ]
        for var in env_vars_to_clear:
            if var in os.environ:
                del os.environ[var]

    def tearDown(self):
        """Clean up after tests."""
        # Clear any environment variables set during tests
        env_vars_to_clear = [
            "SHS_GLOBAL_DISABLED_TOOLS",
            "SHS_DISABLE_TEST_TOOL",
            "SHS_SERVERS_LOCAL_DISABLED_TOOLS",
        ]
        for var in env_vars_to_clear:
            if var in os.environ:
                del os.environ[var]

    def test_is_tool_enabled_default(self):
        """Test that tools are enabled by default."""
        # With non-existent config file, tools should be enabled by default
        self.assertTrue(is_tool_enabled("test_tool", "non_existent_config.yaml"))

    def test_is_tool_enabled_with_global_disabled_tools_env(self):
        """Test disabling tools via global environment variable."""
        os.environ["SHS_GLOBAL_DISABLED_TOOLS"] = "tool1,tool2,test_tool"

        self.assertFalse(is_tool_enabled("test_tool"))
        self.assertFalse(is_tool_enabled("tool1"))
        self.assertTrue(is_tool_enabled("enabled_tool"))

    def test_is_tool_enabled_with_individual_env_var(self):
        """Test disabling individual tool via environment variable."""
        os.environ["SHS_DISABLE_TEST_TOOL"] = "true"

        self.assertFalse(is_tool_enabled("test_tool"))
        self.assertTrue(is_tool_enabled("other_tool"))

    def test_is_tool_enabled_with_config_file(self):
        """Test tool filtering via configuration file."""
        # Create a temporary config file
        config_data = {
            "servers": {
                "local": {
                    "url": "http://localhost:18080",
                    "disabled_tools": ["server_disabled"],
                }
            },
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            import yaml

            yaml.dump(config_data, f)
            config_path = f.name

        try:
            # Test server-specific disabled tool
            self.assertFalse(is_tool_enabled("server_disabled", config_path))

            # Test enabled tool
            self.assertTrue(is_tool_enabled("enabled_tool", config_path))
        finally:
            os.unlink(config_path)

    def test_priority_order(self):
        """Test that environment variables take priority over config file."""
        # Create config file that enables the tool
        config_data = {
            "servers": {
                "local": {"url": "http://localhost:18080", "disabled_tools": []}
            },
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            import yaml

            yaml.dump(config_data, f)
            config_path = f.name

        try:
            # Tool should be enabled by config
            self.assertTrue(is_tool_enabled("test_tool", config_path))

            # Environment variable should override config
            os.environ["SHS_DISABLE_TEST_TOOL"] = "true"
            self.assertFalse(is_tool_enabled("test_tool", config_path))

        finally:
            os.unlink(config_path)

    def test_conditional_tool_decorator_enabled(self):
        """Test conditional tool decorator when tool is enabled."""
        mock_mcp = MagicMock()
        mock_mcp.tool.return_value = lambda func: func

        @conditional_tool(mock_mcp, "enabled_tool")
        def test_function():
            return "test"

        # Should be registered with MCP
        mock_mcp.tool.assert_called_once()

    def test_conditional_tool_decorator_disabled(self):
        """Test conditional tool decorator when tool is disabled."""
        os.environ["SHS_DISABLE_DISABLED_TOOL"] = "true"

        mock_mcp = MagicMock()
        mock_mcp.tool.return_value = lambda func: func

        @conditional_tool(mock_mcp, "disabled_tool")
        def test_function():
            return "test"

        # Should NOT be registered with MCP
        mock_mcp.tool.assert_not_called()

    def test_conditional_tool_decorator_default_name(self):
        """Test conditional tool decorator using function name."""
        mock_mcp = MagicMock()
        mock_mcp.tool.return_value = lambda func: func

        @conditional_tool(mock_mcp)
        def my_test_function():
            return "test"

        # Should use function name as tool name
        mock_mcp.tool.assert_called_once()

    @patch("spark_history_mcp.utils.tool_filter.Config.from_file")
    def test_config_loading_error_handling(self, mock_from_file):
        """Test that config loading errors are handled gracefully."""
        # Make config loading raise an exception
        mock_from_file.side_effect = Exception("Config loading failed")

        # Should default to enabled when config can't be loaded
        self.assertTrue(is_tool_enabled("test_tool"))

    def test_environment_variable_parsing(self):
        """Test parsing of comma-separated environment variables."""
        os.environ["SHS_GLOBAL_DISABLED_TOOLS"] = " tool1 , tool2 ,tool3"

        self.assertFalse(is_tool_enabled("tool1"))
        self.assertFalse(is_tool_enabled("tool2"))
        self.assertFalse(is_tool_enabled("tool3"))
        self.assertTrue(is_tool_enabled("tool4"))

    def test_case_sensitivity(self):
        """Test that tool names are case sensitive in config but not in env vars."""
        # Environment variables use uppercase conversion, so they're not case sensitive
        os.environ["SHS_DISABLE_MYTEST"] = "true"

        # Both should be disabled because env var converts to uppercase
        self.assertFalse(is_tool_enabled("mytest"))
        self.assertFalse(is_tool_enabled("MYTEST"))

        # Test case sensitivity with config file
        config_data = {
            "servers": {
                "local": {
                    "url": "http://localhost:18080",
                    "disabled_tools": ["lowercase_only"],
                }
            },
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            import yaml

            yaml.dump(config_data, f)
            config_path = f.name

        try:
            # Should be case sensitive in config
            self.assertFalse(is_tool_enabled("lowercase_only", config_path))
            self.assertTrue(
                is_tool_enabled("LOWERCASE_ONLY", config_path)
            )  # Different case
        finally:
            os.unlink(config_path)

    def test_individual_env_var_values(self):
        """Test different values for individual environment variables."""
        # Test "true"
        os.environ["SHS_DISABLE_TEST1"] = "true"
        self.assertFalse(is_tool_enabled("test1"))

        # Test "1"
        os.environ["SHS_DISABLE_TEST2"] = "1"
        self.assertFalse(is_tool_enabled("test2"))

        # Test "yes"
        os.environ["SHS_DISABLE_TEST3"] = "yes"
        self.assertFalse(is_tool_enabled("test3"))

        # Test "false" (should not disable)
        os.environ["SHS_DISABLE_TEST4"] = "false"
        self.assertTrue(is_tool_enabled("test4"))

        # Test empty string (should not disable)
        os.environ["SHS_DISABLE_TEST5"] = ""
        self.assertTrue(is_tool_enabled("test5"))

        # Clean up
        for i in range(1, 6):
            del os.environ[f"SHS_DISABLE_TEST{i}"]

    def test_whitespace_handling_in_global_env_var(self):
        """Test that whitespace is properly stripped from global env var."""
        os.environ["SHS_GLOBAL_DISABLED_TOOLS"] = "  tool1  ,  tool2  ,  tool3  "

        self.assertFalse(is_tool_enabled("tool1"))
        self.assertFalse(is_tool_enabled("tool2"))
        self.assertFalse(is_tool_enabled("tool3"))
        self.assertTrue(is_tool_enabled("tool4"))

    def test_empty_global_env_var(self):
        """Test that empty global env var doesn't affect anything."""
        os.environ["SHS_GLOBAL_DISABLED_TOOLS"] = ""

        self.assertTrue(is_tool_enabled("any_tool"))


if __name__ == "__main__":
    unittest.main()
