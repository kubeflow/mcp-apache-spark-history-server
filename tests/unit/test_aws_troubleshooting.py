"""Tests for AWS Spark Troubleshooting tools."""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from spark_history_mcp.config.config import AwsTroubleshootingConfig
from spark_history_mcp.tools.aws_troubleshooting import (
    _call_remote_tool,
    register_troubleshooting_tools,
)


@pytest.fixture
def troubleshooting_config():
    return AwsTroubleshootingConfig(enabled=True, region="us-east-1")


@pytest.fixture
def troubleshooting_config_with_profile():
    return AwsTroubleshootingConfig(
        enabled=True, region="us-west-2", profile="my-profile"
    )


class TestCallRemoteTool:
    @pytest.mark.asyncio
    @patch("spark_history_mcp.tools.aws_troubleshooting.aws_iam_streamablehttp_client")
    async def test_calls_remote_tool_successfully(
        self, mock_client_factory, troubleshooting_config
    ):
        """Test successful remote tool call."""
        expected_result = {"analysis_id": "123", "root_cause": "OOM"}

        # Mock the MCP session chain
        mock_session = AsyncMock()
        mock_content = MagicMock()
        mock_content.text = json.dumps(expected_result)
        mock_session.call_tool.return_value = MagicMock(content=[mock_content])

        mock_session_ctx = AsyncMock()
        mock_session_ctx.__aenter__.return_value = mock_session

        # Mock the transport context
        mock_transport_ctx = AsyncMock()
        mock_transport_ctx.__aenter__.return_value = (
            AsyncMock(),
            AsyncMock(),
            None,
        )

        mock_client_factory.return_value = mock_transport_ctx

        with patch(
            "spark_history_mcp.tools.aws_troubleshooting.ClientSession"
        ) as mock_session_cls:
            mock_session_cls.return_value = mock_session_ctx

            result = await _call_remote_tool(
                troubleshooting_config,
                "spark-troubleshooting",
                "analyze_spark_workload",
                {
                    "platform_type": "EMR_EC2",
                    "platform_params": {"cluster_id": "j-123"},
                },
            )

        assert result == expected_result
        mock_client_factory.assert_called_once_with(
            endpoint="https://sagemaker-unified-studio-mcp.us-east-1.api.aws/spark-troubleshooting/mcp",
            aws_region="us-east-1",
            aws_service="sagemaker-unified-studio-mcp",
        )

    @pytest.mark.asyncio
    @patch("spark_history_mcp.tools.aws_troubleshooting.aws_iam_streamablehttp_client")
    async def test_passes_profile_when_configured(
        self, mock_client_factory, troubleshooting_config_with_profile
    ):
        """Test that AWS profile is passed when configured."""
        mock_session = AsyncMock()
        mock_content = MagicMock()
        mock_content.text = json.dumps({"status": "ok"})
        mock_session.call_tool.return_value = MagicMock(content=[mock_content])

        mock_session_ctx = AsyncMock()
        mock_session_ctx.__aenter__.return_value = mock_session

        mock_transport_ctx = AsyncMock()
        mock_transport_ctx.__aenter__.return_value = (
            AsyncMock(),
            AsyncMock(),
            None,
        )
        mock_client_factory.return_value = mock_transport_ctx

        with patch(
            "spark_history_mcp.tools.aws_troubleshooting.ClientSession"
        ) as mock_session_cls:
            mock_session_cls.return_value = mock_session_ctx

            await _call_remote_tool(
                troubleshooting_config_with_profile,
                "spark-code-recommendation",
                "spark_code_recommendation",
                {"platform_type": "GLUE", "platform_params": {"job_name": "my-job"}},
            )

        mock_client_factory.assert_called_once_with(
            endpoint="https://sagemaker-unified-studio-mcp.us-west-2.api.aws/spark-code-recommendation/mcp",
            aws_region="us-west-2",
            aws_service="sagemaker-unified-studio-mcp",
            aws_profile="my-profile",
        )


class TestRegisterTroubleshootingTools:
    def test_registers_two_tools(self, troubleshooting_config):
        """Test that registration adds two tools to the MCP server."""
        from spark_history_mcp.core.app import mcp

        tools_before = set(mcp._tool_manager._tools.keys())
        register_troubleshooting_tools(troubleshooting_config)
        tools_after = set(mcp._tool_manager._tools.keys())

        new_tools = tools_after - tools_before
        assert "aws_analyze_spark_workload" in new_tools
        assert "aws_spark_code_recommendation" in new_tools


class TestAwsTroubleshootingConfig:
    def test_defaults(self):
        """Test default config values."""
        config = AwsTroubleshootingConfig()
        assert config.enabled is False
        assert config.region == "us-east-1"
        assert config.profile is None

    def test_custom_values(self):
        """Test custom config values."""
        config = AwsTroubleshootingConfig(
            enabled=True, region="eu-west-1", profile="prod"
        )
        assert config.enabled is True
        assert config.region == "eu-west-1"
        assert config.profile == "prod"
