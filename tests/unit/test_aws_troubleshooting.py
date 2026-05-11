"""Tests for AWS Spark Troubleshooting tools."""

import json
from unittest.mock import ANY, AsyncMock, MagicMock, patch

import pytest

from spark_history_mcp.tools.aws_troubleshooting import (
    _call_remote_tool,
    register_troubleshooting_tools,
)


class TestCallRemoteTool:
    @pytest.mark.asyncio
    @patch("spark_history_mcp.tools.aws_troubleshooting.aws_iam_streamablehttp_client")
    async def test_calls_remote_tool_successfully(self, mock_client_factory):
        """Test successful remote tool call."""
        expected_result = {"analysis_id": "123", "root_cause": "OOM"}

        mock_session = AsyncMock()
        mock_content = MagicMock()
        mock_content.text = json.dumps(expected_result)
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

            result = await _call_remote_tool(
                "us-east-1",
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
            headers=ANY,
        )

    @pytest.mark.asyncio
    @patch("spark_history_mcp.tools.aws_troubleshooting.aws_iam_streamablehttp_client")
    async def test_constructs_endpoint_with_region(self, mock_client_factory):
        """Test that endpoint URL uses the provided region."""
        mock_session = AsyncMock()
        mock_content = MagicMock()
        mock_content.text = json.dumps({"status": "ok"})
        mock_session.call_tool.return_value = MagicMock(content=[mock_content])

        mock_session_ctx = AsyncMock()
        mock_session_ctx.__aenter__.return_value = mock_session

        mock_transport_ctx = AsyncMock()
        mock_transport_ctx.__aenter__.return_value = (AsyncMock(), AsyncMock(), None)
        mock_client_factory.return_value = mock_transport_ctx

        with patch(
            "spark_history_mcp.tools.aws_troubleshooting.ClientSession"
        ) as mock_session_cls:
            mock_session_cls.return_value = mock_session_ctx

            await _call_remote_tool(
                "eu-west-1",
                "spark-code-recommendation",
                "spark_code_recommendation",
                {"platform_type": "EMR_SERVERLESS", "platform_params": {}},
            )

        mock_client_factory.assert_called_once_with(
            endpoint="https://sagemaker-unified-studio-mcp.eu-west-1.api.aws/spark-code-recommendation/mcp",
            aws_region="eu-west-1",
            aws_service="sagemaker-unified-studio-mcp",
            headers=ANY,
        )


class TestRegisterTroubleshootingTools:
    def test_registers_two_tools(self):
        """Test that registration adds two tools to the MCP server."""
        from spark_history_mcp.core.app import mcp

        tools_before = set(mcp._tool_manager._tools.keys())
        register_troubleshooting_tools("us-east-1")
        tools_after = set(mcp._tool_manager._tools.keys())

        new_tools = tools_after - tools_before
        assert "aws_analyze_spark_workload" in new_tools
        assert "aws_spark_code_recommendation" in new_tools


class TestAutoDetect:
    @patch("boto3.Session")
    def test_tools_registered_when_credentials_available(self, mock_session_cls):
        """Test that tools are registered when AWS credentials are detected."""
        mock_session = MagicMock()
        mock_session.get_credentials.return_value = MagicMock()
        mock_session.region_name = "us-west-2"
        mock_session_cls.return_value = mock_session

        with patch(
            "spark_history_mcp.tools.aws_troubleshooting.register_troubleshooting_tools"
        ) as mock_register:
            import boto3

            session = boto3.Session()
            creds = session.get_credentials()
            if creds is not None and session.region_name:
                mock_register(session.region_name)

            mock_register.assert_called_once_with("us-west-2")

    @patch("boto3.Session")
    def test_tools_not_registered_when_no_credentials(self, mock_session_cls):
        """Test that tools are NOT registered when credentials are missing."""
        mock_session = MagicMock()
        mock_session.get_credentials.return_value = None
        mock_session.region_name = "us-east-1"
        mock_session_cls.return_value = mock_session

        with patch(
            "spark_history_mcp.tools.aws_troubleshooting.register_troubleshooting_tools"
        ) as mock_register:
            import boto3

            session = boto3.Session()
            creds = session.get_credentials()
            if creds is not None and session.region_name:
                mock_register(session.region_name)

            mock_register.assert_not_called()

    @patch("boto3.Session")
    def test_tools_not_registered_when_no_region(self, mock_session_cls):
        """Test that tools are NOT registered when region is missing."""
        mock_session = MagicMock()
        mock_session.get_credentials.return_value = MagicMock()
        mock_session.region_name = None
        mock_session_cls.return_value = mock_session

        with patch(
            "spark_history_mcp.tools.aws_troubleshooting.register_troubleshooting_tools"
        ) as mock_register:
            import boto3

            session = boto3.Session()
            creds = session.get_credentials()
            if creds is not None and session.region_name:
                mock_register(session.region_name)

            mock_register.assert_not_called()
