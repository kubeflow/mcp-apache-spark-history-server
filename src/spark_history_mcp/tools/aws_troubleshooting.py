"""AWS Spark Troubleshooting tools using mcp-proxy-for-aws.

These tools forward calls to the SageMaker Unified Studio MCP endpoints
for Spark workload analysis and code recommendations. They are only
registered when troubleshooting is enabled in config.yaml.
"""

import json
import logging
from typing import Any, Dict

from mcp.client.session import ClientSession
from mcp_proxy_for_aws.client import aws_iam_streamablehttp_client

from spark_history_mcp.config.config import TroubleshootingConfig
from spark_history_mcp.core.app import mcp

logger = logging.getLogger(__name__)

_BASE_URL = "https://sagemaker-unified-studio-mcp.{region}.api.aws"
_SERVICE = "sagemaker-unified-studio-mcp"


async def _call_remote_tool(
    config: TroubleshootingConfig,
    server_path: str,
    tool_name: str,
    arguments: Dict[str, Any],
) -> Dict[str, Any]:
    """Call a tool on the remote AWS MCP endpoint."""
    endpoint = f"{_BASE_URL.format(region=config.region)}/{server_path}/mcp"

    kwargs: Dict[str, Any] = {
        "endpoint": endpoint,
        "aws_region": config.region,
        "aws_service": _SERVICE,
    }
    if config.profile:
        kwargs["aws_profile"] = config.profile

    client = aws_iam_streamablehttp_client(**kwargs)
    async with client as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()
            result = await session.call_tool(tool_name, arguments)

    # Extract text content from the MCP result
    for content in result.content:
        if hasattr(content, "text"):
            return json.loads(content.text)

    return {"error": "No text content in response"}


def register_troubleshooting_tools(config: TroubleshootingConfig):
    """Register AWS troubleshooting tools with the MCP server."""

    @mcp.tool()
    async def aws_analyze_spark_workload(
        platform_type: str,
        platform_params: Dict[str, str],
    ) -> Dict[str, Any]:
        """Analyze a failed or slow Spark workload to identify root cause.

        Uses the AWS Spark Troubleshooting Agent for one-shot automated
        root cause analysis of Spark applications.

        Args:
            platform_type: One of EMR_EC2 or EMR_SERVERLESS
                (TODO: EMR_EKS support coming soon)
            platform_params: Platform-specific parameters. For EMR_EC2:
                cluster_id and step_id. For EMR_SERVERLESS: application_id
                and job_run_id.

        Returns:
            Analysis result with root cause, recommendations, and next actions.
        """
        return await _call_remote_tool(
            config,
            "spark-troubleshooting",
            "analyze_spark_workload",
            {
                "platform_type": platform_type,
                "platform_params": platform_params,
            },
        )

    @mcp.tool()
    async def aws_spark_code_recommendation(
        platform_type: str,
        platform_params: Dict[str, str],
    ) -> Dict[str, Any]:
        """Get code fix recommendations for a failed Spark workload.

        After analyzing a workload with aws_analyze_spark_workload, use this
        tool to get specific code changes that address the identified issue.

        Args:
            platform_type: One of EMR_EC2 or EMR_SERVERLESS
                (TODO: EMR_EKS support coming soon)
            platform_params: Platform-specific parameters. For EMR_EC2:
                cluster_id and step_id. For EMR_SERVERLESS: application_id
                and job_run_id.

        Returns:
            Code recommendation with suggested fixes.
        """
        return await _call_remote_tool(
            config,
            "spark-code-recommendation",
            "spark_code_recommendation",
            {
                "platform_type": platform_type,
                "platform_params": platform_params,
            },
        )

    logger.info("AWS troubleshooting tools registered (region=%s)", config.region)
