"""AWS Spark Troubleshooting tools using mcp-proxy-for-aws.

These tools forward calls to the SageMaker Unified Studio MCP endpoints
for Spark workload analysis and code recommendations. They are only
registered when AWS credentials and region are detected at startup.
"""

import json
import logging
from collections.abc import Generator
from importlib.metadata import version as pkg_version
from typing import Any, Dict

import anyio
import boto3
import httpx2
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import ReadOnlyCredentials
from mcp import Client
from mcp.client.streamable_http import streamable_http_client
from mcp.types import ToolAnnotations

from spark_history_mcp.core.app import tool

logger = logging.getLogger(__name__)

# Distribution name differs from package name (spark_history_mcp), must be hardcoded
_PKG_NAME = "mcp-apache-spark-history-server"
try:
    _VERSION = pkg_version(_PKG_NAME)
except Exception:
    _VERSION = "unknown"
_USER_AGENT = f"kubeflow/{_PKG_NAME}/{_VERSION}"
_BASE_URL = "https://sagemaker-unified-studio-mcp.{region}.api.aws"
_SERVICE = "sagemaker-unified-studio-mcp"

# The SDK's default MCP HTTP client timeouts (a response stream can stay open).
_TIMEOUT = httpx2.Timeout(30.0, read=300.0)


class SigV4HttpxAuth(httpx2.Auth):
    """Sign each httpx2 request with AWS Signature Version 4 (botocore)."""

    # The signature includes a hash of the body, so httpx2 must read it first.
    requires_request_body = True

    def __init__(self, credentials: ReadOnlyCredentials, service: str, region: str):
        self._signer = SigV4Auth(credentials, service, region)

    def auth_flow(
        self, request: httpx2.Request
    ) -> Generator[httpx2.Request, httpx2.Response, None]:
        headers = dict(request.headers)
        # AWS leaves the hop-by-hop Connection header out of its signature check.
        headers.pop("connection", None)
        aws_request = AWSRequest(
            method=request.method,
            url=str(request.url),
            data=request.content,
            headers=headers,
        )
        self._signer.add_auth(aws_request)
        request.headers.update(dict(aws_request.headers))
        yield request


def _frozen_credentials() -> ReadOnlyCredentials:
    credentials = boto3.Session().get_credentials()
    if credentials is None:
        raise RuntimeError("No AWS credentials are available.")
    return credentials.get_frozen_credentials()


async def _call_remote_tool(
    region: str,
    server_path: str,
    tool_name: str,
    arguments: Dict[str, Any],
) -> Dict[str, Any]:
    """Call a tool on the remote AWS MCP endpoint."""
    endpoint = f"{_BASE_URL.format(region=region)}/{server_path}/mcp"

    # Fresh for each call, so rotated credentials apply. The boto3 chain can
    # block on I/O (for example instance metadata).
    credentials = await anyio.to_thread.run_sync(_frozen_credentials)

    async with httpx2.AsyncClient(
        auth=SigV4HttpxAuth(credentials, _SERVICE, region),
        headers={"User-Agent": _USER_AGENT},
        timeout=_TIMEOUT,
        # The SDK transport ignores follow_redirects and follows same-origin
        # redirects up to max_redirects, so set both.
        follow_redirects=False,
        max_redirects=0,
    ) as http_client:
        # The AWS endpoint is pre-2026. "auto" falls back to initialize now and
        # uses a newer protocol version when the endpoint supports it.
        transport = streamable_http_client(endpoint, http_client=http_client)
        async with Client(transport, mode="auto", cache=None) as client:
            result = await client.call_tool(tool_name, arguments)

    # Extract text content from the MCP result
    for content in result.content:
        if hasattr(content, "text"):
            return json.loads(content.text)

    return {"error": "No text content in response"}


def register_troubleshooting_tools(region: str):
    """Register AWS troubleshooting tools with the MCP server."""

    @tool(annotations=ToolAnnotations(readOnlyHint=True))
    async def aws_analyze_spark_workload(
        platform_type: str,
        platform_params: Dict[str, str],
    ) -> Dict[str, Any]:
        """Analyze a failed or slow Spark workload to identify root cause.

        Uses the AWS Spark Troubleshooting Agent for one-shot automated
        root cause analysis of Spark applications.

        Args:
            platform_type: One of EMR_EC2 or EMR_SERVERLESS
            platform_params: Platform-specific parameters. For EMR_EC2:
                cluster_id and step_id. For EMR_SERVERLESS: application_id
                and job_run_id.

        Returns:
            Analysis result with root cause, recommendations, and next actions.
        """
        return await _call_remote_tool(
            region,
            "spark-troubleshooting",
            "analyze_spark_workload",
            {
                "platform_type": platform_type,
                "platform_params": platform_params,
            },
        )

    @tool(annotations=ToolAnnotations(readOnlyHint=True))
    async def aws_spark_code_recommendation(
        platform_type: str,
        platform_params: Dict[str, str],
    ) -> Dict[str, Any]:
        """Get code fix recommendations for a failed Spark workload.

        After analyzing a workload with aws_analyze_spark_workload, use this
        tool to get specific code changes that address the identified issue.

        Args:
            platform_type: One of EMR_EC2 or EMR_SERVERLESS
            platform_params: Platform-specific parameters. For EMR_EC2:
                cluster_id and step_id. For EMR_SERVERLESS: application_id
                and job_run_id.

        Returns:
            Code recommendation with suggested fixes.
        """
        return await _call_remote_tool(
            region,
            "spark-code-recommendation",
            "spark_code_recommendation",
            {
                "platform_type": platform_type,
                "platform_params": platform_params,
            },
        )

    logger.info("AWS troubleshooting tools registered (region=%s)", region)
