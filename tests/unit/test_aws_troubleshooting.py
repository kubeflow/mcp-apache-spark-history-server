"""Tests for AWS Spark Troubleshooting tools."""

import asyncio
import json
from typing import NamedTuple
from unittest.mock import MagicMock, patch

import httpx2
import pytest
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import ReadOnlyCredentials
from mcp import MCPError
from mcp.server.mcpserver import MCPServer
from mcp.server.transport_security import TransportSecuritySettings

from spark_history_mcp.tools import aws_troubleshooting
from spark_history_mcp.tools.aws_troubleshooting import (
    SigV4HttpxAuth,
    _call_remote_tool,
    register_troubleshooting_tools,
)

CREDS = ReadOnlyCredentials(
    "AKIDEXAMPLE", "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY", "FAKE-SESSION-TOKEN"
)
_REAL_ASYNC_CLIENT = httpx2.AsyncClient


def _expected_signature(method, url, headers, body, region):
    """Compute the SigV4 signature that AWS expects for a received request."""
    authorization = headers["authorization"]
    signed_names = authorization.split("SignedHeaders=")[1].split(",")[0].split(";")
    request = AWSRequest(
        method=method,
        url=url,
        data=body,
        headers={n: headers[n] for n in signed_names if n in headers},
    )
    request.context["timestamp"] = headers["x-amz-date"]
    signer = SigV4Auth(CREDS, aws_troubleshooting._SERVICE, region)
    canonical = signer.canonical_request(request)
    return signer.signature(signer.string_to_sign(request, canonical), request), (
        signed_names
    )


class ReceivedRequest(NamedTuple):
    method: str
    mcp_method: str | None
    signature_ok: bool
    headers: dict[str, str]
    path: str


class FakeAwsEndpoint:
    """An MCP server that rejects bad SigV4 signatures, and discover if ``legacy``."""

    def __init__(self, path, region, legacy=False, redirect_to=None):
        self.region = region
        self.legacy = legacy
        self.redirect_to = redirect_to
        self.requests: list[ReceivedRequest] = []
        self.server = MCPServer("fake-aws")

        @self.server.tool()
        def analyze_spark_workload(platform_type: str, platform_params: dict) -> str:
            return json.dumps({"root_cause": "OOM", "echo": platform_params})

        @self.server.tool()
        def spark_code_recommendation(platform_type: str, platform_params: dict) -> str:
            return json.dumps({"recommendation": "repartition"})

        self.app = self.server.streamable_http_app(
            streamable_http_path=path,
            transport_security=TransportSecuritySettings(
                enable_dns_rebinding_protection=False
            ),
        )

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)
        messages, body = [], b""
        while True:
            message = await receive()
            messages.append(message)
            body += message.get("body", b"")
            if not message.get("more_body"):
                break
        headers = {k.decode(): v.decode() for k, v in scope["headers"]}
        url = f"https://{headers['host']}{scope['path']}"
        expected, signed_names = _expected_signature(
            scope["method"], url, headers, body, self.region
        )
        ok = headers["authorization"].endswith("Signature=" + expected)
        ok = ok and "connection" not in signed_names
        self.requests.append(
            ReceivedRequest(
                scope["method"], headers.get("mcp-method"), ok, headers, scope["path"]
            )
        )
        if not ok:
            return await self._respond(send, 403, b'{"message":"bad signature"}')
        if self.redirect_to and scope["path"] != self.redirect_to:
            await send(
                {
                    "type": "http.response.start",
                    "status": 307,
                    "headers": [(b"location", self.redirect_to.encode())],
                }
            )
            return await send({"type": "http.response.body", "body": b""})
        if self.legacy and headers.get("mcp-method") == "server/discover":
            return await self._respond(
                send,
                403,
                b'{"message":"Error creating MCP response: Unsupported MCP '
                b'Method received in the request: server/discover"}',
            )

        async def replay():
            if messages:
                return messages.pop(0)
            # Block: an http.disconnect message would cancel the response.
            await asyncio.Event().wait()

        return await self.app(scope, replay, send)

    @staticmethod
    async def _respond(send, status, body):
        await send(
            {
                "type": "http.response.start",
                "status": status,
                "headers": [(b"content-type", b"application/json")],
            }
        )
        await send({"type": "http.response.body", "body": body})


async def _call_with_fake(endpoint, *args):
    """Run _call_remote_tool against ``endpoint`` with fixed credentials."""

    def client_factory(**kwargs):
        return _REAL_ASYNC_CLIENT(
            transport=httpx2.ASGITransport(app=endpoint), **kwargs
        )

    session = MagicMock()
    session.get_credentials.return_value.get_frozen_credentials.return_value = CREDS
    with (
        patch.object(aws_troubleshooting.boto3, "Session", return_value=session),
        patch.object(aws_troubleshooting.httpx2, "AsyncClient", client_factory),
    ):
        async with endpoint.server.session_manager.run():
            return await _call_remote_tool(*args)


class TestCallRemoteTool:
    @pytest.mark.asyncio
    async def test_calls_remote_tool_with_valid_signatures(self):
        """A modern endpoint gets server/discover first; each request is signed."""
        endpoint = FakeAwsEndpoint("/spark-troubleshooting/mcp", "us-east-1")

        result = await _call_with_fake(
            endpoint,
            "us-east-1",
            "spark-troubleshooting",
            "analyze_spark_workload",
            {"platform_type": "EMR_EC2", "platform_params": {"cluster_id": "j-1"}},
        )

        assert result == {"root_cause": "OOM", "echo": {"cluster_id": "j-1"}}
        methods = [r.mcp_method for r in endpoint.requests]
        assert methods[0] == "server/discover"
        assert "tools/call" in methods
        assert all(r.signature_ok for r in endpoint.requests)
        for headers in (r.headers for r in endpoint.requests):
            assert headers["host"] == "sagemaker-unified-studio-mcp.us-east-1.api.aws"
            assert headers["user-agent"] == aws_troubleshooting._USER_AGENT
            assert headers["x-amz-security-token"] == "FAKE-SESSION-TOKEN"
            assert (
                "/us-east-1/sagemaker-unified-studio-mcp/aws4_request"
                in headers["authorization"]
            )

    @pytest.mark.asyncio
    async def test_falls_back_to_initialize_on_legacy_endpoint(self):
        """Like the real endpoint, reject discover with 403; the client falls back."""
        endpoint = FakeAwsEndpoint(
            "/spark-code-recommendation/mcp", "eu-west-1", legacy=True
        )

        result = await _call_with_fake(
            endpoint,
            "eu-west-1",
            "spark-code-recommendation",
            "spark_code_recommendation",
            {"platform_type": "EMR_SERVERLESS", "platform_params": {}},
        )

        assert result == {"recommendation": "repartition"}
        assert endpoint.requests[0].mcp_method == "server/discover"
        assert all(r.signature_ok for r in endpoint.requests)
        assert any("mcp-session-id" in r.headers for r in endpoint.requests)
        for headers in (r.headers for r in endpoint.requests):
            assert headers["host"] == "sagemaker-unified-studio-mcp.eu-west-1.api.aws"

    @pytest.mark.asyncio
    async def test_no_credentials_raises(self):
        session = MagicMock()
        session.get_credentials.return_value = None
        with patch.object(aws_troubleshooting.boto3, "Session", return_value=session):
            with pytest.raises(RuntimeError, match="No AWS credentials"):
                await _call_remote_tool("us-east-1", "p", "t", {})


def _leaf_exceptions(exc):
    """Yield the exceptions inside nested exception groups (and exc itself)."""
    if isinstance(exc, BaseExceptionGroup):
        for inner in exc.exceptions:
            yield from _leaf_exceptions(inner)
    else:
        yield exc


class TestRedirects:
    @pytest.mark.asyncio
    async def test_does_not_follow_same_origin_redirect(self):
        """A signed request goes only to the configured path, also after a 307."""
        endpoint = FakeAwsEndpoint(
            "/spark-troubleshooting/mcp",
            "us-east-1",
            redirect_to="/somewhere-else/mcp",
        )

        with pytest.raises(BaseException) as info:  # noqa: B017
            await _call_with_fake(
                endpoint,
                "us-east-1",
                "spark-troubleshooting",
                "analyze_spark_workload",
                {"platform_type": "EMR_EC2", "platform_params": {}},
            )

        # The failure must be the unfollowed redirect, not a different error.
        # The SDK raises it in nested exception groups.
        errors = list(_leaf_exceptions(info.value))
        assert any(
            isinstance(e, MCPError) and "not followed" in str(e) for e in errors
        ), errors

        paths = {r.headers["host"] + r.path for r in endpoint.requests}
        assert paths == {
            "sagemaker-unified-studio-mcp.us-east-1.api.aws/spark-troubleshooting/mcp"
        }


class TestSigV4HttpxAuth:
    def test_signs_request_and_skips_connection_header(self):
        """The Connection header is not signed, because AWS does not check it."""
        request = httpx2.Request(
            "POST",
            "https://sagemaker-unified-studio-mcp.us-west-2.api.aws/x/mcp",
            headers={"Connection": "keep-alive", "Content-Type": "application/json"},
            content=b'{"jsonrpc":"2.0"}',
        )
        auth = SigV4HttpxAuth(CREDS, aws_troubleshooting._SERVICE, "us-west-2")
        signed = next(auth.auth_flow(request))

        headers = {k.lower(): v for k, v in signed.headers.items()}
        expected, signed_names = _expected_signature(
            "POST", str(request.url), headers, request.content, "us-west-2"
        )
        assert "connection" not in signed_names
        assert {"host", "x-amz-date", "x-amz-security-token"} <= set(signed_names)
        assert headers["authorization"].endswith("Signature=" + expected)
        assert headers["x-amz-security-token"] == "FAKE-SESSION-TOKEN"


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
