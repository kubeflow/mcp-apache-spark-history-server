import os
import sys
import unittest
from unittest.mock import MagicMock, patch

# Add root directory to Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from spark_history_mcp.api.emr_persistent_ui_client import EMRPersistentUIClient
from spark_history_mcp.api.spark_client import SparkRestClient
from spark_history_mcp.config.config import ServerConfig


class TestEMRIntegration(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures."""
        self.emr_cluster_arn = (
            "arn:aws:elasticmapreduce:us-east-1:123456789012:cluster/j-2AXXXXXXGAPLF"
        )
        self.server_config = ServerConfig(
            emr_cluster_arn=self.emr_cluster_arn, default=True, verify_ssl=True
        )

    @patch.object(EMRPersistentUIClient, "cookie_header")
    @patch.object(EMRPersistentUIClient, "initialize")
    def test_spark_client_with_emr_cookies(self, mock_initialize, mock_cookie_header):
        """EMR auth is applied to the generated client as a Cookie header."""
        mock_initialize.return_value = ("https://example.com", MagicMock())
        mock_cookie_header.return_value = "session=abc123"

        emr_client = EMRPersistentUIClient(self.server_config)
        base_url, _ = emr_client.initialize()

        emr_server_config = self.server_config.model_copy()
        emr_server_config.url = base_url

        spark_client = SparkRestClient(emr_server_config)
        spark_client.configure_cookies(emr_client.cookie_header())

        # Cookie is installed on the generated client (no requests.Session).
        self.assertEqual(spark_client.base_url, "https://example.com/api/v1")
        self.assertEqual(spark_client._api.api_client.cookie, "session=abc123")

        # Calls route through the generated client.
        spark_client._api = MagicMock()
        spark_client._api.list_applications.return_value = []
        self.assertEqual(spark_client.list_applications(), [])
        spark_client._api.list_applications.assert_called_once()

    @patch("spark_history_mcp.core.app.EMRPersistentUIClient")
    @patch("spark_history_mcp.core.app.Config")
    def test_app_lifespan_with_emr_config(
        self, mock_config_class, mock_emr_client_class
    ):
        """Test app_lifespan context manager with EMR configuration."""
        import asyncio

        from mcp.server.fastmcp import FastMCP

        from spark_history_mcp.core.app import app_lifespan

        # Skip test if asyncio is not available or running in an environment that doesn't support it
        try:
            asyncio.get_event_loop()
        except (RuntimeError, ImportError):
            self.skipTest("Asyncio event loop not available")

        # Mock the EMR client
        mock_emr_client = MagicMock()
        mock_session = MagicMock()
        mock_session.headers = {}
        mock_emr_client.initialize.return_value = ("https://example.com", mock_session)
        mock_emr_client_class.return_value = mock_emr_client

        # Mock the FastMCP server
        mock_server = MagicMock(spec=FastMCP)

        # Set up the mock config
        mock_config = MagicMock()
        mock_config.servers = {
            "emr": ServerConfig(
                emr_cluster_arn=self.emr_cluster_arn, default=True, verify_ssl=True
            )
        }
        mock_config_class.return_value = mock_config

        # Use the app_lifespan context manager
        async def test_lifespan():
            async with app_lifespan(mock_server) as context:
                # Verify EMR client was created and initialized
                mock_emr_client_class.assert_called_once_with(
                    mock_config.servers["emr"]
                )
                mock_emr_client.initialize.assert_called_once()

                # Verify context has clients
                self.assertIn("emr", context.clients)
                self.assertEqual(context.default_client, context.clients["emr"])

        # Run the async test
        try:
            asyncio.run(test_lifespan())
        except RuntimeError as e:
            # Handle case where event loop is already running
            if "Event loop is running" in str(e):
                loop = asyncio.get_event_loop()
                loop.run_until_complete(test_lifespan())
            else:
                raise


if __name__ == "__main__":
    unittest.main()
