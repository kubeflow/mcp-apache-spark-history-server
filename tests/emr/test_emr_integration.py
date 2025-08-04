import os
import sys
import unittest
from unittest.mock import MagicMock, patch

import requests

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

    @patch.object(EMRPersistentUIClient, "initialize")
    def test_spark_client_with_emr_session(self, mock_initialize):
        """Test SparkRestClient using EMR Persistent UI session."""
        # Mock the EMR client initialization
        mock_session = MagicMock(spec=requests.Session)
        # Add headers attribute to the mock session
        mock_session.headers = {}
        mock_initialize.return_value = ("https://example.com", mock_session)

        # Create EMR client
        emr_client = EMRPersistentUIClient(self.server_config)

        # Initialize EMR client
        base_url, session = emr_client.initialize()

        # Create a modified server config with the base URL
        emr_server_config = self.server_config.model_copy()
        emr_server_config.url = base_url

        # Create SparkRestClient with the session
        spark_client = SparkRestClient(emr_server_config)
        spark_client.session = session

        # Verify the SparkRestClient is configured correctly
        self.assertEqual(spark_client.base_url, "https://example.com/api/v1")
        self.assertEqual(spark_client.session, mock_session)

        # Mock a response for list_applications
        mock_response = MagicMock()
        mock_response.json.return_value = []
        mock_response.raise_for_status.return_value = None
        mock_session.get.return_value = mock_response

        # Call a method on the SparkRestClient
        apps = spark_client.list_applications()

        # Verify the session was used for the request
        mock_session.get.assert_called_once()
        self.assertEqual(apps, [])

    @patch("spark_history_mcp.core.app.EMRPersistentUIClient")
    @patch("spark_history_mcp.core.app.Config.from_file")
    def test_app_lifespan_with_emr_config(
        self, mock_config_from_file, mock_emr_client_class
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
        mock_config_from_file.return_value = mock_config

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
