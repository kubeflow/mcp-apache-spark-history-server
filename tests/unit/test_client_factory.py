import unittest
from unittest.mock import MagicMock, patch

from spark_history_mcp.api.client_factory import (
    create_spark_client_from_config,
    create_spark_emr_client,
)
from spark_history_mcp.api.spark_client import SparkRestClient
from spark_history_mcp.config.config import ServerConfig


class TestAPIFactory(unittest.TestCase):
    """Test cases for the API factory functions."""

    def test_create_spark_client_from_config_regular(self):
        """Test creating a regular SparkRestClient from ServerConfig."""
        server_config = ServerConfig(url="http://localhost:18080")

        client = create_spark_client_from_config(server_config)

        self.assertIsInstance(client, SparkRestClient)
        # Note: SparkRestClient stores the config internally but doesn't expose it as server_config

    @patch("spark_history_mcp.api.client_factory.create_spark_emr_client")
    def test_create_spark_client_from_config_emr(self, mock_create_emr_client):
        """Test creating an EMR SparkRestClient from ServerConfig."""
        mock_emr_client = MagicMock(spec=SparkRestClient)
        mock_create_emr_client.return_value = mock_emr_client

        server_config = ServerConfig(
            url="http://localhost:18080",
            emr_cluster_arn="arn:aws:elasticmapreduce:us-east-1:123456789012:cluster/j-1234567890ABC",
        )

        client = create_spark_client_from_config(server_config)

        self.assertEqual(client, mock_emr_client)
        mock_create_emr_client.assert_called_once_with(
            "arn:aws:elasticmapreduce:us-east-1:123456789012:cluster/j-1234567890ABC",
            server_config,
        )

    @patch("spark_history_mcp.api.client_factory.EMRPersistentUIClient")
    @patch("spark_history_mcp.api.client_factory.SparkRestClient")
    def test_create_spark_emr_client_success(
        self, mock_spark_client_class, mock_emr_client_class
    ):
        """Test successful creation of EMR SparkRestClient."""
        # Mock EMR client
        mock_emr_client = MagicMock()
        mock_emr_client.initialize.return_value = ("http://emr-base-url", MagicMock())
        mock_emr_client_class.return_value = mock_emr_client

        # Mock SparkRestClient
        mock_spark_client = MagicMock()
        mock_spark_client_class.return_value = mock_spark_client

        emr_cluster_arn = (
            "arn:aws:elasticmapreduce:us-east-1:123456789012:cluster/j-1234567890ABC"
        )
        server_config = ServerConfig(url="http://original-url:18080")

        result = create_spark_emr_client(emr_cluster_arn, server_config)

        # Verify EMR client was created and initialized
        mock_emr_client_class.assert_called_once_with(emr_cluster_arn=emr_cluster_arn)
        mock_emr_client.initialize.assert_called_once()

        # Verify SparkRestClient was created with modified config
        self.assertEqual(result, mock_spark_client)
        # Check that the server config URL was modified
        call_args = mock_spark_client_class.call_args[0][0]
        self.assertEqual(call_args.url, "http://emr-base-url")

    @patch("spark_history_mcp.api.client_factory.EMRPersistentUIClient")
    @patch("spark_history_mcp.api.client_factory.SparkRestClient")
    def test_create_spark_emr_client_no_server_config(
        self, mock_spark_client_class, mock_emr_client_class
    ):
        """Test creating EMR SparkRestClient without server config."""
        # Mock EMR client
        mock_emr_client = MagicMock()
        mock_emr_client.initialize.return_value = ("http://emr-base-url", MagicMock())
        mock_emr_client_class.return_value = mock_emr_client

        # Mock SparkRestClient
        mock_spark_client = MagicMock()
        mock_spark_client_class.return_value = mock_spark_client

        emr_cluster_arn = (
            "arn:aws:elasticmapreduce:us-east-1:123456789012:cluster/j-1234567890ABC"
        )

        result = create_spark_emr_client(emr_cluster_arn, None)

        # Verify EMR client was created and initialized
        mock_emr_client_class.assert_called_once_with(emr_cluster_arn=emr_cluster_arn)
        mock_emr_client.initialize.assert_called_once()

        # Verify SparkRestClient was created with new config
        self.assertEqual(result, mock_spark_client)
        # Check that a default server config was created
        call_args = mock_spark_client_class.call_args[0][0]
        self.assertEqual(call_args.url, "http://emr-base-url")

    @patch("spark_history_mcp.api.client_factory.EMRPersistentUIClient")
    @patch("spark_history_mcp.api.client_factory.SparkRestClient")
    def test_create_spark_emr_client_session_assignment(
        self, mock_spark_client_class, mock_emr_client_class
    ):
        """Test that the authenticated session is properly assigned."""
        # Mock EMR client
        mock_emr_client = MagicMock()
        mock_session = MagicMock()
        mock_emr_client.initialize.return_value = ("http://emr-base-url", mock_session)
        mock_emr_client_class.return_value = mock_emr_client

        # Mock SparkRestClient
        mock_spark_client = MagicMock()
        mock_spark_client_class.return_value = mock_spark_client

        emr_cluster_arn = (
            "arn:aws:elasticmapreduce:us-east-1:123456789012:cluster/j-1234567890ABC"
        )

        result = create_spark_emr_client(emr_cluster_arn)

        # Verify the session was assigned to the SparkRestClient
        self.assertEqual(result.session, mock_session)


if __name__ == "__main__":
    unittest.main()
