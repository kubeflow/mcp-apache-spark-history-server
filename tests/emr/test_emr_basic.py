import os
import sys
import unittest
from unittest.mock import MagicMock, patch

# Add root directory to Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from spark_history_mcp.api.emr_persistent_ui_client import EMRPersistentUIClient
from spark_history_mcp.config.config import ServerConfig


class TestEMRBasic(unittest.TestCase):
    """Basic tests for EMR Persistent UI Client without complex mocking."""

    def test_init(self):
        """Test basic initialization of the client."""
        # Using a fake ARN for testing
        emr_cluster_arn = (
            "arn:aws:elasticmapreduce:us-east-1:123456789012:cluster/j-2AXXXXXXGAPLF"
        )

        # Create server config
        server_config = ServerConfig(
            emr_cluster_arn=emr_cluster_arn,
            timeout=45,  # Test custom timeout
        )

        # Patch boto3.client to prevent actual AWS calls
        with patch("boto3.client") as mock_boto3_client:
            mock_client = MagicMock()
            mock_boto3_client.return_value = mock_client

            # Create the client
            client = EMRPersistentUIClient(server_config)

            # Basic assertions
            self.assertEqual(client.emr_cluster_arn, emr_cluster_arn)
            self.assertEqual(client.region, "us-east-1")
            self.assertEqual(client.timeout, 45)  # Test that timeout is set correctly
            self.assertIsNone(client.persistent_ui_id)
            self.assertIsNone(client.presigned_url)
            self.assertIsNone(client.base_url)

            # Check boto3 client was created correctly
            mock_boto3_client.assert_called_once_with("emr", region_name="us-east-1")


if __name__ == "__main__":
    unittest.main()
