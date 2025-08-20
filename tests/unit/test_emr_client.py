import unittest
from unittest.mock import MagicMock, patch

from botocore.exceptions import ClientError, NoCredentialsError

from spark_history_mcp.api.emr_client import EMRClient, EMRClusterNotFoundError


class TestEMRClient(unittest.TestCase):
    """Test cases for the EMRClient class."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_emr_client = MagicMock()
        self.mock_emr_client.meta.region_name = "us-east-1"

    @patch("boto3.client")
    def test_init_success(self, mock_boto_client):
        """Test successful EMRClient initialization."""
        mock_boto_client.return_value = self.mock_emr_client

        client = EMRClient()

        mock_boto_client.assert_called_once_with("emr", region_name=None)
        self.assertEqual(client.region_name, "us-east-1")

    @patch("boto3.client")
    def test_init_with_region(self, mock_boto_client):
        """Test EMRClient initialization with specific region."""
        mock_boto_client.return_value = self.mock_emr_client

        EMRClient(region_name="us-west-2")

        mock_boto_client.assert_called_once_with("emr", region_name="us-west-2")

    @patch("boto3.client")
    def test_init_no_credentials(self, mock_boto_client):
        """Test EMRClient initialization fails with no credentials."""
        mock_boto_client.side_effect = NoCredentialsError()

        with self.assertRaises(NoCredentialsError):
            EMRClient()

    @patch("boto3.client")
    def test_get_cluster_arn_by_id_success(self, mock_boto_client):
        """Test successful cluster ARN retrieval by ID."""
        mock_boto_client.return_value = self.mock_emr_client

        cluster_arn = (
            "arn:aws:elasticmapreduce:us-east-1:123456789012:cluster/j-1234567890ABC"
        )
        self.mock_emr_client.describe_cluster.return_value = {
            "Cluster": {"ClusterArn": cluster_arn}
        }

        client = EMRClient()
        result = client.get_cluster_arn_by_id("j-1234567890ABC")

        self.assertEqual(result, cluster_arn)
        self.mock_emr_client.describe_cluster.assert_called_once_with(
            ClusterId="j-1234567890ABC"
        )

    @patch("boto3.client")
    def test_get_cluster_arn_by_id_not_found(self, mock_boto_client):
        """Test cluster ARN retrieval by ID when cluster doesn't exist."""
        mock_boto_client.return_value = self.mock_emr_client

        error_response = {"Error": {"Code": "InvalidRequestException"}}
        self.mock_emr_client.describe_cluster.side_effect = ClientError(
            error_response, "DescribeCluster"
        )

        client = EMRClient()

        with self.assertRaises(EMRClusterNotFoundError) as context:
            client.get_cluster_arn_by_id("j-nonexistent")

        self.assertIn("not found", str(context.exception))

    @patch("boto3.client")
    def test_get_active_cluster_arn_by_name_success(self, mock_boto_client):
        """Test successful cluster ARN retrieval by name."""
        mock_boto_client.return_value = self.mock_emr_client

        cluster_arn = (
            "arn:aws:elasticmapreduce:us-east-1:123456789012:cluster/j-1234567890ABC"
        )

        # Mock list_clusters response
        self.mock_emr_client.list_clusters.return_value = {
            "Clusters": [
                {
                    "Id": "j-1234567890ABC",
                    "Name": "test-cluster",
                    "Status": {"State": "RUNNING"},
                }
            ]
        }

        # Mock describe_cluster response
        self.mock_emr_client.describe_cluster.return_value = {
            "Cluster": {"ClusterArn": cluster_arn}
        }

        client = EMRClient()
        result = client.get_active_cluster_arn_by_name("test-cluster")

        self.assertEqual(result, cluster_arn)

    @patch("boto3.client")
    def test_get_active_cluster_arn_by_name_not_found(self, mock_boto_client):
        """Test cluster ARN retrieval by name when cluster doesn't exist."""
        mock_boto_client.return_value = self.mock_emr_client

        # Mock empty list_clusters response
        self.mock_emr_client.list_clusters.return_value = {"Clusters": []}

        client = EMRClient()

        with self.assertRaises(EMRClusterNotFoundError) as context:
            client.get_active_cluster_arn_by_name("nonexistent-cluster")

        self.assertIn("No cluster found", str(context.exception))

    @patch("boto3.client")
    def test_get_active_cluster_arn_by_name_multiple_found(self, mock_boto_client):
        """Test cluster ARN retrieval by name when multiple clusters exist."""
        mock_boto_client.return_value = self.mock_emr_client

        # Mock list_clusters response with multiple clusters
        self.mock_emr_client.list_clusters.return_value = {
            "Clusters": [
                {
                    "Id": "j-1234567890ABC",
                    "Name": "test-cluster",
                    "Status": {"State": "RUNNING"},
                },
                {
                    "Id": "j-0987654321DEF",
                    "Name": "test-cluster",
                    "Status": {"State": "WAITING"},
                },
            ]
        }

        client = EMRClient()

        with self.assertRaises(EMRClusterNotFoundError) as context:
            client.get_active_cluster_arn_by_name("test-cluster")

        self.assertIn("Multiple clusters found", str(context.exception))

    @patch("boto3.client")
    def test_get_cluster_arn_with_cluster_id(self, mock_boto_client):
        """Test get_cluster_arn with cluster ID."""
        mock_boto_client.return_value = self.mock_emr_client

        cluster_arn = (
            "arn:aws:elasticmapreduce:us-east-1:123456789012:cluster/j-1234567890ABC"
        )
        self.mock_emr_client.describe_cluster.return_value = {
            "Cluster": {"ClusterArn": cluster_arn}
        }

        client = EMRClient()
        result = client.get_cluster_arn("j-1234567890ABC")

        self.assertEqual(result, cluster_arn)

    @patch("boto3.client")
    def test_get_cluster_arn_with_cluster_name(self, mock_boto_client):
        """Test get_cluster_arn with cluster name."""
        mock_boto_client.return_value = self.mock_emr_client

        cluster_arn = (
            "arn:aws:elasticmapreduce:us-east-1:123456789012:cluster/j-1234567890ABC"
        )

        # Mock list_clusters response
        self.mock_emr_client.list_clusters.return_value = {
            "Clusters": [
                {
                    "Id": "j-1234567890ABC",
                    "Name": "test-cluster",
                    "Status": {"State": "RUNNING"},
                }
            ]
        }

        # Mock describe_cluster response
        self.mock_emr_client.describe_cluster.return_value = {
            "Cluster": {"ClusterArn": cluster_arn}
        }

        client = EMRClient()
        result = client.get_cluster_arn("test-cluster")

        self.assertEqual(result, cluster_arn)

    @patch("boto3.client")
    def test_get_cluster_details_success(self, mock_boto_client):
        """Test successful cluster details retrieval."""
        mock_boto_client.return_value = self.mock_emr_client

        cluster_arn = (
            "arn:aws:elasticmapreduce:us-east-1:123456789012:cluster/j-1234567890ABC"
        )
        cluster_details = {
            "Id": "j-1234567890ABC",
            "Name": "test-cluster",
            "ClusterArn": cluster_arn,
            "Status": {"State": "RUNNING"},
        }

        self.mock_emr_client.describe_cluster.return_value = {
            "Cluster": cluster_details
        }

        client = EMRClient()
        result = client.get_cluster_details("j-1234567890ABC")

        self.assertEqual(result, cluster_details)

    @patch("boto3.client")
    def test_find_active_clusters_by_name_pagination(self, mock_boto_client):
        """Test _find_active_clusters_by_name with pagination."""
        mock_boto_client.return_value = self.mock_emr_client

        # Mock paginated responses
        self.mock_emr_client.list_clusters.side_effect = [
            {
                "Clusters": [
                    {
                        "Id": "j-1234567890ABC",
                        "Name": "other-cluster",
                        "Status": {"State": "RUNNING"},
                    }
                ],
                "Marker": "next-page-token",
            },
            {
                "Clusters": [
                    {
                        "Id": "j-0987654321DEF",
                        "Name": "test-cluster",
                        "Status": {"State": "RUNNING"},
                    }
                ]
            },
        ]

        client = EMRClient()
        result = client._find_active_clusters_by_name("test-cluster")

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["Id"], "j-0987654321DEF")
        self.assertEqual(self.mock_emr_client.list_clusters.call_count, 2)


if __name__ == "__main__":
    unittest.main()
