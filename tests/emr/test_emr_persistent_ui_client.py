import os
import sys
import unittest
from unittest.mock import MagicMock, patch

import requests
from botocore.exceptions import ClientError

# Add root directory to Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))


from spark_history_mcp.api.emr_persistent_ui_client import EMRPersistentUIClient
from spark_history_mcp.config.config import ServerConfig


class TestEMRPersistentUIClient(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures."""
        self.emr_cluster_arn = (
            "arn:aws:elasticmapreduce:us-east-1:123456789012:cluster/j-2AXXXXXXGAPLF"
        )
        self.server_config = ServerConfig(
            emr_cluster_arn=self.emr_cluster_arn, timeout=30
        )
        self.client = EMRPersistentUIClient(self.server_config)

        # Mock the boto3 client
        self.mock_emr_client = MagicMock()
        self.client.emr_client = self.mock_emr_client

        # Mock the requests session
        self.mock_session = MagicMock()
        # Configure the mock session with a real dictionary for headers
        self.mock_session.headers = {}
        self.client.session = self.mock_session

    def test_init(self):
        """Test initialization of the EMR Persistent UI client."""
        server_config = ServerConfig(emr_cluster_arn=self.emr_cluster_arn)
        client = EMRPersistentUIClient(server_config)

        # Check that the client was initialized correctly
        self.assertEqual(client.emr_cluster_arn, self.emr_cluster_arn)
        self.assertEqual(client.region, "us-east-1")
        self.assertEqual(client.timeout, 30)  # Default timeout
        self.assertIsNone(client.persistent_ui_id)
        self.assertIsNone(client.presigned_url)
        self.assertIsNone(client.base_url)
        self.assertIsInstance(client.session, requests.Session)

    @patch("boto3.client")
    def test_init_with_boto3_client(self, mock_boto3_client):
        """Test initialization with boto3 client creation."""
        # Create a mock boto3 client
        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client

        # Create a new client instance (don't use self.client which is already set up)
        server_config = ServerConfig(emr_cluster_arn=self.emr_cluster_arn)
        client = EMRPersistentUIClient(server_config)

        # Check that boto3 client was created with correct region
        mock_boto3_client.assert_called_once_with("emr", region_name="us-east-1")
        self.assertEqual(client.emr_client, mock_client)

    def test_create_persistent_app_ui_success(self):
        """Test successful creation of persistent app UI."""
        # Mock the response from create_persistent_app_ui
        mock_response = {
            "PersistentAppUIId": "test-ui-id",
            "RuntimeRoleEnabledCluster": True,
        }
        self.mock_emr_client.create_persistent_app_ui.return_value = mock_response

        # Call the method
        response = self.client.create_persistent_app_ui()

        # Check that the method was called with correct parameters
        self.mock_emr_client.create_persistent_app_ui.assert_called_once_with(
            TargetResourceArn=self.emr_cluster_arn
        )

        # Check that the response was processed correctly
        self.assertEqual(response, mock_response)
        self.assertEqual(self.client.persistent_ui_id, "test-ui-id")

    def test_create_persistent_app_ui_client_error(self):
        """Test handling of ClientError during persistent app UI creation."""
        # Mock ClientError
        error_response = {
            "Error": {"Code": "ValidationException", "Message": "Invalid ARN"}
        }
        self.mock_emr_client.create_persistent_app_ui.side_effect = ClientError(
            error_response, "CreatePersistentAppUI"
        )

        # Check that the exception is propagated
        with self.assertRaises(ClientError) as context:
            self.client.create_persistent_app_ui()

        # Verify the error details
        self.assertEqual(
            context.exception.response["Error"]["Code"], "ValidationException"
        )
        self.assertEqual(context.exception.response["Error"]["Message"], "Invalid ARN")

    def test_describe_persistent_app_ui_success(self):
        """Test successful description of persistent app UI."""
        # Set up the client with a persistent UI ID
        self.client.persistent_ui_id = "test-ui-id"

        # Mock the response from describe_persistent_app_ui
        mock_response = {
            "PersistentAppUI": {
                "PersistentAppUIId": "test-ui-id",
                "PersistentAppUIStatus": "ATTACHED",
                "CreationTime": "2025-07-14T12:00:00Z",
            }
        }
        self.mock_emr_client.describe_persistent_app_ui.return_value = mock_response

        # Call the method
        response = self.client.describe_persistent_app_ui()

        # Check that the method was called with correct parameters
        self.mock_emr_client.describe_persistent_app_ui.assert_called_once_with(
            PersistentAppUIId="test-ui-id"
        )

        # Check that the response was processed correctly
        self.assertEqual(response, mock_response)

    def test_describe_persistent_app_ui_no_id(self):
        """Test describe_persistent_app_ui with no persistent UI ID."""
        # Ensure no persistent UI ID is set
        self.client.persistent_ui_id = None

        # Check that ValueError is raised
        with self.assertRaises(ValueError) as context:
            self.client.describe_persistent_app_ui()

        self.assertIn("No persistent UI ID available", str(context.exception))

    def test_describe_persistent_app_ui_client_error(self):
        """Test handling of ClientError during persistent app UI description."""
        # Set up the client with a persistent UI ID
        self.client.persistent_ui_id = "test-ui-id"

        # Mock ClientError
        error_response = {
            "Error": {
                "Code": "ResourceNotFoundException",
                "Message": "Persistent UI not found",
            }
        }
        self.mock_emr_client.describe_persistent_app_ui.side_effect = ClientError(
            error_response, "DescribePersistentAppUI"
        )

        # Check that the exception is propagated
        with self.assertRaises(ClientError) as context:
            self.client.describe_persistent_app_ui()

        # Verify the error details
        self.assertEqual(
            context.exception.response["Error"]["Code"], "ResourceNotFoundException"
        )
        self.assertEqual(
            context.exception.response["Error"]["Message"], "Persistent UI not found"
        )

    def test_get_presigned_url_success(self):
        """Test successful retrieval of presigned URL."""
        # Set up the client with a persistent UI ID
        self.client.persistent_ui_id = "test-ui-id"

        # Mock the response from get_persistent_app_ui_presigned_url
        mock_response = {
            "PresignedURLReady": True,
            "PresignedURL": "https://example.com/presigned-url",
        }
        self.mock_emr_client.get_persistent_app_ui_presigned_url.return_value = (
            mock_response
        )

        # Call the method
        url = self.client.get_presigned_url()

        # Check that the method was called with correct parameters
        self.mock_emr_client.get_persistent_app_ui_presigned_url.assert_called_once_with(
            PersistentAppUIId="test-ui-id", PersistentAppUIType="SHS"
        )

        # Check that the response was processed correctly
        self.assertEqual(url, "https://example.com/presigned-url")
        self.assertEqual(self.client.presigned_url, "https://example.com/presigned-url")
        self.assertEqual(self.client.base_url, "https://example.com/shs")

    def test_get_presigned_url_no_id(self):
        """Test get_presigned_url with no persistent UI ID."""
        # Ensure no persistent UI ID is set
        self.client.persistent_ui_id = None

        # Check that ValueError is raised
        with self.assertRaises(ValueError) as context:
            self.client.get_presigned_url()

        self.assertIn("No persistent UI ID available", str(context.exception))

    def test_get_presigned_url_client_error(self):
        """Test handling of ClientError during presigned URL retrieval."""
        # Set up the client with a persistent UI ID
        self.client.persistent_ui_id = "test-ui-id"

        # Mock ClientError
        error_response = {
            "Error": {
                "Code": "ResourceNotFoundException",
                "Message": "Persistent UI not found",
            }
        }
        self.mock_emr_client.get_persistent_app_ui_presigned_url.side_effect = (
            ClientError(error_response, "GetPersistentAppUIPresignedUrl")
        )

        # Check that the exception is propagated
        with self.assertRaises(ClientError) as context:
            self.client.get_presigned_url()

        # Verify the error details
        self.assertEqual(
            context.exception.response["Error"]["Code"], "ResourceNotFoundException"
        )
        self.assertEqual(
            context.exception.response["Error"]["Message"], "Persistent UI not found"
        )

    def test_setup_http_session_success(self):
        """Test successful HTTP session setup."""
        # Set up the client with a presigned URL
        self.client.presigned_url = "https://example.com/presigned-url"

        # Mock the response from session.get
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.raise_for_status.return_value = None
        self.mock_session.get.return_value = mock_response

        # Call the method
        session = self.client.setup_http_session()

        # Check that the session was configured correctly
        self.assertEqual(session, self.mock_session)
        self.mock_session.get.assert_called_once_with(
            "https://example.com/presigned-url",
            timeout=self.client.timeout,
            allow_redirects=True,
        )

        # Check that headers were set correctly
        self.assertIn("User-Agent", self.mock_session.headers)
        self.assertIn("Accept", self.mock_session.headers)
        self.assertIn("Accept-Language", self.mock_session.headers)
        self.assertIn("Accept-Encoding", self.mock_session.headers)
        self.assertIn("Connection", self.mock_session.headers)
        self.assertIn("Upgrade-Insecure-Requests", self.mock_session.headers)

    def test_setup_http_session_no_url(self):
        """Test setup_http_session with no presigned URL."""
        # Ensure no presigned URL is set
        self.client.presigned_url = None

        # Check that ValueError is raised
        with self.assertRaises(ValueError) as context:
            self.client.setup_http_session()

        self.assertIn("No presigned URL available", str(context.exception))

    def test_setup_http_session_request_error(self):
        """Test handling of request error during HTTP session setup."""
        # Set up the client with a presigned URL
        self.client.presigned_url = "https://example.com/presigned-url"

        # Mock request exception
        self.mock_session.get.side_effect = requests.exceptions.RequestException(
            "Connection error"
        )

        # Check that the exception is propagated
        with self.assertRaises(requests.exceptions.RequestException) as context:
            self.client.setup_http_session()

        self.assertEqual(str(context.exception), "Connection error")

    @patch.object(EMRPersistentUIClient, "create_persistent_app_ui")
    @patch.object(EMRPersistentUIClient, "describe_persistent_app_ui")
    @patch.object(EMRPersistentUIClient, "get_presigned_url")
    @patch.object(EMRPersistentUIClient, "setup_http_session")
    def test_initialize_success(
        self, mock_setup_session, mock_get_url, mock_describe, mock_create
    ):
        """Test successful initialization of the EMR Persistent UI client."""
        # Mock the responses
        mock_create.return_value = {"PersistentAppUIId": "test-ui-id"}
        mock_describe.return_value = {
            "PersistentAppUI": {"PersistentAppUIStatus": "ATTACHED"}
        }
        mock_get_url.return_value = "https://example.com/presigned-url"
        self.client.base_url = "https://example.com"
        mock_setup_session.return_value = self.mock_session

        # Call the method
        base_url, session = self.client.initialize()

        # Check that all methods were called
        mock_create.assert_called_once()
        mock_describe.assert_called_once()
        mock_get_url.assert_called_once()
        mock_setup_session.assert_called_once()

        # Check that the return values are correct
        self.assertEqual(base_url, "https://example.com")
        self.assertEqual(session, self.mock_session)

    @patch.object(EMRPersistentUIClient, "create_persistent_app_ui")
    @patch.object(EMRPersistentUIClient, "describe_persistent_app_ui")
    def test_initialize_invalid_status(self, mock_describe, mock_create):
        """Test initialization with invalid persistent UI status."""
        # Mock the responses
        mock_create.return_value = {"PersistentAppUIId": "test-ui-id"}
        mock_describe.return_value = {
            "PersistentAppUI": {"PersistentAppUIStatus": "PENDING"}
        }

        # Check that ValueError is raised
        with self.assertRaises(ValueError) as context:
            self.client.initialize()

        self.assertIn(
            "EMR Persistent UI status is PENDING, expected ATTACHED or STARTING",
            str(context.exception),
        )

    @patch.object(EMRPersistentUIClient, "create_persistent_app_ui")
    @patch.object(EMRPersistentUIClient, "describe_persistent_app_ui")
    @patch.object(EMRPersistentUIClient, "get_presigned_url")
    @patch.object(EMRPersistentUIClient, "setup_http_session")
    @patch("time.sleep")  # Mock sleep to avoid waiting in tests
    def test_initialize_with_starting_status(
        self, mock_sleep, mock_setup_session, mock_get_url, mock_describe, mock_create
    ):
        """Test initialization with STARTING status that changes to ATTACHED."""
        # Mock the responses
        mock_create.return_value = {"PersistentAppUIId": "test-ui-id"}

        # First call returns STARTING, second call returns ATTACHED
        mock_describe.side_effect = [
            {"PersistentAppUI": {"PersistentAppUIStatus": "STARTING"}},
            {"PersistentAppUI": {"PersistentAppUIStatus": "ATTACHED"}},
        ]

        mock_get_url.return_value = "https://example.com/presigned-url"
        self.client.base_url = "https://example.com/shs"
        mock_setup_session.return_value = self.mock_session

        # Call the method
        base_url, session = self.client.initialize()

        # Check that describe was called twice (once for STARTING, once for ATTACHED)
        self.assertEqual(mock_describe.call_count, 2)

        # Check that sleep was called once (after the STARTING status)
        mock_sleep.assert_called_once()

        # Check that the other methods were called
        mock_create.assert_called_once()
        mock_get_url.assert_called_once()
        mock_setup_session.assert_called_once()

        # Check that the return values are correct
        self.assertEqual(base_url, "https://example.com/shs")
        self.assertEqual(session, self.mock_session)

    @patch.object(EMRPersistentUIClient, "create_persistent_app_ui")
    @patch.object(EMRPersistentUIClient, "describe_persistent_app_ui")
    @patch.object(EMRPersistentUIClient, "get_presigned_url")
    @patch.object(EMRPersistentUIClient, "setup_http_session")
    @patch("time.sleep")  # Mock sleep to avoid waiting in tests
    def test_initialize_timeout_with_starting_status(
        self, mock_sleep, mock_setup_session, mock_get_url, mock_describe, mock_create
    ):
        """Test initialization with STARTING status that doesn't change to ATTACHED within timeout."""
        # Mock the responses
        mock_create.return_value = {"PersistentAppUIId": "test-ui-id"}

        # Always return STARTING status
        mock_describe.return_value = {
            "PersistentAppUI": {"PersistentAppUIStatus": "STARTING"}
        }

        # Set a small max_wait_time to make the test run faster
        with patch.dict(self.client.__dict__, {"_max_wait_time_for_testing": 0}):
            # Call the method and expect ValueError
            with self.assertRaises(ValueError) as context:
                self.client.initialize()

            # Verify the error message
            self.assertIn(
                "EMR Persistent UI status is still STARTING after waiting",
                str(context.exception),
            )
            self.assertIn("expected ATTACHED", str(context.exception))

        # Verify that describe_persistent_app_ui was called at least once
        mock_describe.assert_called()

        # Verify that sleep was called at least once
        mock_sleep.assert_called()


if __name__ == "__main__":
    unittest.main()
