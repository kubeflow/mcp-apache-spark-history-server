import unittest
from unittest.mock import MagicMock, patch

from spark_history_mcp.api.spark_client import SparkRestClient
from spark_history_mcp.config.config import ServerConfig


class TestSparkClient(unittest.TestCase):
    def setUp(self):
        self.server_config = ServerConfig(url="http://spark-history-server:18080")
        self.client = SparkRestClient(self.server_config)

    @patch("spark_history_mcp.api.spark_client.requests.get")
    def test_get_applications(self, mock_get):
        # Setup mock response
        mock_response = MagicMock()
        mock_response.json.return_value = [
            {
                "id": "app-20230101123456-0001",
                "name": "Test Spark App",
                "coresGranted": 8,
                "maxCores": 16,
                "coresPerExecutor": 2,
                "memoryPerExecutorMB": 4096,
                "attempts": [
                    {
                        "attemptId": "1",
                        "startTime": "2023-01-01T12:34:56.789GMT",
                        "endTime": "2023-01-01T13:34:56.789GMT",
                        "lastUpdated": "2023-01-01T13:34:56.789GMT",
                        "duration": 3600000,
                        "sparkUser": "spark",
                        "appSparkVersion": "3.3.0",
                        "completed": True,
                    }
                ],
            }
        ]
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        # Call the method
        apps = self.client.get_applications(status=["COMPLETED"], limit=10)

        # Assertions
        mock_get.assert_called_once_with(
            "http://spark-history-server:18080/api/v1/applications",
            params={"status": ["COMPLETED"], "limit": 10},
            headers={"Accept": "application/json"},
            auth=None,
            timeout=30,
            verify=True,
        )

        self.assertEqual(len(apps), 1)
        self.assertEqual(apps[0].id, "app-20230101123456-0001")
        self.assertEqual(apps[0].name, "Test Spark App")
        self.assertEqual(apps[0].cores_granted, 8)
        self.assertEqual(len(apps[0].attempts), 1)
        self.assertEqual(apps[0].attempts[0].attempt_id, "1")
        self.assertEqual(apps[0].attempts[0].spark_user, "spark")
        self.assertTrue(apps[0].attempts[0].completed)

    @patch("spark_history_mcp.api.spark_client.requests.get")
    def test_get_applications_with_filters(self, mock_get):
        # Setup mock response
        mock_response = MagicMock()
        mock_response.json.return_value = [
            {
                "id": "app-20230101123456-0001",
                "name": "Test Spark App",
                "attempts": [
                    {
                        "attemptId": "1",
                        "startTime": "2023-01-01T12:34:56.789GMT",
                        "endTime": "2023-01-01T13:34:56.789GMT",
                        "lastUpdated": "2023-01-01T13:34:56.789GMT",
                        "duration": 3600000,
                        "sparkUser": "spark",
                        "completed": True,
                    }
                ],
            }
        ]
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        # Call the method with various filters
        apps = self.client.get_applications(
            status=["COMPLETED"], min_date="2023-01-01", max_date="2023-01-02", limit=5
        )

        # Assertions
        mock_get.assert_called_once_with(
            "http://spark-history-server:18080/api/v1/applications",
            params={
                "status": ["COMPLETED"],
                "minDate": "2023-01-01",
                "maxDate": "2023-01-02",
                "limit": 5,
            },
            headers={"Accept": "application/json"},
            auth=None,
            timeout=30,
            verify=True,
        )

        self.assertEqual(len(apps), 1)

    @patch("spark_history_mcp.api.spark_client.requests.get")
    def test_get_applications_empty_response(self, mock_get):
        # Setup mock response with empty list
        mock_response = MagicMock()
        mock_response.json.return_value = []
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        # Call the method
        apps = self.client.get_applications()

        # Assertions
        mock_get.assert_called_once()
        self.assertEqual(len(apps), 0)
