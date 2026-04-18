import unittest
from unittest.mock import MagicMock, patch

import requests

from spark_history_mcp.api.spark_client import SparkRestClient
from spark_history_mcp.config.config import ServerConfig


class TestSparkClient(unittest.TestCase):
    def setUp(self):
        self.server_config = ServerConfig(url="http://spark-history-server:18080")
        self.client = SparkRestClient(self.server_config)

    @patch("spark_history_mcp.api.spark_client.requests.get")
    def test_list_applications(self, mock_get):
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
        apps = self.client.list_applications(status=["COMPLETED"], limit=10)

        mock_get.assert_called_once_with(
            "http://spark-history-server:18080/api/v1/applications",
            params={"status": ["COMPLETED"], "limit": 10},
            headers={"Accept": "application/json"},
            auth=None,
            timeout=30,
            verify=True,
            proxies=None,
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
    def test_list_applications_with_filters(self, mock_get):
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
        apps = self.client.list_applications(
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
            proxies=None,
        )

        self.assertEqual(len(apps), 1)

    @patch("spark_history_mcp.api.spark_client.requests.get")
    def test_list_applications_empty_response(self, mock_get):
        # Setup mock response with empty list
        mock_response = MagicMock()
        mock_response.json.return_value = []
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        # Call the method
        apps = self.client.list_applications()

        # Assertions
        mock_get.assert_called_once()
        self.assertEqual(len(apps), 0)

    @patch("spark_history_mcp.api.spark_client.requests.get")
    def test_fallback_behavior(self, mock_get):
        # First request fails with 404
        error_response = MagicMock()
        error_response.status_code = 404
        error_response.text = "no such app"
        http_error = requests.exceptions.HTTPError(response=error_response)
        error_response.raise_for_status.side_effect = http_error

        # Second request succeeds
        success_response = MagicMock()
        success_response.json.return_value = {"key": "value"}
        success_response.raise_for_status.return_value = None

        # Configure mock to return different responses
        mock_get.side_effect = [error_response, success_response]

        # Call method that should trigger EMR fallback
        result = self.client._get("applications/app-123/jobs")

        # Verify both URLs were tried
        mock_get.assert_any_call(
            "http://spark-history-server:18080/api/v1/applications/app-123/jobs",
            params=None,
            headers={"Accept": "application/json"},
            auth=None,
            timeout=30,
            verify=True,
            proxies=self.client.proxies,  # Use actual proxies value
        )
        mock_get.assert_any_call(
            "http://spark-history-server:18080/api/v1/applications/app-123/1/jobs",
            params=None,
            headers={"Accept": "application/json"},
            auth=None,
            timeout=30,
            verify=True,
            proxies=self.client.proxies,  # Use actual proxies value
        )

        # Verify we got the success response
        self.assertEqual(result, {"key": "value"})

    @patch("spark_history_mcp.api.spark_client.requests.get")
    def test_fallback_fail(self, mock_get):
        # Create 404 response
        error_response = MagicMock()
        error_response.status_code = 404
        error_response.text = "no such app"
        http_error = requests.exceptions.HTTPError(response=error_response)
        error_response.raise_for_status.side_effect = http_error

        # Both requests fail
        mock_get.side_effect = [error_response, error_response]

        # Call method and expect exception
        with self.assertRaises(requests.exceptions.HTTPError):
            self.client._get("applications/app-123/jobs")

        # Verify both URLs were tried
        self.assertEqual(mock_get.call_count, 2)

    @patch("spark_history_mcp.api.spark_client.requests.get")
    def test_proxy_configuration(self, mock_get):
        # Test with proxy enabled
        client = SparkRestClient(
            ServerConfig(url="http://spark-history-server:18080", use_proxy=True)
        )
        self.assertEqual(
            client.proxies,
            {"http": "socks5h://localhost:8157", "https": "socks5h://localhost:8157"},
        )

        # Test with proxy disabled
        client = SparkRestClient(
            ServerConfig(url="http://spark-history-server:18080", use_proxy=False)
        )
        self.assertIsNone(client.proxies)

    def test_url_modification(self):
        """Test the URL modification logic for different URL patterns"""
        test_cases = [
            # Test case 1: Standard URL that can be modified
            {
                "input": "http://ip-10-0-119-23.ec2.internal:18080/api/v1/applications/application_1753825693853_1003/allexecutors",
                "expected": "http://ip-10-0-119-23.ec2.internal:18080/api/v1/applications/application_1753825693853_1003/1/allexecutors",
            },
            # Test case 2: URL that already has an attempt number (should not be modified)
            {
                "input": "http://ip-10-0-119-23.ec2.internal:18080/api/v1/applications/application_1753825693853_1003/2/allexecutors",
                "expected": "http://ip-10-0-119-23.ec2.internal:18080/api/v1/applications/application_1753825693853_1003/2/allexecutors",
            },
            # Test case 3: URL without applications path (should not be modified)
            {
                "input": "http://ip-10-0-119-23.ec2.internal:18080/api/v1/metrics",
                "expected": "http://ip-10-0-119-23.ec2.internal:18080/api/v1/metrics",
            },
            # Test case 4: URL with another endpoint after application ID
            {
                "input": "http://ip-10-0-119-23.ec2.internal:18080/api/v1/applications/application_1753825693853_1003/stages",
                "expected": "http://ip-10-0-119-23.ec2.internal:18080/api/v1/applications/application_1753825693853_1003/1/stages",
            },
        ]

        for test_case in test_cases:
            input_url = test_case["input"]
            expected_url = test_case["expected"]
            modified_url = self.client._modify_url(input_url)
            self.assertEqual(
                modified_url,
                expected_url,
                f"Failed to correctly modify URL.\nInput: {input_url}\nExpected: {expected_url}\nGot: {modified_url}",
            )

    @patch("spark_history_mcp.api.spark_client.requests.get")
    def test_list_jobs_pagination(self, mock_get):
        """Test client-side pagination for list_jobs"""
        mock_response = MagicMock()
        mock_response.json.return_value = [
            {
                "jobId": i,
                "name": f"job-{i}",
                "status": "SUCCEEDED",
                "stageIds": [],
                "numTasks": 10,
                "numActiveTasks": 0,
                "numCompletedTasks": 10,
                "numSkippedTasks": 0,
                "numFailedTasks": 0,
                "numKilledTasks": 0,
                "numCompletedIndices": 10,
                "numActiveStages": 0,
                "numCompletedStages": 1,
                "numSkippedStages": 0,
                "numFailedStages": 0,
                "killedTasksSummary": {},
            }
            for i in range(5)
        ]
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        # No pagination — returns all
        all_jobs = self.client.list_jobs("app-123")
        self.assertEqual(len(all_jobs), 5)

        # With offset
        jobs = self.client.list_jobs("app-123", offset=2)
        self.assertEqual(len(jobs), 3)
        self.assertEqual(jobs[0].job_id, 2)

        # With offset + length
        jobs = self.client.list_jobs("app-123", offset=1, length=2)
        self.assertEqual(len(jobs), 2)
        self.assertEqual(jobs[0].job_id, 1)
        self.assertEqual(jobs[1].job_id, 2)

        # Offset beyond range
        jobs = self.client.list_jobs("app-123", offset=10)
        self.assertEqual(len(jobs), 0)

    @patch("spark_history_mcp.api.spark_client.requests.get")
    def test_list_executors_pagination(self, mock_get):
        """Test client-side pagination for list_executors"""
        mock_response = MagicMock()
        mock_response.json.return_value = [
            {
                "id": str(i),
                "hostPort": f"host-{i}:1234",
                "isActive": True,
                "rddBlocks": 0,
                "memoryUsed": 0,
                "diskUsed": 0,
                "totalCores": 4,
                "maxTasks": 4,
                "activeTasks": 0,
                "failedTasks": 0,
                "completedTasks": 100,
                "totalTasks": 100,
                "totalDuration": 5000,
                "totalGCTime": 100,
                "totalInputBytes": 0,
                "totalShuffleRead": 0,
                "totalShuffleWrite": 0,
                "isBlacklisted": False,
                "maxMemory": 1000,
                "addTime": "2023-01-01T00:00:00.000GMT",
                "executorLogs": {},
                "blacklistedInStages": [],
                "attributes": {},
                "resources": {},
                "resourceProfileId": 0,
                "isExcluded": False,
                "excludedInStages": [],
            }
            for i in range(10)
        ]
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        # No pagination
        all_executors = self.client.list_executors("app-123")
        self.assertEqual(len(all_executors), 10)

        # With length only
        executors = self.client.list_executors("app-123", length=3)
        self.assertEqual(len(executors), 3)
        self.assertEqual(executors[0].id, "0")

        # With offset + length
        executors = self.client.list_executors("app-123", offset=8, length=5)
        self.assertEqual(len(executors), 2)
        self.assertEqual(executors[0].id, "8")
