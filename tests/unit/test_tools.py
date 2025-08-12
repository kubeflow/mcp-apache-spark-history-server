import unittest
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

from spark_history_mcp.api.spark_client import SparkRestClient
from spark_history_mcp.models.spark_types import (
    ApplicationInfo,
    ExecutionData,
    JobData,
    StageData,
    TaskMetricDistributions,
)
from spark_history_mcp.tools.tools import (
    get_application,
    get_client_or_default,
    get_stage,
    get_stage_task_summary,
    list_jobs,
    list_slowest_jobs,
    list_slowest_sql_queries,
    list_slowest_stages,
    list_stages,
)


class TestTools(unittest.TestCase):
    def setUp(self):
        # Create mock context
        self.mock_ctx = MagicMock()
        self.mock_lifespan_context = MagicMock()
        self.mock_ctx.request_context.lifespan_context = self.mock_lifespan_context

        # Create mock clients
        self.mock_client1 = MagicMock(spec=SparkRestClient)
        self.mock_client2 = MagicMock(spec=SparkRestClient)

        # Set up clients dictionary
        self.mock_lifespan_context.clients = {
            "server1": self.mock_client1,
            "server2": self.mock_client2,
        }

    def test_get_client_with_name(self):
        """Test getting a client by name"""
        self.mock_lifespan_context.default_client = self.mock_client1

        # Get client by name
        client = get_client_or_default(self.mock_ctx, "server2")

        # Should return the requested client
        self.assertEqual(client, self.mock_client2)

    def test_get_default_client(self):
        """Test getting the default client when no name is provided"""
        self.mock_lifespan_context.default_client = self.mock_client1

        # Get client without specifying name
        client = get_client_or_default(self.mock_ctx)

        # Should return the default client
        self.assertEqual(client, self.mock_client1)

    def test_get_client_not_found_with_default(self):
        """Test behavior when requested client is not found but default exists"""
        self.mock_lifespan_context.default_client = self.mock_client1

        # Get non-existent client
        client = get_client_or_default(self.mock_ctx, "non_existent_server")

        # Should fall back to default client
        self.assertEqual(client, self.mock_client1)

    def test_no_client_found(self):
        """Test error when no client is found and no default exists"""
        self.mock_lifespan_context.default_client = None

        # Try to get non-existent client with no default
        with self.assertRaises(ValueError) as context:
            get_client_or_default(self.mock_ctx, "non_existent_server")

        self.assertIn("No Spark client found", str(context.exception))

    def test_no_default_client(self):
        """Test error when no name is provided and no default exists"""
        self.mock_lifespan_context.default_client = None

        # Try to get default client when none exists
        with self.assertRaises(ValueError) as context:
            get_client_or_default(self.mock_ctx)

        self.assertIn("No Spark client found", str(context.exception))

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_get_slowest_jobs_empty(self, mock_get_client):
        """Test list_slowest_jobs when no jobs are found"""
        # Setup mock client
        mock_client = MagicMock()
        mock_client.list_jobs.return_value = []
        mock_get_client.return_value = mock_client

        # Call the function
        result = list_slowest_jobs("app-123", n=3)

        # Verify results
        self.assertEqual(result, [])
        mock_client.list_jobs.assert_called_once_with(app_id="app-123")

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_get_slowest_jobs_exclude_running(self, mock_get_client):
        """Test list_slowest_jobs excluding running jobs"""
        # Setup mock client and jobs
        mock_client = MagicMock()

        # Create mock jobs with different durations and statuses
        job1 = MagicMock(spec=JobData)
        job1.status = "RUNNING"
        job1.submission_time = datetime.now() - timedelta(minutes=10)
        job1.completion_time = None

        job2 = MagicMock(spec=JobData)
        job2.status = "SUCCEEDED"
        job2.submission_time = datetime.now() - timedelta(minutes=5)
        job2.completion_time = datetime.now() - timedelta(minutes=3)  # 2 min duration

        job3 = MagicMock(spec=JobData)
        job3.status = "SUCCEEDED"
        job3.submission_time = datetime.now() - timedelta(minutes=10)
        job3.completion_time = datetime.now() - timedelta(minutes=5)  # 5 min duration

        job4 = MagicMock(spec=JobData)
        job4.status = "FAILED"
        job4.submission_time = datetime.now() - timedelta(minutes=8)
        job4.completion_time = datetime.now() - timedelta(minutes=7)  # 1 min duration

        mock_client.list_jobs.return_value = [job1, job2, job3, job4]
        mock_get_client.return_value = mock_client

        # Call the function with include_running=False (default)
        result = list_slowest_jobs("app-123", n=2)

        # Verify results - should return job3 and job2 (in that order)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0], job3)  # Longest duration (5 min)
        self.assertEqual(result[1], job2)  # Second longest (2 min)

        # Running job (job1) should be excluded
        self.assertNotIn(job1, result)

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_get_slowest_jobs_include_running(self, mock_get_client):
        """Test list_slowest_jobs including running jobs"""
        # Setup mock client and jobs
        mock_client = MagicMock()

        # Create mock jobs with different durations and statuses
        job1 = MagicMock(spec=JobData)
        job1.status = "RUNNING"
        job1.submission_time = datetime.now() - timedelta(
            minutes=20
        )  # Running for 20 min
        job1.completion_time = None

        job2 = MagicMock(spec=JobData)
        job2.status = "SUCCEEDED"
        job2.submission_time = datetime.now() - timedelta(minutes=5)
        job2.completion_time = datetime.now() - timedelta(minutes=3)  # 2 min duration

        job3 = MagicMock(spec=JobData)
        job3.status = "SUCCEEDED"
        job3.submission_time = datetime.now() - timedelta(minutes=10)
        job3.completion_time = datetime.now() - timedelta(minutes=5)  # 5 min duration

        mock_client.list_jobs.return_value = [job1, job2, job3]
        mock_get_client.return_value = mock_client

        # Call the function with include_running=True
        result = list_slowest_jobs("app-123", include_running=True, n=2)

        # Verify results - should include the running job
        self.assertEqual(len(result), 2)
        # Running job should be included but will have duration 0 since completion_time is None
        # So job3 and job2 should be returned
        self.assertEqual(result[0], job3)
        self.assertEqual(result[1], job2)

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_get_slowest_jobs_limit_results(self, mock_get_client):
        """Test list_slowest_jobs limits results to n"""
        # Setup mock client and jobs
        mock_client = MagicMock()

        # Create 5 mock jobs with different durations
        jobs = []
        for i in range(5):
            job = MagicMock(spec=JobData)
            job.status = "SUCCEEDED"
            job.submission_time = datetime.now() - timedelta(minutes=10)
            # Different completion times to create different durations
            job.completion_time = datetime.now() - timedelta(minutes=10 - i)
            jobs.append(job)

        mock_client.list_jobs.return_value = jobs
        mock_get_client.return_value = mock_client

        # Call the function with n=3
        result = list_slowest_jobs("app-123", n=3)

        # Verify results - should return only 3 jobs
        self.assertEqual(len(result), 3)

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_get_stage_with_attempt_id(self, mock_get_client):
        """Test get_stage with a specific attempt ID"""
        # Setup mock client
        mock_client = MagicMock()
        mock_stage = MagicMock(spec=StageData)
        mock_stage.task_metrics_distributions = None
        # Explicitly set the attempt_id attribute on the mock
        mock_stage.attempt_id = 0
        mock_client.get_stage_attempt.return_value = mock_stage
        mock_get_client.return_value = mock_client

        # Call the function with attempt_id
        result = get_stage("app-123", stage_id=1, attempt_id=0)

        # Verify results
        self.assertEqual(result, mock_stage)
        mock_client.get_stage_attempt.assert_called_once_with(
            app_id="app-123",
            stage_id=1,
            attempt_id=0,
            details=False,
            with_summaries=False,
        )

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_get_stage_without_attempt_id_single_stage(self, mock_get_client):
        """Test get_stage without attempt ID when a single stage is returned"""
        # Setup mock client
        mock_client = MagicMock()
        mock_stage = MagicMock(spec=StageData)
        mock_stage.task_metrics_distributions = None
        # Explicitly set the attempt_id attribute on the mock
        mock_stage.attempt_id = 0
        mock_client.list_stage_attempts.return_value = mock_stage
        mock_get_client.return_value = mock_client

        # Call the function without attempt_id
        result = get_stage("app-123", stage_id=1)

        # Verify results
        self.assertEqual(result, mock_stage)
        mock_client.list_stage_attempts.assert_called_once_with(
            app_id="app-123",
            stage_id=1,
            details=False,
            with_summaries=False,
        )

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_get_stage_without_attempt_id_multiple_stages(self, mock_get_client):
        """Test get_stage without attempt ID when multiple stages are returned"""
        # Setup mock client
        mock_client = MagicMock()

        # Create mock stages with different attempt IDs
        mock_stage1 = MagicMock(spec=StageData)
        mock_stage1.attempt_id = 0
        mock_stage1.task_metrics_distributions = None

        mock_stage2 = MagicMock(spec=StageData)
        mock_stage2.attempt_id = 1
        mock_stage2.task_metrics_distributions = None

        mock_client.list_stage_attempts.return_value = [mock_stage1, mock_stage2]
        mock_get_client.return_value = mock_client

        # Call the function without attempt_id
        result = get_stage("app-123", stage_id=1)

        # Verify results - should return the stage with highest attempt_id
        self.assertEqual(result, mock_stage2)
        mock_client.list_stage_attempts.assert_called_once_with(
            app_id="app-123",
            stage_id=1,
            details=False,
            with_summaries=False,
        )

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_get_stage_with_summaries_missing_metrics(self, mock_get_client):
        """Test get_stage with summaries when metrics distributions are missing"""
        # Setup mock client
        mock_client = MagicMock()
        mock_stage = MagicMock(spec=StageData)
        # Explicitly set the attempt_id attribute on the mock
        mock_stage.attempt_id = 0
        # Set task_metrics_distributions to None to trigger the fetch
        mock_stage.task_metrics_distributions = None

        mock_summary = MagicMock(spec=TaskMetricDistributions)

        mock_client.get_stage_attempt.return_value = mock_stage
        mock_client.get_stage_task_summary.return_value = mock_summary
        mock_get_client.return_value = mock_client

        # Call the function with with_summaries=True
        result = get_stage("app-123", stage_id=1, attempt_id=0, with_summaries=True)

        # Verify results
        self.assertEqual(result, mock_stage)
        self.assertEqual(result.task_metrics_distributions, mock_summary)

        mock_client.get_stage_attempt.assert_called_once_with(
            app_id="app-123",
            stage_id=1,
            attempt_id=0,
            details=False,
            with_summaries=True,
        )

        mock_client.get_stage_task_summary.assert_called_once_with(
            app_id="app-123",
            stage_id=1,
            attempt_id=0,
        )

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_get_stage_no_stages_found(self, mock_get_client):
        """Test get_stage when no stages are found"""
        # Setup mock client
        mock_client = MagicMock()
        mock_client.list_stage_attempts.return_value = []
        mock_get_client.return_value = mock_client

        with self.assertRaises(ValueError) as context:
            get_stage("app-123", stage_id=1)

        self.assertIn("No stage found with ID 1", str(context.exception))

    # Tests for get_application tool
    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_get_application_success(self, mock_get_client):
        """Test successful application retrieval"""
        # Setup mock client
        mock_client = MagicMock()
        mock_app = MagicMock(spec=ApplicationInfo)
        mock_app.id = "spark-app-123"
        mock_app.name = "Test Application"
        mock_client.get_application.return_value = mock_app
        mock_get_client.return_value = mock_client

        # Call the function
        result = get_application("spark-app-123")

        # Verify results
        self.assertEqual(result, mock_app)
        mock_client.get_application.assert_called_once_with("spark-app-123")
        mock_get_client.assert_called_once_with(unittest.mock.ANY, None)

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_get_application_with_server(self, mock_get_client):
        """Test application retrieval with specific server"""
        # Setup mock client
        mock_client = MagicMock()
        mock_app = MagicMock(spec=ApplicationInfo)
        mock_client.get_application.return_value = mock_app
        mock_get_client.return_value = mock_client

        # Call the function with server
        get_application("spark-app-123", server="production")

        # Verify server parameter is passed
        mock_get_client.assert_called_once_with(unittest.mock.ANY, "production")

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_get_application_not_found(self, mock_get_client):
        """Test application retrieval when app doesn't exist"""
        # Setup mock client to raise exception
        mock_client = MagicMock()
        mock_client.get_application.side_effect = Exception("Application not found")
        mock_get_client.return_value = mock_client

        # Verify exception is propagated
        with self.assertRaises(Exception) as context:
            get_application("non-existent-app")

        self.assertIn("Application not found", str(context.exception))

    # Tests for list_jobs tool
    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_list_jobs_no_filter(self, mock_get_client):
        """Test job retrieval without status filter"""
        # Setup mock client
        mock_client = MagicMock()
        mock_jobs = [MagicMock(spec=JobData), MagicMock(spec=JobData)]
        mock_client.list_jobs.return_value = mock_jobs
        mock_get_client.return_value = mock_client

        # Call the function
        result = list_jobs("spark-app-123")

        # Verify results
        self.assertEqual(result, mock_jobs)
        mock_client.list_jobs.assert_called_once_with(
            app_id="spark-app-123", status=None
        )

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_list_jobs_with_status_filter(self, mock_get_client):
        """Test job retrieval with status filter"""
        # Setup mock client
        mock_client = MagicMock()
        mock_jobs = [MagicMock(spec=JobData)]
        mock_jobs[0].status = "SUCCEEDED"
        mock_client.list_jobs.return_value = mock_jobs
        mock_get_client.return_value = mock_client

        # Call the function with status filter
        result = list_jobs("spark-app-123", status=["SUCCEEDED"])

        # Verify results
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].status, "SUCCEEDED")

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_list_jobs_empty_result(self, mock_get_client):
        """Test job retrieval with empty result"""
        # Setup mock client
        mock_client = MagicMock()
        mock_client.list_jobs.return_value = []
        mock_get_client.return_value = mock_client

        # Call the function
        result = list_jobs("spark-app-123")

        # Verify results
        self.assertEqual(result, [])

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_list_jobs_status_filtering(self, mock_get_client):
        """Test job status filtering logic"""
        # Setup mock client
        mock_client = MagicMock()

        # Create jobs with different statuses
        job1 = MagicMock(spec=JobData)
        job1.status = "RUNNING"
        job2 = MagicMock(spec=JobData)
        job2.status = "SUCCEEDED"
        job3 = MagicMock(spec=JobData)
        job3.status = "FAILED"

        # Mock client to return only SUCCEEDED job when filtered
        mock_client.list_jobs.return_value = [job2]  # Only return SUCCEEDED job
        mock_get_client.return_value = mock_client

        # Test filtering for SUCCEEDED jobs
        result = list_jobs("spark-app-123", status=["SUCCEEDED"])

        # Should only return SUCCEEDED job
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].status, "SUCCEEDED")

    # Tests for list_stages tool
    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_get_stages_no_filter(self, mock_get_client):
        """Test stage retrieval without filters"""
        # Setup mock client
        mock_client = MagicMock()
        mock_stages = [MagicMock(spec=StageData), MagicMock(spec=StageData)]
        mock_client.list_stages.return_value = mock_stages
        mock_get_client.return_value = mock_client

        # Call the function
        result = list_stages("spark-app-123")

        # Verify results
        self.assertEqual(result, mock_stages)
        mock_client.list_stages.assert_called_once_with(
            app_id="spark-app-123", status=None, with_summaries=False
        )

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_get_stages_with_status_filter(self, mock_get_client):
        """Test stage retrieval with status filter"""
        # Setup mock client
        mock_client = MagicMock()

        # Create stages with different statuses
        stage1 = MagicMock(spec=StageData)
        stage1.status = "COMPLETE"
        stage2 = MagicMock(spec=StageData)
        stage2.status = "ACTIVE"
        stage3 = MagicMock(spec=StageData)
        stage3.status = "FAILED"

        # Mock client to return only COMPLETE stage when filtered
        mock_client.list_stages.return_value = [stage1]  # Only return COMPLETE stage
        mock_get_client.return_value = mock_client

        # Call with status filter
        result = list_stages("spark-app-123", status=["COMPLETE"])

        # Should only return COMPLETE stage
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].status, "COMPLETE")

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_get_stages_with_summaries(self, mock_get_client):
        """Test stage retrieval with summaries enabled"""
        # Setup mock client
        mock_client = MagicMock()
        mock_stages = [MagicMock(spec=StageData)]
        mock_client.list_stages.return_value = mock_stages
        mock_get_client.return_value = mock_client

        # Call with summaries enabled
        list_stages("spark-app-123", with_summaries=True)

        # Verify summaries parameter is passed
        mock_client.list_stages.assert_called_once_with(
            app_id="spark-app-123", status=None, with_summaries=True
        )

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_get_stages_empty_result(self, mock_get_client):
        """Test stage retrieval with empty result"""
        # Setup mock client
        mock_client = MagicMock()
        mock_client.list_stages.return_value = []
        mock_get_client.return_value = mock_client

        # Call the function
        result = list_stages("spark-app-123")

        # Verify results
        self.assertEqual(result, [])

    # Tests for get_stage_task_summary tool
    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_get_stage_task_summary_success(self, mock_get_client):
        """Test successful stage task summary retrieval"""
        # Setup mock client
        mock_client = MagicMock()
        mock_summary = MagicMock(spec=TaskMetricDistributions)
        mock_client.get_stage_task_summary.return_value = mock_summary
        mock_get_client.return_value = mock_client

        # Call the function
        result = get_stage_task_summary("spark-app-123", 1, 0)

        # Verify results
        self.assertEqual(result, mock_summary)
        mock_client.get_stage_task_summary.assert_called_once_with(
            app_id="spark-app-123",
            stage_id=1,
            attempt_id=0,
            quantiles="0.05,0.25,0.5,0.75,0.95",
        )

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_get_stage_task_summary_with_quantiles(self, mock_get_client):
        """Test stage task summary with custom quantiles"""
        # Setup mock client
        mock_client = MagicMock()
        mock_summary = MagicMock(spec=TaskMetricDistributions)
        mock_client.get_stage_task_summary.return_value = mock_summary
        mock_get_client.return_value = mock_client

        # Call with custom quantiles
        get_stage_task_summary("spark-app-123", 1, 0, quantiles="0.25,0.5,0.75")

        # Verify quantiles parameter is passed
        mock_client.get_stage_task_summary.assert_called_once_with(
            app_id="spark-app-123", stage_id=1, attempt_id=0, quantiles="0.25,0.5,0.75"
        )

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_get_stage_task_summary_not_found(self, mock_get_client):
        """Test stage task summary when stage doesn't exist"""
        # Setup mock client to raise exception
        mock_client = MagicMock()
        mock_client.get_stage_task_summary.side_effect = Exception("Stage not found")
        mock_get_client.return_value = mock_client

        # Verify exception is propagated
        with self.assertRaises(Exception) as context:
            get_stage_task_summary("spark-app-123", 999, 0)

        self.assertIn("Stage not found", str(context.exception))

    # Tests for list_slowest_stages tool
    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_list_slowest_stages_execution_time_vs_total_time(self, mock_get_client):
        """Test that list_slowest_stages prioritizes execution time over total stage duration"""
        mock_client = MagicMock()

        # Create Stage A: Longer total duration but shorter execution time
        stage_a = MagicMock(spec=StageData)
        stage_a.stage_id = 1
        stage_a.attempt_id = 0
        stage_a.name = "Stage A"
        stage_a.status = "COMPLETE"
        # Total duration: 10 minutes (submission to completion)
        stage_a.submission_time = datetime.now() - timedelta(minutes=10)
        stage_a.first_task_launched_time = datetime.now() - timedelta(
            minutes=5
        )  # 5 min delay
        stage_a.completion_time = datetime.now()
        # Execution time: 5 minutes (first_task_launched to completion)

        # Create Stage B: Shorter total duration but longer execution time
        stage_b = MagicMock(spec=StageData)
        stage_b.stage_id = 2
        stage_b.attempt_id = 0
        stage_b.name = "Stage B"
        stage_b.status = "COMPLETE"
        # Total duration: 8 minutes (submission to completion)
        stage_b.submission_time = datetime.now() - timedelta(minutes=8)
        stage_b.first_task_launched_time = datetime.now() - timedelta(
            minutes=7
        )  # 1 min delay
        stage_b.completion_time = datetime.now()
        # Execution time: 7 minutes (first_task_launched to completion)

        mock_client.list_stages.return_value = [stage_a, stage_b]
        mock_get_client.return_value = mock_client

        # Call the function
        result = list_slowest_stages("app-123", n=2)

        # Verify results - Stage B should be first (longer execution time: 7 min vs 5 min)
        # even though Stage A has longer total duration (10 min vs 8 min)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0], stage_b)  # Stage B first (7 min execution)
        self.assertEqual(result[1], stage_a)  # Stage A second (5 min execution)

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_list_slowest_stages_exclude_running(self, mock_get_client):
        """Test that list_slowest_stages excludes running stages by default"""
        mock_client = MagicMock()

        # Create running stage with long execution time
        running_stage = MagicMock(spec=StageData)
        running_stage.stage_id = 1
        running_stage.attempt_id = 0
        running_stage.name = "Running Stage"
        running_stage.status = "RUNNING"
        running_stage.submission_time = datetime.now() - timedelta(minutes=20)
        running_stage.first_task_launched_time = datetime.now() - timedelta(minutes=15)
        running_stage.completion_time = None  # Still running

        # Create completed stage with shorter execution time
        completed_stage = MagicMock(spec=StageData)
        completed_stage.stage_id = 2
        completed_stage.attempt_id = 0
        completed_stage.name = "Completed Stage"
        completed_stage.status = "COMPLETE"
        completed_stage.submission_time = datetime.now() - timedelta(minutes=5)
        completed_stage.first_task_launched_time = datetime.now() - timedelta(minutes=4)
        completed_stage.completion_time = datetime.now()

        mock_client.list_stages.return_value = [running_stage, completed_stage]
        mock_get_client.return_value = mock_client

        # Call the function with include_running=False (default)
        result = list_slowest_stages("app-123", include_running=False, n=2)

        # Should only return the completed stage
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0], completed_stage)
        self.assertNotIn(running_stage, result)

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_list_slowest_stages_include_running(self, mock_get_client):
        """Test that list_slowest_stages includes running stages when requested"""
        mock_client = MagicMock()

        # Create running stage
        running_stage = MagicMock(spec=StageData)
        running_stage.stage_id = 1
        running_stage.attempt_id = 0
        running_stage.name = "Running Stage"
        running_stage.status = "RUNNING"
        running_stage.submission_time = datetime.now() - timedelta(minutes=10)
        running_stage.first_task_launched_time = datetime.now() - timedelta(minutes=8)
        running_stage.completion_time = None

        # Create completed stage
        completed_stage = MagicMock(spec=StageData)
        completed_stage.stage_id = 2
        completed_stage.attempt_id = 0
        completed_stage.name = "Completed Stage"
        completed_stage.status = "COMPLETE"
        completed_stage.submission_time = datetime.now() - timedelta(minutes=5)
        completed_stage.first_task_launched_time = datetime.now() - timedelta(minutes=4)
        completed_stage.completion_time = datetime.now()

        mock_client.list_stages.return_value = [running_stage, completed_stage]
        mock_get_client.return_value = mock_client

        # Call the function with include_running=True
        result = list_slowest_stages("app-123", include_running=True, n=2)

        # Should include both stages, but running stage will have duration 0
        # so completed stage should be first
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0], completed_stage)  # Has actual duration
        self.assertEqual(result[1], running_stage)  # Duration 0

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_list_slowest_stages_missing_timestamps(self, mock_get_client):
        """Test list_slowest_stages handles stages with missing timestamps"""
        # Setup mock client
        mock_client = MagicMock()

        # Create stage with missing first_task_launched_time
        stage_missing_launch = MagicMock(spec=StageData)
        stage_missing_launch.stage_id = 1
        stage_missing_launch.attempt_id = 0
        stage_missing_launch.name = "Stage Missing Launch Time"
        stage_missing_launch.status = "COMPLETE"
        stage_missing_launch.submission_time = datetime.now() - timedelta(minutes=10)
        stage_missing_launch.first_task_launched_time = None
        stage_missing_launch.completion_time = datetime.now()

        # Create stage with missing completion_time
        stage_missing_completion = MagicMock(spec=StageData)
        stage_missing_completion.stage_id = 2
        stage_missing_completion.attempt_id = 0
        stage_missing_completion.name = "Stage Missing Completion Time"
        stage_missing_completion.status = "COMPLETE"
        stage_missing_completion.submission_time = datetime.now() - timedelta(minutes=5)
        stage_missing_completion.first_task_launched_time = datetime.now() - timedelta(
            minutes=4
        )
        stage_missing_completion.completion_time = None

        # Create valid stage
        valid_stage = MagicMock(spec=StageData)
        valid_stage.stage_id = 3
        valid_stage.attempt_id = 0
        valid_stage.name = "Valid Stage"
        valid_stage.status = "COMPLETE"
        valid_stage.submission_time = datetime.now() - timedelta(minutes=3)
        valid_stage.first_task_launched_time = datetime.now() - timedelta(minutes=2)
        valid_stage.completion_time = datetime.now()

        mock_client.list_stages.return_value = [
            stage_missing_launch,
            stage_missing_completion,
            valid_stage,
        ]
        mock_get_client.return_value = mock_client

        # Call the function
        result = list_slowest_stages("app-123", n=3)

        # Should return valid stage first, others should have duration 0
        self.assertEqual(len(result), 3)
        self.assertEqual(result[0], valid_stage)  # Only one with valid duration

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_list_slowest_stages_empty_result(self, mock_get_client):
        """Test list_slowest_stages with no stages"""
        # Setup mock client
        mock_client = MagicMock()
        mock_client.list_stages.return_value = []
        mock_get_client.return_value = mock_client

        # Call the function
        result = list_slowest_stages("app-123", n=5)

        # Should return empty list
        self.assertEqual(result, [])

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_list_slowest_stages_limit_results(self, mock_get_client):
        """Test list_slowest_stages limits results to n"""
        # Setup mock client
        mock_client = MagicMock()

        # Create 5 stages with different execution times
        stages = []
        for i in range(5):
            stage = MagicMock(spec=StageData)
            stage.stage_id = i
            stage.attempt_id = 0
            stage.name = f"Stage {i}"
            stage.status = "COMPLETE"
            stage.submission_time = datetime.now() - timedelta(minutes=10)
            # Different execution times: 1, 2, 3, 4, 5 minutes
            stage.first_task_launched_time = datetime.now() - timedelta(minutes=i + 1)
            stage.completion_time = datetime.now()
            stages.append(stage)

        mock_client.list_stages.return_value = stages
        mock_get_client.return_value = mock_client

        # Call the function with n=3
        result = list_slowest_stages("app-123", n=3)

        # Should return only 3 stages (the ones with longest execution times)
        self.assertEqual(len(result), 3)
        # Should be sorted by execution time descending (5, 4, 3 minutes)
        self.assertEqual(result[0].stage_id, 4)  # 5 minutes
        self.assertEqual(result[1].stage_id, 3)  # 4 minutes
        self.assertEqual(result[2].stage_id, 2)  # 3 minutes

    # Tests for list_slowest_sql_queries tool
    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_get_slowest_sql_queries_success(self, mock_get_client):
        """Test successful SQL query retrieval and sorting"""
        # Setup mock client
        mock_client = MagicMock()

        # Create mock SQL executions with different durations
        sql1 = MagicMock(spec=ExecutionData)
        sql1.id = 1
        sql1.duration = 5000  # 5 seconds
        sql1.status = "COMPLETED"

        sql2 = MagicMock(spec=ExecutionData)
        sql2.id = 2
        sql2.duration = 10000  # 10 seconds
        sql2.status = "COMPLETED"

        sql3 = MagicMock(spec=ExecutionData)
        sql3.id = 3
        sql3.duration = 2000  # 2 seconds
        sql3.status = "COMPLETED"

        mock_client.get_sql_list.return_value = [sql1, sql2, sql3]
        mock_get_client.return_value = mock_client

        # Call the function
        result = list_slowest_sql_queries("spark-app-123", top_n=2)

        # Verify results are sorted by duration (descending)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0].duration, 10000)  # Slowest first
        self.assertEqual(result[1].duration, 5000)  # Second slowest

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_get_slowest_sql_queries_exclude_running(self, mock_get_client):
        """Test SQL query retrieval excluding running queries"""
        # Setup mock client
        mock_client = MagicMock()

        # Create mock SQL executions with different statuses
        sql1 = MagicMock(spec=ExecutionData)
        sql1.id = 1
        sql1.duration = 5000
        sql1.status = "RUNNING"

        sql2 = MagicMock(spec=ExecutionData)
        sql2.id = 2
        sql2.duration = 10000
        sql2.status = "COMPLETED"

        mock_client.get_sql_list.return_value = [sql1, sql2]
        mock_get_client.return_value = mock_client

        # Call the function (include_running=False by default)
        result = list_slowest_sql_queries("spark-app-123")

        # Should exclude running query
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].status, "COMPLETED")

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_get_slowest_sql_queries_include_running(self, mock_get_client):
        """Test SQL query retrieval including running queries"""
        # Setup mock client
        mock_client = MagicMock()

        # Create mock SQL executions
        sql1 = MagicMock(spec=ExecutionData)
        sql1.id = 1
        sql1.duration = 5000
        sql1.status = "RUNNING"

        sql2 = MagicMock(spec=ExecutionData)
        sql2.id = 2
        sql2.duration = 10000
        sql2.status = "COMPLETED"

        mock_client.get_sql_list.return_value = [sql1, sql2]
        mock_get_client.return_value = mock_client

        # Call the function with include_running=True and top_n=2
        result = list_slowest_sql_queries(
            "spark-app-123", include_running=True, top_n=2
        )

        # Should include both queries
        self.assertEqual(len(result), 2)

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_get_slowest_sql_queries_empty_result(self, mock_get_client):
        """Test SQL query retrieval with empty result"""
        # Setup mock client
        mock_client = MagicMock()
        mock_client.get_sql_list.return_value = []
        mock_get_client.return_value = mock_client

        # Call the function
        result = list_slowest_sql_queries("spark-app-123")

        # Verify results
        self.assertEqual(result, [])

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_get_slowest_sql_queries_limit(self, mock_get_client):
        """Test SQL query retrieval with limit"""
        # Setup mock client
        mock_client = MagicMock()

        # Create mock SQL executions
        sql_execs = []
        for i in range(10):
            sql = MagicMock(spec=ExecutionData)
            sql.id = i
            sql.duration = (10 - i) * 1000  # Decreasing durations
            sql.status = "COMPLETED"
            sql_execs.append(sql)

        mock_client.get_sql_list.return_value = sql_execs
        mock_get_client.return_value = mock_client

        # Call the function with top_n=3
        result = list_slowest_sql_queries("spark-app-123", top_n=3)

        # Verify results - should return only 3 queries
        self.assertEqual(len(result), 3)
        # Should be sorted by duration (descending)
        self.assertEqual(result[0].duration, 10000)
        self.assertEqual(result[1].duration, 9000)
        self.assertEqual(result[2].duration, 8000)
