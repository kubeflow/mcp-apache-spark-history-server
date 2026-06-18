import unittest
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

from spark_history_mcp.api.spark_client import SparkRestClient
from spark_history_mcp.api_client.models.application import Application
from spark_history_mcp.api_client.models.job import Job
from spark_history_mcp.api_client.models.sql_execution import SQLExecution
from spark_history_mcp.api_client.models.stage_data import StageData
from spark_history_mcp.api_client.models.task_metrics_summary import TaskMetricsSummary
from spark_history_mcp.tools.tools import (
    _calculate_executor_metrics,
    compare_sql_execution_plans,
    compare_sql_executions,
    get_application,
    get_client_or_default,
    get_sql_execution,
    get_stage,
    get_stage_task_summary,
    list_applications,
    list_executors,
    list_jobs,
    list_slowest_jobs,
    list_slowest_stages,
    list_sql_executions,
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
        mock_client = MagicMock()
        mock_client.list_jobs.return_value = []
        mock_get_client.return_value = mock_client

        result = list_slowest_jobs("app-123", n=3)

        self.assertEqual(result, [])
        mock_client.list_jobs.assert_called_once_with(app_id="app-123")

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_get_slowest_jobs_exclude_running(self, mock_get_client):
        """Test list_slowest_jobs excluding running jobs"""
        mock_client = MagicMock()

        job1 = MagicMock(spec=Job)
        job1.status = "RUNNING"
        job1.submission_time = datetime.now() - timedelta(minutes=10)
        job1.completion_time = None

        job2 = MagicMock(spec=Job)
        job2.status = "SUCCEEDED"
        job2.submission_time = datetime.now() - timedelta(minutes=5)
        job2.completion_time = datetime.now() - timedelta(minutes=3)  # 2 min duration

        job3 = MagicMock(spec=Job)
        job3.status = "SUCCEEDED"
        job3.submission_time = datetime.now() - timedelta(minutes=10)
        job3.completion_time = datetime.now() - timedelta(minutes=5)  # 5 min duration

        job4 = MagicMock(spec=Job)
        job4.status = "FAILED"
        job4.submission_time = datetime.now() - timedelta(minutes=8)
        job4.completion_time = datetime.now() - timedelta(minutes=7)  # 1 min duration

        mock_client.list_jobs.return_value = [job1, job2, job3, job4]
        mock_get_client.return_value = mock_client

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
        mock_client = MagicMock()

        job1 = MagicMock(spec=Job)
        job1.status = "RUNNING"
        job1.submission_time = datetime.now() - timedelta(
            minutes=20
        )  # Running for 20 min
        job1.completion_time = None

        job2 = MagicMock(spec=Job)
        job2.status = "SUCCEEDED"
        job2.submission_time = datetime.now() - timedelta(minutes=5)
        job2.completion_time = datetime.now() - timedelta(minutes=3)  # 2 min duration

        job3 = MagicMock(spec=Job)
        job3.status = "SUCCEEDED"
        job3.submission_time = datetime.now() - timedelta(minutes=10)
        job3.completion_time = datetime.now() - timedelta(minutes=5)  # 5 min duration

        mock_client.list_jobs.return_value = [job1, job2, job3]
        mock_get_client.return_value = mock_client

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
        mock_client = MagicMock()

        # Create 5 mock jobs with different durations
        jobs = []
        for i in range(5):
            job = MagicMock(spec=Job)
            job.status = "SUCCEEDED"
            job.submission_time = datetime.now() - timedelta(minutes=10)
            # Different completion times to create different durations
            job.completion_time = datetime.now() - timedelta(minutes=10 - i)
            jobs.append(job)

        mock_client.list_jobs.return_value = jobs
        mock_get_client.return_value = mock_client

        result = list_slowest_jobs("app-123", n=3)

        # Verify results - should return only 3 jobs
        self.assertEqual(len(result), 3)

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_get_stage_with_attempt_id(self, mock_get_client):
        """Test get_stage with a specific attempt ID"""
        mock_client = MagicMock()
        mock_stage = MagicMock(spec=StageData)
        mock_stage.task_metrics_distributions = None
        # Explicitly set the attempt_id attribute on the mock
        mock_stage.attempt_id = 0
        mock_client.get_stage_attempt.return_value = mock_stage
        mock_get_client.return_value = mock_client

        # Call the function with attempt_id
        result = get_stage("app-123", stage_id=1, attempt_id=0)

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
        mock_client = MagicMock()
        mock_stage = MagicMock(spec=StageData)
        mock_stage.task_metrics_distributions = None
        # Explicitly set the attempt_id attribute on the mock
        mock_stage.attempt_id = 0
        mock_client.list_stage_attempts.return_value = mock_stage
        mock_get_client.return_value = mock_client

        result = get_stage("app-123", stage_id=1)

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
        mock_client = MagicMock()
        mock_stage = MagicMock(spec=StageData)
        # Explicitly set the attempt_id attribute on the mock
        mock_stage.attempt_id = 0
        # Set task_metrics_distributions to None to trigger the fetch
        mock_stage.task_metrics_distributions = None

        mock_summary = MagicMock(spec=TaskMetricsSummary)

        mock_client.get_stage_attempt.return_value = mock_stage
        mock_client.get_stage_task_summary.return_value = mock_summary
        mock_get_client.return_value = mock_client

        result = get_stage("app-123", stage_id=1, attempt_id=0, with_summaries=True)

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
        mock_client = MagicMock()
        mock_app = MagicMock(spec=Application)
        mock_app.id = "spark-app-123"
        mock_app.name = "Test Application"
        mock_client.get_application.return_value = mock_app
        mock_get_client.return_value = mock_client

        result = get_application("spark-app-123")

        self.assertEqual(result, mock_app)
        mock_client.get_application.assert_called_once_with("spark-app-123")
        mock_get_client.assert_called_once_with(
            unittest.mock.ANY, None, "spark-app-123"
        )

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_get_application_with_server(self, mock_get_client):
        """Test application retrieval with specific server"""
        mock_client = MagicMock()
        mock_app = MagicMock(spec=Application)
        mock_client.get_application.return_value = mock_app
        mock_get_client.return_value = mock_client

        # Call the function with server
        get_application("spark-app-123", server="production")

        # Verify server parameter is passed
        mock_get_client.assert_called_once_with(
            unittest.mock.ANY, "production", "spark-app-123"
        )

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_get_application_not_found(self, mock_get_client):
        """Test application retrieval when app doesn't exist"""
        mock_client = MagicMock()
        mock_client.get_application.side_effect = Exception("Application not found")
        mock_get_client.return_value = mock_client

        # Verify exception is propagated
        with self.assertRaises(Exception) as context:
            get_application("non-existent-app")

        self.assertIn("Application not found", str(context.exception))

    def test_calculate_executor_metrics_handles_missing_memory_metrics(self):
        """Test executor summary handles executors without memoryMetrics.

        Executor.memory_metrics and the inner used_*_storage_memory
        fields are declared Optional in the generated models, and Spark History
        Server may return executors (e.g. the driver entry, or executors from
        replayed event logs missing executor metrics events) without these
        fields populated. The summary aggregation must not crash in that case.
        """
        executor_without_memory = MagicMock()
        executor_without_memory.is_active = True
        executor_without_memory.memory_metrics = None
        executor_without_memory.disk_used = 10
        executor_without_memory.completed_tasks = 2
        executor_without_memory.failed_tasks = 1
        executor_without_memory.total_duration = 100
        executor_without_memory.total_gc_time = 5
        executor_without_memory.total_input_bytes = 20
        executor_without_memory.total_shuffle_read = 30
        executor_without_memory.total_shuffle_write = 40

        memory_metrics = MagicMock()
        memory_metrics.used_on_heap_storage_memory = 7
        memory_metrics.used_off_heap_storage_memory = None
        executor_with_partial_memory = MagicMock()
        executor_with_partial_memory.is_active = False
        executor_with_partial_memory.memory_metrics = memory_metrics
        executor_with_partial_memory.disk_used = 1
        executor_with_partial_memory.completed_tasks = 3
        executor_with_partial_memory.failed_tasks = 0
        executor_with_partial_memory.total_duration = 200
        executor_with_partial_memory.total_gc_time = 6
        executor_with_partial_memory.total_input_bytes = 21
        executor_with_partial_memory.total_shuffle_read = 31
        executor_with_partial_memory.total_shuffle_write = 41

        result = _calculate_executor_metrics(
            [executor_without_memory, executor_with_partial_memory]
        )

        self.assertEqual(result["total_executors"], 2)
        self.assertEqual(result["active_executors"], 1)
        self.assertEqual(result["memory_used"], 7)
        self.assertEqual(result["disk_used"], 11)

    # Tests for list_applications tool
    @patch("spark_history_mcp.tools.tools.mcp.get_context")
    def test_list_applications_no_filter(self, mock_get_context):
        """Test application listing without filters"""
        mock_context = MagicMock()
        mock_context.request_context.lifespan_context.clients = {
            "server1": self.mock_client1
        }
        mock_get_context.return_value = mock_context

        mock_apps = [MagicMock(spec=Application), MagicMock(spec=Application)]
        mock_apps[0].id = "app-1"
        mock_apps[1].id = "app-2"
        self.mock_client1.list_applications.return_value = mock_apps

        result = list_applications()

        self.assertEqual(result, mock_apps)
        self.mock_client1.list_applications.assert_called_once_with(
            status=None,
            min_date=None,
            max_date=None,
            min_end_date=None,
            max_end_date=None,
            limit=None,
        )

    @patch("spark_history_mcp.tools.tools.mcp.get_context")
    def test_list_applications_with_filters(self, mock_get_context):
        """Test application listing with filters"""
        mock_context = MagicMock()
        mock_context.request_context.lifespan_context.clients = {
            "server1": self.mock_client1
        }
        mock_get_context.return_value = mock_context

        mock_apps = [MagicMock(spec=Application)]
        mock_apps[0].id = "completed-app"
        self.mock_client1.list_applications.return_value = mock_apps

        # Call with filters
        result = list_applications(
            status=["COMPLETED"], min_date="2024-01-01", limit=10
        )

        self.assertEqual(result, mock_apps)
        self.mock_client1.list_applications.assert_called_once_with(
            status=["COMPLETED"],
            min_date="2024-01-01",
            max_date=None,
            min_end_date=None,
            max_end_date=None,
            limit=10,
        )

    @patch("spark_history_mcp.tools.tools.mcp.get_context")
    def test_list_applications_empty_result(self, mock_get_context):
        """Test application listing with empty result"""
        mock_context = MagicMock()
        mock_context.request_context.lifespan_context.clients = {
            "server1": self.mock_client1
        }
        mock_get_context.return_value = mock_context

        self.mock_client1.list_applications.return_value = []

        result = list_applications()

        self.assertEqual(result, [])

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_list_applications_with_server(self, mock_get_client):
        """Test application listing with specific server"""
        mock_client = MagicMock()
        mock_apps = [MagicMock(spec=Application)]
        mock_client.list_applications.return_value = mock_apps
        mock_get_client.return_value = mock_client

        # Call with server
        list_applications(server="production")

        # Verify server parameter is passed
        mock_get_client.assert_called_once_with(unittest.mock.ANY, "production")

    # Tests for list_jobs tool
    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_list_jobs_no_filter(self, mock_get_client):
        """Test job retrieval without status filter"""
        mock_client = MagicMock()
        mock_jobs = [MagicMock(spec=Job), MagicMock(spec=Job)]
        mock_client.list_jobs.return_value = mock_jobs
        mock_get_client.return_value = mock_client

        result = list_jobs("spark-app-123")

        self.assertEqual(result, mock_jobs)
        mock_client.list_jobs.assert_called_once_with(
            app_id="spark-app-123",
            status=None,
            app_attempt_id=None,
            offset=0,
            length=None,
        )

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_list_jobs_with_status_filter(self, mock_get_client):
        """Test job retrieval with status filter"""
        mock_client = MagicMock()
        mock_jobs = [MagicMock(spec=Job)]
        mock_jobs[0].status = "SUCCEEDED"
        mock_client.list_jobs.return_value = mock_jobs
        mock_get_client.return_value = mock_client

        result = list_jobs("spark-app-123", status=["SUCCEEDED"])

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].status, "SUCCEEDED")

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_list_jobs_empty_result(self, mock_get_client):
        """Test job retrieval with empty result"""
        mock_client = MagicMock()
        mock_client.list_jobs.return_value = []
        mock_get_client.return_value = mock_client

        result = list_jobs("spark-app-123")

        self.assertEqual(result, [])

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_list_jobs_status_filtering(self, mock_get_client):
        """Test job status filtering logic"""
        mock_client = MagicMock()

        # Create jobs with different statuses
        job1 = MagicMock(spec=Job)
        job1.status = "RUNNING"
        job2 = MagicMock(spec=Job)
        job2.status = "SUCCEEDED"
        job3 = MagicMock(spec=Job)
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
        mock_client = MagicMock()
        mock_stages = [MagicMock(spec=StageData), MagicMock(spec=StageData)]
        mock_client.list_stages.return_value = mock_stages
        mock_get_client.return_value = mock_client

        result = list_stages("spark-app-123")

        self.assertEqual(result, mock_stages)
        mock_client.list_stages.assert_called_once_with(
            app_id="spark-app-123",
            status=None,
            with_summaries=False,
            app_attempt_id=None,
            offset=0,
            length=None,
        )

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_get_stages_with_status_filter(self, mock_get_client):
        """Test stage retrieval with status filter"""
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

        result = list_stages("spark-app-123", status=["COMPLETE"])

        # Should only return COMPLETE stage
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].status, "COMPLETE")

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_get_stages_with_summaries(self, mock_get_client):
        """Test stage retrieval with summaries enabled"""
        mock_client = MagicMock()
        mock_stages = [MagicMock(spec=StageData)]
        mock_client.list_stages.return_value = mock_stages
        mock_get_client.return_value = mock_client

        list_stages("spark-app-123", with_summaries=True)

        # Verify summaries parameter is passed
        mock_client.list_stages.assert_called_once_with(
            app_id="spark-app-123",
            status=None,
            with_summaries=True,
            app_attempt_id=None,
            offset=0,
            length=None,
        )

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_get_stages_empty_result(self, mock_get_client):
        """Test stage retrieval with empty result"""
        mock_client = MagicMock()
        mock_client.list_stages.return_value = []
        mock_get_client.return_value = mock_client

        result = list_stages("spark-app-123")

        self.assertEqual(result, [])

    # Tests for get_stage_task_summary tool
    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_get_stage_task_summary_success(self, mock_get_client):
        """Test successful stage task summary retrieval"""
        mock_client = MagicMock()
        mock_summary = MagicMock(spec=TaskMetricsSummary)
        mock_client.get_stage_task_summary.return_value = mock_summary
        mock_get_client.return_value = mock_client

        result = get_stage_task_summary("spark-app-123", 1, 0)

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
        mock_client = MagicMock()
        mock_summary = MagicMock(spec=TaskMetricsSummary)
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

        result = list_slowest_stages("app-123", include_running=True, n=2)

        # Should include both stages, but running stage will have duration 0
        # so completed stage should be first
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0], completed_stage)  # Has actual duration
        self.assertEqual(result[1], running_stage)  # Duration 0

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_list_slowest_stages_missing_timestamps(self, mock_get_client):
        """Test list_slowest_stages handles stages with missing timestamps"""
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

        result = list_slowest_stages("app-123", n=3)

        # Should return valid stage first, others should have duration 0
        self.assertEqual(len(result), 3)
        self.assertEqual(result[0], valid_stage)  # Only one with valid duration

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_list_slowest_stages_empty_result(self, mock_get_client):
        """Test list_slowest_stages with no stages"""
        mock_client = MagicMock()
        mock_client.list_stages.return_value = []
        mock_get_client.return_value = mock_client

        result = list_slowest_stages("app-123", n=5)

        # Should return empty list
        self.assertEqual(result, [])

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_list_slowest_stages_limit_results(self, mock_get_client):
        """Test list_slowest_stages limits results to n"""
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

        result = list_slowest_stages("app-123", n=3)

        # Should return only 3 stages (the ones with longest execution times)
        self.assertEqual(len(result), 3)
        # Should be sorted by execution time descending (5, 4, 3 minutes)
        self.assertEqual(result[0].stage_id, 4)  # 5 minutes
        self.assertEqual(result[1].stage_id, 3)  # 4 minutes
        self.assertEqual(result[2].stage_id, 2)  # 3 minutes

    # Tests for list_sql_executions tool
    def _mk_sql(
        self,
        sql_id,
        duration,
        status="COMPLETED",
        description="Query",
        success=None,
        failed=None,
        running=None,
    ):
        sql = MagicMock(spec=SQLExecution)
        sql.id = sql_id
        sql.duration = duration
        sql.status = status
        sql.description = description
        sql.submission_time = "2025-08-05T00:23:38.607GMT"
        sql.success_job_ids = success if success is not None else []
        sql.failed_job_ids = failed if failed is not None else []
        sql.running_job_ids = running if running is not None else []
        return sql

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_list_sql_executions_sort_by_duration(self, mock_get_client):
        """list_sql_executions sorts by duration descending and returns summaries"""
        mock_client = MagicMock()
        mock_client.get_sql_list.return_value = [
            self._mk_sql(1, 5000, success=[1, 2]),
            self._mk_sql(2, 10000, success=[3]),
            self._mk_sql(3, 2000, success=[4]),
        ]
        mock_get_client.return_value = mock_client

        result = list_sql_executions("spark-app-123", sort_by="duration")

        self.assertEqual([r.id for r in result], [2, 1, 3])
        self.assertEqual(result[0].duration, 10000)
        self.assertEqual(result[0].success_job_ids, [3])
        # List view must be lightweight: no plan text or node details fetched.
        mock_client.get_sql_list.assert_called_with(
            app_id="spark-app-123",
            app_attempt_id=None,
            details=False,
            plan_description=False,
            offset=0,
            length=100,
        )

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_list_sql_executions_default_sort_failed_first(self, mock_get_client):
        """Default ordering puts FAILED first, then RUNNING, then COMPLETED"""
        mock_client = MagicMock()
        mock_client.get_sql_list.return_value = [
            self._mk_sql(1, 10000, status="COMPLETED"),
            self._mk_sql(2, 1000, status="FAILED"),
            self._mk_sql(3, 5000, status="RUNNING"),
        ]
        mock_get_client.return_value = mock_client

        result = list_sql_executions("spark-app-123")

        self.assertEqual([r.status for r in result], ["FAILED", "RUNNING", "COMPLETED"])

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_list_sql_executions_status_filter(self, mock_get_client):
        """status filter keeps only matching executions (case-insensitive)"""
        mock_client = MagicMock()
        mock_client.get_sql_list.return_value = [
            self._mk_sql(1, 5000, status="COMPLETED"),
            self._mk_sql(2, 1000, status="FAILED"),
        ]
        mock_get_client.return_value = mock_client

        result = list_sql_executions("spark-app-123", status="failed")

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].status, "FAILED")

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_list_sql_executions_description_filter(self, mock_get_client):
        """description filter does a case-insensitive substring match"""
        mock_client = MagicMock()
        mock_client.get_sql_list.return_value = [
            self._mk_sql(1, 5000, description="benchmark q5"),
            self._mk_sql(2, 1000, description="warmup"),
        ]
        mock_get_client.return_value = mock_client

        result = list_sql_executions("spark-app-123", description="BENCHMARK")

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].id, 1)

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_list_sql_executions_limit(self, mock_get_client):
        """limit caps the number of returned executions"""
        mock_client = MagicMock()
        mock_client.get_sql_list.return_value = [
            self._mk_sql(i, (10 - i) * 1000) for i in range(10)
        ]
        mock_get_client.return_value = mock_client

        result = list_sql_executions("spark-app-123", sort_by="duration", limit=3)

        self.assertEqual(len(result), 3)
        self.assertEqual([r.duration for r in result], [10000, 9000, 8000])

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_list_sql_executions_empty(self, mock_get_client):
        """Empty result returns an empty list"""
        mock_client = MagicMock()
        mock_client.get_sql_list.return_value = []
        mock_get_client.return_value = mock_client

        result = list_sql_executions("spark-app-123")

        self.assertEqual(result, [])

    # Tests for get_sql_execution tool
    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_get_sql_execution_header_only_default(self, mock_get_client):
        """By default returns only the header and fetches no plan/details"""
        mock_client = MagicMock()
        mock_client.config.include_plan_description = None
        mock_client.get_sql_execution.return_value = self._mk_sql(
            42, 12345, description="SELECT * FROM t", success=[1, 2]
        )
        mock_get_client.return_value = mock_client

        result = get_sql_execution("spark-app-123", execution_id=42)

        self.assertEqual(result.execution.id, 42)
        self.assertEqual(result.execution.success_job_ids, [1, 2])
        self.assertIsNone(result.plan_description)
        self.assertIsNone(result.node_metrics)
        self.assertIsNone(result.jobs)
        self.assertIsNone(result.stage_metrics)
        self.assertIsNone(result.stages)
        mock_client.get_sql_execution.assert_called_once_with(
            app_id="spark-app-123",
            execution_id=42,
            app_attempt_id=None,
            details=False,
            plan_description=False,
        )

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_get_sql_execution_include_plan_strips_initial(self, mock_get_client):
        """include_plan strips AQE initial plans and returns node metrics"""
        mock_client = MagicMock()
        execution = self._mk_sql(7, 5000)
        execution.plan_description = (
            "== Physical Plan ==\n"
            "*(1) Project\n"
            "+- == Initial Plan ==\n"
            "   Sort\n"
            "   +- Exchange\n"
        )
        node = MagicMock()
        node.node_id = 1
        node.node_name = "Project"
        metric = MagicMock()
        metric.name = "rows"
        metric.value = "  100  "
        node.metrics = [metric]
        execution.nodes = [node]
        mock_client.get_sql_execution.return_value = execution
        mock_get_client.return_value = mock_client

        result = get_sql_execution("spark-app-123", execution_id=7, include_plan=True)

        self.assertIn("Physical Plan", result.plan_description)
        self.assertNotIn("Initial Plan", result.plan_description)
        self.assertEqual(len(result.node_metrics), 1)
        self.assertEqual(result.node_metrics[0].node_name, "Project")
        self.assertEqual(result.node_metrics[0].metrics["rows"], "100")
        mock_client.get_sql_execution.assert_called_once_with(
            app_id="spark-app-123",
            execution_id=7,
            app_attempt_id=None,
            details=True,
            plan_description=True,
        )

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_get_sql_execution_include_initial_plan_keeps(self, mock_get_client):
        """include_initial_plan retains initial plans and implies include_plan"""
        mock_client = MagicMock()
        execution = self._mk_sql(7, 5000)
        execution.plan_description = (
            "== Physical Plan ==\n+- == Initial Plan ==\n   Sort\n"
        )
        execution.nodes = []
        mock_client.get_sql_execution.return_value = execution
        mock_get_client.return_value = mock_client

        result = get_sql_execution(
            "spark-app-123", execution_id=7, include_initial_plan=True
        )

        self.assertIn("Initial Plan", result.plan_description)

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_get_sql_execution_config_fallback_includes_plan(self, mock_get_client):
        """When include_plan is unset, the server config default is used"""
        mock_client = MagicMock()
        mock_client.config.include_plan_description = True
        execution = self._mk_sql(7, 5000)
        execution.plan_description = "== Physical Plan ==\nProject\n"
        execution.nodes = []
        mock_client.get_sql_execution.return_value = execution
        mock_get_client.return_value = mock_client

        result = get_sql_execution("spark-app-123", execution_id=7)

        self.assertIsNotNone(result.plan_description)
        mock_client.get_sql_execution.assert_called_once_with(
            app_id="spark-app-123",
            execution_id=7,
            app_attempt_id=None,
            details=True,
            plan_description=True,
        )

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_get_sql_execution_plan_max_length(self, mock_get_client):
        """plan_max_length truncates the plan text"""
        mock_client = MagicMock()
        execution = self._mk_sql(7, 5000)
        execution.plan_description = "X" * 500
        execution.nodes = []
        mock_client.get_sql_execution.return_value = execution
        mock_get_client.return_value = mock_client

        result = get_sql_execution(
            "spark-app-123", execution_id=7, include_plan=True, plan_max_length=50
        )

        self.assertIn("[truncated]", result.plan_description)

    def _mk_job(self, job_id, status, stage_ids, num_tasks=10, num_failed=0):
        job = MagicMock(spec=Job)
        job.job_id = job_id
        job.status = status
        job.description = f"job {job_id}"
        job.name = f"job {job_id}"
        job.submission_time = "2025-08-05T00:00:00.000GMT"
        job.completion_time = "2025-08-05T00:00:10.000GMT"
        job.stage_ids = stage_ids
        job.num_tasks = num_tasks
        job.num_failed_tasks = num_failed
        return job

    def _mk_stage(self, stage_id, status="COMPLETE", tasks=10):
        stage = MagicMock(spec=StageData)
        stage.stage_id = stage_id
        stage.attempt_id = 0
        stage.status = status
        stage.description = f"stage {stage_id}"
        stage.name = f"stage {stage_id}"
        stage.num_tasks = tasks
        stage.num_failed_tasks = 0
        stage.submission_time = "2025-08-05T00:00:00.000GMT"
        stage.completion_time = "2025-08-05T00:00:05.000GMT"
        stage.input_bytes = 1000
        stage.shuffle_read_bytes = 200
        stage.shuffle_write_bytes = 300
        stage.disk_bytes_spilled = 50
        stage.jvm_gc_time = 25
        return stage

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_get_sql_execution_include_aggregated_metrics(self, mock_get_client):
        """include_aggregated_metrics returns associated jobs and aggregated stage metrics"""
        mock_client = MagicMock()
        mock_client.config.include_plan_description = None
        execution = self._mk_sql(7, 5000, success=[1, 2])
        mock_client.get_sql_execution.return_value = execution
        mock_client.list_jobs.return_value = [
            self._mk_job(1, "SUCCEEDED", [10]),
            self._mk_job(2, "SUCCEEDED", [11]),
            self._mk_job(3, "SUCCEEDED", [12]),  # not part of this SQL execution
        ]
        mock_client.list_stages.return_value = [
            self._mk_stage(10),
            self._mk_stage(11),
            self._mk_stage(12),  # excluded (job 3 not in SQL execution)
        ]
        mock_get_client.return_value = mock_client

        result = get_sql_execution(
            "spark-app-123", execution_id=7, include_aggregated_metrics=True
        )

        self.assertEqual({j.job_id for j in result.jobs}, {1, 2})
        self.assertIsNotNone(result.stage_metrics)
        self.assertEqual(result.stage_metrics.stage_count, 2)
        self.assertEqual(result.stage_metrics.tasks, 20)
        self.assertEqual(result.stage_metrics.input_bytes, 2000)
        self.assertEqual(result.stage_metrics.shuffle_read_bytes, 400)
        self.assertIsNone(result.stages)

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_get_sql_execution_include_stages(self, mock_get_client):
        """include_stages returns the individual stage rows for the execution"""
        mock_client = MagicMock()
        mock_client.config.include_plan_description = None
        execution = self._mk_sql(7, 5000, success=[1])
        mock_client.get_sql_execution.return_value = execution
        mock_client.list_jobs.return_value = [self._mk_job(1, "SUCCEEDED", [10, 11])]
        mock_client.list_stages.return_value = [
            self._mk_stage(10),
            self._mk_stage(11),
        ]
        mock_get_client.return_value = mock_client

        result = get_sql_execution("spark-app-123", execution_id=7, include_stages=True)

        self.assertIsNotNone(result.stages)
        self.assertEqual({s.stage_id for s in result.stages}, {10, 11})

    # Tests for compare_sql_executions tool
    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_compare_sql_executions(self, mock_get_client):
        """compare_sql_executions aggregates per-execution stage metrics for each side"""
        client_a = MagicMock()
        client_a.get_sql_execution.return_value = self._mk_sql(
            1, 5000, description="q", success=[1]
        )
        client_a.list_jobs.return_value = [self._mk_job(1, "SUCCEEDED", [10])]
        client_a.list_stages.return_value = [self._mk_stage(10, tasks=10)]

        client_b = MagicMock()
        client_b.get_sql_execution.return_value = self._mk_sql(
            2, 8000, description="q", success=[5]
        )
        client_b.list_jobs.return_value = [self._mk_job(5, "SUCCEEDED", [20])]
        client_b.list_stages.return_value = [self._mk_stage(20, tasks=30)]

        mock_get_client.side_effect = [client_a, client_b]

        result = compare_sql_executions("app-a", "app-b", 1, 2)

        self.assertEqual(result.a.app, "app-a")
        self.assertEqual(result.a.sql_id, 1)
        self.assertEqual(result.a.duration, 5000)
        self.assertEqual(result.a.tasks, 10)
        self.assertEqual(result.b.app, "app-b")
        self.assertEqual(result.b.sql_id, 2)
        self.assertEqual(result.b.tasks, 30)

    # Tests for compare_sql_execution_plans tool
    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_compare_sql_execution_plans(self, mock_get_client):
        """compare_sql_execution_plans returns node/edge counts and differing types"""

        def node(name):
            n = MagicMock()
            n.node_name = name
            return n

        exec_a = MagicMock(spec=SQLExecution)
        exec_a.nodes = [node("Filter"), node("Scan"), node("Scan")]
        exec_a.edges = [MagicMock(), MagicMock()]

        exec_b = MagicMock(spec=SQLExecution)
        exec_b.nodes = [node("Filter"), node("Scan")]
        exec_b.edges = [MagicMock()]

        client_a = MagicMock()
        client_a.get_sql_execution.return_value = exec_a
        client_b = MagicMock()
        client_b.get_sql_execution.return_value = exec_b
        mock_get_client.side_effect = [client_a, client_b]

        result = compare_sql_execution_plans("app-a", "app-b", 1, 2)

        self.assertEqual(result.app_a, "app-a")
        self.assertEqual(result.exec_id_a, 1)
        self.assertEqual(result.node_count_a, 3)
        self.assertEqual(result.node_count_b, 2)
        self.assertEqual(result.edge_count_a, 2)
        self.assertEqual(result.edge_count_b, 1)
        diffs = {d.node_type: (d.a, d.b) for d in result.node_type_diffs}
        self.assertEqual(diffs, {"Scan": (2, 1)})

    # Tests for pagination support

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_list_jobs_with_pagination(self, mock_get_client):
        """Test list_jobs forwards offset and length to client"""
        mock_client = MagicMock()
        mock_client.list_jobs.return_value = [MagicMock(spec=Job)]
        mock_get_client.return_value = mock_client

        list_jobs("spark-app-123", offset=5, length=10)

        mock_client.list_jobs.assert_called_once_with(
            app_id="spark-app-123",
            status=None,
            app_attempt_id=None,
            offset=5,
            length=10,
        )

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_list_jobs_pagination_defaults(self, mock_get_client):
        """Test list_jobs uses correct defaults (offset=0, length=None)"""
        mock_client = MagicMock()
        mock_client.list_jobs.return_value = []
        mock_get_client.return_value = mock_client

        list_jobs("spark-app-123")

        mock_client.list_jobs.assert_called_once_with(
            app_id="spark-app-123",
            status=None,
            app_attempt_id=None,
            offset=0,
            length=None,
        )

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_list_jobs_negative_offset_raises(self, mock_get_client):
        """Test list_jobs rejects negative offset"""
        mock_get_client.return_value = MagicMock()

        with self.assertRaises(ValueError):
            list_jobs("spark-app-123", offset=-1)

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_list_stages_with_pagination(self, mock_get_client):
        """Test list_stages forwards offset and length to client"""
        mock_client = MagicMock()
        mock_client.list_stages.return_value = [MagicMock(spec=StageData)]
        mock_get_client.return_value = mock_client

        list_stages("spark-app-123", offset=2, length=5)

        mock_client.list_stages.assert_called_once_with(
            app_id="spark-app-123",
            status=None,
            with_summaries=False,
            app_attempt_id=None,
            offset=2,
            length=5,
        )

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_list_stages_negative_length_raises(self, mock_get_client):
        """Test list_stages rejects negative length"""
        mock_get_client.return_value = MagicMock()

        with self.assertRaises(ValueError):
            list_stages("spark-app-123", length=-1)

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_list_executors_with_pagination(self, mock_get_client):
        """Test list_executors forwards offset and length to client"""
        mock_client = MagicMock()
        mock_client.list_executors.return_value = []
        mock_get_client.return_value = mock_client

        list_executors("spark-app-123", offset=3, length=10)

        mock_client.list_executors.assert_called_once_with(
            app_id="spark-app-123", app_attempt_id=None, offset=3, length=10
        )

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_list_executors_inactive_with_pagination(self, mock_get_client):
        """Test list_executors with include_inactive uses list_all_executors"""
        mock_client = MagicMock()
        mock_client.list_all_executors.return_value = []
        mock_get_client.return_value = mock_client

        list_executors("spark-app-123", include_inactive=True, offset=0, length=20)

        mock_client.list_all_executors.assert_called_once_with(
            app_id="spark-app-123", app_attempt_id=None, offset=0, length=20
        )
