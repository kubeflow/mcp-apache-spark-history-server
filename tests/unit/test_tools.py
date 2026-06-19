import unittest
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

from spark_history_mcp.api.spark_client import SparkRestClient
from spark_history_mcp.api_client.models.application import Application
from spark_history_mcp.api_client.models.environment import Environment
from spark_history_mcp.api_client.models.executor import Executor
from spark_history_mcp.api_client.models.job import Job
from spark_history_mcp.api_client.models.sql_execution import SQLExecution
from spark_history_mcp.api_client.models.stage_data import StageData
from spark_history_mcp.api_client.models.task_metrics_summary import TaskMetricsSummary
from spark_history_mcp.api_client.models.thread_stack_trace import ThreadStackTrace
from spark_history_mcp.tools.tools import (
    _build_stage_task_quantiles,
    _calculate_executor_metrics,
    _filter_environment_section,
    _filter_threads,
    compare_sql_executions,
    compare_stages,
    get_client_or_default,
    get_environment,
    get_executor_thread_dump,
    get_sql_execution,
    get_stage,
    list_applications,
    list_executors,
    list_jobs,
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

    def test_get_client_not_found_errors(self):
        """A requested server that does not exist raises, even with a default."""
        self.mock_lifespan_context.default_client = self.mock_client1

        with self.assertRaises(ValueError) as context:
            get_client_or_default(self.mock_ctx, "non_existent_server")

        self.assertIn("non_existent_server", str(context.exception))

    def test_no_client_found(self):
        """An unknown server with no default still raises (now naming the server)."""
        self.mock_lifespan_context.default_client = None

        # Try to get non-existent client with no default
        with self.assertRaises(ValueError) as context:
            get_client_or_default(self.mock_ctx, "non_existent_server")

        self.assertIn("non_existent_server", str(context.exception))

    def test_no_default_client(self):
        """Test error when no name is provided and no default exists"""
        self.mock_lifespan_context.default_client = None

        # Try to get default client when none exists
        with self.assertRaises(ValueError) as context:
            get_client_or_default(self.mock_ctx)

        self.assertIn("No Spark client found", str(context.exception))

    @staticmethod
    def _job(
        job_id=0,
        status="SUCCEEDED",
        failed=0,
        sub="2025-08-05T00:00:00.000GMT",
        comp="2025-08-05T00:00:10.000GMT",
    ):
        """Build a Job mock with the attributes list_jobs sorting reads."""
        j = MagicMock(spec=Job)
        j.job_id = job_id
        j.status = status
        j.num_failed_tasks = failed
        j.submission_time = sub
        j.completion_time = comp
        return j

    @staticmethod
    def _stage(
        stage_id=0,
        status="COMPLETE",
        failed=0,
        sub="2025-08-05T00:00:00.000GMT",
        comp="2025-08-05T00:00:10.000GMT",
    ):
        """Build a StageData mock with the attributes list_stages sorting reads."""
        s = MagicMock(spec=StageData)
        s.stage_id = stage_id
        s.status = status
        s.num_failed_tasks = failed
        s.submission_time = sub
        s.completion_time = comp
        return s

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_list_jobs_sort_by_duration(self, mock_get_client):
        """sort_by='duration' with length returns the N slowest (running jobs last)"""
        mock_client = MagicMock()

        job1 = MagicMock(spec=Job)
        job1.job_id = 1
        job1.status = "RUNNING"
        job1.submission_time = datetime.now() - timedelta(minutes=10)
        job1.completion_time = None  # no duration -> sorts last

        job2 = MagicMock(spec=Job)
        job2.job_id = 2
        job2.status = "SUCCEEDED"
        job2.submission_time = datetime.now() - timedelta(minutes=5)
        job2.completion_time = datetime.now() - timedelta(minutes=3)  # 2 min

        job3 = MagicMock(spec=Job)
        job3.job_id = 3
        job3.status = "SUCCEEDED"
        job3.submission_time = datetime.now() - timedelta(minutes=10)
        job3.completion_time = datetime.now() - timedelta(minutes=5)  # 5 min

        mock_client.list_jobs.return_value = [job1, job2, job3]
        mock_get_client.return_value = mock_client

        result = list_jobs("app-123", sort_by="duration", length=2)

        self.assertEqual([j.job_id for j in result], [3, 2])
        # When sorting, the full set is fetched (no server-side pagination).
        mock_client.list_jobs.assert_called_once_with(
            app_id="app-123", status=None, app_attempt_id=None
        )

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_list_jobs_sort_by_id(self, mock_get_client):
        """sort_by='id' orders jobs by descending job_id"""
        mock_client = MagicMock()
        job_a = MagicMock(spec=Job)
        job_a.job_id = 3
        job_b = MagicMock(spec=Job)
        job_b.job_id = 1
        job_c = MagicMock(spec=Job)
        job_c.job_id = 2
        mock_client.list_jobs.return_value = [job_a, job_b, job_c]
        mock_get_client.return_value = mock_client

        result = list_jobs("app-123", sort_by="id")

        self.assertEqual([j.job_id for j in result], [3, 2, 1])

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_list_jobs_sort_by_failed_tasks(self, mock_get_client):
        """sort_by='failed-tasks' orders jobs by descending failed task count"""
        mock_client = MagicMock()
        job_a = MagicMock(spec=Job)
        job_a.job_id = 1
        job_a.num_failed_tasks = 2
        job_b = MagicMock(spec=Job)
        job_b.job_id = 2
        job_b.num_failed_tasks = 9
        job_c = MagicMock(spec=Job)
        job_c.job_id = 3
        job_c.num_failed_tasks = 0
        mock_client.list_jobs.return_value = [job_a, job_b, job_c]
        mock_get_client.return_value = mock_client

        result = list_jobs("app-123", sort_by="failed-tasks")

        self.assertEqual([j.job_id for j in result], [2, 1, 3])

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_list_jobs_default_order_failed_first(self, mock_get_client):
        """Default ordering puts failed jobs first, then by duration descending"""
        mock_client = MagicMock()

        succeeded = MagicMock(spec=Job)
        succeeded.job_id = 1
        succeeded.status = "SUCCEEDED"
        succeeded.submission_time = datetime.now() - timedelta(minutes=10)
        succeeded.completion_time = datetime.now() - timedelta(minutes=1)  # 9 min

        failed = MagicMock(spec=Job)
        failed.job_id = 2
        failed.status = "FAILED"
        failed.submission_time = datetime.now() - timedelta(minutes=5)
        failed.completion_time = datetime.now() - timedelta(minutes=4)  # 1 min

        mock_client.list_jobs.return_value = [succeeded, failed]
        mock_get_client.return_value = mock_client

        result = list_jobs("app-123")

        # Failed job first despite its shorter duration.
        self.assertEqual([j.job_id for j in result], [2, 1])

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_list_jobs_sort_by_invalid(self, mock_get_client):
        """An unknown sort_by value raises ValueError"""
        mock_client = MagicMock()
        mock_client.list_jobs.return_value = [MagicMock(spec=Job)]
        mock_get_client.return_value = mock_client

        with self.assertRaises(ValueError):
            list_jobs("app-123", sort_by="bogus")

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

    # Tests for the list_applications app_id filter (single-application lookup)
    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_list_applications_by_id(self, mock_get_client):
        """app_id returns the single application as a one-element list"""
        mock_client = MagicMock()
        mock_app = MagicMock(spec=Application)
        mock_app.id = "spark-app-123"
        mock_app.name = "Test Application"
        mock_client.get_application.return_value = mock_app
        mock_get_client.return_value = mock_client

        result = list_applications(app_id="spark-app-123")

        self.assertEqual(result, [mock_app])
        mock_client.get_application.assert_called_once_with("spark-app-123")
        mock_get_client.assert_called_once_with(
            unittest.mock.ANY, None, "spark-app-123"
        )

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_list_applications_by_id_with_server(self, mock_get_client):
        """app_id honors an explicit server"""
        mock_client = MagicMock()
        mock_app = MagicMock(spec=Application)
        mock_client.get_application.return_value = mock_app
        mock_get_client.return_value = mock_client

        list_applications(app_id="spark-app-123", server="production")

        mock_get_client.assert_called_once_with(
            unittest.mock.ANY, "production", "spark-app-123"
        )

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_list_applications_by_id_not_found(self, mock_get_client):
        """app_id propagates a not-found error"""
        mock_client = MagicMock()
        mock_client.get_application.side_effect = Exception("Application not found")
        mock_get_client.return_value = mock_client

        with self.assertRaises(Exception) as context:
            list_applications(app_id="non-existent-app")

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
        mock_jobs = [self._job(0), self._job(1)]
        mock_client.list_jobs.return_value = mock_jobs
        mock_get_client.return_value = mock_client

        result = list_jobs("spark-app-123")

        self.assertEqual({j.job_id for j in result}, {0, 1})
        mock_client.list_jobs.assert_called_once_with(
            app_id="spark-app-123",
            status=None,
            app_attempt_id=None,
        )

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_list_jobs_with_status_filter(self, mock_get_client):
        """Test job retrieval with status filter"""
        mock_client = MagicMock()
        mock_jobs = [self._job(1, "SUCCEEDED")]
        mock_client.list_jobs.return_value = mock_jobs
        mock_get_client.return_value = mock_client

        result = list_jobs("spark-app-123", status=["SUCCEEDED"])

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].status, "SUCCEEDED")

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_list_jobs_with_job_id_filter(self, mock_get_client):
        """job_id returns only the matching job"""
        mock_client = MagicMock()
        mock_client.list_jobs.return_value = [self._job(1), self._job(2)]
        mock_get_client.return_value = mock_client

        result = list_jobs("spark-app-123", job_id=2)

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].job_id, 2)

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_list_jobs_with_job_id_filter_no_match(self, mock_get_client):
        """job_id with no matching job returns an empty list"""
        mock_client = MagicMock()
        mock_client.list_jobs.return_value = [self._job(1)]
        mock_get_client.return_value = mock_client

        result = list_jobs("spark-app-123", job_id=99)

        self.assertEqual(result, [])

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

        # Client returns only the SUCCEEDED job when filtered.
        mock_client.list_jobs.return_value = [self._job(2, "SUCCEEDED")]
        mock_get_client.return_value = mock_client

        result = list_jobs("spark-app-123", status=["SUCCEEDED"])

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].status, "SUCCEEDED")

    # Tests for list_stages tool
    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_get_stages_no_filter(self, mock_get_client):
        """Test stage retrieval without filters"""
        mock_client = MagicMock()
        mock_stages = [self._stage(0), self._stage(1)]
        mock_client.list_stages.return_value = mock_stages
        mock_get_client.return_value = mock_client

        result = list_stages("spark-app-123")

        self.assertEqual({s.stage_id for s in result}, {0, 1})
        mock_client.list_stages.assert_called_once_with(
            app_id="spark-app-123",
            status=None,
            with_summaries=False,
            app_attempt_id=None,
        )

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_get_stages_with_status_filter(self, mock_get_client):
        """Test stage retrieval with status filter"""
        mock_client = MagicMock()
        # Client returns only the COMPLETE stage when filtered.
        mock_client.list_stages.return_value = [self._stage(1, "COMPLETE")]
        mock_get_client.return_value = mock_client

        result = list_stages("spark-app-123", status=["COMPLETE"])

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].status, "COMPLETE")

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_get_stages_with_summaries(self, mock_get_client):
        """Test stage retrieval with summaries enabled"""
        mock_client = MagicMock()
        mock_client.list_stages.return_value = [self._stage(0)]
        mock_get_client.return_value = mock_client

        list_stages("spark-app-123", with_summaries=True)

        # Verify summaries parameter is passed
        mock_client.list_stages.assert_called_once_with(
            app_id="spark-app-123",
            status=None,
            with_summaries=True,
            app_attempt_id=None,
        )

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_get_stages_empty_result(self, mock_get_client):
        """Test stage retrieval with empty result"""
        mock_client = MagicMock()
        mock_client.list_stages.return_value = []
        mock_get_client.return_value = mock_client

        result = list_stages("spark-app-123")

        self.assertEqual(result, [])

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_get_stage_with_summaries_custom_quantiles(self, mock_get_client):
        """get_stage forwards custom quantiles to the task summary fetch"""
        mock_client = MagicMock()
        mock_stage = MagicMock(spec=StageData)
        mock_stage.attempt_id = 0
        mock_stage.task_metrics_distributions = None
        mock_summary = MagicMock(spec=TaskMetricsSummary)
        mock_client.get_stage_attempt.return_value = mock_stage
        mock_client.get_stage_task_summary.return_value = mock_summary
        mock_get_client.return_value = mock_client

        get_stage(
            "app-123",
            stage_id=1,
            attempt_id=0,
            with_summaries=True,
            quantiles="0.25,0.5,0.75",
        )

        mock_client.get_stage_task_summary.assert_called_once_with(
            app_id="app-123",
            stage_id=1,
            attempt_id=0,
            quantiles="0.25,0.5,0.75",
        )

    # Tests for list_stages sorting
    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_list_stages_sort_by_duration(self, mock_get_client):
        """sort_by='duration' with length returns the N longest stages"""
        mock_client = MagicMock()
        mock_client.list_stages.return_value = [
            self._stage(1, comp="2025-08-05T00:00:02.000GMT"),  # 2s
            self._stage(2, comp="2025-08-05T00:00:09.000GMT"),  # 9s
            self._stage(3, comp="2025-08-05T00:00:05.000GMT"),  # 5s
        ]
        mock_get_client.return_value = mock_client

        result = list_stages("app-123", sort_by="duration", length=2)

        self.assertEqual([s.stage_id for s in result], [2, 3])
        mock_client.list_stages.assert_called_once_with(
            app_id="app-123",
            status=None,
            with_summaries=False,
            app_attempt_id=None,
        )

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_list_stages_sort_by_failed_tasks(self, mock_get_client):
        """sort_by='failed-tasks' orders by descending failed task count"""
        mock_client = MagicMock()
        mock_client.list_stages.return_value = [
            self._stage(1, failed=2),
            self._stage(2, failed=9),
            self._stage(3, failed=0),
        ]
        mock_get_client.return_value = mock_client

        result = list_stages("app-123", sort_by="failed-tasks")

        self.assertEqual([s.stage_id for s in result], [2, 1, 3])

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_list_stages_sort_by_id(self, mock_get_client):
        """sort_by='id' orders by descending stage_id"""
        mock_client = MagicMock()
        mock_client.list_stages.return_value = [
            self._stage(3),
            self._stage(1),
            self._stage(2),
        ]
        mock_get_client.return_value = mock_client

        result = list_stages("app-123", sort_by="id")

        self.assertEqual([s.stage_id for s in result], [3, 2, 1])

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_list_stages_default_order_failed_first(self, mock_get_client):
        """Default ordering puts failed stages first, then by duration descending"""
        mock_client = MagicMock()
        mock_client.list_stages.return_value = [
            self._stage(1, "COMPLETE", comp="2025-08-05T00:00:09.000GMT"),  # 9s
            self._stage(2, "FAILED", comp="2025-08-05T00:00:01.000GMT"),  # 1s
        ]
        mock_get_client.return_value = mock_client

        result = list_stages("app-123")

        # Failed stage first despite its shorter duration.
        self.assertEqual([s.stage_id for s in result], [2, 1])

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_list_stages_sort_by_invalid(self, mock_get_client):
        """An unknown sort_by value raises ValueError"""
        mock_client = MagicMock()
        mock_client.list_stages.return_value = [self._stage(1)]
        mock_get_client.return_value = mock_client

        with self.assertRaises(ValueError):
            list_stages("app-123", sort_by="bogus")

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
        # No plan diff unless requested.
        self.assertIsNone(result.plan_comparison)

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_compare_sql_executions_with_plan_diff(self, mock_get_client):
        """include_plan_diff attaches a plan_comparison with node/edge counts and diffs"""

        def node(name):
            n = MagicMock()
            n.node_name = name
            return n

        plan_a = MagicMock(spec=SQLExecution)
        plan_a.nodes = [node("Filter"), node("Scan"), node("Scan")]
        plan_a.edges = [MagicMock(), MagicMock()]

        plan_b = MagicMock(spec=SQLExecution)
        plan_b.nodes = [node("Filter"), node("Scan")]
        plan_b.edges = [MagicMock()]

        client_a = MagicMock()
        # First call (details=False) feeds metrics; second (details=True) feeds the plan diff.
        client_a.get_sql_execution.side_effect = [
            self._mk_sql(1, 5000, description="q", success=[1]),
            plan_a,
        ]
        client_a.list_jobs.return_value = [self._mk_job(1, "SUCCEEDED", [10])]
        client_a.list_stages.return_value = [self._mk_stage(10, tasks=10)]

        client_b = MagicMock()
        client_b.get_sql_execution.side_effect = [
            self._mk_sql(2, 8000, description="q", success=[5]),
            plan_b,
        ]
        client_b.list_jobs.return_value = [self._mk_job(5, "SUCCEEDED", [20])]
        client_b.list_stages.return_value = [self._mk_stage(20, tasks=30)]

        mock_get_client.side_effect = [client_a, client_b]

        result = compare_sql_executions("app-a", "app-b", 1, 2, include_plan_diff=True)

        self.assertEqual(result.a.tasks, 10)
        self.assertEqual(result.b.tasks, 30)
        pc = result.plan_comparison
        self.assertIsNotNone(pc)
        self.assertEqual(pc.app_a, "app-a")
        self.assertEqual(pc.exec_id_a, 1)
        self.assertEqual(pc.node_count_a, 3)
        self.assertEqual(pc.node_count_b, 2)
        self.assertEqual(pc.edge_count_a, 2)
        self.assertEqual(pc.edge_count_b, 1)
        diffs = {d.node_type: (d.a, d.b) for d in pc.node_type_diffs}
        self.assertEqual(diffs, {"Scan": (2, 1)})

    # Tests for pagination support

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_list_jobs_with_pagination(self, mock_get_client):
        """Test list_jobs applies offset and length client-side"""
        mock_client = MagicMock()
        # Equal sort keys keep input order stable, so slicing is predictable.
        mock_client.list_jobs.return_value = [self._job(i) for i in range(20)]
        mock_get_client.return_value = mock_client

        result = list_jobs("spark-app-123", offset=5, length=10)

        self.assertEqual([j.job_id for j in result], list(range(5, 15)))
        mock_client.list_jobs.assert_called_once_with(
            app_id="spark-app-123",
            status=None,
            app_attempt_id=None,
        )

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_list_jobs_pagination_defaults(self, mock_get_client):
        """Test list_jobs fetches the full set when no pagination is given"""
        mock_client = MagicMock()
        mock_client.list_jobs.return_value = []
        mock_get_client.return_value = mock_client

        list_jobs("spark-app-123")

        mock_client.list_jobs.assert_called_once_with(
            app_id="spark-app-123",
            status=None,
            app_attempt_id=None,
        )

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_list_jobs_negative_offset_raises(self, mock_get_client):
        """Test list_jobs rejects negative offset"""
        mock_get_client.return_value = MagicMock()

        with self.assertRaises(ValueError):
            list_jobs("spark-app-123", offset=-1)

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_list_stages_with_pagination(self, mock_get_client):
        """Test list_stages applies offset and length client-side"""
        mock_client = MagicMock()
        mock_client.list_stages.return_value = [self._stage(i) for i in range(20)]
        mock_get_client.return_value = mock_client

        result = list_stages("spark-app-123", offset=2, length=5)

        self.assertEqual([s.stage_id for s in result], [2, 3, 4, 5, 6])
        mock_client.list_stages.assert_called_once_with(
            app_id="spark-app-123",
            status=None,
            with_summaries=False,
            app_attempt_id=None,
        )

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_list_stages_negative_length_raises(self, mock_get_client):
        """Test list_stages rejects negative length"""
        mock_get_client.return_value = MagicMock()

        with self.assertRaises(ValueError):
            list_stages("spark-app-123", length=-1)

    @staticmethod
    def _exec(exec_id="1", active=True, duration=0, gc=0, failed=0):
        e = MagicMock(spec=Executor)
        e.id = exec_id
        e.is_active = active
        e.total_duration = duration
        e.total_gc_time = gc
        e.failed_tasks = failed
        return e

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_list_executors_with_pagination(self, mock_get_client):
        """Test list_executors applies offset and length client-side"""
        mock_client = MagicMock()
        # Equal sort keys (all active, duration 0) keep input order stable.
        mock_client.list_executors.return_value = [
            self._exec(str(i)) for i in range(10)
        ]
        mock_get_client.return_value = mock_client

        result = list_executors("spark-app-123", offset=3, length=2)

        self.assertEqual([e.id for e in result], ["3", "4"])
        mock_client.list_executors.assert_called_once_with(
            app_id="spark-app-123", app_attempt_id=None
        )

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_list_executors_inactive_uses_list_all(self, mock_get_client):
        """Test list_executors with include_inactive uses list_all_executors"""
        mock_client = MagicMock()
        mock_client.list_all_executors.return_value = []
        mock_get_client.return_value = mock_client

        list_executors("spark-app-123", include_inactive=True)

        mock_client.list_all_executors.assert_called_once_with(
            app_id="spark-app-123", app_attempt_id=None
        )

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_list_executors_executor_id_filter(self, mock_get_client):
        """executor_id searches all executors and returns only the match"""
        mock_client = MagicMock()
        mock_client.list_all_executors.return_value = [
            self._exec("driver"),
            self._exec("1", active=False),
        ]
        mock_get_client.return_value = mock_client

        result = list_executors("spark-app-123", executor_id="1")

        self.assertEqual([e.id for e in result], ["1"])
        # Lookup searches all executors (incl. inactive).
        mock_client.list_all_executors.assert_called_once_with(
            app_id="spark-app-123", app_attempt_id=None
        )

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_list_executors_executor_id_no_match(self, mock_get_client):
        """executor_id with no match returns an empty list"""
        mock_client = MagicMock()
        mock_client.list_all_executors.return_value = [self._exec("driver")]
        mock_get_client.return_value = mock_client

        result = list_executors("spark-app-123", executor_id="99")

        self.assertEqual(result, [])

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_list_executors_sort_by_gc(self, mock_get_client):
        """sort_by='gc' orders by descending GC time"""
        mock_client = MagicMock()
        mock_client.list_executors.return_value = [
            self._exec("1", gc=10),
            self._exec("2", gc=90),
            self._exec("3", gc=50),
        ]
        mock_get_client.return_value = mock_client

        result = list_executors("spark-app-123", sort_by="gc")

        self.assertEqual([e.id for e in result], ["2", "3", "1"])

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_list_executors_sort_by_id_ascending(self, mock_get_client):
        """sort_by='id' orders by ascending string ID"""
        mock_client = MagicMock()
        mock_client.list_executors.return_value = [
            self._exec("2"),
            self._exec("driver"),
            self._exec("10"),
            self._exec("1"),
        ]
        mock_get_client.return_value = mock_client

        result = list_executors("spark-app-123", sort_by="id")

        # Ascending lexicographic string order.
        self.assertEqual([e.id for e in result], ["1", "10", "2", "driver"])

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_list_executors_default_order_active_first(self, mock_get_client):
        """Default ordering puts active executors first, then by duration desc"""
        mock_client = MagicMock()
        mock_client.list_executors.return_value = [
            self._exec("dead-long", active=False, duration=999),
            self._exec("active-short", active=True, duration=1),
        ]
        mock_get_client.return_value = mock_client

        result = list_executors("spark-app-123")

        # Active executor first despite shorter duration.
        self.assertEqual([e.id for e in result], ["active-short", "dead-long"])

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_list_executors_sort_by_invalid(self, mock_get_client):
        """An unknown sort_by value raises ValueError"""
        mock_client = MagicMock()
        mock_client.list_executors.return_value = [self._exec("1")]
        mock_get_client.return_value = mock_client

        with self.assertRaises(ValueError):
            list_executors("spark-app-123", sort_by="bogus")

    # Tests for get_environment section filtering
    @staticmethod
    def _environment():
        return Environment.from_dict(
            {
                "runtime": {"javaVersion": "17", "scalaVersion": "2.12"},
                "sparkProperties": [["spark.app.name", "demo"]],
                "systemProperties": [["os.name", "Linux"]],
                "hadoopProperties": [["fs.defaultFS", "file:///"]],
                "metricsProperties": [["*.sink.csv.class", "x"]],
                "classpathEntries": [["/opt/spark/jars/x.jar", "System Classpath"]],
            }
        )

    def test_filter_environment_section_keeps_only_requested(self):
        """Filtering keeps the requested section and clears the others."""
        env = _filter_environment_section(self._environment(), "spark_properties")
        self.assertTrue(env.spark_properties)
        self.assertIsNone(env.runtime)
        self.assertIsNone(env.system_properties)
        self.assertIsNone(env.hadoop_properties)
        self.assertIsNone(env.metrics_properties)
        self.assertIsNone(env.classpath_entries)

    def test_filter_environment_section_runtime(self):
        """The runtime section maps to the runtime field."""
        env = _filter_environment_section(self._environment(), "runtime")
        self.assertIsNotNone(env.runtime)
        self.assertIsNone(env.spark_properties)

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_get_environment_invalid_section(self, mock_get_client):
        """An unknown section raises ValueError."""
        mock_client = MagicMock()
        mock_client.get_environment.return_value = self._environment()
        mock_get_client.return_value = mock_client

        with self.assertRaises(ValueError):
            get_environment("spark-app-123", section="bogus")

    # Tests for compare_stages
    def test_build_stage_task_quantiles_maps_nested_metrics(self):
        """Nested input/output/shuffle metrics are flattened into the model."""
        summary = TaskMetricsSummary.from_dict(
            {
                "quantiles": [0.25, 0.5, 0.75, 1.0],
                "duration": [1, 2, 3, 4],
                "jvmGcTime": [0, 1, 1, 2],
                "schedulerDelay": [1, 1, 1, 1],
                "inputMetrics": {"bytesRead": [10, 20, 30, 40]},
                "outputMetrics": {"bytesWritten": [1, 2, 3, 4]},
                "shuffleReadMetrics": {"readBytes": [5, 6, 7, 8]},
                "shuffleWriteMetrics": {"writeBytes": [0, 0, 0, 0]},
            }
        )
        q = _build_stage_task_quantiles(summary)
        self.assertEqual(q.quantiles, [0.25, 0.5, 0.75, 1.0])
        self.assertEqual(q.duration, [1, 2, 3, 4])
        self.assertEqual(q.gc_time, [0, 1, 1, 2])
        self.assertEqual(q.input_bytes, [10, 20, 30, 40])
        self.assertEqual(q.output_bytes, [1, 2, 3, 4])
        self.assertEqual(q.shuffle_read_bytes, [5, 6, 7, 8])
        self.assertEqual(q.shuffle_write_bytes, [0, 0, 0, 0])

    def test_build_stage_task_quantiles_none(self):
        """A missing summary maps to None."""
        self.assertIsNone(_build_stage_task_quantiles(None))

    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_compare_stages_not_found(self, mock_get_client):
        """A stage with no attempts raises ValueError."""
        mock_client = MagicMock()
        mock_client.list_stage_attempts.return_value = []
        mock_get_client.return_value = mock_client

        with self.assertRaises(ValueError):
            compare_stages("app-1", 999, "app-2", 1)

    # Tests for get_executor_thread_dump / _filter_threads
    @staticmethod
    def _sample_threads():
        return [
            ThreadStackTrace(threadId=1, threadName="main", threadState="RUNNABLE"),
            ThreadStackTrace(
                threadId=2,
                threadName="dispatcher-event-loop-0",
                threadState="WAITING",
                blockedByThreadId=1,
            ),
            ThreadStackTrace(
                threadId=3,
                threadName="Executor task launch worker for task 42",
                threadState="BLOCKED",
                lockName="java.util.concurrent.locks.ReentrantLock",
            ),
            ThreadStackTrace(
                threadId=4,
                threadName="DispatcherWatcher",
                threadState="TIMED_WAITING",
            ),
        ]

    def test_filter_threads(self):
        cases = [
            ("no filter", None, None, False, [1, 2, 3, 4]),
            ("state RUNNABLE", "RUNNABLE", None, False, [1]),
            ("state lowercase matches", "blocked", None, False, [3]),
            ("name substring case-insensitive", None, "DISPATCHER", False, [2, 4]),
            ("blocked-only catches blocked_by", None, None, True, [2, 3]),
            ("combine state + name", "WAITING", "dispatcher", False, [2]),
            ("no match returns empty", None, "nonexistent", False, []),
        ]
        for label, state, name, blocked_only, want_ids in cases:
            with self.subTest(label):
                got = _filter_threads(self._sample_threads(), state, name, blocked_only)
                self.assertEqual([t.thread_id for t in got], want_ids)

    @patch("spark_history_mcp.tools.tools.mcp")
    @patch("spark_history_mcp.tools.tools.get_client_or_default")
    def test_get_executor_thread_dump_sorts_by_id(self, mock_get_client, mock_mcp):
        mock_client = MagicMock()
        # Returned out of order; the tool sorts by thread ID.
        mock_client.get_executor_thread_dump.return_value = list(
            reversed(self._sample_threads())
        )
        mock_get_client.return_value = mock_client

        threads = get_executor_thread_dump("app-1", "driver")
        self.assertEqual([t.thread_id for t in threads], [1, 2, 3, 4])
        mock_client.get_executor_thread_dump.assert_called_once_with(
            app_id="app-1", executor_id="driver", app_attempt_id=None
        )
