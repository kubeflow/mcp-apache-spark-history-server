import unittest
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

from spark_history_mcp.api.spark_client import SparkRestClient
from spark_history_mcp.models.server_spec import (
    DynamicEMRServerSpec,
    ServerSpec,
    StaticServerSpec,
)
from spark_history_mcp.models.spark_types import (
    ApplicationInfo,
    ExecutionData,
    JobData,
    StageData,
    TaskMetricDistributions,
)
from spark_history_mcp.tools.tools import (
    get_application,
    get_client,
    get_stage,
    get_stage_task_summary,
    list_jobs,
    list_slowest_jobs,
    list_slowest_sql_queries,
    list_stages,
)


class MockRequestContext:
    """Simple mock request context that can store attributes properly"""

    def __init__(self, lifespan_context):
        self.lifespan_context = lifespan_context


class TestTools(unittest.TestCase):
    # Common server spec for tests that use default client
    DEFAULT_SERVER_SPEC = ServerSpec(
        static_server_spec=StaticServerSpec(default_client=True)
    )

    def setUp(self):
        # Create mock context
        self.mock_ctx = MagicMock()
        self.mock_lifespan_context = MagicMock()

        # Create a request context that can properly store attributes
        self.mock_request_context = MockRequestContext(self.mock_lifespan_context)
        self.mock_ctx.request_context = self.mock_request_context

        # Create mock clients
        self.mock_client1 = MagicMock(spec=SparkRestClient)
        self.mock_client2 = MagicMock(spec=SparkRestClient)

        # Set up static clients structure
        self.mock_static_clients = MagicMock()
        self.mock_static_clients.clients = {
            "server1": self.mock_client1,
            "server2": self.mock_client2,
        }
        self.mock_static_clients.default_client = self.mock_client1

        # Set up lifespan context for static mode
        self.mock_lifespan_context.dynamic_emr_clusters_mode = False
        self.mock_lifespan_context.static_clients = self.mock_static_clients
        self.mock_lifespan_context.emr_client = None

    def test_get_client_with_static_server_name(self):
        """Test getting a client by server name with static spec"""
        server_spec = ServerSpec(
            static_server_spec=StaticServerSpec(server_name="server2")
        )

        # Get client by name
        client = get_client(self.mock_ctx, server_spec)

        # Should return the requested client
        self.assertEqual(client, self.mock_client2)

    def test_get_default_client_static(self):
        """Test getting the default client with static spec"""
        server_spec = ServerSpec(
            static_server_spec=StaticServerSpec(default_client=True)
        )

        # Get default client
        client = get_client(self.mock_ctx, server_spec)

        # Should return the default client
        self.assertEqual(client, self.mock_client1)

    def test_get_client_not_found_static(self):
        """Test error when requested static client is not found"""
        server_spec = ServerSpec(
            static_server_spec=StaticServerSpec(server_name="non_existent_server")
        )

        # Try to get non-existent client
        with self.assertRaises(ValueError) as context:
            get_client(self.mock_ctx, server_spec)

        self.assertIn("No server configured with name", str(context.exception))

    def test_no_default_client_static(self):
        """Test error when no default client exists in static mode"""
        self.mock_static_clients.default_client = None
        server_spec = ServerSpec(
            static_server_spec=StaticServerSpec(default_client=True)
        )

        # Try to get default client when none exists
        with self.assertRaises(ValueError) as context:
            get_client(self.mock_ctx, server_spec)

        self.assertIn("No default client configured", str(context.exception))

    def test_dynamic_emr_mode_with_static_spec_error(self):
        """Test error when using static spec in dynamic EMR mode"""
        self.mock_lifespan_context.dynamic_emr_clusters_mode = True
        server_spec = ServerSpec(
            static_server_spec=StaticServerSpec(default_client=True)
        )

        with self.assertRaises(ValueError) as context:
            get_client(self.mock_ctx, server_spec)

        self.assertIn(
            "MCP is running in dynamic EMR mode, but static server spec was provided",
            str(context.exception),
        )

    def test_static_mode_with_dynamic_spec_error(self):
        """Test error when using dynamic spec in static mode"""
        server_spec = ServerSpec(
            dynamic_emr_server_spec=DynamicEMRServerSpec(emr_cluster_id="j-123456")
        )

        with self.assertRaises(ValueError) as context:
            get_client(self.mock_ctx, server_spec)

        self.assertIn(
            "MCP is not running in dynamic EMR mode, but dynamic server spec was provided",
            str(context.exception),
        )

    @patch("spark_history_mcp.tools.tools.create_spark_emr_client")
    def test_dynamic_emr_client_by_cluster_arn(self, mock_create_client):
        """Test getting EMR client by cluster ARN in dynamic mode"""
        # Set up dynamic EMR mode
        self.mock_lifespan_context.dynamic_emr_clusters_mode = True
        self.mock_lifespan_context.static_clients = None

        # Mock the create_spark_emr_client function
        mock_emr_client = MagicMock(spec=SparkRestClient)
        mock_create_client.return_value = mock_emr_client

        server_spec = ServerSpec(
            dynamic_emr_server_spec=DynamicEMRServerSpec(
                emr_cluster_arn="arn:aws:elasticmapreduce:us-east-1:123456789012:cluster/j-1234567890ABC"
            )
        )

        client = get_client(self.mock_ctx, server_spec)

        self.assertEqual(client, mock_emr_client)
        mock_create_client.assert_called_once_with(
            "arn:aws:elasticmapreduce:us-east-1:123456789012:cluster/j-1234567890ABC"
        )

    @patch("spark_history_mcp.tools.tools.create_spark_emr_client")
    def test_dynamic_emr_client_by_cluster_id(self, mock_create_client):
        """Test getting EMR client by cluster ID in dynamic mode"""
        # Set up dynamic EMR mode with mock EMR client
        self.mock_lifespan_context.dynamic_emr_clusters_mode = True
        self.mock_lifespan_context.static_clients = None

        # Clear caches to ensure clean test
        from spark_history_mcp.tools.tools import (
            arn_to_spark_emr_client_cache,
            emr_cluster_id_to_arn_cache,
        )

        arn_to_spark_emr_client_cache.clear()
        emr_cluster_id_to_arn_cache.clear()

        mock_emr_client = MagicMock()
        mock_emr_client.get_cluster_arn_by_id.return_value = (
            "arn:aws:elasticmapreduce:us-east-1:123456789012:cluster/j-1234567890ABC"
        )
        self.mock_lifespan_context.emr_client = mock_emr_client

        # Mock the create_spark_emr_client function
        mock_spark_client = MagicMock(spec=SparkRestClient)
        mock_create_client.return_value = mock_spark_client

        server_spec = ServerSpec(
            dynamic_emr_server_spec=DynamicEMRServerSpec(
                emr_cluster_id="j-1234567890ABC"
            )
        )

        client = get_client(self.mock_ctx, server_spec)

        self.assertEqual(client, mock_spark_client)
        mock_emr_client.get_cluster_arn_by_id.assert_called_once_with("j-1234567890ABC")
        mock_create_client.assert_called_once_with(
            "arn:aws:elasticmapreduce:us-east-1:123456789012:cluster/j-1234567890ABC"
        )

    @patch("spark_history_mcp.tools.tools.create_spark_emr_client")
    def test_dynamic_emr_client_by_cluster_name(self, mock_create_client):
        """Test getting EMR client by cluster name in dynamic mode"""
        # Set up dynamic EMR mode with mock EMR client
        self.mock_lifespan_context.dynamic_emr_clusters_mode = True
        self.mock_lifespan_context.static_clients = None

        # Clear caches to ensure clean test
        from spark_history_mcp.tools.tools import arn_to_spark_emr_client_cache

        arn_to_spark_emr_client_cache.clear()

        mock_emr_client = MagicMock()
        mock_emr_client.get_active_cluster_arn_by_name.return_value = (
            "arn:aws:elasticmapreduce:us-east-1:123456789012:cluster/j-1234567890ABC"
        )
        self.mock_lifespan_context.emr_client = mock_emr_client

        # Mock the create_spark_emr_client function
        mock_spark_client = MagicMock(spec=SparkRestClient)
        mock_create_client.return_value = mock_spark_client

        server_spec = ServerSpec(
            dynamic_emr_server_spec=DynamicEMRServerSpec(
                emr_cluster_name="test-cluster"
            )
        )

        client = get_client(self.mock_ctx, server_spec)

        self.assertEqual(client, mock_spark_client)
        mock_emr_client.get_active_cluster_arn_by_name.assert_called_once_with(
            "test-cluster"
        )
        mock_create_client.assert_called_once_with(
            "arn:aws:elasticmapreduce:us-east-1:123456789012:cluster/j-1234567890ABC"
        )

    @patch("spark_history_mcp.tools.tools.create_spark_emr_client")
    def test_dynamic_emr_cluster_name_caching(self, mock_create_client):
        """Test that cluster name to ARN mapping is cached (request-scoped)"""
        # Set up dynamic EMR mode with mock EMR client
        self.mock_lifespan_context.dynamic_emr_clusters_mode = True
        self.mock_lifespan_context.static_clients = None

        # Clear caches to ensure clean test
        from spark_history_mcp.tools.tools import arn_to_spark_emr_client_cache

        arn_to_spark_emr_client_cache.clear()
        # Note: cluster name cache is now request-scoped and automatically fresh per test

        mock_emr_client = MagicMock()
        mock_emr_client.get_active_cluster_arn_by_name.return_value = (
            "arn:aws:elasticmapreduce:us-east-1:123456789012:cluster/j-1234567890ABC"
        )
        self.mock_lifespan_context.emr_client = mock_emr_client

        # Mock the create_spark_emr_client function
        mock_spark_client = MagicMock(spec=SparkRestClient)
        mock_create_client.return_value = mock_spark_client

        server_spec = ServerSpec(
            dynamic_emr_server_spec=DynamicEMRServerSpec(
                emr_cluster_name="test-cluster"
            )
        )

        # First call should trigger EMR client call
        client1 = get_client(self.mock_ctx, server_spec)
        # Second call should use cached ARN
        client2 = get_client(self.mock_ctx, server_spec)

        # Both calls should return the same client
        self.assertEqual(client1, client2)
        # EMR client method should only be called once (caching works)
        mock_emr_client.get_active_cluster_arn_by_name.assert_called_once_with(
            "test-cluster"
        )
        # Spark client should only be created once (ARN caching leads to client caching)
        mock_create_client.assert_called_once_with(
            "arn:aws:elasticmapreduce:us-east-1:123456789012:cluster/j-1234567890ABC"
        )

    @patch("spark_history_mcp.tools.tools.create_spark_emr_client")
    def test_dynamic_emr_cluster_name_session_isolation(self, mock_create_client):
        """Test that cluster name caching is isolated between different sessions"""
        # Set up dynamic EMR mode with mock EMR client
        self.mock_lifespan_context.dynamic_emr_clusters_mode = True
        self.mock_lifespan_context.static_clients = None

        # Clear caches to ensure clean test
        from spark_history_mcp.tools.tools import (
            arn_to_spark_emr_client_cache,
            session_emr_cluster_name_to_arn_cache,
        )

        arn_to_spark_emr_client_cache.clear()
        session_emr_cluster_name_to_arn_cache.clear()

        mock_emr_client = MagicMock()
        # Configure the EMR client to return different ARNs for the same cluster name
        # to demonstrate that caching is working independently per session
        mock_emr_client.get_active_cluster_arn_by_name.side_effect = [
            "arn:aws:elasticmapreduce:us-east-1:123456789012:cluster/j-session1cluster",
            "arn:aws:elasticmapreduce:us-east-1:123456789012:cluster/j-session2cluster",
        ]
        self.mock_lifespan_context.emr_client = mock_emr_client

        # Create two different sessions
        session1 = MagicMock()
        session2 = MagicMock()

        # Create two different contexts with different sessions
        ctx1 = MagicMock()
        ctx1.session = session1
        ctx1.request_context = MockRequestContext(self.mock_lifespan_context)

        ctx2 = MagicMock()
        ctx2.session = session2
        ctx2.request_context = MockRequestContext(self.mock_lifespan_context)

        # Mock the create_spark_emr_client function
        mock_spark_client1 = MagicMock(spec=SparkRestClient)
        mock_spark_client2 = MagicMock(spec=SparkRestClient)
        mock_create_client.side_effect = [mock_spark_client1, mock_spark_client2]

        server_spec = ServerSpec(
            dynamic_emr_server_spec=DynamicEMRServerSpec(
                emr_cluster_name="shared-cluster-name"
            )
        )

        # Make calls from both sessions with the same cluster name
        client1 = get_client(ctx1, server_spec)
        client2 = get_client(ctx2, server_spec)

        # Both calls should trigger EMR client calls (no cross-session caching)
        self.assertEqual(mock_emr_client.get_active_cluster_arn_by_name.call_count, 2)

        # Verify each session got its own client
        self.assertEqual(client1, mock_spark_client1)
        self.assertEqual(client2, mock_spark_client2)

        # Verify different ARNs were used for Spark client creation
        expected_calls = [
            unittest.mock.call(
                "arn:aws:elasticmapreduce:us-east-1:123456789012:cluster/j-session1cluster"
            ),
            unittest.mock.call(
                "arn:aws:elasticmapreduce:us-east-1:123456789012:cluster/j-session2cluster"
            ),
        ]
        mock_create_client.assert_has_calls(expected_calls)

        # Now test that caching works within each session
        # Reset the mock to track additional calls
        mock_emr_client.reset_mock()

        # Make another call from session1 - should use cache
        client1_cached = get_client(ctx1, server_spec)
        # Make another call from session2 - should use cache
        client2_cached = get_client(ctx2, server_spec)

        # No additional EMR client calls should be made (caching is working)
        mock_emr_client.get_active_cluster_arn_by_name.assert_not_called()

        # Should return the same clients as before (from ARN cache)
        self.assertEqual(client1_cached, mock_spark_client1)
        self.assertEqual(client2_cached, mock_spark_client2)

    @patch("spark_history_mcp.tools.tools.create_spark_emr_client")
    def test_dynamic_emr_client_caching(self, mock_create_client):
        """Test that EMR clients are cached by ARN"""
        # Set up dynamic EMR mode
        self.mock_lifespan_context.dynamic_emr_clusters_mode = True
        self.mock_lifespan_context.static_clients = None

        arn = "arn:aws:elasticmapreduce:us-east-1:123456789012:cluster/j-1234567890ABC"

        # Clear the cache to ensure clean test
        from spark_history_mcp.tools.tools import arn_to_spark_emr_client_cache

        arn_to_spark_emr_client_cache.clear()

        # Mock the create_spark_emr_client function
        mock_spark_client = MagicMock(spec=SparkRestClient)
        mock_create_client.return_value = mock_spark_client

        server_spec = ServerSpec(
            dynamic_emr_server_spec=DynamicEMRServerSpec(emr_cluster_arn=arn)
        )

        # First call
        client1 = get_client(self.mock_ctx, server_spec)
        # Second call
        client2 = get_client(self.mock_ctx, server_spec)

        # Should return the same cached client
        self.assertEqual(client1, client2)
        # Should only create the client once
        mock_create_client.assert_called_once_with(arn)

    def test_dynamic_emr_no_emr_client_error(self):
        """Test error when EMR client is not initialized in dynamic mode"""
        # Set up dynamic EMR mode without EMR client
        self.mock_lifespan_context.dynamic_emr_clusters_mode = True
        self.mock_lifespan_context.static_clients = None
        self.mock_lifespan_context.emr_client = None

        # Clear caches to ensure the ID lookup is attempted
        from spark_history_mcp.tools.tools import (
            emr_cluster_id_to_arn_cache,
        )

        emr_cluster_id_to_arn_cache.clear()

        server_spec = ServerSpec(
            dynamic_emr_server_spec=DynamicEMRServerSpec(
                emr_cluster_id="j-1234567890ABC"
            )
        )

        with self.assertRaises(ValueError) as context:
            get_client(self.mock_ctx, server_spec)

        self.assertIn(
            "EMR client is not initialized in dynamic mode", str(context.exception)
        )

    def test_dynamic_emr_no_emr_client_error_cluster_name(self):
        """Test error when EMR client is not initialized in dynamic mode with cluster name"""
        # Set up dynamic EMR mode without EMR client
        self.mock_lifespan_context.dynamic_emr_clusters_mode = True
        self.mock_lifespan_context.static_clients = None
        self.mock_lifespan_context.emr_client = None

        server_spec = ServerSpec(
            dynamic_emr_server_spec=DynamicEMRServerSpec(
                emr_cluster_name="test-cluster"
            )
        )

        with self.assertRaises(ValueError) as context:
            get_client(self.mock_ctx, server_spec)

        self.assertIn(
            "EMR client is not initialized in dynamic mode", str(context.exception)
        )

    def test_dynamic_emr_invalid_server_spec(self):
        """Test error when dynamic server spec is invalid"""
        # Set up dynamic EMR mode
        self.mock_lifespan_context.dynamic_emr_clusters_mode = True
        self.mock_lifespan_context.static_clients = None

        # Empty dynamic server spec
        server_spec = ServerSpec(dynamic_emr_server_spec=DynamicEMRServerSpec())

        with self.assertRaises(ValueError) as context:
            get_client(self.mock_ctx, server_spec)

        self.assertIn("Invalid server_spec", str(context.exception))

    @patch("spark_history_mcp.tools.tools.get_client")
    def test_get_slowest_jobs_empty(self, mock_get_client):
        """Test list_slowest_jobs when no jobs are found"""
        # Setup mock client
        mock_client = MagicMock()
        mock_client.list_jobs.return_value = []
        mock_get_client.return_value = mock_client

        # Call the function
        result = list_slowest_jobs("app-123", self.DEFAULT_SERVER_SPEC, n=3)

        # Verify results
        self.assertEqual(result, [])
        mock_client.list_jobs.assert_called_once_with(app_id="app-123")

    @patch("spark_history_mcp.tools.tools.get_client")
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
        result = list_slowest_jobs("app-123", self.DEFAULT_SERVER_SPEC, n=2)

        # Verify results - should return job3 and job2 (in that order)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0], job3)  # Longest duration (5 min)
        self.assertEqual(result[1], job2)  # Second longest (2 min)

        # Running job (job1) should be excluded
        self.assertNotIn(job1, result)

    @patch("spark_history_mcp.tools.tools.get_client")
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
        result = list_slowest_jobs(
            "app-123", self.DEFAULT_SERVER_SPEC, include_running=True, n=2
        )

        # Verify results - should include the running job
        self.assertEqual(len(result), 2)
        # Running job should be included but will have duration 0 since completion_time is None
        # So job3 and job2 should be returned
        self.assertEqual(result[0], job3)
        self.assertEqual(result[1], job2)

    @patch("spark_history_mcp.tools.tools.get_client")
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
        result = list_slowest_jobs("app-123", self.DEFAULT_SERVER_SPEC, n=3)

        # Verify results - should return only 3 jobs
        self.assertEqual(len(result), 3)

    @patch("spark_history_mcp.tools.tools.get_client")
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
        result = get_stage("app-123", 1, self.DEFAULT_SERVER_SPEC, attempt_id=0)

        # Verify results
        self.assertEqual(result, mock_stage)
        mock_client.get_stage_attempt.assert_called_once_with(
            app_id="app-123",
            stage_id=1,
            attempt_id=0,
            details=False,
            with_summaries=False,
        )

    @patch("spark_history_mcp.tools.tools.get_client")
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
        server_spec = ServerSpec(
            static_server_spec=StaticServerSpec(default_client=True)
        )
        result = get_stage("app-123", 1, server_spec)

        # Verify results
        self.assertEqual(result, mock_stage)
        mock_client.list_stage_attempts.assert_called_once_with(
            app_id="app-123",
            stage_id=1,
            details=False,
            with_summaries=False,
        )

    @patch("spark_history_mcp.tools.tools.get_client")
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
        server_spec = ServerSpec(
            static_server_spec=StaticServerSpec(default_client=True)
        )
        result = get_stage("app-123", 1, server_spec)

        # Verify results - should return the stage with highest attempt_id
        self.assertEqual(result, mock_stage2)
        mock_client.list_stage_attempts.assert_called_once_with(
            app_id="app-123",
            stage_id=1,
            details=False,
            with_summaries=False,
        )

    @patch("spark_history_mcp.tools.tools.get_client")
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
        result = get_stage(
            "app-123", 1, self.DEFAULT_SERVER_SPEC, attempt_id=0, with_summaries=True
        )

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

    @patch("spark_history_mcp.tools.tools.get_client")
    def test_get_stage_no_stages_found(self, mock_get_client):
        """Test get_stage when no stages are found"""
        # Setup mock client
        mock_client = MagicMock()
        mock_client.list_stage_attempts.return_value = []
        mock_get_client.return_value = mock_client

        with self.assertRaises(ValueError) as context:
            get_stage("app-123", 1, self.DEFAULT_SERVER_SPEC)

        self.assertIn("No stage found with ID 1", str(context.exception))

    # Tests for get_application tool
    @patch("spark_history_mcp.tools.tools.get_client")
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
        result = get_application("spark-app-123", self.DEFAULT_SERVER_SPEC)

        # Verify results
        self.assertEqual(result, mock_app)
        mock_client.get_application.assert_called_once_with("spark-app-123")
        mock_get_client.assert_called_once_with(
            unittest.mock.ANY, self.DEFAULT_SERVER_SPEC
        )

    @patch("spark_history_mcp.tools.tools.get_client")
    def test_get_application_with_server(self, mock_get_client):
        """Test application retrieval with specific server"""
        # Setup mock client
        mock_client = MagicMock()
        mock_app = MagicMock(spec=ApplicationInfo)
        mock_client.get_application.return_value = mock_app
        mock_get_client.return_value = mock_client

        # Call the function with server
        server_spec = ServerSpec(
            static_server_spec=StaticServerSpec(server_name="production")
        )
        get_application("spark-app-123", server_spec)

        # Verify server parameter is passed
        mock_get_client.assert_called_once_with(unittest.mock.ANY, server_spec)

    @patch("spark_history_mcp.tools.tools.get_client")
    def test_get_application_not_found(self, mock_get_client):
        """Test application retrieval when app doesn't exist"""
        # Setup mock client to raise exception
        mock_client = MagicMock()
        mock_client.get_application.side_effect = Exception("Application not found")
        mock_get_client.return_value = mock_client

        # Verify exception is propagated
        with self.assertRaises(Exception) as context:
            get_application("non-existent-app", self.DEFAULT_SERVER_SPEC)

        self.assertIn("Application not found", str(context.exception))

    # Tests for list_jobs tool
    @patch("spark_history_mcp.tools.tools.get_client")
    def test_list_jobs_no_filter(self, mock_get_client):
        """Test job retrieval without status filter"""
        # Setup mock client
        mock_client = MagicMock()
        mock_jobs = [MagicMock(spec=JobData), MagicMock(spec=JobData)]
        mock_client.list_jobs.return_value = mock_jobs
        mock_get_client.return_value = mock_client

        # Call the function
        server_spec = ServerSpec(
            static_server_spec=StaticServerSpec(default_client=True)
        )
        result = list_jobs("spark-app-123", server_spec)

        # Verify results
        self.assertEqual(result, mock_jobs)
        mock_client.list_jobs.assert_called_once_with(
            app_id="spark-app-123", status=None
        )

    @patch("spark_history_mcp.tools.tools.get_client")
    def test_list_jobs_with_status_filter(self, mock_get_client):
        """Test job retrieval with status filter"""
        # Setup mock client
        mock_client = MagicMock()
        mock_jobs = [MagicMock(spec=JobData)]
        mock_jobs[0].status = "SUCCEEDED"
        mock_client.list_jobs.return_value = mock_jobs
        mock_get_client.return_value = mock_client

        # Call the function with status filter
        result = list_jobs(
            "spark-app-123", self.DEFAULT_SERVER_SPEC, status=["SUCCEEDED"]
        )

        # Verify results
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].status, "SUCCEEDED")

    @patch("spark_history_mcp.tools.tools.get_client")
    def test_list_jobs_empty_result(self, mock_get_client):
        """Test job retrieval with empty result"""
        # Setup mock client
        mock_client = MagicMock()
        mock_client.list_jobs.return_value = []
        mock_get_client.return_value = mock_client

        # Call the function
        server_spec = ServerSpec(
            static_server_spec=StaticServerSpec(default_client=True)
        )
        result = list_jobs("spark-app-123", server_spec)

        # Verify results
        self.assertEqual(result, [])

    @patch("spark_history_mcp.tools.tools.get_client")
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
        result = list_jobs(
            "spark-app-123", self.DEFAULT_SERVER_SPEC, status=["SUCCEEDED"]
        )

        # Should only return SUCCEEDED job
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].status, "SUCCEEDED")

    # Tests for list_stages tool
    @patch("spark_history_mcp.tools.tools.get_client")
    def test_get_stages_no_filter(self, mock_get_client):
        """Test stage retrieval without filters"""
        # Setup mock client
        mock_client = MagicMock()
        mock_stages = [MagicMock(spec=StageData), MagicMock(spec=StageData)]
        mock_client.list_stages.return_value = mock_stages
        mock_get_client.return_value = mock_client

        # Call the function
        server_spec = ServerSpec(
            static_server_spec=StaticServerSpec(default_client=True)
        )
        result = list_stages("spark-app-123", server_spec)

        # Verify results
        self.assertEqual(result, mock_stages)
        mock_client.list_stages.assert_called_once_with(
            app_id="spark-app-123", status=None, with_summaries=False
        )

    @patch("spark_history_mcp.tools.tools.get_client")
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
        result = list_stages(
            "spark-app-123", self.DEFAULT_SERVER_SPEC, status=["COMPLETE"]
        )

        # Should only return COMPLETE stage
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].status, "COMPLETE")

    @patch("spark_history_mcp.tools.tools.get_client")
    def test_get_stages_with_summaries(self, mock_get_client):
        """Test stage retrieval with summaries enabled"""
        # Setup mock client
        mock_client = MagicMock()
        mock_stages = [MagicMock(spec=StageData)]
        mock_client.list_stages.return_value = mock_stages
        mock_get_client.return_value = mock_client

        # Call with summaries enabled
        list_stages("spark-app-123", self.DEFAULT_SERVER_SPEC, with_summaries=True)

        # Verify summaries parameter is passed
        mock_client.list_stages.assert_called_once_with(
            app_id="spark-app-123", status=None, with_summaries=True
        )

    @patch("spark_history_mcp.tools.tools.get_client")
    def test_get_stages_empty_result(self, mock_get_client):
        """Test stage retrieval with empty result"""
        # Setup mock client
        mock_client = MagicMock()
        mock_client.list_stages.return_value = []
        mock_get_client.return_value = mock_client

        # Call the function
        server_spec = ServerSpec(
            static_server_spec=StaticServerSpec(default_client=True)
        )
        result = list_stages("spark-app-123", server_spec)

        # Verify results
        self.assertEqual(result, [])

    # Tests for get_stage_task_summary tool
    @patch("spark_history_mcp.tools.tools.get_client")
    def test_get_stage_task_summary_success(self, mock_get_client):
        """Test successful stage task summary retrieval"""
        # Setup mock client
        mock_client = MagicMock()
        mock_summary = MagicMock(spec=TaskMetricDistributions)
        mock_client.get_stage_task_summary.return_value = mock_summary
        mock_get_client.return_value = mock_client

        # Call the function
        result = get_stage_task_summary("spark-app-123", 1, self.DEFAULT_SERVER_SPEC, 0)

        # Verify results
        self.assertEqual(result, mock_summary)
        mock_client.get_stage_task_summary.assert_called_once_with(
            app_id="spark-app-123",
            stage_id=1,
            attempt_id=0,
            quantiles="0.05,0.25,0.5,0.75,0.95",
        )

    @patch("spark_history_mcp.tools.tools.get_client")
    def test_get_stage_task_summary_with_quantiles(self, mock_get_client):
        """Test stage task summary with custom quantiles"""
        # Setup mock client
        mock_client = MagicMock()
        mock_summary = MagicMock(spec=TaskMetricDistributions)
        mock_client.get_stage_task_summary.return_value = mock_summary
        mock_get_client.return_value = mock_client

        # Call with custom quantiles
        get_stage_task_summary(
            "spark-app-123", 1, self.DEFAULT_SERVER_SPEC, 0, quantiles="0.25,0.5,0.75"
        )

        # Verify quantiles parameter is passed
        mock_client.get_stage_task_summary.assert_called_once_with(
            app_id="spark-app-123", stage_id=1, attempt_id=0, quantiles="0.25,0.5,0.75"
        )

    @patch("spark_history_mcp.tools.tools.get_client")
    def test_get_stage_task_summary_not_found(self, mock_get_client):
        """Test stage task summary when stage doesn't exist"""
        # Setup mock client to raise exception
        mock_client = MagicMock()
        mock_client.get_stage_task_summary.side_effect = Exception("Stage not found")
        mock_get_client.return_value = mock_client

        # Verify exception is propagated
        with self.assertRaises(Exception) as context:
            get_stage_task_summary("spark-app-123", 999, self.DEFAULT_SERVER_SPEC, 0)

        self.assertIn("Stage not found", str(context.exception))

    # Tests for list_slowest_sql_queries tool
    @patch("spark_history_mcp.tools.tools.get_client")
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
        result = list_slowest_sql_queries(
            "spark-app-123", self.DEFAULT_SERVER_SPEC, top_n=2
        )

        # Verify results are sorted by duration (descending)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0].duration, 10000)  # Slowest first
        self.assertEqual(result[1].duration, 5000)  # Second slowest

    @patch("spark_history_mcp.tools.tools.get_client")
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
        result = list_slowest_sql_queries("spark-app-123", self.DEFAULT_SERVER_SPEC)

        # Should exclude running query
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].status, "COMPLETED")

    @patch("spark_history_mcp.tools.tools.get_client")
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
            "spark-app-123", self.DEFAULT_SERVER_SPEC, include_running=True, top_n=2
        )

        # Should include both queries
        self.assertEqual(len(result), 2)

    @patch("spark_history_mcp.tools.tools.get_client")
    def test_get_slowest_sql_queries_empty_result(self, mock_get_client):
        """Test SQL query retrieval with empty result"""
        # Setup mock client
        mock_client = MagicMock()
        mock_client.get_sql_list.return_value = []
        mock_get_client.return_value = mock_client

        # Call the function
        result = list_slowest_sql_queries("spark-app-123", self.DEFAULT_SERVER_SPEC)

        # Verify results
        self.assertEqual(result, [])

    @patch("spark_history_mcp.tools.tools.get_client")
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
        result = list_slowest_sql_queries(
            "spark-app-123", self.DEFAULT_SERVER_SPEC, top_n=3
        )

        # Verify results - should return only 3 queries
        self.assertEqual(len(result), 3)
        # Should be sorted by duration (descending)
        self.assertEqual(result[0].duration, 10000)
        self.assertEqual(result[1].duration, 9000)
        self.assertEqual(result[2].duration, 8000)
