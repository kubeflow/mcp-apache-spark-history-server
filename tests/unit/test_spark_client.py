import unittest
from unittest.mock import MagicMock

from spark_history_mcp.api.spark_client import AttemptRequiredError, SparkRestClient
from spark_history_mcp.api_client.exceptions import (
    ForbiddenException,
    NotFoundException,
    UnauthorizedException,
)
from spark_history_mcp.api_client.models.application import Application
from spark_history_mcp.api_client.models.application_attempt import ApplicationAttempt
from spark_history_mcp.api_client.models.executor import Executor
from spark_history_mcp.api_client.models.job import Job
from spark_history_mcp.config.config import ServerConfig


def _make_jobs(count):
    return [Job(jobId=i, name=f"job-{i}", status="SUCCEEDED") for i in range(count)]


def _make_executors(count):
    return [Executor(id=str(i), isActive=True) for i in range(count)]


class TestAppPath(unittest.TestCase):
    def test_no_attempt(self):
        self.assertEqual(SparkRestClient._app_path("app-1"), "app-1")
        self.assertEqual(SparkRestClient._app_path("app-1", None), "app-1")

    def test_with_attempt(self):
        self.assertEqual(SparkRestClient._app_path("app-1", "2"), "app-1/2")


class TestSparkRestClient(unittest.TestCase):
    """All calls go through the generated DefaultApi (single transport)."""

    def setUp(self):
        self.server_config = ServerConfig(url="http://spark-history-server:18080")
        self.client = SparkRestClient(self.server_config)
        self.mock_api = MagicMock()
        self.client._api = self.mock_api
        self.timeout = self.client.timeout

    def test_base_url(self):
        self.assertEqual(
            self.client.base_url, "http://spark-history-server:18080/api/v1"
        )

    def test_path_params_keep_slash(self):
        """The composite app id must not be percent-encoded by the client."""
        api = self.client._build_api_client()
        self.assertEqual(api.api_client.configuration.safe_chars_for_path_param, "/")

    def test_requests_library_not_used(self):
        """The facade no longer depends on requests (urllib3 everywhere)."""
        import spark_history_mcp.api.spark_client as module

        self.assertFalse(hasattr(module, "requests"))

    def test_list_applications_passes_single_status_and_timeout(self):
        apps = [Application(id="app-1", name="Test App")]
        self.mock_api.list_applications.return_value = apps

        result = self.client.list_applications(status=["COMPLETED"], limit=10)

        self.assertEqual(result, apps)
        self.mock_api.list_applications.assert_called_once_with(
            status="COMPLETED",
            min_date=None,
            max_date=None,
            min_end_date=None,
            max_end_date=None,
            limit=10,
            _request_timeout=self.timeout,
        )

    def test_get_application(self):
        app = Application(id="app-1", name="Test App")
        self.mock_api.get_application.return_value = app

        self.assertEqual(self.client.get_application("app-1"), app)
        self.mock_api.get_application.assert_called_once_with(
            "app-1", _request_timeout=self.timeout
        )

    def test_list_jobs_plain_app_path(self):
        self.mock_api.list_jobs.return_value = _make_jobs(2)

        self.client.list_jobs("app-123")

        self.mock_api.list_jobs.assert_called_once_with(
            "app-123", _request_timeout=self.timeout
        )

    def test_list_jobs_composite_app_path(self):
        self.mock_api.list_jobs.return_value = _make_jobs(2)

        self.client.list_jobs("app-123", app_attempt_id="2")

        self.mock_api.list_jobs.assert_called_once_with(
            "app-123/2", _request_timeout=self.timeout
        )

    def test_get_stage_attempt_composite_path_and_stage_attempt(self):
        self.mock_api.get_stage_attempt.return_value = "stage"

        self.client.get_stage_attempt(
            "app-123", stage_id=3, attempt_id=0, app_attempt_id="1"
        )

        args, _ = self.mock_api.get_stage_attempt.call_args
        self.assertEqual(args[:3], ("app-123/1", 3, 0))

    def test_list_jobs_pagination(self):
        self.mock_api.list_jobs.return_value = _make_jobs(5)

        self.assertEqual(len(self.client.list_jobs("app-123")), 5)

        jobs = self.client.list_jobs("app-123", offset=2)
        self.assertEqual([j.job_id for j in jobs], [2, 3, 4])

        jobs = self.client.list_jobs("app-123", offset=1, length=2)
        self.assertEqual([j.job_id for j in jobs], [1, 2])

        self.assertEqual(self.client.list_jobs("app-123", offset=10), [])

    def test_list_jobs_status_filter(self):
        self.mock_api.list_jobs.return_value = [
            Job(jobId=0, name="a", status="RUNNING"),
            Job(jobId=1, name="b", status="SUCCEEDED"),
            Job(jobId=2, name="c", status="FAILED"),
        ]

        result = self.client.list_jobs("app-123", status=["SUCCEEDED"])

        self.assertEqual([j.status for j in result], ["SUCCEEDED"])

    def test_list_executors_pagination(self):
        self.mock_api.list_active_executors.return_value = _make_executors(10)

        self.assertEqual(len(self.client.list_executors("app-123")), 10)
        executors = self.client.list_executors("app-123", offset=8, length=5)
        self.assertEqual([e.id for e in executors], ["8", "9"])

    # ----- attempt-aware 404 enrichment -----
    def test_missing_app_404_propagates(self):
        """A genuinely missing app: the attempt lookup also 404s, original raised."""
        self.mock_api.list_jobs.side_effect = NotFoundException(status=404)
        self.mock_api.get_application.side_effect = NotFoundException(status=404)

        with self.assertRaises(NotFoundException):
            self.client.list_jobs("app-123")

    def test_missing_attempt_raises_attempt_required(self):
        self.mock_api.list_jobs.side_effect = NotFoundException(status=404)
        self.mock_api.get_application.return_value = Application(
            id="app-123",
            name="multi",
            attempts=[
                ApplicationAttempt(attemptId="2"),
                ApplicationAttempt(attemptId="1"),
            ],
        )

        with self.assertRaises(AttemptRequiredError) as ctx:
            self.client.list_jobs("app-123")

        message = str(ctx.exception)
        self.assertIn("multiple attempts (2, 1)", message)
        self.assertIn("app_attempt_id='2'", message)

    def test_404_with_explicit_attempt_not_enriched(self):
        self.mock_api.list_jobs.side_effect = NotFoundException(status=404)

        with self.assertRaises(NotFoundException):
            self.client.list_jobs("app-123", app_attempt_id="9")
        self.mock_api.get_application.assert_not_called()

    def test_404_single_attempt_app_propagates(self):
        self.mock_api.list_jobs.side_effect = NotFoundException(status=404)
        self.mock_api.get_application.return_value = Application(
            id="app-123", name="single", attempts=[ApplicationAttempt()]
        )

        with self.assertRaises(NotFoundException):
            self.client.list_jobs("app-123")

    def test_proxy_configuration(self):
        client = SparkRestClient(
            ServerConfig(url="http://spark-history-server:18080", use_proxy=True)
        )
        self.assertEqual(
            client._api.api_client.configuration.proxy, "socks5h://localhost:8157"
        )


class TestCookieAuth(unittest.TestCase):
    """EMR-style cookie auth and re-auth-on-401/403 behaviour."""

    def setUp(self):
        self.client = SparkRestClient(ServerConfig(url="https://example.com/shs"))

    def test_configure_cookies_sets_header(self):
        self.client.configure_cookies("session=abc")
        self.assertEqual(self.client._api.api_client.cookie, "session=abc")

    def test_reauth_retries_once_on_unauthorized(self):
        self.client._api = MagicMock()
        self.client._api.list_jobs.side_effect = [
            UnauthorizedException(status=401),
            _make_jobs(1),
        ]
        reauth = MagicMock(return_value="session=fresh")
        self.client.configure_cookies("session=stale", reauth=reauth)

        result = self.client.list_jobs("app-1")

        self.assertEqual(len(result), 1)
        reauth.assert_called_once()
        self.assertEqual(self.client._api.api_client.cookie, "session=fresh")
        self.assertEqual(self.client._api.list_jobs.call_count, 2)

    def test_reauth_on_forbidden(self):
        self.client._api = MagicMock()
        self.client._api.get_environment.side_effect = [
            ForbiddenException(status=403),
            "env",
        ]
        reauth = MagicMock(return_value="session=fresh")
        self.client.configure_cookies("session=stale", reauth=reauth)

        self.assertEqual(self.client.get_environment("app-1"), "env")
        reauth.assert_called_once()

    def test_unauthorized_without_reauth_propagates(self):
        self.client._api = MagicMock()
        self.client._api.list_jobs.side_effect = UnauthorizedException(status=401)

        with self.assertRaises(UnauthorizedException):
            self.client.list_jobs("app-1")


if __name__ == "__main__":
    unittest.main()
