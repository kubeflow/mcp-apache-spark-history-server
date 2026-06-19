"""Thin facade over the OpenAPI-generated Spark History Server client.

:class:`SparkRestClient` wraps the generated ``DefaultApi`` and returns the
generated Pydantic models directly. All HTTP goes through the generated
(``urllib3``-based) client, so the OpenAPI spec is the single source of truth
for the SHS REST surface. The facade adds client-side ``offset``/``length``
pagination and list-of-string status filtering on top.

A specific YARN application attempt is addressed by embedding the attempt id in
the application path (``/applications/{base-app-id}/{attempt-id}/...``). Every
application-scoped method takes an optional ``app_attempt_id`` that is composed
into the application id; omitting it selects the latest/only attempt. There is
no implicit retry against a hard-coded attempt id.

**Authentication.** Username/password (basic) and bearer tokens are applied as
an ``Authorization`` header. EMR persistent-UI servers authenticate with a
session cookie instead: :meth:`configure_cookies` installs a ``Cookie`` header
on the generated client and an optional re-auth callback that refreshes it when
a request is rejected with 401/403.
"""

import functools
import inspect
from typing import Callable, List, Optional

from spark_history_mcp.api_client.api.default_api import DefaultApi
from spark_history_mcp.api_client.api_client import ApiClient
from spark_history_mcp.api_client.configuration import Configuration
from spark_history_mcp.api_client.exceptions import (
    ForbiddenException,
    NotFoundException,
    UnauthorizedException,
)
from spark_history_mcp.api_client.models.application import Application
from spark_history_mcp.api_client.models.environment import Environment
from spark_history_mcp.api_client.models.executor import Executor
from spark_history_mcp.api_client.models.job import Job
from spark_history_mcp.api_client.models.sql_execution import SQLExecution
from spark_history_mcp.api_client.models.stage_data import StageData
from spark_history_mcp.api_client.models.task import Task
from spark_history_mcp.api_client.models.task_metrics_summary import TaskMetricsSummary
from spark_history_mcp.api_client.models.thread_stack_trace import ThreadStackTrace
from spark_history_mcp.config.config import ServerConfig

_DEFAULT_QUANTILES = "0.05, 0.25, 0.5, 0.75, 0.95"
_PROXY_URL = "socks5h://localhost:8157"


class AttemptRequiredError(Exception):
    """Raised when an application has multiple attempts and none was specified.

    Replaces the History Server's misleading ``404 no such app`` for a
    multi-attempt YARN application with an actionable message listing the
    available attempts. No data is returned; the caller must retry with
    ``app_attempt_id``.
    """


def _resilient_call(method):
    """Cross-cutting error handling for application-scoped methods.

    * On 401/403, if a re-auth callback is configured, refresh the cookie and
      retry once (covers EMR session-cookie rotation).
    * On 404 with no ``app_attempt_id`` for an app that has named attempts,
      re-raise as :class:`AttemptRequiredError`; otherwise propagate. The
      attempt lookup only happens on the failure path.
    """
    signature = inspect.signature(method)

    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        try:
            return method(self, *args, **kwargs)
        except (UnauthorizedException, ForbiddenException):
            if not self._reauth:
                raise
            self._apply_cookie(self._reauth())
            return method(self, *args, **kwargs)
        except NotFoundException as exc:
            bound = signature.bind(self, *args, **kwargs)
            bound.apply_defaults()
            if bound.arguments.get("app_attempt_id"):
                raise
            app_id = bound.arguments.get("app_id")
            attempts = self._attempt_ids(app_id) if app_id else []
            if attempts:
                raise AttemptRequiredError(
                    f"Application '{app_id}' has multiple attempts "
                    f"({', '.join(attempts)}); retry with app_attempt_id "
                    f"(e.g. app_attempt_id={attempts[0]!r})."
                ) from exc
            raise

    return wrapper


class SparkRestClient:
    """Facade over the generated Spark History Server API client.

    Methods return the generated Pydantic models from
    :mod:`spark_history_mcp.api_client.models`.
    """

    def __init__(self, server_config: ServerConfig):
        self.config = server_config
        self.base_url = self.config.url.rstrip("/") + "/api/v1"
        self.use_proxy = self.config.use_proxy
        self.verify_ssl = self.config.verify_ssl
        self.timeout = self.config.timeout

        # Optional callback returning a fresh Cookie header (EMR re-auth).
        self._reauth: Optional[Callable[[], str]] = None

        self._api = self._build_api_client()

    def _build_api_client(self) -> DefaultApi:
        configuration = Configuration(host=self.base_url, verify_ssl=self.verify_ssl)

        # Keep "/" unescaped in path parameters so a composite application id
        # ("{base-app-id}/{attempt-id}") is sent as a real path, not "%2F".
        configuration.safe_chars_for_path_param = "/"

        if self.use_proxy:
            configuration.proxy = _PROXY_URL

        api_client = ApiClient(configuration)

        # The generated client does not auto-apply auth, so set it explicitly.
        auth = self.config.auth
        if auth:
            if auth.username and auth.password:
                configuration.username = auth.username
                configuration.password = auth.password
                token = configuration.get_basic_auth_token()
                if token:
                    api_client.set_default_header("Authorization", token)
            elif auth.token:
                api_client.set_default_header("Authorization", f"Bearer {auth.token}")

        return DefaultApi(api_client)

    # ------------------------------------------------------------------
    # Auth / transport configuration
    # ------------------------------------------------------------------
    def configure_cookies(
        self, cookie_header: str, reauth: Optional[Callable[[], str]] = None
    ) -> None:
        """Authenticate via a ``Cookie`` header (used for EMR persistent UI).

        Args:
            cookie_header: Serialized ``name=value; ...`` cookie string.
            reauth: Optional callable returning a fresh cookie header, invoked
                once to recover from a 401/403 (e.g. cookie rotation).
        """
        self._apply_cookie(cookie_header)
        self._reauth = reauth

    def _apply_cookie(self, cookie_header: str) -> None:
        self._api.api_client.cookie = cookie_header

    def _invoke(self, fn, *args, **kwargs):
        """Call a generated API method applying the configured request timeout."""
        return fn(*args, _request_timeout=self.timeout, **kwargs)

    @staticmethod
    def _app_path(app_id: str, app_attempt_id: Optional[str] = None) -> str:
        """Compose the application path segment (``{app_id}/{attempt}`` or bare)."""
        if app_attempt_id:
            return f"{app_id}/{app_attempt_id}"
        return app_id

    def _attempt_ids(self, app_id: str) -> List[str]:
        """Named attempt ids for an application; ``[]`` on any failure."""
        try:
            application = self.get_application(app_id)
            attempts = getattr(application, "attempts", None) or []
            return [a.attempt_id for a in attempts if getattr(a, "attempt_id", None)]
        except Exception:
            return []

    @staticmethod
    def _paginate(items: List, offset: int, length: Optional[int]) -> List:
        if length is not None:
            return items[offset : offset + length]
        return items[offset:] if offset else items

    # ------------------------------------------------------------------
    # Applications
    # ------------------------------------------------------------------
    def list_applications(
        self,
        status: Optional[List[str]] = None,
        min_date: Optional[str] = None,
        max_date: Optional[str] = None,
        min_end_date: Optional[str] = None,
        max_end_date: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[Application]:
        """List applications, optionally filtered by status/date and limited."""
        # The generated client accepts a single status string.
        status_param = status[0] if status else None
        return self._invoke(
            self._api.list_applications,
            status=status_param,
            min_date=min_date,
            max_date=max_date,
            min_end_date=min_end_date,
            max_end_date=max_end_date,
            limit=limit,
        )

    def get_application(self, app_id: str) -> Application:
        """Get a single application (with all its attempts)."""
        return self._invoke(self._api.get_application, app_id)

    # ------------------------------------------------------------------
    # Jobs
    # ------------------------------------------------------------------
    @_resilient_call
    def list_jobs(
        self,
        app_id: str,
        status: Optional[List[str]] = None,
        app_attempt_id: Optional[str] = None,
        offset: int = 0,
        length: Optional[int] = None,
    ) -> List[Job]:
        """List jobs for an application, with optional status filter/pagination."""
        app_path = self._app_path(app_id, app_attempt_id)
        jobs = self._invoke(self._api.list_jobs, app_path)

        if status:
            wanted = {s.upper() for s in status}
            jobs = [j for j in jobs if j.status in wanted]

        return self._paginate(jobs, offset, length)

    # ------------------------------------------------------------------
    # Stages
    # ------------------------------------------------------------------
    @_resilient_call
    def list_stages(
        self,
        app_id: str,
        status: Optional[List[str]] = None,
        details: bool = False,
        with_summaries: bool = False,
        quantiles: str = _DEFAULT_QUANTILES,
        task_status: Optional[List[str]] = None,
        app_attempt_id: Optional[str] = None,
        offset: int = 0,
        length: Optional[int] = None,
    ) -> List[StageData]:
        """List stages for an application, with optional status filter/pagination."""
        app_path = self._app_path(app_id, app_attempt_id)
        task_status_param = task_status[0] if (task_status and details) else None
        stages = self._invoke(
            self._api.list_stages,
            app_path,
            details=details,
            task_status=task_status_param,
            with_summaries=with_summaries,
            quantiles=quantiles,
        )

        if status:
            wanted = {s.upper() for s in status}
            stages = [s for s in stages if s.status in wanted]

        return self._paginate(stages, offset, length)

    @_resilient_call
    def list_stage_attempts(
        self,
        app_id: str,
        stage_id: int,
        details: bool = False,
        task_status: Optional[List[str]] = None,
        with_summaries: bool = True,
        quantiles: str = _DEFAULT_QUANTILES,
        app_attempt_id: Optional[str] = None,
    ) -> List[StageData]:
        """Get all attempts for a specific stage."""
        app_path = self._app_path(app_id, app_attempt_id)
        task_status_param = task_status[0] if task_status else None
        return self._invoke(
            self._api.list_stage_attempts,
            app_path,
            stage_id,
            details=details,
            task_status=task_status_param,
            with_summaries=with_summaries,
            quantiles=quantiles,
        )

    @_resilient_call
    def get_stage_attempt(
        self,
        app_id: str,
        stage_id: int,
        attempt_id: int,
        details: bool = True,
        task_status: Optional[List[str]] = None,
        with_summaries: bool = False,
        quantiles: str = _DEFAULT_QUANTILES,
        app_attempt_id: Optional[str] = None,
    ) -> StageData:
        """Get a specific stage attempt (``attempt_id`` is the *stage* attempt)."""
        app_path = self._app_path(app_id, app_attempt_id)
        task_status_param = task_status[0] if task_status else None
        return self._invoke(
            self._api.get_stage_attempt,
            app_path,
            stage_id,
            attempt_id,
            details=details,
            task_status=task_status_param,
            with_summaries=with_summaries,
            quantiles=quantiles,
        )

    @_resilient_call
    def get_stage_task_summary(
        self,
        app_id: str,
        stage_id: int,
        attempt_id: int,
        quantiles: str = _DEFAULT_QUANTILES,
        app_attempt_id: Optional[str] = None,
    ) -> TaskMetricsSummary:
        """Get task summary metrics for a specific stage attempt."""
        app_path = self._app_path(app_id, app_attempt_id)
        return self._invoke(
            self._api.get_task_summary,
            app_path,
            stage_id,
            attempt_id,
            quantiles=quantiles,
        )

    @_resilient_call
    def list_stage_tasks(
        self,
        app_id: str,
        stage_id: int,
        attempt_id: int,
        status: Optional[str] = None,
        sort_by: Optional[str] = None,
        offset: Optional[int] = None,
        length: Optional[int] = None,
        app_attempt_id: Optional[str] = None,
    ) -> List[Task]:
        """List tasks for a specific stage attempt, optionally filtered by status."""
        app_path = self._app_path(app_id, app_attempt_id)
        return self._invoke(
            self._api.list_tasks,
            app_path,
            stage_id,
            attempt_id,
            status=status,
            sort_by=sort_by,
            offset=offset,
            length=length,
        )

    # ------------------------------------------------------------------
    # Executors
    # ------------------------------------------------------------------
    @_resilient_call
    def list_executors(
        self,
        app_id: str,
        app_attempt_id: Optional[str] = None,
        offset: int = 0,
        length: Optional[int] = None,
    ) -> List[Executor]:
        """List active executors for an application."""
        app_path = self._app_path(app_id, app_attempt_id)
        executors = self._invoke(self._api.list_active_executors, app_path)
        return self._paginate(executors, offset, length)

    @_resilient_call
    def list_all_executors(
        self,
        app_id: str,
        app_attempt_id: Optional[str] = None,
        offset: int = 0,
        length: Optional[int] = None,
    ) -> List[Executor]:
        """List all executors (active and inactive) for an application."""
        app_path = self._app_path(app_id, app_attempt_id)
        executors = self._invoke(self._api.list_all_executors, app_path)
        return self._paginate(executors, offset, length)

    @_resilient_call
    def get_executor_thread_dump(
        self,
        app_id: str,
        executor_id: str,
        app_attempt_id: Optional[str] = None,
    ) -> List[ThreadStackTrace]:
        """Get the JVM thread dump for a driver or executor.

        Use ``"driver"`` for the driver. The application must be running;
        completed applications return 404 because the History Server does not
        persist thread dumps.
        """
        app_path = self._app_path(app_id, app_attempt_id)
        return self._invoke(self._api.get_executor_threads, app_path, executor_id)

    # ------------------------------------------------------------------
    # Environment
    # ------------------------------------------------------------------
    @_resilient_call
    def get_environment(
        self, app_id: str, app_attempt_id: Optional[str] = None
    ) -> Environment:
        """Get environment/configuration information for an application."""
        app_path = self._app_path(app_id, app_attempt_id)
        return self._invoke(self._api.get_environment, app_path)

    # ------------------------------------------------------------------
    # SQL
    # ------------------------------------------------------------------
    @_resilient_call
    def get_sql_list(
        self,
        app_id: str,
        app_attempt_id: Optional[str] = None,
        details: bool = True,
        plan_description: bool = False,
        offset: int = 0,
        length: int = 20,
    ) -> List[SQLExecution]:
        """List SQL executions for an application (pagination is server-side)."""
        app_path = self._app_path(app_id, app_attempt_id)
        return self._invoke(
            self._api.list_sql_executions,
            app_path,
            details=details,
            plan_description=plan_description,
            offset=offset,
            length=length,
        )

    @_resilient_call
    def get_sql_execution(
        self,
        app_id: str,
        execution_id: int,
        app_attempt_id: Optional[str] = None,
        details: bool = True,
        plan_description: bool = True,
    ) -> SQLExecution:
        """Get a specific SQL execution."""
        app_path = self._app_path(app_id, app_attempt_id)
        return self._invoke(
            self._api.get_sql_execution,
            app_path,
            execution_id,
            details=details,
            plan_description=plan_description,
        )
