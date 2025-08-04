import re
from typing import Any, Dict, List, Optional, Type, TypeVar
from urllib.parse import urljoin

import requests
from pydantic import BaseModel

from spark_history_mcp.config.config import ServerConfig
from spark_history_mcp.models.spark_types import (
    ApplicationAttemptInfo,
    ApplicationEnvironmentInfo,
    ApplicationInfo,
    ExecutionData,
    ExecutorSummary,
    JobData,
    JobExecutionStatus,
    ProcessSummary,
    RDDStorageInfo,
    StageData,
    StageStatus,
    TaskData,
    TaskMetricDistributions,
    TaskStatus,
    ThreadStackTrace,
    VersionInfo,
)

T = TypeVar("T", bound=BaseModel)


class SparkRestClient:
    """
    Python client for the Spark REST API.
    """

    def __init__(self, server_config: ServerConfig):
        """
        Initialize the Spark REST client.

        Args:
            server_config: Configuration object
        """
        self.config = server_config
        self.base_url = self.config.url.rstrip("/") + "/api/v1"
        self.auth = None
        self.session = None
        self.use_proxy = self.config.use_proxy
        self.proxies = (
            self.use_proxy
            and {
                "http": "socks5h://localhost:8157",
                "https": "socks5h://localhost:8157",
            }
            or None
        )
        self.pattern = re.compile(r"(.*?/applications/[^/]+/)(.+)")

        # Determine whether to verify SSL certificates and timeout
        # Default to True for verify_ssl and 30 seconds for timeout if not specified
        self.verify_ssl = self.config.verify_ssl
        self.timeout = self.config.timeout

        # Set up authentication if provided
        if self.config.auth:
            if self.config.auth.username and self.config.auth.password:
                self.auth = (self.config.auth.username, self.config.auth.password)

    def _make_request(
        self, request_url: str, params: Optional[Dict[str, Any]]
    ) -> requests.Response:
        """
        Make a GET request to the Spark REST API.

        Args:
            request_url: The request URL
            params: Optional query parameters

        Returns:
            The response from the API
        """
        headers = {"Accept": "application/json"}

        # Add token to headers if provided
        if self.config.auth and self.config.auth.token:
            headers["Authorization"] = f"Bearer {self.config.auth.token}"

        # Use the verify_ssl setting for HTTPS requests
        verify = self.verify_ssl

        # Use the session if available, otherwise use requests directly
        if self.session:
            # Add headers to the session
            for key, value in headers.items():
                self.session.headers[key] = value

            response = self.session.get(
                request_url,
                params=params,
                timeout=self.timeout,
                verify=verify,
                proxies=self.proxies,
            )
        else:
            response = requests.get(
                request_url,
                params=params,
                headers=headers,
                auth=self.auth,
                timeout=self.timeout,
                verify=verify,
                proxies=self.proxies,
            )
        return response

    def _modify_url(self, url):
        match = self.pattern.search(url)
        if match:
            prefix = match.group(1)
            suffix = match.group(2)
            # Check if the suffix already starts with a number (attempt ID)
            if not re.match(r"^\d+/", suffix):
                # If no attempt ID present, add the first (and probably only) attempt of the app running on YARN
                app_attempt_id = 1
                return f"{prefix}{app_attempt_id}/{suffix}"
        return url

    def _get(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """
        Make a GET request to the Spark REST API.

        Args:
            endpoint: The API endpoint to call
            params: Optional query parameters

        Returns:
            The JSON response from the API
        """
        url = urljoin(self.base_url + "/", endpoint.lstrip("/"))

        try:
            # Try original URL first
            first_response = self._make_request(url, params)
            first_response.raise_for_status()
            return first_response.json()
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404 and "/applications/" in url:
                modified_url = self._modify_url(url)
                try:
                    second_response = self._make_request(modified_url, params)
                    second_response.raise_for_status()
                    return second_response.json()
                except requests.exceptions.HTTPError as e2:
                    raise e2 from e  # Chain the exception with the original error
            # Raise the original error
            raise e from None

    def _parse_model(self, data: Dict[str, Any], model_class: Type[T]) -> T:
        """
        Parse JSON data into a Pydantic model.

        Args:
            data: The JSON data to parse
            model_class: The Pydantic model class to use

        Returns:
            An instance of the model class
        """
        return model_class.model_validate(data)

    def _parse_model_list(
        self, data: List[Dict[str, Any]], model_class: Type[T]
    ) -> List[T]:
        """
        Parse a list of JSON data into a list of Pydantic models.

        Args:
            data: The list of JSON data to parse
            model_class: The Pydantic model class to use

        Returns:
            A list of instances of the model class
        """
        return [self._parse_model(item, model_class) for item in data]

    def get_version(self) -> VersionInfo:
        """Get the Spark version."""
        data = self._get("version")
        return self._parse_model(data, VersionInfo)

    def list_applications(
        self,
        status: Optional[List[str]] = None,
        min_date: Optional[str] = None,
        max_date: Optional[str] = None,
        min_end_date: Optional[str] = None,
        max_end_date: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[ApplicationInfo]:
        """
        Get a list of all applications.

        Args:
            status: Filter by application status (COMPLETED, RUNNING)
            min_date: Minimum start date (yyyy-MM-dd'T'HH:mm:ss.SSSz or yyyy-MM-dd)
            max_date: Maximum start date
            min_end_date: Minimum end date
            max_end_date: Maximum end date
            limit: Maximum number of applications to return

        Returns:
            List of ApplicationInfo objects
        """
        params = {}
        if status:
            params["status"] = status
        if min_date:
            params["minDate"] = min_date
        if max_date:
            params["maxDate"] = max_date
        if min_end_date:
            params["minEndDate"] = min_end_date
        if max_end_date:
            params["maxEndDate"] = max_end_date
        if limit:
            params["limit"] = limit

        data = self._get("applications", params)
        return self._parse_model_list(data, ApplicationInfo)

    def get_application(self, app_id: str) -> ApplicationInfo:
        """
        Get information about a specific application.

        Args:
            app_id: The application ID

        Returns:
            ApplicationInfo object
        """
        data = self._get(f"applications/{app_id}")
        return self._parse_model(data, ApplicationInfo)

    def get_application_attempt(
        self, app_id: str, attempt_id: str
    ) -> ApplicationAttemptInfo:
        """
        Get information about a specific application attempt.

        Args:
            app_id: The application ID
            attempt_id: The attempt ID

        Returns:
            ApplicationAttemptInfo object
        """
        data = self._get(f"applications/{app_id}/{attempt_id}")
        return self._parse_model(data, ApplicationAttemptInfo)

    def list_jobs(
        self, app_id: str, status: Optional[List[JobExecutionStatus]] = None
    ) -> List[JobData]:
        """
        Get a list of all jobs for an application.

        Args:
            app_id: The application ID
            status: Filter by job status

        Returns:
            List of JobData objects
        """
        params = {}
        if status:
            params["status"] = [s.value for s in status]

        data = self._get(f"applications/{app_id}/jobs", params)
        return self._parse_model_list(data, JobData)

    def get_job(self, app_id: str, job_id: int) -> JobData:
        """
        Get information about a specific job.

        Args:
            app_id: The application ID
            job_id: The job ID

        Returns:
            JobData object
        """
        data = self._get(f"applications/{app_id}/jobs/{job_id}")
        return self._parse_model(data, JobData)

    def list_stages(
        self,
        app_id: str,
        status: Optional[List[StageStatus]] = None,
        details: bool = False,
        with_summaries: bool = False,
        quantiles: str = "0.05, 0.25, 0.5, 0.75, 0.95",
        task_status: Optional[List[TaskStatus]] = None,
    ) -> List[StageData]:
        """
        Get a list of all stages for an application.

        Args:
            app_id: The application ID
            status: Filter by stage status
            details: Whether to include task details
            with_summaries: Whether to include summary metrics
            quantiles: Comma-separated list of quantiles to use for summary metrics
            task_status: Filter by task status

        Returns:
            List of StageData objects
        """
        params = {
            "details": str(details).lower(),
            "withSummaries": str(with_summaries).lower(),
            "quantiles": quantiles,
        }

        if status:
            params["status"] = [s.value for s in status]
        if task_status:
            params["taskStatus"] = [s.value for s in task_status]

        data = self._get(f"applications/{app_id}/stages", params)
        return self._parse_model_list(data, StageData)

    def list_stage_attempts(
        self,
        app_id: str,
        stage_id: int,
        details: bool = False,  # Setting this to true is NOT recommended due to the amount of data returned.
        task_status: Optional[List[TaskStatus]] = None,
        with_summaries: bool = True,
        quantiles: str = "0.05, 0.25, 0.5, 0.75, 0.95",
    ) -> List[StageData]:
        """
        Get information about a specific stage.

        Args:
            app_id: The application ID
            stage_id: The stage ID
            details: Whether to include task details
            task_status: Filter by task status
            with_summaries: Whether to include summary metrics
            quantiles: Comma-separated list of quantiles to use for summary metrics

        Returns:
            List of StageData objects (one per attempt)
        """
        params = {
            "details": str(details).lower(),
            "withSummaries": str(with_summaries).lower(),
            "quantiles": quantiles,
        }

        if task_status:
            params["taskStatus"] = [s.value for s in task_status]

        data = self._get(f"applications/{app_id}/stages/{stage_id}", params)
        return self._parse_model_list(data, StageData)

    def get_stage_attempt(
        self,
        app_id: str,
        stage_id: int,
        attempt_id: int,
        details: bool = True,
        task_status: Optional[List[TaskStatus]] = None,
        with_summaries: bool = False,
        quantiles: str = "0.05, 0.25, 0.5, 0.75, 0.95",
    ) -> StageData:
        """
        Get information about a specific stage attempt.

        Args:
            app_id: The application ID
            stage_id: The stage ID
            attempt_id: The attempt ID
            details: Whether to include task details
            task_status: Filter by task status
            with_summaries: Whether to include summary metrics
            quantiles: Comma-separated list of quantiles to use for summary metrics

        Returns:
            StageData object
        """
        params = {
            "details": str(details).lower(),
            "withSummaries": str(with_summaries).lower(),
            "quantiles": quantiles,
        }

        if task_status:
            params["taskStatus"] = [s.value for s in task_status]

        data = self._get(
            f"applications/{app_id}/stages/{stage_id}/{attempt_id}", params
        )
        return self._parse_model(data, StageData)

    def get_stage_task_summary(
        self,
        app_id: str,
        stage_id: int,
        attempt_id: int,
        quantiles: str = "0.05, 0.25, 0.5, 0.75, 0.95",
    ) -> TaskMetricDistributions:
        """
        Get task summary metrics for a specific stage attempt.

        Args:
            app_id: The application ID
            stage_id: The stage ID
            attempt_id: The attempt ID
            quantiles: Comma-separated list of quantiles to use for summary metrics

        Returns:
            TaskMetricDistributions object
        """
        params = {"quantiles": quantiles}
        data = self._get(
            f"applications/{app_id}/stages/{stage_id}/{attempt_id}/taskSummary", params
        )
        return self._parse_model(data, TaskMetricDistributions)

    def list_stage_tasks(
        self,
        app_id: str,
        stage_id: int,
        attempt_id: int,
        offset: int = 0,
        length: int = 20,
        sort_by: str = "ID",
        status: Optional[List[TaskStatus]] = None,
    ) -> List[TaskData]:
        """
        Get tasks for a specific stage attempt.

        Args:
            app_id: The application ID
            stage_id: The stage ID
            attempt_id: The attempt ID
            offset: Pagination offset
            length: Number of tasks to return
            sort_by: Field to sort by
            status: Filter by task status

        Returns:
            List of TaskData objects
        """
        params = {"offset": offset, "length": length, "sortBy": sort_by}

        if status:
            params["status"] = [s.value for s in status]

        data = self._get(
            f"applications/{app_id}/stages/{stage_id}/{attempt_id}/taskList", params
        )
        return self._parse_model_list(data, TaskData)

    def list_executors(self, app_id: str) -> List[ExecutorSummary]:
        """
        Get a list of all executors for an application.

        Args:
            app_id: The application ID

        Returns:
            List of ExecutorSummary objects
        """
        data = self._get(f"applications/{app_id}/executors")
        return self._parse_model_list(data, ExecutorSummary)

    def list_all_executors(self, app_id: str) -> List[ExecutorSummary]:
        """
        Get a list of all executors (active and inactive) for an application.

        Args:
            app_id: The application ID

        Returns:
            List of ExecutorSummary objects
        """
        data = self._get(f"applications/{app_id}/allexecutors")
        return self._parse_model_list(data, ExecutorSummary)

    def list_executor_thread_dump(
        self, app_id: str, executor_id: str
    ) -> List[ThreadStackTrace]:
        """
        Get thread dump for a specific executor.

        Args:
            app_id: The application ID
            executor_id: The executor ID

        Returns:
            List of ThreadStackTrace objects
        """
        data = self._get(f"applications/{app_id}/executors/{executor_id}/threads")
        return self._parse_model_list(data, ThreadStackTrace)

    def get_task_thread_dump(
        self, app_id: str, task_id: int, executor_id: str
    ) -> ThreadStackTrace:
        """
        Get thread dump for a specific task.

        Args:
            app_id: The application ID
            task_id: The task ID
            executor_id: The executor ID

        Returns:
            ThreadStackTrace object
        """
        params = {"taskId": task_id, "executorId": executor_id}
        data = self._get(f"applications/{app_id}/threads", params)
        return self._parse_model(data, ThreadStackTrace)

    def list_all_processes(self, app_id: str) -> List[ProcessSummary]:
        """
        Get a list of all processes for an application.

        Args:
            app_id: The application ID

        Returns:
            List of ProcessSummary objects
        """
        data = self._get(f"applications/{app_id}/allmiscellaneousprocess")
        return self._parse_model_list(data, ProcessSummary)

    def list_rdds(self, app_id: str) -> List[RDDStorageInfo]:
        """
        Get a list of all RDDs for an application.

        Args:
            app_id: The application ID

        Returns:
            List of RDDStorageInfo objects
        """
        data = self._get(f"applications/{app_id}/storage/rdd")
        return self._parse_model_list(data, RDDStorageInfo)

    def get_rdd(self, app_id: str, rdd_id: int) -> RDDStorageInfo:
        """
        Get information about a specific RDD.

        Args:
            app_id: The application ID
            rdd_id: The RDD ID

        Returns:
            RDDStorageInfo object
        """
        data = self._get(f"applications/{app_id}/storage/rdd/{rdd_id}")
        return self._parse_model(data, RDDStorageInfo)

    def get_environment(self, app_id: str) -> ApplicationEnvironmentInfo:
        """
        Get environment information for an application.

        Args:
            app_id: The application ID

        Returns:
            ApplicationEnvironmentInfo object
        """
        data = self._get(f"applications/{app_id}/environment")
        return self._parse_model(data, ApplicationEnvironmentInfo)

    def get_metrics_prometheus(self, app_id: str) -> str:
        """
        Get Prometheus metrics for an application.

        Args:
            app_id: The application ID

        Returns:
            Prometheus metrics as a string
        """
        url = urljoin(
            self.base_url.replace("/api/v1", "/metrics/executors"), "prometheus"
        )

        if self.session:
            response = self.session.get(url, timeout=self.timeout, proxies=self.proxies)
        else:
            response = requests.get(url, timeout=self.timeout, proxies=self.proxies)

        response.raise_for_status()
        return response.text

    def get_sql_list(
        self,
        app_id: str,
        attempt_id: Optional[str] = None,
        details: bool = True,
        plan_description: bool = False,
        offset: int = 0,
        length: int = 20,
    ) -> List[ExecutionData]:
        """
        Get a list of all SQL executions for an application.

        Args:
            app_id: The application ID
            attempt_id: Optional attempt ID
            details: Whether to include execution details
            plan_description: Whether to include plan description
            offset: Pagination offset
            length: Number of executions to return

        Returns:
            List of ExecutionData objects
        """
        params = {
            "details": str(details).lower(),
            "planDescription": str(plan_description).lower(),
            "offset": offset,
            "length": length,
        }

        if attempt_id:
            endpoint = f"applications/{app_id}/{attempt_id}/sql"
        else:
            endpoint = f"applications/{app_id}/sql"

        data = self._get(endpoint, params)
        return [ExecutionData.from_dict(item) for item in data]

    def get_sql_execution(
        self,
        app_id: str,
        execution_id: int,
        attempt_id: Optional[str] = None,
        details: bool = True,
        plan_description: bool = True,
    ) -> ExecutionData:
        """
        Get information about a specific SQL execution.

        Args:
            app_id: The application ID
            execution_id: The execution ID
            attempt_id: Optional attempt ID
            details: Whether to include execution details
            plan_description: Whether to include plan description

        Returns:
            ExecutionData object
        """
        params = {
            "details": str(details).lower(),
            "planDescription": str(plan_description).lower(),
        }

        if attempt_id:
            endpoint = f"applications/{app_id}/{attempt_id}/sql/{execution_id}"
        else:
            endpoint = f"applications/{app_id}/sql/{execution_id}"

        data = self._get(endpoint, params)
        return ExecutionData.from_dict(data)
