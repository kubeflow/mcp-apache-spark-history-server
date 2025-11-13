import logging
from datetime import datetime
from threading import Lock

from datadog_api_client import Configuration, ApiClient
from datadog_api_client.v2.api.logs_api import LogsApi
from datadog_api_client.v2.model.logs_list_request import LogsListRequest
from datadog_api_client.v2.model.logs_list_request_page import LogsListRequestPage
from datadog_api_client.v2.model.logs_query_filter import LogsQueryFilter
from datadog_api_client.v2.model.logs_sort import LogsSort
from pydantic import BaseModel, Field

from spark_history_mcp.common.variable import (
    POD_NAMESPACE,
    POD_SERVICE_ACCOUNT,
)
from spark_history_mcp.common.vault import VaultApi

logger = logging.getLogger(__name__)

DATADOG_SECRET_KEYS = f"k8s/{POD_NAMESPACE}/{POD_SERVICE_ACCOUNT}/datadog"


class LogDD(BaseModel):
    timestamp: datetime = Field(description="Timestamp when the log has been emitted")
    message: str = Field(description="Log message")
    status: str = Field(description="Log level")
    host: str = Field(description="Host where the logs has been emitted")
    service: str = Field(description="Service where the logs has been emitted")
    pod_name: str = Field(description="Pod name where the logs has been emitted")


class SingletonMeta(type):
    """
    This is a thread-safe implementation of Singleton.
    """

    _instances = {}

    _lock: Lock = Lock()
    """
    We now have a lock object that will be used to synchronize threads during
    first access to the Singleton.
    """

    def __call__(cls, *args, **kwargs):
        """
        Possible changes to the value of the `__init__` argument do not affect
        the returned instance.
        """
        # Now, imagine that the program has just been launched. Since there's no
        # Singleton instance yet, multiple threads can simultaneously pass the
        # previous conditional and reach this point almost at the same time. The
        # first of them will acquire lock and will proceed further, while the
        # rest will wait here.
        with cls._lock:
            # The first thread to acquire the lock, reaches this conditional,
            # goes inside and creates the Singleton instance. Once it leaves the
            # lock block, a thread that might have been waiting for the lock
            # release may then enter this section. But since the Singleton field
            # is already initialized, the thread won't create a new object.
            if cls not in cls._instances:
                instance = super().__call__(*args, **kwargs)
                cls._instances[cls] = instance
        return cls._instances[cls]


class Datadog(metaclass=SingletonMeta):
    LIMIT_PER_QUERY_LOGS = 1000
    MAX_RETURN_LOGS = 100000

    def __init__(self):
        vault_api = VaultApi()

        logger.info(
            f"Retrieving open lineage API Key with {DATADOG_SECRET_KEYS}: dd_api_key"
        )
        api_key = vault_api.get_secret_kv_store(DATADOG_SECRET_KEYS, "dd_api_key")
        logger.info(
            f"Retrieving open lineage API Key with {DATADOG_SECRET_KEYS}: dd_app_key"
        )
        app_key = vault_api.get_secret_kv_store(DATADOG_SECRET_KEYS, "dd_app_key")

        self.configuration = Configuration()
        self.configuration.server_variables["site"] = "datadoghq.com"
        self.configuration.api_key["apiKeyAuth"] = api_key
        self.configuration.api_key["appKeyAuth"] = app_key
        self.configuration.enable_retry = True
        self.configuration.max_retries = 5

    def get_logs(
        self, index_names: list[str], query: str, _from: datetime, to: datetime
    ) -> list[LogDD]:
        with ApiClient(self.configuration) as api_client:
            logs_api_instance = LogsApi(api_client)
            request = LogsListRequest(
                filter=LogsQueryFilter(
                    query=query,
                    indexes=index_names,
                    _from=_from.isoformat(),
                    to=to.isoformat(),
                ),
                sort=LogsSort.TIMESTAMP_ASCENDING,
                page=LogsListRequestPage(
                    limit=self.LIMIT_PER_QUERY_LOGS,
                ),
            )
            try:
                logs: list[LogDD] = []
                # Use list_logs_with_pagination for automatic pagination
                for log in logs_api_instance.list_logs_with_pagination(body=request):
                    pod_name = next(
                        (
                            tag
                            for tag in log.attributes.get("tags", [])
                            if tag.startswith("pod_name:")
                        ),
                        None,
                    ).replace("pod_name:", "")
                    logs.append(
                        LogDD(
                            timestamp=log.attributes.timestamp,
                            message=log.attributes.get("message", ""),
                            status=log.attributes.get("status", ""),
                            host=log.attributes.get("host", ""),
                            service=log.attributes.get("service", ""),
                            pod_name=pod_name,
                        )
                    )

                    if len(logs) >= self.MAX_RETURN_LOGS:
                        break

                return logs
            except Exception as e:
                logger.error(f"Error retrieving logs: {e}")
                raise
