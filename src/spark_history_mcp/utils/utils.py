"""
Simple timeout handling and parallel execution for MCP tools.
"""

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Callable, Dict, List, Optional, Tuple

import requests

from spark_history_mcp.api.spark_client import SparkRestClient

logger = logging.getLogger(__name__)


def parallel_execute(
    api_calls: List[Tuple[str, Callable]], max_workers: int = 6, timeout: int = 180
) -> Dict[str, Any]:
    """
    Execute multiple API calls in parallel with error handling.

    Args:
        api_calls: List of (name, function) tuples
        max_workers: Maximum number of parallel workers
        timeout: Total timeout for all operations

    Returns:
        Dictionary with results and errors
    """
    results = {}
    errors = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_name = {executor.submit(func): name for name, func in api_calls}

        # Collect results as they complete
        for future in as_completed(future_to_name, timeout=timeout):
            name = future_to_name[future]
            try:
                result = future.result()
                results[name] = result
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 500:
                    # Try to extract the actual error from response text
                    error_text = (
                        e.response.text if hasattr(e.response, "text") else str(e)
                    )
                    if (
                        "OutOfMemoryError" in error_text
                        or "Java heap space" in error_text
                    ):
                        error_msg = f"{name} failed: Spark History Server out of memory (increase SPARK_DAEMON_MEMORY)"
                else:
                    error_msg = (
                        f"{name} failed: HTTP {e.response.status_code} - {str(e)}"
                    )
                errors.append(error_msg)
            except Exception as e:
                error_msg = f"{name} failed: {str(e)}"
                errors.append(error_msg)

    return {"results": results, "errors": errors}


"""
Application discovery with TTL cache and collision handling.
"""


class ApplicationDiscovery:
    def __init__(self, clients: Dict[str, SparkRestClient], ttl: int = 300):
        self.clients = clients
        self.ttl = ttl
        self._cache: Dict[str, Dict] = {}

    def _is_expired(self, entry: Dict) -> bool:
        return time.time() - entry["last_updated"] > self.ttl

    def find_application_servers(self, app_id: str) -> List[str]:
        if app_id in self._cache and not self._is_expired(self._cache[app_id]):
            return self._cache[app_id]["servers"]

        # Search all servers
        servers = []
        for server_name, client in self.clients.items():
            logger.debug(f"Checking for application '{app_id}' in '{server_name}'")
            try:
                client.get_application(app_id)
                servers.append(server_name)
            except Exception as e:
                logger.debug(
                    f"Application '{app_id}' not found on server '{server_name}': {e}"
                )
                continue

        self._cache[app_id] = {"servers": servers, "last_updated": time.time()}

        return servers

    def get_client_for_app(
        self, app_id: str, server_name: Optional[str] = None
    ) -> Tuple[SparkRestClient, str]:
        if server_name:
            if server_name not in self.clients:
                raise ValueError(f"Server '{server_name}' not found")
            return self.clients[server_name], server_name

        servers = self.find_application_servers(app_id)
        if not servers:
            raise ValueError(f"Application '{app_id}' not found on any server")

        # Use first server found
        chosen_server = servers[0]
        return self.clients[chosen_server], chosen_server
