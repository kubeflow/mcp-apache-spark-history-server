"""
Simple timeout handling and parallel execution for MCP tools.
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Callable, Dict, List, Tuple

import requests


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
