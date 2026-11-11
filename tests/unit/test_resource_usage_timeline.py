"""Tests for the running resource totals of get_resource_usage_timeline."""

from unittest.mock import MagicMock, patch

from spark_history_mcp.api_client.models.executor import Executor
from spark_history_mcp.tools.tools import get_resource_usage_timeline

GIB = 1024 * 1024 * 1024


def _timeline_summary(executors):
    """Run the tool against a mocked client returning ``executors``."""
    client = MagicMock()
    client.list_all_executors.return_value = executors
    client.list_stages.return_value = []
    with patch(
        "spark_history_mcp.tools.tools.get_client_or_default", return_value=client
    ):
        result = get_resource_usage_timeline("app-1", ctx=MagicMock())
    return result["summary"]


def test_removed_executors_release_their_resources():
    """Peak totals count only executors that are alive at the same time."""
    # Dynamic allocation: three 4-core executors, at most two alive at once.
    summary = _timeline_summary(
        [
            Executor(
                id="1",
                totalCores=4,
                maxMemory=2 * GIB,
                addTime="2026-01-01T00:00:00.000GMT",
                removeTime="2026-01-01T00:02:00.000GMT",
                removeReason="Executor killed by driver (idle).",
            ),
            Executor(
                id="2",
                totalCores=4,
                maxMemory=2 * GIB,
                addTime="2026-01-01T00:01:00.000GMT",
                removeTime="2026-01-01T00:04:00.000GMT",
                removeReason="Executor killed by driver (idle).",
            ),
            Executor(
                id="3",
                totalCores=4,
                maxMemory=2 * GIB,
                addTime="2026-01-01T00:03:00.000GMT",
            ),
        ]
    )

    assert summary["executor_additions"] == 3
    assert summary["executor_removals"] == 2
    assert summary["peak_executors"] == 2
    assert summary["peak_cores"] == 8


def test_executor_without_total_cores_counts_as_zero_cores():
    summary = _timeline_summary(
        [
            Executor(
                id="driver",
                addTime="2026-01-01T00:00:00.000GMT",
                removeTime="2026-01-01T00:05:00.000GMT",
            ),
        ]
    )

    assert summary["peak_executors"] == 1
    assert summary["peak_cores"] == 0
