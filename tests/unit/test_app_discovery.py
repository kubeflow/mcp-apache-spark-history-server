"""Tests for cross-server application discovery and its cache."""

from unittest.mock import MagicMock, patch

import pytest

from spark_history_mcp.api_client.exceptions import NotFoundException
from spark_history_mcp.utils.utils import ApplicationDiscovery


def _not_found(*_args, **_kwargs):
    raise NotFoundException(status=404, reason="Not Found")


def _client(has_app: bool) -> MagicMock:
    client = MagicMock()
    if not has_app:
        client.get_application.side_effect = _not_found
    return client


def test_finds_application_on_every_server_that_has_it():
    discovery = ApplicationDiscovery(
        {"a": _client(False), "b": _client(True), "c": _client(True)}
    )

    assert discovery.find_application_servers("app-1") == ["b", "c"]


def test_hit_is_cached_until_ttl_expires():
    client = _client(True)
    discovery = ApplicationDiscovery({"local": client}, ttl=300)

    with patch("spark_history_mcp.utils.utils.time.time", return_value=1000.0):
        assert discovery.find_application_servers("app-1") == ["local"]
        assert discovery.find_application_servers("app-1") == ["local"]
    assert client.get_application.call_count == 1

    with patch("spark_history_mcp.utils.utils.time.time", return_value=1301.0):
        assert discovery.find_application_servers("app-1") == ["local"]
    assert client.get_application.call_count == 2


def test_miss_is_not_cached():
    """An app the server has not loaded yet is found as soon as it appears."""
    client = _client(False)
    discovery = ApplicationDiscovery({"local": client})

    with pytest.raises(ValueError, match="not found on any server"):
        discovery.get_client_for_app("app-1")

    client.get_application.side_effect = None
    found, server_name = discovery.get_client_for_app("app-1")

    assert (found, server_name) == (client, "local")
    assert client.get_application.call_count == 2


def test_unreachable_server_is_not_cached_as_missing():
    client = MagicMock()
    client.get_application.side_effect = ConnectionError("connection refused")
    discovery = ApplicationDiscovery({"local": client})

    assert discovery.find_application_servers("app-1") == []

    client.get_application.side_effect = None
    assert discovery.find_application_servers("app-1") == ["local"]


def test_expired_hit_is_dropped_when_app_is_gone():
    client = _client(True)
    discovery = ApplicationDiscovery({"local": client}, ttl=300)

    with patch("spark_history_mcp.utils.utils.time.time", return_value=1000.0):
        assert discovery.find_application_servers("app-1") == ["local"]

    client.get_application.side_effect = _not_found
    with patch("spark_history_mcp.utils.utils.time.time", return_value=1301.0):
        assert discovery.find_application_servers("app-1") == []

    assert "app-1" not in discovery._cache
