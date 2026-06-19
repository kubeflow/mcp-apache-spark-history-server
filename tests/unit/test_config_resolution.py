"""Tests for the config file resolution cascade.

Precedence (highest to lowest):
  1. --config flag / SHS_MCP_CONFIG env var (explicit)
  2. ./config.yaml (current working directory)
  3. ~/.config/spark-mcp/config.yaml (XDG config home)
"""

import os
from pathlib import Path

import pytest
import yaml

from spark_history_mcp.config.config import (
    Config,
    resolve_config_path,
    user_config_path,
)


def _write_yaml(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        yaml.dump(data, f)


@pytest.fixture(autouse=True)
def _isolate_env(monkeypatch, tmp_path):
    """Isolate cwd, HOME, and XDG/SHS env vars for each test."""
    monkeypatch.delenv("SHS_MCP_CONFIG", raising=False)
    # Point HOME and XDG_CONFIG_HOME at a clean temp area so the real user
    # config (if any) cannot leak into the test.
    home = tmp_path / "home"
    home.mkdir()
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.delenv("XDG_CONFIG_HOME", raising=False)
    # Run from a clean, empty working directory.
    cwd = tmp_path / "cwd"
    cwd.mkdir()
    monkeypatch.chdir(cwd)
    yield


class TestUserConfigPath:
    def test_uses_xdg_config_home_when_set(self, monkeypatch, tmp_path):
        xdg = tmp_path / "custom-xdg"
        monkeypatch.setenv("XDG_CONFIG_HOME", str(xdg))
        assert user_config_path() == str(xdg / "spark-mcp" / "config.yaml")

    def test_falls_back_to_dot_config_when_xdg_unset(self, monkeypatch, tmp_path):
        monkeypatch.delenv("XDG_CONFIG_HOME", raising=False)
        expected = str(Path.home() / ".config" / "spark-mcp" / "config.yaml")
        assert user_config_path() == expected


class TestResolveConfigPath:
    def test_explicit_env_var_wins(self, monkeypatch, tmp_path):
        # Even with a cwd config present, the explicit env var takes precedence.
        _write_yaml(str(tmp_path / "cwd" / "config.yaml"), {"servers": {}})
        explicit = tmp_path / "explicit.yaml"
        explicit.write_text("servers: {}\n")
        monkeypatch.setenv("SHS_MCP_CONFIG", str(explicit))

        path, is_explicit = resolve_config_path()
        assert path == str(explicit)
        assert is_explicit is True

    def test_explicit_missing_is_still_explicit(self, monkeypatch):
        monkeypatch.setenv("SHS_MCP_CONFIG", "/nope/missing.yaml")
        path, is_explicit = resolve_config_path()
        assert path == "/nope/missing.yaml"
        assert is_explicit is True

    def test_cwd_used_when_no_env_var(self, tmp_path):
        cwd_config = tmp_path / "cwd" / "config.yaml"
        _write_yaml(str(cwd_config), {"servers": {}})
        path, is_explicit = resolve_config_path()
        assert path == "config.yaml"
        assert is_explicit is False

    def test_cwd_takes_precedence_over_user_config(self, monkeypatch, tmp_path):
        xdg = tmp_path / "xdg"
        monkeypatch.setenv("XDG_CONFIG_HOME", str(xdg))
        _write_yaml(str(xdg / "spark-mcp" / "config.yaml"), {"servers": {}})
        _write_yaml(str(tmp_path / "cwd" / "config.yaml"), {"servers": {}})

        path, is_explicit = resolve_config_path()
        assert path == "config.yaml"
        assert is_explicit is False

    def test_user_config_used_when_no_cwd_config(self, monkeypatch, tmp_path):
        xdg = tmp_path / "xdg"
        monkeypatch.setenv("XDG_CONFIG_HOME", str(xdg))
        user_config = xdg / "spark-mcp" / "config.yaml"
        _write_yaml(str(user_config), {"servers": {}})

        path, is_explicit = resolve_config_path()
        assert path == str(user_config)
        assert is_explicit is False

    def test_nothing_found_returns_none(self, monkeypatch, tmp_path):
        xdg = tmp_path / "xdg"
        monkeypatch.setenv("XDG_CONFIG_HOME", str(xdg))
        path, is_explicit = resolve_config_path()
        assert path is None
        assert is_explicit is False


class TestConfigUsesResolution:
    def test_loads_from_cwd_config(self, tmp_path):
        _write_yaml(
            str(tmp_path / "cwd" / "config.yaml"),
            {"servers": {"from_cwd": {"url": "http://cwd:18080", "default": True}}},
        )
        config = Config()
        assert "from_cwd" in config.servers
        assert config.servers["from_cwd"].url == "http://cwd:18080"

    def test_loads_from_user_config(self, monkeypatch, tmp_path):
        xdg = tmp_path / "xdg"
        monkeypatch.setenv("XDG_CONFIG_HOME", str(xdg))
        _write_yaml(
            str(xdg / "spark-mcp" / "config.yaml"),
            {"servers": {"from_user": {"url": "http://user:18080", "default": True}}},
        )
        config = Config()
        assert "from_user" in config.servers
        assert config.servers["from_user"].url == "http://user:18080"

    def test_defaults_when_no_file(self, monkeypatch, tmp_path):
        monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "xdg"))
        config = Config()
        # Built-in default server.
        assert "local" in config.servers
        assert config.servers["local"].url == "http://localhost:18080"

    def test_missing_explicit_raises(self, monkeypatch):
        monkeypatch.setenv("SHS_MCP_CONFIG", "/nope/missing.yaml")
        with pytest.raises(FileNotFoundError):
            Config()
