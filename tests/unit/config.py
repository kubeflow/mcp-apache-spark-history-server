import os
import tempfile
import unittest
from unittest.mock import patch

import yaml

from spark_history_mcp.config.config import AuthConfig, Config, ServerConfig


class TestConfig(unittest.TestCase):
    """Test cases for the Config class."""

    def setUp(self):
        """Set up test fixtures."""
        # Sample config data for testing
        self.config_data = {
            "servers": {
                "test_server": {
                    "url": "http://test-server:18080",
                    "auth": {"username": "test_user", "password": "test_pass"},
                    "default": True,
                    "verify_ssl": True,
                }
            },
            "mcp": {
                "address": "test_host",
                "port": 9999,
                "transports": ["streamable-http", "sse"],
                "debug": False,
            },
        }

    def test_config_from_file(self):
        """Test loading configuration from a file."""
        # Create a temporary config file
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as temp_file:
            yaml.dump(self.config_data, temp_file)
            temp_file_path = temp_file.name

        try:
            # Load config from the file
            config = Config.from_file(temp_file_path)

            # Verify the loaded configuration
            self.assertEqual(config.mcp.address, "test_host")
            self.assertEqual(config.mcp.port, 9999)
            self.assertEqual(len(config.mcp.transports), 2)
            self.assertIn("streamable-http", config.mcp.transports)
            self.assertIn("sse", config.mcp.transports)
            self.assertFalse(config.mcp.debug)

            # Verify server config
            self.assertIn("test_server", config.servers)
            server = config.servers["test_server"]
            self.assertEqual(server.url, "http://test-server:18080")
            self.assertEqual(server.auth.username, "test_user")
            self.assertEqual(server.auth.password, "test_pass")
            self.assertTrue(server.default)
            self.assertTrue(server.verify_ssl)
        finally:
            # Clean up the temporary file
            os.unlink(temp_file_path)

    def test_nonexistent_config_file(self):
        """Test behavior when config file doesn't exist."""
        with self.assertRaises(FileNotFoundError):
            Config.from_file("nonexistent_file.yaml")

    @patch.dict(
        os.environ,
        {
            "SHS_MCP_ADDRESS": "env_host",
            "SHS_MCP_PORT": "8888",
            "SHS_MCP_DEBUG": "true",
            "SHS_SERVERS_ENV_SERVER_URL": "http://env-server:18080",
            "SHS_SERVERS_ENV_SERVER_AUTH_USERNAME": "env_user",
            "SHS_SERVERS_ENV_SERVER_AUTH_PASSWORD": "env_pass",
            "SHS_SERVERS_ENV_SERVER_DEFAULT": "true",
        },
    )
    def test_config_from_env_vars(self):
        """Test loading configuration from environment variables."""
        # Create minimal config with empty servers dict to be populated from env
        minimal_config = {"servers": {}}

        with tempfile.NamedTemporaryFile(mode="w", delete=False) as temp_file:
            yaml.dump(minimal_config, temp_file)
            temp_file_path = temp_file.name

        try:
            config = Config.from_file(temp_file_path)

            # Verify MCP config from env vars
            self.assertEqual(config.mcp.address, "env_host")
            self.assertEqual(config.mcp.port, "8888")
            self.assertTrue(config.mcp.debug)

            # Verify server config from env vars
            self.assertIn("env_server", config.servers)
            server = config.servers["env_server"]
            self.assertEqual(server.url, "http://env-server:18080")
            self.assertEqual(server.auth.username, "env_user")
            self.assertEqual(server.auth.password, "env_pass")
            self.assertTrue(server.default)
        finally:
            os.unlink(temp_file_path)

    @patch.dict(
        os.environ,
        {
            "SHS_MCP_ADDRESS": "override_host",
            "SHS_MCP_PORT": "7777",
            "SHS_SERVERS_TEST_SERVER_URL": "http://override-server:18080",
            "SHS_SERVERS_TEST_SERVER_AUTH_USERNAME": "override_user",
        },
    )
    def test_env_vars_override_file_config(self):
        """Test that environment variables take precedence over file configuration."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as temp_file:
            yaml.dump(self.config_data, temp_file)
            temp_file_path = temp_file.name

        try:
            config = Config.from_file(temp_file_path)

            # Verify that env vars override file config
            self.assertEqual(config.mcp.address, "override_host")
            self.assertEqual(config.mcp.port, "7777")

            # Verify that server config is overridden
            server = config.servers["test_server"]
            self.assertEqual(server.url, "http://override-server:18080")
            self.assertEqual(server.auth.username, "override_user")

            # Password should still be from file as it wasn't overridden
            self.assertEqual(server.auth.password, "test_pass")
        finally:
            os.unlink(temp_file_path)

    def test_default_values(self):
        """Test that default values are set correctly when not specified."""
        minimal_config = {"servers": {"minimal": {"url": "http://minimal:18080"}}}

        with tempfile.NamedTemporaryFile(mode="w", delete=False) as temp_file:
            yaml.dump(minimal_config, temp_file)
            temp_file_path = temp_file.name

        try:
            config = Config.from_file(temp_file_path)

            # Check MCP defaults
            self.assertEqual(config.mcp.address, "localhost")
            self.assertEqual(config.mcp.port, "18888")
            self.assertFalse(config.mcp.debug)
            self.assertEqual(config.mcp.transports, ["streamable-http"])

            # Check server defaults
            server = config.servers["minimal"]
            self.assertEqual(server.url, "http://minimal:18080")
            self.assertFalse(server.default)
            self.assertTrue(server.verify_ssl)
            self.assertIsNone(server.emr_cluster_arn)
            self.assertIsNotNone(server.auth)
            self.assertIsNone(server.auth.username)
            self.assertIsNone(server.auth.password)
            self.assertIsNone(server.auth.token)
        finally:
            os.unlink(temp_file_path)

    def test_model_serialization(self):
        """Test that models serialize correctly, especially with excluded fields."""
        auth = AuthConfig(username="test_user", password="")
        server = ServerConfig(url="http://test:18080", auth=auth)

        # Test that auth is excluded from serialization
        server_dict = server.model_dump()
        self.assertIn("auth", server_dict)

        # Test with explicit exclude
        server_dict = server.model_dump(exclude={"auth"})
        self.assertNotIn("auth", server_dict)

    def test_dynamic_emr_clusters_mode_default(self):
        """Test that dynamic_emr_clusters_mode defaults to False."""
        minimal_config = {"servers": {"minimal": {"url": "http://minimal:18080"}}}

        with tempfile.NamedTemporaryFile(mode="w", delete=False) as temp_file:
            yaml.dump(minimal_config, temp_file)
            temp_file_path = temp_file.name

        try:
            config = Config.from_file(temp_file_path)
            self.assertFalse(config.dynamic_emr_clusters_mode)
        finally:
            os.unlink(temp_file_path)

    def test_dynamic_emr_clusters_mode_enabled(self):
        """Test that dynamic_emr_clusters_mode can be enabled."""
        config_data = {"dynamic_emr_clusters_mode": True}

        with tempfile.NamedTemporaryFile(mode="w", delete=False) as temp_file:
            yaml.dump(config_data, temp_file)
            temp_file_path = temp_file.name

        try:
            config = Config.from_file(temp_file_path)
            self.assertTrue(config.dynamic_emr_clusters_mode)
            self.assertIsNone(config.servers)
        finally:
            os.unlink(temp_file_path)

    def test_dynamic_emr_clusters_mode_mutually_exclusive_with_servers(self):
        """Test that dynamic_emr_clusters_mode and servers are mutually exclusive."""
        config_data = {
            "dynamic_emr_clusters_mode": True,
            "servers": {"test": {"url": "http://test:18080"}},
        }

        with tempfile.NamedTemporaryFile(mode="w", delete=False) as temp_file:
            yaml.dump(config_data, temp_file)
            temp_file_path = temp_file.name

        try:
            with self.assertRaises(ValueError) as context:
                Config.from_file(temp_file_path)
            self.assertIn(
                "dynamic_emr_clusters_mode cannot be True when servers is not empty",
                str(context.exception),
            )
        finally:
            os.unlink(temp_file_path)

    def test_default_servers_when_dynamic_mode_disabled(self):
        """Test that default servers are set when dynamic_emr_clusters_mode is False."""
        minimal_config = {}  # Empty config

        with tempfile.NamedTemporaryFile(mode="w", delete=False) as temp_file:
            yaml.dump(minimal_config, temp_file)
            temp_file_path = temp_file.name

        try:
            config = Config.from_file(temp_file_path)
            self.assertFalse(config.dynamic_emr_clusters_mode)
            self.assertIsNotNone(config.servers)
            self.assertIn("local", config.servers)
            self.assertEqual(config.servers["local"].url, "http://localhost:18080")
            self.assertTrue(config.servers["local"].default)
        finally:
            os.unlink(temp_file_path)
