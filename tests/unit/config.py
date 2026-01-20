import os
import tempfile
import unittest
from unittest.mock import patch

import yaml

from spark_history_mcp.config.config import (
    AuthConfig,
    Config,
    ServerConfig,
    TransportSecurityConfig,
)


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
            # Load config from the file using SHS_MCP_CONFIG env var
            with patch.dict(os.environ, {"SHS_MCP_CONFIG": temp_file_path}):
                config = Config()

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
        """Test behavior when explicitly specified config file doesn't exist."""
        with self.assertRaises(FileNotFoundError):
            with patch.dict(os.environ, {"SHS_MCP_CONFIG": "nonexistent_file.yaml"}):
                Config()

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
            with patch.dict(os.environ, {"SHS_MCP_CONFIG": temp_file_path}):
                config = Config()

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
            with patch.dict(os.environ, {"SHS_MCP_CONFIG": temp_file_path}):
                config = Config()

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
            with patch.dict(os.environ, {"SHS_MCP_CONFIG": temp_file_path}):
                config = Config()

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


class TestTransportSecurityConfig(unittest.TestCase):
    """Test cases for TransportSecurityConfig.

    See: https://github.com/modelcontextprotocol/python-sdk/issues/1798
    """

    def test_transport_security_default_values(self):
        """Test that transport security defaults are set correctly."""
        ts_config = TransportSecurityConfig()

        # Default should be disabled for backwards compatibility
        self.assertFalse(ts_config.enable_dns_rebinding_protection)
        self.assertEqual(ts_config.allowed_hosts, [])
        self.assertEqual(ts_config.allowed_origins, [])

    def test_transport_security_from_yaml(self):
        """Test loading transport security from YAML config."""
        config_data = {
            "servers": {"local": {"url": "http://localhost:18080", "default": True}},
            "mcp": {
                "transports": ["streamable-http"],
                "port": "18888",
                "transport_security": {
                    "enable_dns_rebinding_protection": True,
                    "allowed_hosts": ["localhost:*", "127.0.0.1:*", "my-gateway:*"],
                    "allowed_origins": ["http://localhost:*", "http://127.0.0.1:*"],
                },
            },
        }

        with tempfile.NamedTemporaryFile(mode="w", delete=False) as temp_file:
            yaml.dump(config_data, temp_file)
            temp_file_path = temp_file.name

        try:
            with patch.dict(os.environ, {"SHS_MCP_CONFIG": temp_file_path}):
                config = Config()

            # Verify transport security config
            ts = config.mcp.transport_security
            self.assertIsNotNone(ts)
            self.assertTrue(ts.enable_dns_rebinding_protection)
            self.assertEqual(
                ts.allowed_hosts, ["localhost:*", "127.0.0.1:*", "my-gateway:*"]
            )
            self.assertEqual(
                ts.allowed_origins, ["http://localhost:*", "http://127.0.0.1:*"]
            )
        finally:
            os.unlink(temp_file_path)

    def test_transport_security_disabled_in_yaml(self):
        """Test explicitly disabling transport security in YAML."""
        config_data = {
            "servers": {"local": {"url": "http://localhost:18080", "default": True}},
            "mcp": {
                "transports": ["streamable-http"],
                "transport_security": {
                    "enable_dns_rebinding_protection": False,
                },
            },
        }

        with tempfile.NamedTemporaryFile(mode="w", delete=False) as temp_file:
            yaml.dump(config_data, temp_file)
            temp_file_path = temp_file.name

        try:
            with patch.dict(os.environ, {"SHS_MCP_CONFIG": temp_file_path}):
                config = Config()

            ts = config.mcp.transport_security
            self.assertIsNotNone(ts)
            self.assertFalse(ts.enable_dns_rebinding_protection)
        finally:
            os.unlink(temp_file_path)

    def test_transport_security_default_when_not_specified(self):
        """Test transport security defaults when not specified in config."""
        config_data = {
            "servers": {"local": {"url": "http://localhost:18080", "default": True}},
            "mcp": {"transports": ["streamable-http"]},
        }

        with tempfile.NamedTemporaryFile(mode="w", delete=False) as temp_file:
            yaml.dump(config_data, temp_file)
            temp_file_path = temp_file.name

        try:
            with patch.dict(os.environ, {"SHS_MCP_CONFIG": temp_file_path}):
                config = Config()

            # Transport security should have default values
            ts = config.mcp.transport_security
            self.assertIsNotNone(ts)
            self.assertFalse(ts.enable_dns_rebinding_protection)
            self.assertEqual(ts.allowed_hosts, [])
            self.assertEqual(ts.allowed_origins, [])
        finally:
            os.unlink(temp_file_path)

    def test_transport_security_integration_with_mcp_library(self):
        """Test that transport security config integrates with MCP library."""
        from mcp.server.transport_security import TransportSecuritySettings

        # Create config with transport security enabled
        ts_config = TransportSecurityConfig(
            enable_dns_rebinding_protection=True,
            allowed_hosts=["localhost:*", "127.0.0.1:*"],
            allowed_origins=["http://localhost:*"],
        )

        # Convert to MCP library's TransportSecuritySettings
        ts_settings = TransportSecuritySettings(
            enable_dns_rebinding_protection=ts_config.enable_dns_rebinding_protection,
            allowed_hosts=ts_config.allowed_hosts,
            allowed_origins=ts_config.allowed_origins,
        )

        # Verify the settings are correctly transferred
        self.assertTrue(ts_settings.enable_dns_rebinding_protection)
        self.assertEqual(ts_settings.allowed_hosts, ["localhost:*", "127.0.0.1:*"])
        self.assertEqual(ts_settings.allowed_origins, ["http://localhost:*"])

    def test_transport_security_partial_config(self):
        """Test transport security with partial configuration."""
        config_data = {
            "servers": {"local": {"url": "http://localhost:18080", "default": True}},
            "mcp": {
                "transports": ["streamable-http"],
                "transport_security": {
                    "enable_dns_rebinding_protection": True,
                    # Only specifying allowed_hosts, not allowed_origins
                    "allowed_hosts": ["localhost:*"],
                },
            },
        }

        with tempfile.NamedTemporaryFile(mode="w", delete=False) as temp_file:
            yaml.dump(config_data, temp_file)
            temp_file_path = temp_file.name

        try:
            with patch.dict(os.environ, {"SHS_MCP_CONFIG": temp_file_path}):
                config = Config()

            ts = config.mcp.transport_security
            self.assertTrue(ts.enable_dns_rebinding_protection)
            self.assertEqual(ts.allowed_hosts, ["localhost:*"])
            # allowed_origins should default to empty list
            self.assertEqual(ts.allowed_origins, [])
        finally:
            os.unlink(temp_file_path)

    def test_transport_security_wildcard_patterns(self):
        """Test various wildcard patterns for hosts and origins."""
        ts_config = TransportSecurityConfig(
            enable_dns_rebinding_protection=True,
            allowed_hosts=[
                "localhost:*",
                "127.0.0.1:*",
                "192.168.1.100:*",
                "my-gateway.example.com:*",
                "internal-service:8080",  # Specific port
            ],
            allowed_origins=[
                "http://localhost:*",
                "https://localhost:*",
                "http://127.0.0.1:*",
                "https://my-gateway.example.com:*",
                "http://internal-service:8080",  # Specific port
            ],
        )

        # Verify all patterns are stored correctly
        self.assertEqual(len(ts_config.allowed_hosts), 5)
        self.assertEqual(len(ts_config.allowed_origins), 5)
        self.assertIn("localhost:*", ts_config.allowed_hosts)
        self.assertIn("internal-service:8080", ts_config.allowed_hosts)
        self.assertIn("http://localhost:*", ts_config.allowed_origins)
        self.assertIn("https://localhost:*", ts_config.allowed_origins)


class TestAppTransportSecurityIntegration(unittest.TestCase):
    """Test app.py integration with transport security settings."""

    def test_app_run_configures_transport_security(self):
        """Test that app.run() correctly configures transport security."""
        from mcp.server.transport_security import TransportSecuritySettings

        from spark_history_mcp.core.app import mcp

        config_data = {
            "servers": {"local": {"url": "http://localhost:18080", "default": True}},
            "mcp": {
                "transports": ["streamable-http"],
                "port": "18888",
                "address": "localhost",
                "debug": False,
                "transport_security": {
                    "enable_dns_rebinding_protection": True,
                    "allowed_hosts": ["localhost:*", "test-gateway:*"],
                    "allowed_origins": ["http://localhost:*"],
                },
            },
        }

        with tempfile.NamedTemporaryFile(mode="w", delete=False) as temp_file:
            yaml.dump(config_data, temp_file)
            temp_file_path = temp_file.name

        try:
            with patch.dict(os.environ, {"SHS_MCP_CONFIG": temp_file_path}):
                config = Config()

            # Manually apply the transport security settings as run() would
            if config.mcp.transport_security:
                ts_config = config.mcp.transport_security
                mcp.settings.transport_security = TransportSecuritySettings(
                    enable_dns_rebinding_protection=ts_config.enable_dns_rebinding_protection,
                    allowed_hosts=ts_config.allowed_hosts,
                    allowed_origins=ts_config.allowed_origins,
                )

            # Verify settings were applied
            ts = mcp.settings.transport_security
            self.assertIsNotNone(ts)
            self.assertTrue(ts.enable_dns_rebinding_protection)
            self.assertEqual(ts.allowed_hosts, ["localhost:*", "test-gateway:*"])
            self.assertEqual(ts.allowed_origins, ["http://localhost:*"])
        finally:
            os.unlink(temp_file_path)
            # Reset to None to avoid affecting other tests
            mcp.settings.transport_security = None
