import logging
import os
import warnings
from pathlib import Path
from typing import Annotated, Any, Dict, List, Literal, Optional, Tuple

import yaml
from pydantic import Field
from pydantic.fields import FieldInfo
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
)

logger = logging.getLogger(__name__)

# Per-user config lives at ~/.config/spark-mcp/config.yaml.
DEFAULT_CONFIG_FILENAME = "config.yaml"
APP_CONFIG_DIR = "spark-mcp"
ENV_VAR_PREFIX = "SHS_"

# Legacy single-underscore SHS_* env vars are deprecated in favor of the
# double-underscore (__) delimiter.

# Direct-read vars (not Config fields); excluded from delimiter detection.
_DIRECT_READ_VARS = frozenset({"SHS_MCP_CONFIG", "SHS_MCP_TRANSPORT"})


def _warn_legacy_env(names: List[str]) -> None:
    message = (
        "Single-underscore SHS_* environment variables are deprecated and will "
        "be removed in a future major release; use the double-underscore (__) "
        f"delimiter instead. In use: {', '.join(names)}"
    )
    warnings.warn(message, DeprecationWarning, stacklevel=2)
    logger.warning(message)


def _legacy_config_vars() -> List[str]:
    """SHS_* config vars (excluding direct-read vars) using a single underscore."""
    return [
        env
        for env in os.environ
        if env.startswith(ENV_VAR_PREFIX)
        and env not in _DIRECT_READ_VARS
        and "__" not in env
    ]


def legacy_env_mode() -> bool:
    for env in os.environ:
        if (
            env.startswith(ENV_VAR_PREFIX)
            and env not in _DIRECT_READ_VARS
            and "__" not in env
        ):
            return True
    return False


def user_config_path() -> str:
    """Per-user config path: $XDG_CONFIG_HOME (if set) or ~/.config."""
    xdg_config_home = os.environ.get("XDG_CONFIG_HOME") or str(Path.home() / ".config")
    return os.path.join(xdg_config_home, APP_CONFIG_DIR, DEFAULT_CONFIG_FILENAME)


def resolve_config_path() -> Tuple[Optional[str], bool]:
    """Resolve which config file to load, highest precedence first.

    1. ``SHS_MCP_CONFIG`` env var / ``--config`` flag (explicit)
    2. ``./config.yaml``
    3. ``~/.config/spark-mcp/config.yaml``

    Returns ``(path, is_explicit)``; ``path`` is ``None`` when nothing is found
    (use defaults) and ``is_explicit`` marks a missing file as fatal.
    """
    explicit_path = os.getenv("SHS_MCP_CONFIG")
    if explicit_path:
        return explicit_path, True

    if os.path.exists(DEFAULT_CONFIG_FILENAME):
        return DEFAULT_CONFIG_FILENAME, False

    user_path = user_config_path()
    if os.path.exists(user_path):
        return user_path, False

    return None, False


class YamlConfigSettingsSource(PydanticBaseSettingsSource):
    """Settings source that loads YAML located via :func:`resolve_config_path`."""

    def get_field_value(
        self, field: FieldInfo, field_name: str
    ) -> Tuple[Any, str, bool]:
        # Not used for this implementation
        return None, field_name, False

    def __call__(self) -> Dict[str, Any]:
        """Load and return the YAML configuration data."""
        config_path, is_explicit = resolve_config_path()

        if config_path is None:
            return {}

        if not os.path.exists(config_path):
            # Explicitly requested but missing -> fatal; discovered -> defaults.
            if is_explicit:
                raise FileNotFoundError(
                    f"Config file not found: {config_path}\n"
                    f"Specified via: --config flag or SHS_MCP_CONFIG environment variable"
                )
            return {}

        with open(config_path, "r") as f:
            config_data = yaml.safe_load(f)

        return config_data or {}


class AuthConfig(BaseSettings):
    """Authentication configuration for the Spark server."""

    username: Optional[str] = Field(None)
    password: Optional[str] = Field(None)
    token: Optional[str] = Field(None)
    # Ignore unknown keys for compatibility with the shared shs CLI config.
    model_config = SettingsConfigDict(extra="ignore")


class ServerConfig(BaseSettings):
    """Server configuration for the Spark server."""

    url: Optional[str] = None
    auth: AuthConfig = Field(default_factory=AuthConfig, exclude=True)
    default: bool = False
    verify_ssl: bool = True
    emr_cluster_arn: Optional[str] = None  # EMR specific field
    use_proxy: bool = False
    timeout: int = 30  # HTTP request timeout in seconds
    include_plan_description: Optional[bool] = None
    # Ignore unknown keys for compatibility with the shared shs CLI config.
    model_config = SettingsConfigDict(extra="ignore")


class TransportSecurityConfig(BaseSettings):
    """Transport security configuration for DNS rebinding protection.

    See: https://github.com/modelcontextprotocol/python-sdk/issues/1798
    """

    enable_dns_rebinding_protection: bool = Field(
        default=False,
        description="Enable DNS rebinding protection. Set to True for production "
        "deployments with proper allowed_hosts configuration.",
    )
    allowed_hosts: List[str] = Field(
        default_factory=list,
        description="List of allowed Host header values. Supports wildcard ports "
        '(e.g., "localhost:*", "127.0.0.1:*", "your-gateway:*").',
    )
    allowed_origins: List[str] = Field(
        default_factory=list,
        description="List of allowed Origin header values. Supports wildcard ports "
        '(e.g., "http://localhost:*", "http://your-gateway:*").',
    )
    model_config = SettingsConfigDict(extra="ignore")


class McpConfig(BaseSettings):
    """Configuration for the MCP server."""

    transport: Optional[Literal["stdio", "streamable-http"]] = None
    transports: Annotated[
        Optional[List[Literal["stdio", "streamable-http"]]],
        Field(
            default=None,
            deprecated="mcp.transports is deprecated; use the singular mcp.transport instead.",
        ),
    ]
    address: Optional[str] = "localhost"
    port: Optional[int | str] = "18888"
    debug: Optional[bool] = False
    transport_security: Optional[TransportSecurityConfig] = Field(
        default=None,
        description="Transport security settings for DNS rebinding protection.",
    )
    model_config = SettingsConfigDict(extra="ignore")


class Config(BaseSettings):
    """Configuration for the Spark client."""

    servers: Dict[str, ServerConfig] = {
        "local": ServerConfig(url="http://localhost:18080", default=True),
    }
    mcp: Optional[McpConfig] = McpConfig()
    model_config = SettingsConfigDict(
        env_prefix=ENV_VAR_PREFIX,
        env_nested_delimiter="__",
        env_file=".env",
        env_file_encoding="utf-8",
    )

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        # Precedence order (highest to lowest):
        # 1. Environment variables
        # 2. .env file
        # 3. YAML config file (from SHS_MCP_CONFIG)
        # 4. Init settings (constructor arguments)
        # 5. File secrets
        return (
            env_settings,
            dotenv_settings,
            YamlConfigSettingsSource(settings_cls),
            init_settings,
            file_secret_settings,
        )


class LegacyConfig(Config):
    """Config parsed with the deprecated single-underscore nesting delimiter."""

    model_config = SettingsConfigDict(
        env_prefix=ENV_VAR_PREFIX,
        env_nested_delimiter="_",
        env_file=".env",
        env_file_encoding="utf-8",
    )


def load_config() -> Config:
    """Build the configuration, falling back to the legacy delimiter when in use."""
    if legacy_env_mode():
        _warn_legacy_env(_legacy_config_vars())
        return LegacyConfig()
    return Config()
