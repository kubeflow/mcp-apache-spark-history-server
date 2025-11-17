import os
from typing import Any, Dict, List, Literal, Optional, Tuple

import yaml
from pydantic import Field
from pydantic.fields import FieldInfo
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
)


class YamlConfigSettingsSource(PydanticBaseSettingsSource):
    """Custom settings source that loads configuration from a YAML file.

    The file path is determined by the SHS_MCP_CONFIG environment variable,
    defaulting to 'config.yaml' if not set.
    """

    def get_field_value(
        self, field: FieldInfo, field_name: str
    ) -> Tuple[Any, str, bool]:
        # Not used for this implementation
        return None, field_name, False

    def __call__(self) -> Dict[str, Any]:
        """Load and return the YAML configuration data."""
        config_path = os.getenv("SHS_MCP_CONFIG", "config.yaml")
        is_explicitly_set = "SHS_MCP_CONFIG" in os.environ

        if not os.path.exists(config_path):
            # If the config file was explicitly specified but doesn't exist, fail fast
            if is_explicitly_set:
                raise FileNotFoundError(
                    f"Config file not found: {config_path}\n"
                    f"Specified via: SHS_MCP_CONFIG environment variable"
                )
            # If using default and it doesn't exist, return empty (will use defaults)
            return {}

        with open(config_path, "r") as f:
            config_data = yaml.safe_load(f)

        return config_data or {}


class AuthConfig(BaseSettings):
    """Authentication configuration for the Spark server."""

    username: Optional[str] = Field(None)
    password: Optional[str] = Field(None)
    token: Optional[str] = Field(None)


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


class McpConfig(BaseSettings):
    """Configuration for the MCP server."""

    transports: List[Literal["stdio", "sse", "streamable-http"]] = Field(
        default_factory=list
    )
    address: Optional[str] = "localhost"
    port: Optional[int | str] = "18888"
    debug: Optional[bool] = False
    model_config = SettingsConfigDict(extra="ignore")


class Config(BaseSettings):
    """Configuration for the Spark client."""

    servers: Dict[str, ServerConfig] = {
        "local": ServerConfig(url="http://localhost:18080", default=True),
    }
    mcp: Optional[McpConfig] = McpConfig(transports=["streamable-http"])
    model_config = SettingsConfigDict(
        env_prefix="SHS_",
        env_nested_delimiter="_",
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
