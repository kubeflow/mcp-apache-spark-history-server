import os
from typing import Dict, List, Literal, Optional

import yaml
from pydantic import Field, model_validator
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
)


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

    dynamic_emr_clusters_mode: bool = False
    servers: Optional[Dict[str, ServerConfig]] = None
    mcp: Optional[McpConfig] = McpConfig(transports=["streamable-http"])
    model_config = SettingsConfigDict(
        env_prefix="SHS_",
        env_nested_delimiter="_",
        env_file=".env",
        env_file_encoding="utf-8",
    )

    @model_validator(mode="after")
    def validate_and_set_default_values(self) -> "Config":
        """Set default servers and validate that dynamic_emr_clusters_mode and servers are mutually exclusive."""
        if self.servers is None and not self.dynamic_emr_clusters_mode:
            self.servers = {
                "local": ServerConfig(url="http://localhost:18080", default=True),
            }

        # Validate mutual exclusivity
        if self.dynamic_emr_clusters_mode and self.servers:
            raise ValueError(
                "dynamic_emr_clusters_mode cannot be True when servers is not empty. "
                "These modes are mutually exclusive."
            )
        return self

    @classmethod
    def from_file(cls, file_path: str) -> "Config":
        """Load configuration from a YAML file."""
        if not os.path.exists(file_path):
            return Config()

        with open(file_path, "r") as f:
            config_data = yaml.safe_load(f)

        return cls.model_validate(config_data)

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        return env_settings, dotenv_settings, init_settings, file_secret_settings
