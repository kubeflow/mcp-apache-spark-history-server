import os
from typing import Dict, List, Literal, Optional

import yaml
from pydantic import BaseModel, Field


class AuthConfig(BaseModel):
    """Authentication configuration for the Spark server."""

    username: str = Field(None, alias="username")
    password: str = Field(None, alias="password")
    token: str = Field(None, alias="token")

    def __init__(self, **data):
        # Support environment variables for sensitive data
        if not data.get("username"):
            data["username"] = os.getenv("SHS_SPARK_USERNAME")
        if not data.get("password"):
            data["password"] = os.getenv("SHS_SPARK_PASSWORD")
        if not data.get("token"):
            data["token"] = os.getenv("SHS_SPARK_TOKEN")
        super().__init__(**data)


class ServerConfig(BaseModel):
    """Server configuration for the Spark server."""

    url: str
    auth: AuthConfig = Field(None, alias="auth")
    default: bool = Field(None, alias="default")
    verify_ssl: bool = Field(True, alias="verify_ssl")


class McpConfig(BaseModel):
    """Configuration for the MCP server."""

    transports: List[Literal["stdio", "sse", "streamable-http"]] = Field(
        default_factory=list
    )
    address: str = Field(default="localhost")
    port: str = Field(default="18888")
    debug: bool = Field(default=False)


class Config(BaseModel):
    """Configuration for the Spark client."""

    servers: Dict[str, ServerConfig]
    mcp: Optional[McpConfig] = None

    @classmethod
    def from_file(cls, file_path: str) -> "Config":
        """Load configuration from a YAML file."""
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Config file not found: {file_path}")

        with open(file_path, "r") as f:
            config_data = yaml.safe_load(f)

        return cls.model_validate(config_data)
