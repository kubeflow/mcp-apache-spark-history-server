from typing import Optional

from pydantic import BaseModel, Field


class StaticServerSpec(BaseModel):
    """Model for specifying static Spark server configuration in the tool call."""

    default_client: bool = Field(
        False, description="Use the default client from config.yaml if True. "
    )
    server_name: Optional[str] = Field(
        None, description="Name of a pre-configured server from config.yaml"
    )


class DynamicEMRServerSpec(BaseModel):
    """Model for specifying dynamic EMR server in the tool call."""

    emr_cluster_arn: Optional[str] = Field(None, description="ARN of the EMR cluster. ")
    emr_cluster_id: Optional[str] = Field(
        None, description="ID of the EMR cluster. Starts with 'j-'"
    )
    emr_cluster_name: Optional[str] = Field(
        None,
        description="Name of the *active* EMR cluster. Terminated clusters are not supported.",
    )


class ServerSpec(BaseModel):
    """Model for specifying which Spark server to use in the tool call."""

    static_server_spec: Optional[StaticServerSpec] = Field(
        None,
        description="spec to be used with static Spark servers defined in config.yaml.",
    )
    dynamic_emr_server_spec: Optional[DynamicEMRServerSpec] = Field(
        None, description="spec to be used in dynamic EMR server mode. "
    )
