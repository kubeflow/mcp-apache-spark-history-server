from typing import Optional

from spark_history_mcp.api.emr_persistent_ui_client import EMRPersistentUIClient
from spark_history_mcp.api.spark_client import SparkRestClient
from spark_history_mcp.config.config import ServerConfig


def create_spark_client_from_config(server_config: ServerConfig) -> SparkRestClient:
    """
    Create a SparkRestClient from a ServerConfig.

    This function handles both regular Spark History Servers and EMR Persistent UI configurations.

    Args:
        server_config: The server configuration

    Returns:
        SparkRestClient instance properly configured
    """
    # Check if this is an EMR server configuration
    if server_config.emr_cluster_arn:
        return create_spark_emr_client(server_config.emr_cluster_arn, server_config)
    else:
        # Regular Spark REST client
        return SparkRestClient(server_config)


def create_spark_emr_client(
    emr_cluster_arn: str, server_config: Optional[ServerConfig] = None
) -> SparkRestClient:
    """
    Create a SparkRestClient from EMR cluster arn and optional ServerConfig.

    This function handles EMR Persistent UI applications.

    Args:
        emr_cluster_arn: The EMR cluster ARN
        server_config: The server configuration

    Returns:
        SparkRestClient instance properly configured
    """
    emr_client = EMRPersistentUIClient(emr_cluster_arn=emr_cluster_arn)

    # Initialize EMR client (create persistent UI, get presigned URL, setup session)
    base_url, session = emr_client.initialize()

    # Create a modified server config with the base URL
    if server_config is None:
        server_config = ServerConfig()
    else:
        server_config = server_config.model_copy()
    server_config.url = base_url

    # Create SparkRestClient with the session
    spark_client = SparkRestClient(server_config)
    spark_client.session = session  # Use the authenticated session

    return spark_client
