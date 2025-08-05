#!/usr/bin/env python3
"""
EMR Client

This module provides functionality to interact with AWS EMR clusters,
specifically to get cluster ARNs by cluster ID or cluster name.
"""

import logging
from typing import Dict, List, Optional

import boto3
from botocore.exceptions import ClientError, NoCredentialsError

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class EMRClusterNotFoundError(Exception):
    """Raised when an EMR cluster cannot be found."""

    pass


class EMRClient:
    """Client for interacting with AWS EMR clusters."""

    def __init__(self, region_name: Optional[str] = None):
        """
        Initialize the EMR client.

        Args:
            region_name: AWS region name. If not provided, uses default region from AWS config.
        """
        try:
            self.emr_client = boto3.client("emr", region_name=region_name)
            self.region_name = region_name or self.emr_client.meta.region_name
            logger.info(f"Initialized EMR client for region: {self.region_name}")
        except NoCredentialsError:
            logger.error("AWS credentials not found. Please configure AWS credentials.")
            raise
        except Exception as e:
            logger.error(f"Failed to initialize EMR client: {str(e)}")
            raise

    def get_cluster_arn_by_id(self, cluster_id: str) -> str:
        """
        Get cluster ARN by cluster ID.

        Args:
            cluster_id: EMR cluster ID (e.g., 'j-1234567890ABC')

        Returns:
            Cluster ARN

        Raises:
            EMRClusterNotFoundError: If cluster is not found
            ClientError: If AWS API call fails
        """
        try:
            logger.info(f"Getting cluster details for cluster ID: {cluster_id}")
            response = self.emr_client.describe_cluster(ClusterId=cluster_id)
            cluster_arn = response["Cluster"]["ClusterArn"]
            logger.info(f"Found cluster ARN: {cluster_arn}")
            return cluster_arn
        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code == "InvalidRequestException":
                raise EMRClusterNotFoundError(
                    f"Cluster with ID '{cluster_id}' not found"
                ) from e
            else:
                logger.error(f"AWS API error getting cluster by ID: {str(e)}")
                raise
        except Exception as e:
            logger.error(f"Unexpected error getting cluster by ID: {str(e)}")
            raise

    def get_active_cluster_arn_by_name(self, cluster_name: str) -> str:
        """
        Get cluster ARN by cluster name. This only searches for RUNNING or WAITING clusters.

        Args:
            cluster_name: EMR cluster name

        Returns:
            Cluster ARN

        Raises:
            EMRClusterNotFoundError: If cluster is not found or multiple clusters with same name exist
            ClientError: If AWS API call fails
        """
        try:
            logger.info(f"Searching for cluster with name: {cluster_name}")

            # List clusters to find matching name
            matching_clusters = self._find_active_clusters_by_name(cluster_name)

            if not matching_clusters:
                raise EMRClusterNotFoundError(
                    f"No cluster found with name '{cluster_name}'"
                )

            if len(matching_clusters) > 1:
                cluster_ids = [cluster["Id"] for cluster in matching_clusters]
                raise EMRClusterNotFoundError(
                    f"Multiple clusters found with name '{cluster_name}': {cluster_ids}. "
                    "Please use cluster ID instead."
                )

            cluster = matching_clusters[0]
            cluster_id = cluster["Id"]

            # Get full cluster details to retrieve ARN
            return self.get_cluster_arn_by_id(cluster_id)

        except EMRClusterNotFoundError:
            raise
        except Exception as e:
            logger.error(f"Unexpected error getting cluster by name: {str(e)}")
            raise

    def _find_active_clusters_by_name(self, cluster_name: str) -> List[Dict]:
        """
        Find clusters by name using list_clusters API. This only searches for RUNNING or WAITING clusters.

        Args:
            cluster_name: Name of the cluster to search for

        Returns:
            List of cluster summaries matching the name
        """
        matching_clusters = []
        marker = None

        try:
            while True:
                # List clusters with pagination
                list_params = {}
                if marker:
                    list_params["Marker"] = marker
                    list_params["ClusterStates"] = ["RUNNING", "WAITING"]

                response = self.emr_client.list_clusters(**list_params)
                clusters = response.get("Clusters", [])

                # Filter clusters by name
                for cluster in clusters:
                    if cluster.get("Name") == cluster_name:
                        matching_clusters.append(cluster)

                # Check if there are more pages
                marker = response.get("Marker")
                if not marker:
                    break

        except ClientError as e:
            logger.error(f"AWS API error listing clusters: {str(e)}")
            raise

        return matching_clusters

    def get_cluster_arn(self, cluster_identifier: str) -> str:
        """
        Get cluster ARN by cluster identifier (ID or name).

        This method automatically detects whether the identifier is a cluster ID or name:
        - Cluster IDs follow the pattern 'j-' followed by alphanumeric characters
        - Everything else is treated as a cluster name

        Args:
            cluster_identifier: Either cluster ID (e.g., 'j-1234567890ABC') or cluster name

        Returns:
            Cluster ARN

        Raises:
            EMRClusterNotFoundError: If cluster is not found
            ClientError: If AWS API call fails
        """
        # Check if it's a cluster ID (starts with 'j-')
        if cluster_identifier.startswith("j-"):
            logger.info(f"Treating '{cluster_identifier}' as cluster ID")
            return self.get_cluster_arn_by_id(cluster_identifier)
        else:
            logger.info(f"Treating '{cluster_identifier}' as cluster name")
            return self.get_active_cluster_arn_by_name(cluster_identifier)

    def get_cluster_details(self, cluster_identifier: str) -> Dict:
        """
        Get full cluster details by cluster identifier (ID or name).

        Args:
            cluster_identifier: Either cluster ID or cluster name

        Returns:
            Cluster details dictionary

        Raises:
            EMRClusterNotFoundError: If cluster is not found
            ClientError: If AWS API call fails
        """
        # First get the cluster ARN to ensure we have the correct cluster
        cluster_arn = self.get_cluster_arn(cluster_identifier)

        # Extract cluster ID from ARN if we started with a name
        if not cluster_identifier.startswith("j-"):
            # ARN format: arn:aws:elasticmapreduce:region:account:cluster/cluster-id
            cluster_id = cluster_arn.split("/")[-1]
        else:
            cluster_id = cluster_identifier

        try:
            response = self.emr_client.describe_cluster(ClusterId=cluster_id)
            return response["Cluster"]
        except ClientError as e:
            logger.error(f"AWS API error getting cluster details: {str(e)}")
            raise
