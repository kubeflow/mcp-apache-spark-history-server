#!/usr/bin/env python3
"""
EMR Persistent App UI Client

This module provides functionality to create an EMR Persistent App UI, retrieve its details and presigned URL,
and establish an HTTP session with proper cookie management for Spark History Server access.
"""

import logging
import time
from typing import Dict, Optional, Tuple
from urllib.parse import urlparse

import boto3
import requests
from botocore.exceptions import ClientError

from spark_history_mcp.config.config import ServerConfig

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class EMRPersistentUIClient:
    """Client for managing EMR Persistent App UI and HTTP sessions."""

    def __init__(self, server_config: ServerConfig):
        """
        Initialize the EMR client.

        Args:
            server_config: ServerConfig object
        """
        self.emr_cluster_arn = server_config.emr_cluster_arn
        self.region = self.emr_cluster_arn.split(":")[3]  # Extract region from ARN

        # Initialize boto3 client with credentials
        self.emr_client = boto3.client(
            "emr",
            region_name=self.region,
        )

        self.session = requests.Session()
        self.persistent_ui_id: Optional[str] = None
        self.presigned_url: Optional[str] = None
        self.base_url: Optional[str] = None
        self.timeout: int = server_config.timeout

    def create_persistent_app_ui(self) -> Dict:
        """
        Create a persistent app UI for the given cluster.

        Returns:
            Response from create-persistent-app-ui API call

        Raises:
            ClientError: If the API call fails
        """
        logger.info(f"Creating persistent app UI for cluster: {self.emr_cluster_arn}")

        try:
            response = self.emr_client.create_persistent_app_ui(
                TargetResourceArn=self.emr_cluster_arn
            )

            self.persistent_ui_id = response.get("PersistentAppUIId")
            runtime_role_enabled = response.get("RuntimeRoleEnabledCluster", False)

            logger.info("✅ Persistent App UI created successfully")
            logger.info(f"   Persistent UI ID: {self.persistent_ui_id}")
            logger.info(f"   Runtime Role Enabled: {runtime_role_enabled}")

            return response

        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            error_message = e.response["Error"]["Message"]
            logger.error(
                f"❌ Failed to create persistent app UI: {error_code} - {error_message}"
            )
            raise
        except Exception as e:
            logger.error(f"❌ Unexpected error creating persistent app UI: {str(e)}")
            raise

    def describe_persistent_app_ui(self) -> Dict:
        """
        Describe the persistent app UI.

        Returns:
            Response from describe-persistent-app-ui API call

        Raises:
            ValueError: If no persistent UI ID is available
            ClientError: If the API call fails
        """
        if not self.persistent_ui_id:
            raise ValueError("No persistent UI ID available. Create one first.")

        logger.info(f"Describing persistent app UI: {self.persistent_ui_id}")

        try:
            response = self.emr_client.describe_persistent_app_ui(
                PersistentAppUIId=self.persistent_ui_id
            )

            ui_details = response.get("PersistentAppUI", {})
            logger.info("✅ Persistent App UI details retrieved")
            logger.info(f"   Status: {ui_details.get('PersistentAppUIStatus')}")
            logger.info(f"   Creation Time: {ui_details.get('CreationTime')}")

            return response

        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            error_message = e.response["Error"]["Message"]
            logger.error(
                f"❌ Failed to describe persistent app UI: {error_code} - {error_message}"
            )
            raise
        except Exception as e:
            logger.error(f"❌ Unexpected error describing persistent app UI: {str(e)}")
            raise

    def get_presigned_url(self, ui_type: str = "SHS") -> str:
        """
        Get presigned URL for the persistent app UI.

        Args:
            ui_type: Type of UI ('SHS' for Spark History Server)

        Returns:
            Presigned URL string

        Raises:
            ValueError: If no persistent UI ID is available
            ClientError: If the API call fails
        """
        if not self.persistent_ui_id:
            raise ValueError("No persistent UI ID available. Create one first.")

        logger.info(
            f"Getting presigned URL for persistent app UI: {self.persistent_ui_id} (type: {ui_type})"
        )

        try:
            response = self.emr_client.get_persistent_app_ui_presigned_url(
                PersistentAppUIId=self.persistent_ui_id, PersistentAppUIType=ui_type
            )

            self.presigned_url_ready = response.get("PresignedURLReady")
            self.presigned_url = response.get("PresignedURL")

            # Extract base URL from presigned URL
            parsed_url = urlparse(self.presigned_url)
            self.base_url = f"{parsed_url.scheme}://{parsed_url.netloc}/shs"

            logger.info("✅ Presigned URL obtained successfully")
            logger.info(f"   Base URL: {self.base_url}")

            return self.presigned_url

        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            error_message = e.response["Error"]["Message"]
            logger.error(
                f"❌ Failed to get presigned URL: {error_code} - {error_message}"
            )
            raise
        except Exception as e:
            logger.error(f"❌ Unexpected error getting presigned URL: {str(e)}")
            raise

    def setup_http_session(self) -> requests.Session:
        """
        Set up HTTP session with proper headers and cookie management.

        Returns:
            Configured requests.Session object

        Raises:
            ValueError: If no presigned URL is available
        """
        if not self.presigned_url:
            raise ValueError("No presigned URL available. Get one first.")

        logger.info("Setting up HTTP session with cookie management")

        # Configure session with appropriate headers
        self.session.headers.update(
            {
                "User-Agent": "EMR-Persistent-UI-Client/1.0",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
                "Accept-Encoding": "gzip, deflate",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
            }
        )

        try:
            # Make initial request to establish session and get cookies
            logger.info("Making initial request")
            response = self.session.get(
                self.presigned_url, timeout=self.timeout, allow_redirects=True
            )
            response.raise_for_status()

            logger.info("✅ HTTP session established successfully")
            logger.info(f"   Status Code: {response.status_code}")
            logger.info(f"   Cookies: {len(self.session.cookies)} cookie(s) stored")

            # Log cookie details (without sensitive values)
            for cookie in self.session.cookies:
                logger.debug(f"   Cookie: {cookie.name} (domain: {cookie.domain})")

            return self.session

        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Failed to establish HTTP session: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"❌ Unexpected error setting up HTTP session: {str(e)}")
            raise

    def initialize(self) -> Tuple[str, requests.Session]:
        """
        Initialize the EMR Persistent UI client by creating a persistent app UI,
        verifying its status, getting a presigned URL, and setting up an HTTP session.

        If the status is STARTING, it will wait for the status
        to change to ATTACHED before proceeding.

        Returns:
            Tuple containing the base URL and configured session

        Raises:
            ValueError: If the persistent UI status is not ATTACHED after waiting
        """
        # Step 1: Create persistent app UI
        self.create_persistent_app_ui()

        # Step 2: Describe persistent app UI and verify status
        # Wait for up to 3 minutes if status is STARTING
        max_wait_time = 180  # 3 minutes in seconds
        wait_interval = 10  # Check every 10 seconds
        total_waited = 0
        ui_status = ""

        while total_waited < max_wait_time:
            describe_response = self.describe_persistent_app_ui()
            ui_status = describe_response.get("PersistentAppUI", {}).get(
                "PersistentAppUIStatus"
            )

            if ui_status == "ATTACHED":
                # Status is good, proceed
                break
            elif ui_status == "STARTING":
                # Status is starting, wait and check again
                logger.info(
                    f"EMR Persistent UI status is {ui_status}, waiting for ATTACHED status..."
                )
                time.sleep(wait_interval)
                total_waited += wait_interval
            else:
                # Status is something else (not STARTING or ATTACHED), raise error
                raise ValueError(
                    f"EMR Persistent UI status is {ui_status}, expected ATTACHED or STARTING"
                )

        # After waiting, check if we have the correct status
        if ui_status != "ATTACHED":
            raise ValueError(
                f"EMR Persistent UI status is still {ui_status} after waiting {total_waited} seconds, expected ATTACHED"
            )

        # Step 3: Get presigned URL
        self.get_presigned_url()

        # Step 4: Setup HTTP session
        self.setup_http_session()

        return self.base_url, self.session
