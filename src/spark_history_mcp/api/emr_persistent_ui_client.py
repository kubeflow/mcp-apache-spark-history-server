#!/usr/bin/env python3
"""EMR Persistent App UI client.

Creates an EMR Persistent App UI, retrieves its presigned URL, and establishes
an authenticated HTTP session (cookie-based) for Spark History Server access.
"""

import logging
import time
from contextlib import contextmanager
from typing import Dict, Optional, Tuple
from urllib.parse import urlparse

import boto3
import requests
from botocore.exceptions import ClientError

from spark_history_mcp.config.config import ServerConfig

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


@contextmanager
def _log_errors(action: str):
    """Log and re-raise any error from a block, with a consistent message."""
    try:
        yield
    except ClientError as e:
        err = e.response["Error"]
        logger.error("Failed to %s: %s - %s", action, err["Code"], err["Message"])
        raise
    except Exception as e:
        logger.error("Unexpected error during %s: %s", action, e)
        raise


class EMRPersistentUIClient:
    """Client for managing EMR Persistent App UI and HTTP sessions."""

    def __init__(self, server_config: ServerConfig):
        self.emr_cluster_arn = server_config.emr_cluster_arn
        self.region = self.emr_cluster_arn.split(":")[3]  # region from ARN
        self.emr_client = boto3.client("emr", region_name=self.region)

        self.session = requests.Session()
        self.persistent_ui_id: Optional[str] = None
        self.presigned_url: Optional[str] = None
        self.base_url: Optional[str] = None
        self.timeout: int = server_config.timeout

    def create_persistent_app_ui(self) -> Dict:
        """Create a persistent app UI for the cluster."""
        logger.info("Creating persistent app UI for cluster: %s", self.emr_cluster_arn)
        with _log_errors("create persistent app UI"):
            response = self.emr_client.create_persistent_app_ui(
                TargetResourceArn=self.emr_cluster_arn
            )
        self.persistent_ui_id = response.get("PersistentAppUIId")
        logger.info(
            "Persistent App UI created (id=%s, runtimeRole=%s)",
            self.persistent_ui_id,
            response.get("RuntimeRoleEnabledCluster", False),
        )
        return response

    def describe_persistent_app_ui(self) -> Dict:
        """Describe the persistent app UI (requires a created UI)."""
        if not self.persistent_ui_id:
            raise ValueError("No persistent UI ID available. Create one first.")

        logger.info("Describing persistent app UI: %s", self.persistent_ui_id)
        with _log_errors("describe persistent app UI"):
            response = self.emr_client.describe_persistent_app_ui(
                PersistentAppUIId=self.persistent_ui_id
            )
        status = response.get("PersistentAppUI", {}).get("PersistentAppUIStatus")
        logger.info("Persistent App UI status: %s", status)
        return response

    def get_presigned_url(self, ui_type: str = "SHS") -> str:
        """Get the presigned URL for the persistent app UI and derive base_url."""
        if not self.persistent_ui_id:
            raise ValueError("No persistent UI ID available. Create one first.")

        logger.info("Getting presigned URL (type: %s)", ui_type)
        with _log_errors("get presigned URL"):
            response = self.emr_client.get_persistent_app_ui_presigned_url(
                PersistentAppUIId=self.persistent_ui_id, PersistentAppUIType=ui_type
            )
        self.presigned_url = response.get("PresignedURL")
        parsed_url = urlparse(self.presigned_url)
        self.base_url = f"{parsed_url.scheme}://{parsed_url.netloc}/shs"
        logger.info("Presigned URL obtained (base URL: %s)", self.base_url)
        return self.presigned_url

    def setup_http_session(self) -> requests.Session:
        """Establish the HTTP session and capture auth cookies via the presigned URL."""
        if not self.presigned_url:
            raise ValueError("No presigned URL available. Get one first.")

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

        with _log_errors("establish HTTP session"):
            response = self.session.get(
                self.presigned_url, timeout=self.timeout, allow_redirects=True
            )
            response.raise_for_status()

        logger.info(
            "HTTP session established (%d cookie(s))", len(self.session.cookies)
        )
        return self.session

    def cookie_header(self) -> str:
        """Serialize the session cookies into a ``Cookie`` header value.

        The generated API client has no cookie jar, so EMR auth is carried as a
        static ``Cookie`` header.
        """
        return "; ".join(f"{c.name}={c.value}" for c in self.session.cookies)

    def initialize(self) -> Tuple[str, requests.Session]:
        """Create the UI, wait for ATTACHED, get the presigned URL, and set up the session.

        Returns the base URL and the authenticated session. Raises ``ValueError``
        if the UI does not reach ATTACHED within the wait window.
        """
        self.create_persistent_app_ui()

        max_wait_time = 180  # seconds
        wait_interval = 10
        total_waited = 0
        ui_status = ""

        while total_waited < max_wait_time:
            describe_response = self.describe_persistent_app_ui()
            ui_status = describe_response.get("PersistentAppUI", {}).get(
                "PersistentAppUIStatus"
            )
            if ui_status == "ATTACHED":
                break
            if ui_status != "STARTING":
                raise ValueError(
                    f"EMR Persistent UI status is {ui_status}, expected ATTACHED or STARTING"
                )
            logger.info("EMR Persistent UI is %s, waiting for ATTACHED...", ui_status)
            time.sleep(wait_interval)
            total_waited += wait_interval

        if ui_status != "ATTACHED":
            raise ValueError(
                f"EMR Persistent UI status is still {ui_status} after waiting "
                f"{total_waited} seconds, expected ATTACHED"
            )

        self.get_presigned_url()
        self.setup_http_session()
        return self.base_url, self.session
