#!/usr/bin/env python3
"""
EMR Serverless Client

This module provides functionality to access EMR Serverless Spark History Server
through AWS APIs and presigned URLs.
"""

import logging
import time
from typing import Dict, Optional, Tuple
from urllib.parse import urlparse, urljoin

import boto3
import requests
from botocore.exceptions import ClientError

from spark_history_mcp.config.config import ServerConfig

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class EMRServerlessClient:
    """Client for managing EMR Serverless Spark History Server access."""

    def __init__(self, server_config: ServerConfig):
        """
        Initialize the EMR Serverless client.

        Args:
            server_config: ServerConfig object with emr_serverless_app_id
        """
        self.emr_serverless_app_id = server_config.emr_serverless_app_id
        self.region = server_config.region or "us-east-1"

        # Initialize boto3 client
        self.emr_serverless_client = boto3.client(
            "emr-serverless",
            region_name=self.region,
        )

        self.session = requests.Session()
        self.base_url: Optional[str] = None
        self.timeout: int = server_config.timeout

    def get_presigned_spark_ui_url(self, job_run_id: str) -> Optional[str]:
        """
        Get a presigned URL for the Spark UI using EMR API.
        
        Args:
            job_run_id: The job run ID
            
        Returns:
            Presigned URL or None if not available
        """
        try:
            # Use EMR API to get presigned URL for Spark UI
            response = self.emr_serverless_client.get_dashboard_for_job_run(
                applicationId=self.emr_serverless_app_id,
                jobRunId=job_run_id
            )
            
            if 'url' in response:
                spark_ui_url = response['url']
                # Convert Spark UI URL to Spark History Server URL
                if '/proxy/' in spark_ui_url:
                    # Replace proxy URL with history server URL
                    base_url = spark_ui_url.split('/proxy/')[0]
                    self.base_url = f"{base_url}/shs/"
                    logger.info(f"Got presigned Spark History Server URL: {self.base_url}")
                    return self.base_url
                    
            return None
            
        except Exception as e:
            logger.error(f"Failed to get presigned Spark UI URL: {e}")
            return None

    def get_spark_history_server_url(self) -> Optional[str]:
        """
        Get the Spark History Server URL for EMR Serverless application.
        
        Returns:
            The Spark History Server URL or None if not available
        """
        try:
            # Get application details
            response = self.emr_serverless_client.get_application(
                applicationId=self.emr_serverless_app_id
            )
            
            app_state = response["application"]["state"]
            logger.info(f"EMR Serverless application {self.emr_serverless_app_id} is in state: {app_state}")

            # Get recent job runs (including completed ones for Spark History Server access)
            job_runs = self.emr_serverless_client.list_job_runs(
                applicationId=self.emr_serverless_app_id,
                maxResults=10  # Get more recent runs
            )
            
            if not job_runs.get("jobRuns"):
                logger.warning(f"No job runs found for application {self.emr_serverless_app_id}")
                return None
            
            # Find the most recent job run that has completed (for Spark History Server access)
            latest_job_run = None
            for job_run in job_runs["jobRuns"]:
                if job_run["state"] in ["SUCCESS", "FAILED", "CANCELLED"]:
                    latest_job_run = job_run
                    break
            
            if not latest_job_run:
                # If no completed runs, try the most recent one
                latest_job_run = job_runs["jobRuns"][0]
                logger.info(f"Using most recent job run {latest_job_run['id']} in state {latest_job_run['state']}")
            
            job_run_id = latest_job_run["id"]
            
            # Try to get presigned URL first
            presigned_url = self.get_presigned_spark_ui_url(job_run_id)
            if presigned_url:
                return presigned_url
            
            # Fallback to constructing the persistent UI URL
            # Format: https://p-{job_run_id}-{app_id}.emrappui-prod.{region}.amazonaws.com/shs/
            self.base_url = f"https://p-{job_run_id}-{self.emr_serverless_app_id}.emrappui-prod.{self.region}.amazonaws.com/shs/"
            
            logger.info(f"EMR Serverless Spark History Server URL: {self.base_url}")
            logger.info(f"Based on job run: {job_run_id} (state: {latest_job_run['state']})")
            
            return self.base_url
            
        except ClientError as e:
            logger.error(f"Failed to get EMR Serverless application details: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error getting Spark History Server URL: {e}")
            return None

    def get_presigned_url(self, path: str = "") -> Optional[str]:
        """
        Get a presigned URL for accessing EMR Serverless Spark History Server.
        
        Args:
            path: Additional path to append to the base URL
            
        Returns:
            Presigned URL or None if not available
        """
        if not self.base_url:
            self.base_url = self.get_spark_history_server_url()
            
        if not self.base_url:
            return None
            
        try:
            # For EMR Serverless, we need to use AWS credentials to access the UI
            # This requires generating presigned URLs through the EMR service
            
            full_url = urljoin(self.base_url, path)
            
            # Use STS to get temporary credentials for the request
            sts_client = boto3.client("sts", region_name=self.region)
            credentials = sts_client.get_session_token()
            
            # Add AWS signature to the request
            from botocore.auth import SigV4Auth
            from botocore.awsrequest import AWSRequest
            
            request = AWSRequest(method="GET", url=full_url)
            SigV4Auth(credentials["Credentials"], "emr-serverless", self.region).add_auth(request)
            
            return request.url
            
        except Exception as e:
            logger.error(f"Failed to generate presigned URL: {e}")
            return full_url  # Return unsigned URL as fallback

    def make_authenticated_request(self, path: str, method: str = "GET", params=None, **kwargs) -> requests.Response:
        """
        Make an authenticated request to the EMR Serverless Spark History Server.
        
        Args:
            path: API path to request
            method: HTTP method
            params: Query parameters
            **kwargs: Additional arguments for requests
            
        Returns:
            Response object
        """
        if not self.base_url:
            self.base_url = self.get_spark_history_server_url()
            
        if not self.base_url:
            raise Exception("Unable to get Spark History Server URL")
            
        url = urljoin(self.base_url, path)
        
        # Add AWS authentication headers for EMR Serverless Persistent UI
        try:
            import boto3
            from botocore.auth import SigV4Auth
            from botocore.awsrequest import AWSRequest
            from urllib.parse import urlparse, parse_qs
            
            # Get current session credentials
            session = boto3.Session()
            credentials = session.get_credentials()
            
            if not credentials:
                raise Exception("No AWS credentials found")
            
            # Parse URL to get query parameters
            parsed_url = urlparse(url)
            query_params = parse_qs(parsed_url.query) if parsed_url.query else {}
            
            # Merge with provided params
            if params:
                for key, value in params.items():
                    query_params[key] = [str(value)]
            
            # Create AWS request for signing
            aws_request = AWSRequest(
                method=method.upper(),
                url=f"{parsed_url.scheme}://{parsed_url.netloc}{parsed_url.path}",
                params=query_params,
                headers={
                    'Host': parsed_url.netloc,
                    'Accept': 'application/json',
                    'User-Agent': 'spark-history-mcp/1.0'
                }
            )
            
            # Sign the request with EMR service
            SigV4Auth(credentials, "elasticmapreduce", self.region).add_auth(aws_request)
            
            # Convert to requests format
            headers = dict(aws_request.headers)
            final_url = aws_request.url
            
        except Exception as e:
            logger.error(f"Failed to add AWS authentication: {e}")
            # Fallback to unsigned request
            headers = {
                'Accept': 'application/json',
                'User-Agent': 'spark-history-mcp/1.0'
            }
            final_url = url
        
        # Merge with any provided headers
        if "headers" in kwargs:
            headers.update(kwargs["headers"])
        kwargs["headers"] = headers
        
        kwargs.setdefault("timeout", self.timeout)
        kwargs.setdefault("verify", True)
        
        logger.debug(f"Making request to: {final_url}")
        logger.debug(f"Headers: {headers}")
        
        response = self.session.request(method, final_url, params=params, **kwargs)
        
        # Log response details for debugging
        logger.debug(f"Response status: {response.status_code}")
        logger.debug(f"Response headers: {dict(response.headers)}")
        
        response.raise_for_status()
        return response
