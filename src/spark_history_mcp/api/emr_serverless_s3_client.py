#!/usr/bin/env python3
"""
EMR Serverless S3 Event Log Client

This module downloads Spark event logs from S3 and serves them through a local Spark History Server.
"""

import logging
import os
import tempfile
import subprocess
import time
from typing import Optional
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

from spark_history_mcp.config.config import ServerConfig

logger = logging.getLogger(__name__)


class EMRServerlessS3Client:
    """Client for accessing EMR Serverless Spark event logs from S3."""

    def __init__(self, server_config: ServerConfig):
        """
        Initialize the EMR Serverless S3 client.

        Args:
            server_config: ServerConfig object with emr_serverless_app_id
        """
        self.emr_serverless_app_id = server_config.emr_serverless_app_id
        self.region = server_config.region or "us-east-1"
        self.timeout = server_config.timeout

        # Initialize boto3 clients
        self.emr_serverless_client = boto3.client(
            "emr-serverless", region_name=self.region
        )
        self.s3_client = boto3.client("s3", region_name=self.region)

        # Local Spark History Server setup
        self.temp_dir = None
        self.spark_history_server_process = None
        self.local_port = 18081  # Use different port to avoid conflicts
        self.base_url = f"http://localhost:{self.local_port}"

    def get_s3_event_logs_path(self, job_run_id: str) -> Optional[str]:
        """
        Get the S3 path for event logs of a specific job run.
        
        Args:
            job_run_id: The job run ID
            
        Returns:
            S3 path or None if not found
        """
        try:
            response = self.emr_serverless_client.get_job_run(
                applicationId=self.emr_serverless_app_id,
                jobRunId=job_run_id
            )
            
            job_run = response["jobRun"]
            monitoring_config = job_run.get("configurationOverrides", {}).get("monitoringConfiguration", {})
            s3_config = monitoring_config.get("s3MonitoringConfiguration", {})
            
            if "logUri" in s3_config:
                log_uri = s3_config["logUri"]
                # Construct event log path
                s3_path = f"{log_uri.rstrip('/')}/applications/{self.emr_serverless_app_id}/jobs/{job_run_id}/sparklogs/"
                logger.info(f"Event logs S3 path: {s3_path}")
                return s3_path
                
            return None
            
        except Exception as e:
            logger.error(f"Failed to get S3 event logs path: {e}")
            return None

    def download_event_logs(self, s3_path: str) -> Optional[str]:
        """
        Download event logs from S3 to local directory.
        
        Args:
            s3_path: S3 path to event logs
            
        Returns:
            Local directory path or None if failed
        """
        try:
            # Create temporary directory
            if not self.temp_dir:
                self.temp_dir = tempfile.mkdtemp(prefix="spark-events-")
            
            # Parse S3 path
            if not s3_path.startswith("s3://"):
                return None
                
            s3_path_parts = s3_path[5:].split("/", 1)
            bucket = s3_path_parts[0]
            prefix = s3_path_parts[1] if len(s3_path_parts) > 1 else ""
            
            logger.info(f"Downloading from s3://{bucket}/{prefix} to {self.temp_dir}")
            
            # List and download event log files
            paginator = self.s3_client.get_paginator("list_objects_v2")
            
            downloaded_files = 0
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    if key.endswith(".inprogress") or key.endswith("/"):
                        continue
                        
                    # Create local file path
                    local_file = os.path.join(self.temp_dir, os.path.basename(key))
                    
                    # Download file
                    self.s3_client.download_file(bucket, key, local_file)
                    downloaded_files += 1
                    logger.debug(f"Downloaded: {key}")
            
            if downloaded_files > 0:
                logger.info(f"Downloaded {downloaded_files} event log files to {self.temp_dir}")
                return self.temp_dir
            else:
                logger.warning("No event log files found")
                return None
                
        except Exception as e:
            logger.error(f"Failed to download event logs: {e}")
            return None

    def start_local_spark_history_server(self, event_logs_dir: str) -> bool:
        """
        Start a local Spark History Server with the downloaded event logs.
        
        Args:
            event_logs_dir: Directory containing event logs
            
        Returns:
            True if started successfully, False otherwise
        """
        try:
            # Check if Spark is available
            spark_home = os.environ.get("SPARK_HOME")
            if not spark_home:
                # Try to find spark in common locations
                for spark_path in ["/opt/spark", "/usr/local/spark", "/usr/spark"]:
                    if os.path.exists(spark_path):
                        spark_home = spark_path
                        break
            
            if not spark_home:
                logger.error("SPARK_HOME not set and Spark not found in common locations")
                return False
            
            # Start Spark History Server
            history_server_script = os.path.join(spark_home, "sbin", "start-history-server.sh")
            if not os.path.exists(history_server_script):
                logger.error(f"Spark History Server script not found: {history_server_script}")
                return False
            
            # Set environment variables
            env = os.environ.copy()
            env["SPARK_HISTORY_OPTS"] = f"-Dspark.history.fs.logDirectory=file://{event_logs_dir} -Dspark.history.ui.port={self.local_port}"
            
            # Start the server
            self.spark_history_server_process = subprocess.Popen(
                [history_server_script],
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            
            # Wait for server to start
            for _ in range(30):  # Wait up to 30 seconds
                try:
                    import requests
                    response = requests.get(f"{self.base_url}/api/v1/applications", timeout=2)
                    if response.status_code == 200:
                        logger.info(f"Spark History Server started on port {self.local_port}")
                        return True
                except:
                    pass
                time.sleep(1)
            
            logger.error("Failed to start Spark History Server")
            return False
            
        except Exception as e:
            logger.error(f"Failed to start local Spark History Server: {e}")
            return False

    def get_spark_history_server_url(self) -> Optional[str]:
        """
        Get the local Spark History Server URL with EMR Serverless event logs.
        
        Returns:
            Local Spark History Server URL or None if not available
        """
        try:
            # Get recent job runs
            job_runs = self.emr_serverless_client.list_job_runs(
                applicationId=self.emr_serverless_app_id,
                maxResults=5
            )
            
            if not job_runs.get("jobRuns"):
                logger.warning(f"No job runs found for application {self.emr_serverless_app_id}")
                return None
            
            # Try to download event logs for recent job runs
            for job_run in job_runs["jobRuns"]:
                job_run_id = job_run["id"]
                logger.info(f"Trying job run: {job_run_id} (state: {job_run['state']})")
                
                s3_path = self.get_s3_event_logs_path(job_run_id)
                if not s3_path:
                    continue
                
                event_logs_dir = self.download_event_logs(s3_path)
                if not event_logs_dir:
                    continue
                
                # Start local Spark History Server
                if self.start_local_spark_history_server(event_logs_dir):
                    return self.base_url
            
            logger.error("Failed to set up Spark History Server for any job run")
            return None
            
        except Exception as e:
            logger.error(f"Failed to get Spark History Server URL: {e}")
            return None

    def cleanup(self):
        """Clean up resources."""
        if self.spark_history_server_process:
            self.spark_history_server_process.terminate()
            self.spark_history_server_process = None
        
        if self.temp_dir and os.path.exists(self.temp_dir):
            import shutil
            shutil.rmtree(self.temp_dir)
            self.temp_dir = None
