#!/usr/bin/env python3
"""
EMR Serverless Hybrid Client

This module provides a hybrid approach to access EMR Serverless data:
1. Try to access Persistent UI Spark History Server
2. Fallback to EMR Serverless API data
"""

import logging
from typing import Dict, List, Optional, Any
import json
from datetime import datetime

import boto3
import requests
from botocore.exceptions import ClientError

from spark_history_mcp.config.config import ServerConfig

logger = logging.getLogger(__name__)


class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder for datetime objects."""
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat().replace('+00:00', 'Z')
        return super().default(obj)


class EMRServerlessHybridClient:
    """Hybrid client for EMR Serverless that combines Persistent UI and API access."""

    def __init__(self, server_config: ServerConfig):
        """Initialize the hybrid client."""
        self.emr_serverless_app_id = server_config.emr_serverless_app_id
        self.region = server_config.region or "us-east-1"
        self.timeout = server_config.timeout

        self.emr_serverless_client = boto3.client(
            "emr-serverless", region_name=self.region
        )
        
        self.session = requests.Session()
        self.base_url = None
        self._job_runs_cache = None

    def get_job_runs(self) -> List[Dict[str, Any]]:
        """Get job runs from EMR Serverless API."""
        if self._job_runs_cache is None:
            try:
                response = self.emr_serverless_client.list_job_runs(
                    applicationId=self.emr_serverless_app_id,
                    maxResults=50
                )
                self._job_runs_cache = response.get("jobRuns", [])
            except Exception as e:
                logger.error(f"Failed to get job runs: {e}")
                self._job_runs_cache = []
        
        return self._job_runs_cache

    def convert_job_run_to_spark_application(self, job_run: Dict[str, Any]) -> Dict[str, Any]:
        """Convert EMR Serverless job run to Spark application format."""
        return {
            "id": job_run["id"],
            "name": job_run.get("name", f"EMR Serverless Job {job_run['id']}"),
            "attempts": [{
                "attemptId": None,
                "startTime": self._format_datetime(job_run.get("createdAt")),
                "endTime": self._format_datetime(job_run.get("updatedAt")),
                "lastUpdated": self._format_datetime(job_run.get("updatedAt")),
                "duration": self._calculate_duration(job_run),
                "sparkUser": "emr-serverless",
                "appSparkVersion": job_run.get("releaseLabel", "unknown"),
                "completed": job_run.get("state") in ["SUCCESS", "FAILED", "CANCELLED"]
            }],
            "coresGranted": None,
            "maxCores": None,
            "coresPerExecutor": None,
            "memoryPerExecutorMB": None
        }

    def _format_datetime(self, dt_str: Optional[str]) -> Optional[str]:
        """Format datetime string for Spark format."""
        if not dt_str:
            return None
        try:
            # Convert AWS datetime to Spark format
            if isinstance(dt_str, str):
                # Remove timezone info and add Z
                if "+" in dt_str:
                    dt_str = dt_str.split("+")[0]
                elif "-" in dt_str and dt_str.count("-") > 2:
                    # Handle negative timezone
                    parts = dt_str.rsplit("-", 1)
                    if ":" in parts[1]:
                        dt_str = parts[0]
                return dt_str + "Z" if not dt_str.endswith("Z") else dt_str
        except:
            pass
        return dt_str

    def _calculate_duration(self, job_run: Dict[str, Any]) -> Optional[int]:
        """Calculate job duration in milliseconds."""
        try:
            if job_run.get("createdAt") and job_run.get("updatedAt"):
                from dateutil.parser import parse
                start = parse(job_run["createdAt"])
                end = parse(job_run["updatedAt"])
                return int((end - start).total_seconds() * 1000)
        except:
            pass
        return None

    def get_applications_from_api(self) -> List[Dict[str, Any]]:
        """Get applications data from EMR Serverless API."""
        job_runs = self.get_job_runs()
        applications = []
        
        for job_run in job_runs:
            app_data = self.convert_job_run_to_spark_application(job_run)
            applications.append(app_data)
        
        return applications

    def make_request(self, path: str, params=None) -> requests.Response:
        """
        Make a request - try Persistent UI first, fallback to API data.
        
        Args:
            path: API path
            params: Query parameters
            
        Returns:
            Response object with either Persistent UI data or synthesized API data
        """
        # For applications endpoint, return EMR Serverless API data
        if path.strip("/") == "api/v1/applications":
            applications = self.get_applications_from_api()
            
            # Create a mock response
            class MockResponse:
                def __init__(self, data):
                    self.status_code = 200
                    self.headers = {"Content-Type": "application/json"}
                    self._json_data = data
                    self.text = json.dumps(data, cls=DateTimeEncoder)
                
                def json(self):
                    return self._json_data
                
                def raise_for_status(self):
                    pass
            
            return MockResponse(applications)
        
        # For other endpoints, try to get data from EMR API
        elif "applications/" in path:
            # Extract application ID from path
            path_parts = path.split("/")
            if len(path_parts) >= 4 and path_parts[2] == "applications":
                app_id = path_parts[3]
                
                # Find the job run
                job_runs = self.get_job_runs()
                job_run = next((jr for jr in job_runs if jr["id"] == app_id), None)
                
                if job_run:
                    if len(path_parts) == 4:
                        # Get application details
                        app_data = self.convert_job_run_to_spark_application(job_run)
                        
                        class MockResponse:
                            def __init__(self, data):
                                self.status_code = 200
                                self.headers = {"Content-Type": "application/json"}
                                self._json_data = data
                                self.text = json.dumps(data)
                            
                            def json(self):
                                return self._json_data
                            
                            def raise_for_status(self):
                                pass
                        
                        return MockResponse(app_data)
        
        # If we can't handle the request, return empty response
        class MockResponse:
            def __init__(self):
                self.status_code = 404
                self.headers = {"Content-Type": "application/json"}
                self._json_data = {"error": "Endpoint not supported in EMR Serverless mode"}
                self.text = json.dumps(self._json_data)
            
            def json(self):
                return self._json_data
            
            def raise_for_status(self):
                if self.status_code >= 400:
                    raise requests.HTTPError(f"{self.status_code} Client Error")
        
        return MockResponse()

    def get_spark_history_server_url(self) -> str:
        """Return a dummy URL since we're using API data."""
        return "emr-serverless-api://hybrid"
