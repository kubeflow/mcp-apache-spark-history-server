#!/usr/bin/env python3
"""
Download EMR Serverless event logs and analyze with MCP tools
"""

import os
import sys
import tempfile
import subprocess
import time
import shutil
from pathlib import Path

import boto3
import requests

def download_s3_event_logs(bucket: str, prefix: str, local_dir: str) -> int:
    """Download event logs from S3 to local directory."""
    s3_client = boto3.client('s3', region_name='us-east-1')
    
    # List and download event log files
    paginator = s3_client.get_paginator("list_objects_v2")
    
    downloaded_files = 0
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(("events_", "appstatus_")):
                continue
                
            # Create local file path maintaining structure
            relative_path = key[len(prefix):].lstrip('/')
            local_file = os.path.join(local_dir, relative_path)
            
            # Create directory if needed
            os.makedirs(os.path.dirname(local_file), exist_ok=True)
            
            # Download file
            print(f"Downloading: {key}")
            s3_client.download_file(bucket, key, local_file)
            downloaded_files += 1
    
    return downloaded_files

def start_spark_history_server(event_logs_dir: str, port: int = 18081) -> subprocess.Popen:
    """Start Spark History Server with event logs."""
    
    # Use Docker to run Spark History Server
    cmd = [
        "docker", "run", "-d",
        "--name", "spark-history-temp",
        "-p", f"{port}:18080",
        "-v", f"{event_logs_dir}:/opt/spark/spark-events",
        "-e", "SPARK_HISTORY_OPTS=-Dspark.history.fs.logDirectory=file:///opt/spark/spark-events",
        "apache/spark:3.5.3-scala2.12-java11-python3-ubuntu",
        "/opt/spark/sbin/start-history-server.sh"
    ]
    
    # Stop any existing container
    subprocess.run(["docker", "rm", "-f", "spark-history-temp"], 
                  capture_output=True, check=False)
    
    process = subprocess.run(cmd, capture_output=True, text=True)
    if process.returncode != 0:
        print(f"Failed to start Docker container: {process.stderr}")
        return None
    
    # Wait for server to start
    for i in range(30):
        try:
            response = requests.get(f"http://localhost:{port}/api/v1/applications", timeout=2)
            if response.status_code == 200:
                print(f"✅ Spark History Server started on port {port}")
                return True
        except:
            pass
        time.sleep(1)
        print(f"Waiting for Spark History Server... ({i+1}/30)")
    
    return False

def main():
    job_id = "00fvksaecf02j00b"
    app_id = "00fmao79eo73n909"
    
    # S3 details
    bucket = "aws-logs-591317119253-us-east-1"
    prefix = f"emr_serverless/applications/{app_id}/jobs/{job_id}/sparklogs/"
    
    # Create temp directory
    temp_dir = tempfile.mkdtemp(prefix="spark-events-")
    print(f"📁 Created temp directory: {temp_dir}")
    
    try:
        # Download event logs
        print(f"📥 Downloading event logs from s3://{bucket}/{prefix}")
        downloaded = download_s3_event_logs(bucket, prefix, temp_dir)
        print(f"✅ Downloaded {downloaded} files")
        
        if downloaded == 0:
            print("❌ No event logs found")
            return 1
        
        # Start Spark History Server
        print("🚀 Starting Spark History Server...")
        if not start_spark_history_server(temp_dir):
            print("❌ Failed to start Spark History Server")
            return 1
        
        print("🎉 Ready for MCP analysis!")
        print("Now you can use MCP tools to analyze the job:")
        print("- list_applications")
        print("- get_job_bottlenecks")
        print("- list_slowest_stages")
        print("\nPress Ctrl+C to stop...")
        
        # Keep running
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\n🛑 Stopping...")
    
    finally:
        # Cleanup
        print("🧹 Cleaning up...")
        subprocess.run(["docker", "rm", "-f", "spark-history-temp"], 
                      capture_output=True, check=False)
        shutil.rmtree(temp_dir, ignore_errors=True)

if __name__ == "__main__":
    sys.exit(main())
