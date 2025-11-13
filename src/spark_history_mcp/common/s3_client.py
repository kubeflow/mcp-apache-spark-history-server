import os

import boto3
import requests

from src.spark_history_mcp.common.decorators import backoff_retry
from src.spark_history_mcp.common.variable import POD_NAME


class S3Client:
    def __init__(self, datacenter: str):
        self.client = boto3.resource("s3")
        self.bucket_name = f"dd-spark-history-server-{datacenter.replace(".", "-")}" # e.g dd-spark-history-server-us1-staging-dog
        self.dst_prefix = "indexed_spark_logs/"

        shs_url_prefix =f"https://spark-history-server.{datacenter}"
        if POD_NAME:
            shs_url_prefix = "https://spark-history-server.spark.all-clusters.local-dc.fabric.dog:5554"
        self.shs_url_prefix = shs_url_prefix

    def list_contents_by_prefix(self, prefix, bucket):
        b = self.client.Bucket(bucket)
        keys = [obj.key for obj in b.objects.filter(Prefix=prefix)]

        return keys

    def is_spark_event_logs_already_indexed(self, spark_app_id: str) -> bool:
        prefix = self.dst_prefix + str(spark_app_id)
        if self.list_contents_by_prefix(prefix, self.bucket_name):
            return True

        return False

    @backoff_retry(retries=5, delay=2)
    def poll_spark_history_server(self, spark_app_id: str) -> Exception | None:
        print("entered function")
        full_url = f"{self.shs_url_prefix}/history/{spark_app_id}/jobs/"
        try:
           resp = requests.get(full_url, timeout=3)
        except requests.exceptions.Timeout:
            raise Exception(f"Spark History Server request timed out: {full_url}", 408)
        except requests.exceptions.ConnectionError:
            raise Exception("Spark History Server unavailable, please try again shortly", 503)

        if resp.status_code == 404:
            raise Exception(f"Spark History Server didn't finish parsing event logs: {full_url}", 404)

        return None

    def copy_spark_events_logs(self, spark_app_id: str) -> Exception | None:
        # get spark events logs file to copy/index
        src_prefix = f"spark_logs/{spark_app_id}"
        base_logs = self.list_contents_by_prefix(src_prefix, self.bucket_name)
        if not base_logs:
            raise Exception(f"Logs for {spark_app_id} not found. Is the job older than one month?", 404)

        # copy log file to new prefix
        src_key = base_logs[0]
        dst_key = self.dst_prefix + os.path.basename(src_key)
        copy_source = {
            'Bucket': self.bucket_name,
            'Key': src_key
        }

        bucket = self.client.Bucket(self.bucket_name)
        bucket.copy(copy_source, dst_key)

        # poll SHS until event logs are parsed
        try:
            self.poll_spark_history_server(spark_app_id)
        except Exception as e:
            raise Exception(f"Error polling Spark History Server: {e}") from e

        return None
