import os

from yoshi_client.domains.data_eng_infra.shared.libs.py.yoshi_client import (
    Configuration,
    ApiClient,
    JobApi,
    Job,
)

from spark_history_mcp.common import vault
from spark_history_mcp.common.vault import JWT


class Yoshi:
    AUDIENCE = "rapid-data-eng-infra"

    def __init__(self, datacenter: str):
        host =f"https://yoshi.{datacenter}"
        if os.getenv("POD_NAME"):
            host = f"https://yoshi.{self.AUDIENCE}.all-clusters.local-dc.fabric.dog:8443"
        self.configuration = Configuration(
            host=host,
            access_token=JWT(audience=self.AUDIENCE,datacenter=datacenter).get_token(),
        )

    def get_job_definition(self, job_id: str) -> Job:
        with ApiClient(self.configuration) as api_client:
            job_api = JobApi(api_client)
            return job_api.v2_get_job(job_id=job_id)
