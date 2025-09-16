from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, ConfigDict, Field

from spark_history_mcp.models.spark_types import JobData, StageData


class JobSummary(BaseModel):
    job_id: Optional[int] = Field(None, alias="jobId")
    name: str
    description: Optional[str] = None
    status: str
    submission_time: Optional[datetime] = Field(None, alias="submissionTime")
    completion_time: Optional[datetime] = Field(None, alias="completionTime")
    duration_seconds: Optional[float] = None
    succeeded_stage_ids: List[int] = Field(
        default_factory=list, alias="succeededStageIds"
    )
    failed_stage_ids: List[int] = Field(default_factory=list, alias="failedStageIds")
    active_stage_ids: List[int] = Field(default_factory=list, alias="activeStageIds")
    pending_stage_ids: List[int] = Field(default_factory=list, alias="pendingStageIds")
    skipped_stage_ids: List[int] = Field(default_factory=list, alias="skippedStageIds")

    model_config = ConfigDict(populate_by_name=True, arbitrary_types_allowed=True)

    @classmethod
    def parse_datetime(cls, value):
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return datetime.fromtimestamp(value / 1000)
        if isinstance(value, str) and value.endswith("GMT"):
            try:
                dt_str = value.replace("GMT", "+0000")
                return datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%S.%f%z")
            except ValueError:
                pass
        return value

    @classmethod
    def from_job_data(
        cls, job_data: JobData, stages: List[StageData] = None
    ) -> "JobSummary":
        """Create a JobSummary from full JobData and optional stage data."""
        duration = None
        if job_data.completion_time and job_data.submission_time:
            duration = (
                job_data.completion_time - job_data.submission_time
            ).total_seconds()

        # Initialize stage ID lists
        succeeded_stage_ids = []
        failed_stage_ids = []
        active_stage_ids = []
        pending_stage_ids = []
        skipped_stage_ids = []

        # Group stage IDs by status if stage data is provided
        if stages and job_data.stage_ids:
            stage_status_map = {stage.stage_id: stage.status for stage in stages}

            for stage_id in job_data.stage_ids:
                stage_status = stage_status_map.get(stage_id, "UNKNOWN")
                if stage_status == "COMPLETE":
                    succeeded_stage_ids.append(stage_id)
                elif stage_status == "FAILED":
                    failed_stage_ids.append(stage_id)
                elif stage_status == "ACTIVE":
                    active_stage_ids.append(stage_id)
                elif stage_status == "PENDING":
                    pending_stage_ids.append(stage_id)
                elif stage_status == "SKIPPED":
                    skipped_stage_ids.append(stage_id)

        return cls(
            job_id=job_data.job_id,
            name=job_data.name,
            description=job_data.description,
            status=job_data.status,
            submission_time=job_data.submission_time,
            completion_time=job_data.completion_time,
            duration_seconds=duration,
            succeeded_stage_ids=succeeded_stage_ids,
            failed_stage_ids=failed_stage_ids,
            active_stage_ids=active_stage_ids,
            pending_stage_ids=pending_stage_ids,
            skipped_stage_ids=skipped_stage_ids,
        )


class SqlQuerySummary(BaseModel):
    """Simplified summary of a SQL query execution for LLM consumption."""

    id: int
    duration: Optional[int] = None  # Duration in milliseconds
    description: Optional[str] = None
    status: str
    submission_time: Optional[str] = Field(None, alias="submissionTime")
    plan_description: str = Field(..., alias="planDescription")
    success_job_ids: List[int] = Field(..., alias="successJobIds")
    failed_job_ids: List[int] = Field(..., alias="failedJobIds")
    running_job_ids: List[int] = Field(..., alias="runningJobIds")

    model_config = ConfigDict(populate_by_name=True)
